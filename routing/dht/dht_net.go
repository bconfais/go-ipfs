package dht

import (
	"fmt"
	"sync"
	"time"
	"os"

	pb "github.com/ipfs/go-ipfs/routing/dht/pb"
	peer "gx/ipfs/QmRBqJF7hb8ZSpRcMwUt8hNhydWcxGEhtk81HKq6oUwKvs/go-libp2p-peer"
	inet "gx/ipfs/QmVCe3SNMjkcPgnpFhZs719dheq6xE7gJwjzV7aWcUM4Ms/go-libp2p/p2p/net"
	ctxio "gx/ipfs/QmX6DhWrpBB5NtadXmPSXYNdVvuLfJXoFNMvUMoVvP5UJa/go-context/io"
	ggio "gx/ipfs/QmZ4Qi3GaRbjcx28Sme5eMH7RQjGkt8wHxt2a65oLaeFEV/gogo-protobuf/io"
	context "gx/ipfs/QmZy2y8t9zQH2a1b8q2ZSLKp17ATuJoCNxxyMFG5qFExpt/go-net/context"
//	b58 "gx/ipfs/QmT8rehPR3F6bmwL6zjUN8XpiDBFFpMP2myPdC6ApsWfJf/go-base58"
)

var dhtReadMessageTimeout = time.Minute
var ErrReadTimeout = fmt.Errorf("timed out reading response")

// handleNewStream implements the inet.StreamHandler
func (dht *IpfsDHT) handleNewStream(s inet.Stream) {
	go dht.handleNewMessage(s)
}

func (dht *IpfsDHT) handleNewMessage(s inet.Stream) {
	defer s.Close()

	ctx := dht.Context()
	cr := ctxio.NewReader(ctx, s) // ok to use. we defer close stream in this func
	cw := ctxio.NewWriter(ctx, s) // ok to use. we defer close stream in this func
	r := ggio.NewDelimitedReader(cr, inet.MessageSizeMax*5)
	w := ggio.NewDelimitedWriter(cw)
	mPeer := s.Conn().RemotePeer()

	for {
		// receive msg
		pmes := new(pb.Message)
		if err := r.ReadMsg(pmes); err != nil {
			log.Debugf("Error unmarshaling data: %s", err)
			return
		}

		// update the peer (on valid msgs only)
		dht.updateFromMessage(ctx, mPeer, pmes)

		// get handler for this msg type.
		handler := dht.handlerForMsgType(pmes.GetType())
		if handler == nil {
			log.Debug("got back nil handler from handlerForMsgType")
			return
		}

		// dispatch handler.
		rpmes, err := handler(ctx, mPeer, pmes)
		if err != nil {
			log.Debugf("handle message error: %s", err)
			return
		}

		// if nil response, return it before serializing
		if rpmes == nil {
			log.Debug("got back nil response from request")
			continue
		}

		// send out response msg
		if err := w.WriteMsg(rpmes); err != nil {
			log.Debugf("send response error: %s", err)
			return
		}
	}
}

// sendRequest sends out a request, but also makes sure to
// measure the RTT for latency measurements.
func (dht *IpfsDHT) sendRequest(ctx context.Context, p peer.ID, pmes *pb.Message) (*pb.Message, error) {
//        f, _ := os.OpenFile("/tmp/log", os.O_APPEND|os.O_WRONLY, 0644)
//        defer f.Close()

//	s := time.Now()

	ms := dht.messageSenderForPeer(p)

//	ss := time.Since(s)
//	f.WriteString(fmt.Sprintf("dht.messageSenderForPeer took %s\n", ss))


	start := time.Now()

	rpmes, err := ms.SendRequest(ctx, pmes)
	if err != nil {
		return nil, err
	}


//	ss = time.Since(start)
//	f.WriteString(fmt.Sprintf("ms.SendRequest took %s\n", ss))

//	s = time.Now()
	// update the peer (on valid msgs only)
	dht.updateFromMessage(ctx, p, rpmes)

//	ss = time.Since(s)
//	f.WriteString(fmt.Sprintf("dht.updateFromMessage took %s\n", ss))


	dht.peerstore.RecordLatency(p, time.Since(start))
	log.Event(ctx, "dhtReceivedMessage", dht.self, p, rpmes)
	return rpmes, nil
}

// sendMessage sends out a message
func (dht *IpfsDHT) sendMessage(ctx context.Context, p peer.ID, pmes *pb.Message) error {
	ff, _ := os.OpenFile("/tmp/log", os.O_APPEND|os.O_WRONLY, 0644)
	defer ff.Close()
	ff.WriteString(fmt.Sprintf("update\n"))

	ms := dht.messageSenderForPeer(p)

	if err := ms.SendMessage(ctx, pmes); err != nil {
		return err
	}
	log.Event(ctx, "dhtSentMessage", dht.self, p, pmes)
	return nil
}

func (dht *IpfsDHT) updateFromMessage(ctx context.Context, p peer.ID, mes *pb.Message) error {
	dht.Update(ctx, p)
	return nil
}

func (dht *IpfsDHT) messageSenderForPeer(p peer.ID) *messageSender {
	dht.smlk.Lock()
	defer dht.smlk.Unlock()

	ms, ok := dht.strmap[p]
	if !ok {
		ms = dht.newMessageSender(p)
		dht.strmap[p] = ms
	}

	return ms
}

type messageSender struct {
	s   inet.Stream
	r   ggio.ReadCloser
	w   ggio.WriteCloser
	lk  sync.Mutex
	p   peer.ID
	dht *IpfsDHT

	singleMes int
}

type socketMe struct {
	s   inet.Stream
	r   ggio.ReadCloser
	w   ggio.WriteCloser
}

func (dht *IpfsDHT) newMessageSender(p peer.ID) *messageSender {
	return &messageSender{p: p, dht: dht}
}

func (ms *messageSender) prep() error {
	if ms.s != nil {
		return nil
	}

//	ff, _ := os.OpenFile("/tmp/log", os.O_APPEND|os.O_WRONLY, 0644)
//	defer ff.Close()
//	start := time.Now()
	nstr, err := ms.dht.host.NewStream(ms.dht.ctx, ProtocolDHT, ms.p)
	if err != nil {
		return err
	}

	ms.r = ggio.NewDelimitedReader(nstr, inet.MessageSizeMax)
	ms.w = ggio.NewDelimitedWriter(nstr)
	ms.s = nstr

//	elapsed := time.Since(start)
//	ff.WriteString(fmt.Sprintf("new stream took %s \n", elapsed))
	return nil
}

func (ms *messageSender) prepMe() (*socketMe, error) {
	nstr, err := ms.dht.host.NewStream(ms.dht.ctx, ProtocolDHT, ms.p)
	if err != nil {
		return nil, err
	}

	mms := socketMe{}

	mms.r = ggio.NewDelimitedReader(nstr, inet.MessageSizeMax)
	mms.w = ggio.NewDelimitedWriter(nstr)
	mms.s = nstr

	return &mms, nil
}

// streamReuseTries is the number of times we will try to reuse a stream to a
// given peer before giving up and reverting to the old one-message-per-stream
// behaviour.
const streamReuseTries = 3

func (ms *messageSender) SendMessage(ctx context.Context, pmes *pb.Message) error {
	ms.lk.Lock()
	defer ms.lk.Unlock()
	if err := ms.prep(); err != nil {
		return err
	}

	if err := ms.writeMessage(pmes); err != nil {
		return err
	}

	if ms.singleMes > streamReuseTries {
		ms.s.Close()
		ms.s = nil
	}

	return nil
}

func (ms *messageSender) writeMessage(pmes *pb.Message) error {
	err := ms.w.WriteMsg(pmes)
	if err != nil {
		// If the other side isnt expecting us to be reusing streams, we're gonna
		// end up erroring here. To make sure things work seamlessly, lets retry once
		// before continuing

		log.Infof("error writing message: ", err)
		ms.s.Close()
		ms.s = nil
		if err := ms.prep(); err != nil {
			return err
		}

		if err := ms.w.WriteMsg(pmes); err != nil {
			return err
		}

		// keep track of this happening. If it happens a few times, its
		// likely we can assume the otherside will never support stream reuse
		ms.singleMes++
	}
	return nil
}

func (ms *messageSender) writeMessageMe(socket *socketMe, pmes *pb.Message) (*socketMe, error) {
	err := socket.w.WriteMsg(pmes)
	if err != nil {
		// If the other side isnt expecting us to be reusing streams, we're gonna
		// end up erroring here. To make sure things work seamlessly, lets retry once
		// before continuing

		log.Infof("error writing message: ", err)
		socket.s.Close()
		socket.s = nil
		socket2, err := ms.prepMe()
		if err != nil {
			return nil, err
		}

		if err := socket2.w.WriteMsg(pmes); err != nil {
			return nil, err
		}

		// keep track of this happening. If it happens a few times, its
		// likely we can assume the otherside will never support stream reuse
		ms.singleMes++
		return socket2, nil
	}
	return socket, nil
}

func (ms *messageSender) SendRequest(ctx context.Context, pmes *pb.Message) (*pb.Message, error) {
//        f, _ := os.OpenFile("/tmp/log", os.O_APPEND|os.O_WRONLY, 0644)
//        defer f.Close()

//	start := time.Now()
	socket, err := ms.prepMe();
	if err != nil {
		return nil, err
	}
//	elapsed := time.Since(start)
//	f.WriteString(fmt.Sprintf("prep took %s\n", elapsed))

//	start = time.Now()
	socket2, err := ms.writeMessageMe(socket, pmes);
	if err != nil {
		return nil, err
	}
//	elapsed = time.Since(start)
//	f.WriteString(fmt.Sprintf("writemessage took %s (%s) - %s\n", elapsed, string(b58.Encode([]byte(*(pmes.Key)))), pmes.Type))


	log.Event(ctx, "dhtSentMessage", ms.dht.self, ms.p, pmes)

//	start = time.Now()
	mes := new(pb.Message)
	if err := ms.ctxReadMsgMe(socket2, ctx, mes); err != nil {
		return nil, err
	}
//	elapsed = time.Since(start)
//	f.WriteString(fmt.Sprintf("readmsg took %s (%s) - %s\n", elapsed, string(b58.Encode([]byte(*(pmes.Key)))), pmes.Type))

	socket2.s.Close()

	return mes, nil
}

func (ms *messageSender) SendRequestOld(ctx context.Context, pmes *pb.Message) (*pb.Message, error) {
//        f, _ := os.OpenFile("/tmp/log", os.O_APPEND|os.O_WRONLY, 0644)
//        defer f.Close()
//	start := time.Now()
	ms.lk.Lock()
	defer ms.lk.Unlock()
//	elapsed := time.Since(start)
//	f.WriteString(fmt.Sprintf("wait lock took %s\n", elapsed))


//	start = time.Now()
	if err := ms.prep(); err != nil {
		return nil, err
	}
//	elapsed = time.Since(start)
//	f.WriteString(fmt.Sprintf("prep took %s\n", elapsed))

//	start = time.Now()
	if err := ms.writeMessage(pmes); err != nil {
		return nil, err
	}
//	elapsed = time.Since(start)
//	f.WriteString(fmt.Sprintf("writemessage took %s (%s) - %s\n", elapsed, string(b58.Encode([]byte(*(pmes.Key)))), pmes.Type))


	log.Event(ctx, "dhtSentMessage", ms.dht.self, ms.p, pmes)

//	start = time.Now()
	mes := new(pb.Message)
	if err := ms.ctxReadMsg(ctx, mes); err != nil {
		ms.s.Close()
		ms.s = nil
		return nil, err
	}
//	elapsed = time.Since(start)
//	f.WriteString(fmt.Sprintf("readmsg took %s (%s) - %s\n", elapsed, string(b58.Encode([]byte(*(pmes.Key)))), pmes.Type))


	if ms.singleMes > streamReuseTries {
		ms.s.Close()
		ms.s = nil
	}

	return mes, nil
}

func (ms *messageSender) ctxReadMsg(ctx context.Context, mes *pb.Message) error {
	errc := make(chan error, 1)
	go func(r ggio.ReadCloser) {
		errc <- r.ReadMsg(mes)
	}(ms.r)

	t := time.NewTimer(dhtReadMessageTimeout)
	defer t.Stop()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return ErrReadTimeout
	}
}

func (ms *messageSender) ctxReadMsgMe(socket *socketMe, ctx context.Context, mes *pb.Message) error {
	errc := make(chan error, 1)
	go func(r ggio.ReadCloser) {
		errc <- r.ReadMsg(mes)
	}(socket.r)

	t := time.NewTimer(dhtReadMessageTimeout)
	defer t.Stop()

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return ErrReadTimeout
	}
}
