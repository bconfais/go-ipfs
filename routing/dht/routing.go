package dht

import (
	"bytes"
	"fmt"
	"runtime"
	"sync"
	"time"
	"os"

	key "github.com/ipfs/go-ipfs/blocks/key"
	notif "github.com/ipfs/go-ipfs/notifications"
	"github.com/ipfs/go-ipfs/routing"
	pb "github.com/ipfs/go-ipfs/routing/dht/pb"
	kb "github.com/ipfs/go-ipfs/routing/kbucket"
	record "github.com/ipfs/go-ipfs/routing/record"
	pset "github.com/ipfs/go-ipfs/thirdparty/peerset"

	pstore "gx/ipfs/QmQdnfvZQuhdT93LNc5bos52wAmdr3G2p6G8teLJMEN32P/go-libp2p-peerstore"
	peer "gx/ipfs/QmRBqJF7hb8ZSpRcMwUt8hNhydWcxGEhtk81HKq6oUwKvs/go-libp2p-peer"
	inet "gx/ipfs/QmVCe3SNMjkcPgnpFhZs719dheq6xE7gJwjzV7aWcUM4Ms/go-libp2p/p2p/net"
	context "gx/ipfs/QmZy2y8t9zQH2a1b8q2ZSLKp17ATuJoCNxxyMFG5qFExpt/go-net/context"
	b58 "gx/ipfs/QmT8rehPR3F6bmwL6zjUN8XpiDBFFpMP2myPdC6ApsWfJf/go-base58"
)

// asyncQueryBuffer is the size of buffered channels in async queries. This
// buffer allows multiple queries to execute simultaneously, return their
// results and continue querying closer peers. Note that different query
// results will wait for the channel to drain.
var asyncQueryBuffer = 1000

// This file implements the Routing interface for the IpfsDHT struct.

// Basic Put/Get

// PutValue adds value corresponding to given Key.
// This is the top level "Store" operation of the DHT
func (dht *IpfsDHT) PutValue(ctx context.Context, key key.Key, value []byte) error {
	log.Debugf("PutValue %s", key)
	sk, err := dht.getOwnPrivateKey()
	if err != nil {
		return err
	}

	sign, err := dht.Validator.IsSigned(key)
	if err != nil {
		return err
	}

	rec, err := record.MakePutRecord(sk, key, value, sign)
	if err != nil {
		log.Debug("creation of record failed!")
		return err
	}

	err = dht.putLocal(key, rec)
	if err != nil {
		return err
	}

	pchan, err := dht.GetClosestPeers(ctx, key)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	for p := range pchan {
		wg.Add(1)
		go func(p peer.ID) {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			defer wg.Done()
			notif.PublishQueryEvent(ctx, &notif.QueryEvent{
				Type: notif.Value,
				ID:   p,
			})

			err := dht.putValueToPeer(ctx, p, key, rec)
			if err != nil {
				log.Debugf("failed putting value to peer: %s", err)
			}
		}(p)
	}
	wg.Wait()
	return nil
}

// GetValue searches for the value corresponding to given Key.
func (dht *IpfsDHT) GetValue(ctx context.Context, key key.Key) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	vals, err := dht.GetValues(ctx, key, 16)
	if err != nil {
		return nil, err
	}

	var recs [][]byte
	for _, v := range vals {
		if v.Val != nil {
			recs = append(recs, v.Val)
		}
	}

	i, err := dht.Selector.BestRecord(key, recs)
	if err != nil {
		return nil, err
	}

	best := recs[i]
	log.Debugf("GetValue %v %v", key, best)
	if best == nil {
		log.Errorf("GetValue yielded correct record with nil value.")
		return nil, routing.ErrNotFound
	}

	fixupRec, err := record.MakePutRecord(dht.peerstore.PrivKey(dht.self), key, best, true)
	if err != nil {
		// probably shouldnt actually 'error' here as we have found a value we like,
		// but this call failing probably isnt something we want to ignore
		return nil, err
	}

	for _, v := range vals {
		// if someone sent us a different 'less-valid' record, lets correct them
		if !bytes.Equal(v.Val, best) {
			go func(v routing.RecvdVal) {
				if v.From == dht.self {
					err := dht.putLocal(key, fixupRec)
					if err != nil {
						log.Error("Error correcting local dht entry:", err)
					}
					return
				}
				ctx, cancel := context.WithTimeout(dht.Context(), time.Second*30)
				defer cancel()
				err := dht.putValueToPeer(ctx, v.From, key, fixupRec)
				if err != nil {
					log.Error("Error correcting DHT entry: ", err)
				}
			}(v)
		}
	}

	return best, nil
}

func (dht *IpfsDHT) GetValues(ctx context.Context, key key.Key, nvals int) ([]routing.RecvdVal, error) {
	var vals []routing.RecvdVal
	var valslock sync.Mutex

	// If we have it local, dont bother doing an RPC!
	lrec, err := dht.getLocal(key)
	if err == nil {
		// TODO: this is tricky, we dont always want to trust our own value
		// what if the authoritative source updated it?
		log.Debug("have it locally")
		vals = append(vals, routing.RecvdVal{
			Val:  lrec.GetValue(),
			From: dht.self,
		})

		if nvals <= 1 {
			return vals, nil
		}
	} else if nvals == 0 {
		return nil, err
	}

	// get closest peers in the routing table
	rtp := dht.routingTable.NearestPeers(kb.ConvertKey(key), KValue)
	log.Debugf("peers in rt: %s", len(rtp), rtp)
	if len(rtp) == 0 {
		log.Warning("No peers from routing table!")
		return nil, kb.ErrLookupFailure
	}

	// setup the Query
	parent := ctx
	query := dht.newQuery(key, func(ctx context.Context, p peer.ID) (*dhtQueryResult, error) {
		notif.PublishQueryEvent(parent, &notif.QueryEvent{
			Type: notif.SendingQuery,
			ID:   p,
		})

		rec, peers, err := dht.getValueOrPeers(ctx, p, key)
		switch err {
		case routing.ErrNotFound:
			// in this case, they responded with nothing,
			// still send a notification so listeners can know the
			// request has completed 'successfully'
			notif.PublishQueryEvent(parent, &notif.QueryEvent{
				Type: notif.PeerResponse,
				ID:   p,
			})
			return nil, err
		default:
			return nil, err

		case nil, errInvalidRecord:
			// in either of these cases, we want to keep going
		}

		res := &dhtQueryResult{closerPeers: peers}

		if rec.GetValue() != nil || err == errInvalidRecord {
			rv := routing.RecvdVal{
				Val:  rec.GetValue(),
				From: p,
			}
			valslock.Lock()
			vals = append(vals, rv)

			// If weve collected enough records, we're done
			if len(vals) >= nvals {
				res.success = true
			}
			valslock.Unlock()
		}

		notif.PublishQueryEvent(parent, &notif.QueryEvent{
			Type:      notif.PeerResponse,
			ID:        p,
			Responses: pointerizePeerInfos(peers),
		})

		return res, nil
	})

	// run it!
	_, err = query.Run(ctx, rtp)
	if len(vals) == 0 {
		if err != nil {
			return nil, err
		}
	}

	return vals, nil

}

// Value provider layer of indirection.
// This is what DSHTs (Coral and MainlineDHT) do to store large values in a DHT.

// Provide makes this node announce that it can provide a value for the given key
func (dht *IpfsDHT) Provide(ctx context.Context, key key.Key) error {
	start := time.Now()
	defer log.EventBegin(ctx, "provide", &key).Done()

	// add self locally
	dht.providers.AddProvider(ctx, key, dht.self)

	peers, err := dht.GetClosestPeers(ctx, key)
	if err != nil {
		return err
	}

	mes, err := dht.makeProvRecord(key)
	if err != nil {
		return err
	}

	wg := sync.WaitGroup{}
	for p := range peers {
		wg.Add(1)
		go func(p peer.ID) {
			defer wg.Done()
			log.Debugf("putProvider(%s, %s)", key, p)
			err := dht.sendMessage(ctx, p, mes)
			if err != nil {
				log.Debug(err)
			}
		}(p)
	}
	wg.Wait()
	f, _ := os.OpenFile("/tmp/log", os.O_APPEND|os.O_WRONLY, 0644)
	defer f.Close()
	elapsed := time.Since(start)
	f.WriteString(fmt.Sprintf("provides took %s (%s)\n", elapsed,  string(b58.Encode([]byte(key)))))
	return nil
}
func (dht *IpfsDHT) makeProvRecord(skey key.Key) (*pb.Message, error) {
	pi := pstore.PeerInfo{
		ID:    dht.self,
		Addrs: dht.host.Addrs(),
	}

	// // only share WAN-friendly addresses ??
	// pi.Addrs = addrutil.WANShareableAddrs(pi.Addrs)
	if len(pi.Addrs) < 1 {
		return nil, fmt.Errorf("no known addresses for self. cannot put provider.")
	}

	pmes := pb.NewMessage(pb.Message_ADD_PROVIDER, string(skey), 0)
	pmes.ProviderPeers = pb.RawPeerInfosToPBPeers([]pstore.PeerInfo{pi})
	return pmes, nil
}

// FindProviders searches until the context expires.
func (dht *IpfsDHT) FindProviders(ctx context.Context, key key.Key) ([]pstore.PeerInfo, error) {
	var providers []pstore.PeerInfo
	for p := range dht.FindProvidersAsync(ctx, key, 1) {
		providers = append(providers, p)
	}
	return providers, nil
}

// FindProvidersAsync is the same thing as FindProviders, but returns a channel.
// Peers will be returned on the channel as soon as they are found, even before
// the search query completes.
func (dht *IpfsDHT) FindProvidersAsync(ctx context.Context, key key.Key, count int) <-chan pstore.PeerInfo {
	log.Event(ctx, "findProviders", &key)
	peerOut := make(chan pstore.PeerInfo, count)
	go dht.findProvidersAsyncRoutine(ctx, key, count, peerOut)
	return peerOut
}

func (dht *IpfsDHT) findProvidersAsyncRoutine(ctx context.Context, key key.Key, count int, peerOut chan pstore.PeerInfo) {
	start := time.Now()
	defer log.EventBegin(ctx, "findProvidersAsync", &key).Done()
	defer close(peerOut)

	ps := pset.NewLimited(count)
	answer := 0
	f, _ := os.OpenFile("/tmp/log", os.O_APPEND|os.O_WRONLY, 0644)
	defer f.Close()

/*
	provs := dht.providers.GetProviders(ctx, key)
	for _, p := range provs {
		answer = answer + 1
		// NOTE: Assuming that this list of peers is unique
		if ps.TryAdd(p) {
			select {
			case peerOut <- dht.peerstore.PeerInfo(p):
			case <-ctx.Done():
				f.WriteString(fmt.Sprintf("found %d answers (%s)\n", answer, string(key)))
				return
			}
		}

		// If we have enough peers locally, dont bother with remote RPC
		if ps.Size() >= count {
			f.WriteString(fmt.Sprintf("found %d answers (%s)\n", answer, string(key)))
			return
		}
	}
*/

	// setup the Query
	first := true
	parent := ctx
	query := dht.newQuery(key, func(ctx context.Context, p peer.ID) (*dhtQueryResult, error) {
		s := time.Now()
		notif.PublishQueryEvent(parent, &notif.QueryEvent{
			Type: notif.SendingQuery,
			ID:   p,
		})
		pmes, err := dht.findProvidersSingle(ctx, p, key)
		if err != nil {
			return nil, err
		}
		ss := time.Since(s)
		f.WriteString(fmt.Sprintf("findproviderssingle took %s \n", ss))
		

		log.Debugf("%d provider entries", len(pmes.GetProviderPeers()))
		provs := pb.PBPeersToPeerInfos(pmes.GetProviderPeers())
		log.Debugf("%d provider entries decoded", len(provs))

		if (true == first) {
			first = false;
			elapsed := time.Since(start)
			f.WriteString(fmt.Sprintf("findprovidersasync_ took %s (%s)\n", elapsed,  string(b58.Encode([]byte(key)))))
		}

		for _, prov := range provs {
		// Add unique providers from request, up to 'count'
			answer = answer + 1
			log.Debugf("got provider: %s", prov)
			if ps.TryAdd(prov.ID) {
				log.Debugf("using provider: %s", prov)
				select {
				case peerOut <- prov:
				case <-ctx.Done():
					log.Debug("context timed out sending more providers")
					return nil, ctx.Err()
				}
			}
			if ps.Size() >= count {
				log.Debugf("got enough providers (%d/%d)", ps.Size(), count)
				return &dhtQueryResult{success: true}, nil
			}
		}

		return &dhtQueryResult{success: true}, nil

		// Give closer peers back to the query to be queried
		closer := pmes.GetCloserPeers()
		clpeers := pb.PBPeersToPeerInfos(closer)
		log.Debugf("got closer peers: %d %s", len(clpeers), clpeers)
/*
		if 0 ==  len(clpeers) {
			dht.findProvidersAsyncRoutine(ctx, key, count, peerOut)
		}
*/

		notif.PublishQueryEvent(parent, &notif.QueryEvent{
			Type:      notif.PeerResponse,
			ID:        p,
			Responses: pointerizePeerInfos(clpeers),
		})
		return &dhtQueryResult{closerPeers: clpeers}, nil
	})
	f.WriteString(fmt.Sprintf("found %d answers (%s)\n", answer, string(b58.Encode([]byte(key)))))

	peers := dht.routingTable.NearestPeers(kb.ConvertKey(key), 1)
	_, err := query.Run(ctx, peers)
	if err != nil {
		log.Debugf("Query error: %s", err)
		// Special handling for issue: https://github.com/ipfs/go-ipfs/issues/3032
		if fmt.Sprint(err) == "<nil>" {
			log.Error("reproduced bug 3032:")
			log.Errorf("Errors type information: %#v", err)
			log.Errorf("go version: %s", runtime.Version())
			log.Error("please report this information to: https://github.com/ipfs/go-ipfs/issues/3032")

			// replace problematic error with something that won't crash the daemon
			err = fmt.Errorf("<nil>")
		}
		notif.PublishQueryEvent(ctx, &notif.QueryEvent{
			Type:  notif.QueryError,
			Extra: err.Error(),
		})
	}

	elapsed := time.Since(start)
	f.WriteString(fmt.Sprintf("findprovidersasync_> took %s (%s)\n", elapsed,  string(b58.Encode([]byte(key)))))

}

// FindPeer searches for a peer with given ID.
func (dht *IpfsDHT) FindPeer(ctx context.Context, id peer.ID) (pstore.PeerInfo, error) {
	defer log.EventBegin(ctx, "FindPeer", id).Done()

	// Check if were already connected to them
	if pi := dht.FindLocal(id); pi.ID != "" {
		return pi, nil
	}

	peers := dht.routingTable.NearestPeers(kb.ConvertPeerID(id), KValue)
	if len(peers) == 0 {
		return pstore.PeerInfo{}, kb.ErrLookupFailure
	}

	// Sanity...
	for _, p := range peers {
		if p == id {
			log.Debug("found target peer in list of closest peers...")
			return dht.peerstore.PeerInfo(p), nil
		}
	}

	// setup the Query
	parent := ctx
	query := dht.newQuery(key.Key(id), func(ctx context.Context, p peer.ID) (*dhtQueryResult, error) {
		notif.PublishQueryEvent(parent, &notif.QueryEvent{
			Type: notif.SendingQuery,
			ID:   p,
		})

		pmes, err := dht.findPeerSingle(ctx, p, id)
		if err != nil {
			return nil, err
		}

		closer := pmes.GetCloserPeers()
		clpeerInfos := pb.PBPeersToPeerInfos(closer)

		// see it we got the peer here
		for _, npi := range clpeerInfos {
			if npi.ID == id {
				return &dhtQueryResult{
					peer:    npi,
					success: true,
				}, nil
			}
		}

		notif.PublishQueryEvent(parent, &notif.QueryEvent{
			Type:      notif.PeerResponse,
			Responses: pointerizePeerInfos(clpeerInfos),
		})

		return &dhtQueryResult{closerPeers: clpeerInfos}, nil
	})

	// run it!
	result, err := query.Run(ctx, peers)
	if err != nil {
		return pstore.PeerInfo{}, err
	}

	log.Debugf("FindPeer %v %v", id, result.success)
	if result.peer.ID == "" {
		return pstore.PeerInfo{}, routing.ErrNotFound
	}

	return result.peer, nil
}

// FindPeersConnectedToPeer searches for peers directly connected to a given peer.
func (dht *IpfsDHT) FindPeersConnectedToPeer(ctx context.Context, id peer.ID) (<-chan pstore.PeerInfo, error) {

	peerchan := make(chan pstore.PeerInfo, asyncQueryBuffer)
	peersSeen := make(map[peer.ID]struct{})

	peers := dht.routingTable.NearestPeers(kb.ConvertPeerID(id), KValue)
	if len(peers) == 0 {
		return nil, kb.ErrLookupFailure
	}

	// setup the Query
	query := dht.newQuery(key.Key(id), func(ctx context.Context, p peer.ID) (*dhtQueryResult, error) {

		pmes, err := dht.findPeerSingle(ctx, p, id)
		if err != nil {
			return nil, err
		}

		var clpeers []pstore.PeerInfo
		closer := pmes.GetCloserPeers()
		for _, pbp := range closer {
			pi := pb.PBPeerToPeerInfo(pbp)

			// skip peers already seen
			if _, found := peersSeen[pi.ID]; found {
				continue
			}
			peersSeen[pi.ID] = struct{}{}

			// if peer is connected, send it to our client.
			if pb.Connectedness(*pbp.Connection) == inet.Connected {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case peerchan <- pi:
				}
			}

			// if peer is the peer we're looking for, don't bother querying it.
			// TODO maybe query it?
			if pb.Connectedness(*pbp.Connection) != inet.Connected {
				clpeers = append(clpeers, pi)
			}
		}

		return &dhtQueryResult{closerPeers: clpeers}, nil
	})

	// run it! run it asynchronously to gen peers as results are found.
	// this does no error checking
	go func() {
		if _, err := query.Run(ctx, peers); err != nil {
			log.Debug(err)
		}

		// close the peerchan channel when done.
		close(peerchan)
	}()

	return peerchan, nil
}
