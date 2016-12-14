package cloudrouting

import (
	"encoding/json"
	"fmt"
	"net"
	config "github.com/ipfs/go-ipfs/repo/config"
	repo "github.com/ipfs/go-ipfs/repo"
	routing "github.com/ipfs/go-ipfs/routing"
	key "github.com/ipfs/go-ipfs/blocks/key"
	context "gx/ipfs/QmZy2y8t9zQH2a1b8q2ZSLKp17ATuJoCNxxyMFG5qFExpt/go-net/context"
	ds "gx/ipfs/QmTxLSvdhwg68WJimdS6icLPhZi28aTp6b7uihC2Yb47Xk/go-datastore"
	logging "gx/ipfs/QmNQynaz7qfriSUJkiEZUrm2Wen1u3Kj9goZzWtrPyu7XR/go-log"
	p2phost "gx/ipfs/QmVCe3SNMjkcPgnpFhZs719dheq6xE7gJwjzV7aWcUM4Ms/go-libp2p/p2p/host"
	peer "gx/ipfs/QmRBqJF7hb8ZSpRcMwUt8hNhydWcxGEhtk81HKq6oUwKvs/go-libp2p-peer"
	pstore "gx/ipfs/QmQdnfvZQuhdT93LNc5bos52wAmdr3G2p6G8teLJMEN32P/go-libp2p-peerstore"
	host "gx/ipfs/QmVCe3SNMjkcPgnpFhZs719dheq6xE7gJwjzV7aWcUM4Ms/go-libp2p/p2p/host"
)

const ProtocolCloud = "/ipfs/cloudrouting"
var log = logging.Logger("routing/cloud")
var Cfg *config.Config

type CloudClient struct {
	self            peer.ID
	host            host.Host
	peerstore       pstore.Peerstore
	datastore       repo.Datastore
	ctx             context.Context

	cloud_addr	string

	/* for the cloud ipfs node */
	listen_port	string
	listen_conn	net.Listener
}

type Provider struct {
	ID      string
	Addrs   []string
}

type Message struct {
	Type string
	Key string
	ID string
	Addrs []string
}

func ConstructCloudRouting(ctx context.Context, h p2phost.Host, d repo.Datastore) (routing.IpfsRouting, error) {
	log.Debugf("Cloud routing")

	client := new(CloudClient)
	if "" != Cfg.Cloud.Connect {
		client.cloud_addr = Cfg.Cloud.Connect
	} else if "" != Cfg.Cloud.Listen {
		client.listen_port = Cfg.Cloud.Listen
	}
	

	client.self = h.ID()
	client.peerstore = h.Peerstore()
	client.host = h

	client.datastore = d
	client.ctx = ctx

	return client, nil
}

func (c *CloudClient) Bootstrap(ctx context.Context) error {
	log.Debugf("Bootstrap")
	if "" != c.cloud_addr {
		log.Debugf("Connect to %s", c.cloud_addr)
	} else if "" != c.listen_port {
		log.Debugf("Listen to %s", c.listen_port)
		conn, err := net.Listen("tcp", c.listen_port)
		if nil != err { log.Debugf("%s", err); return err }
		c.listen_conn = conn
		go c.ListenHandler()
	} else {
		panic("Cloud routing not configured")
	}

	return nil
}

func (c *CloudClient) ListenHandler() error {
	/* only the cloud metadata server use this function */
	for {
		conn, err := c.listen_conn.Accept()
		if nil != err { log.Debugf("%s", err); return err }
		go c.ClientHandler(conn)
		
	}
}

/* loop for received messages */
func (c *CloudClient) ClientHandler(client_conn net.Conn) error {
	buffer := make([]byte, 2048)
	length, err := client_conn.Read(buffer)
	if nil != err { log.Debugf("%s %s", client_conn.RemoteAddr(), err); return err }
	message := Message{}
	err = json.Unmarshal(buffer[:length], &message)
	if nil != err { log.Debugf("%s", err); return err }
	if "PUT" == message.Type {
		c.ProviderHandler(client_conn, message)
	} else if "GET" == message.Type {
		c.FindHandler(client_conn, message)
	}
	return nil
}

func (c *CloudClient) FindPeer(ctx context.Context, _ peer.ID) (pstore.PeerInfo, error) {
	log.Debugf("FindPeer")
	return pstore.PeerInfo{}, nil
}

/* called when an object is get */
func (c *CloudClient) FindProvidersAsync(ctx context.Context, k key.Key, count int) <-chan pstore.PeerInfo {
	log.Debugf("FindProvidersAsync")
	out := make(chan pstore.PeerInfo, count)
	go c.FindProvidersAsync_(ctx, k, out)
	return out
}

func (c *CloudClient) FindProvidersAsync_(ctx context.Context, k key.Key, out chan pstore.PeerInfo) error {
	/* send get to cloud */
	m := Message{"GET", key.B58KeyEncode(k), "", []string{}}
	if "" == c.cloud_addr {
		log.Debugf("No cloud to put metadata")
		return nil
	}
	m_json, err := json.Marshal(m)
	if nil != err { log.Debugf("%s", err); return err }
	conn, err := net.Dial("tcp", c.cloud_addr)
	if nil != err { log.Debugf("%s", err); return err }
	fmt.Fprintf(conn, string(m_json))
	buffer := make([]byte, 2048)
	length, err := conn.Read(buffer)
	if nil != err { log.Debugf("%s", err); return err }
	if 0 >= length {
		log.Errorf("strange length received: %d", length)
	}
	conn.Close()

	/* parse answer */
	err = json.Unmarshal(buffer[:length], &m)
	if nil != err { log.Debugf("%s", err); return err }
	if "OK" != m.Type {
		log.Debugf("Non ok message received")
		return nil
	}
	p := new(pstore.PeerInfo)
	err = p.UnmarshalJSON([]byte(buffer[:length]))
	c.peerstore.AddAddrs(p.ID, p.Addrs, pstore.TempAddrTTL)
	out <- c.peerstore.PeerInfo(p.ID)
	ctx.Done()
	return nil
	
}


/* called when a GET message is received */
func (c *CloudClient) FindHandler(conn net.Conn, message Message) error {
	providers := []Provider{}
	providers_json, err := c.datastore.Get(ds.NewKey(message.Key))
	if nil != err {
		log.Debugf("%s", err); return err;
	}
	e := []byte(providers_json.([]byte))
	err = json.Unmarshal(e, &providers)
	if nil != err { log.Debugf("%s", err); return err }
	message.Type = "OK"
	message.ID = providers[0].ID
	message.Addrs = providers[0].Addrs
	m_json, err := json.Marshal(message)
	if nil != err { log.Debugf("%s", err); return err }
	fmt.Fprintf(conn, string(m_json))
	conn.Close()
	return nil
}

func (c *CloudClient) GetValue(ctx context.Context, k key.Key) ([]byte, error) {
	log.Debugf("GetValue")
	return nil, nil
}

func (c *CloudClient) GetValues(ctx context.Context, k key.Key, nvals int) ([]routing.RecvdVal, error) {
	log.Debugf("GetValues")
	return nil, nil
}

/* called when an object is added on the node */
func (c *CloudClient) Provide(ctx context.Context, k key.Key) error {
	log.Debugf("Provide")
	addrs := []string{}
	a := c.host.Addrs()
	for i := 0; i < len(a); i++ {
		addrs = append(addrs, a[i].String())
	}

	/* add key in the local datastore */
	err := c.AddProvider(k, c.self.Pretty(), addrs)
	if nil != err { log.Debugf("%s", err); return err }

	/* add key in the cloud */
	m := Message{"PUT", key.B58KeyEncode(k), c.self.Pretty(), addrs}
	if "" == c.cloud_addr {
		log.Debugf("No cloud to put metadata")
		return nil
	}
	m_json, err := json.Marshal(m)
	if nil != err { log.Debugf("%s", err); return err }
	conn, err := net.Dial("tcp", c.cloud_addr)
	if nil != err { log.Debugf("%s", err); return err }
	fmt.Fprintf(conn, string(m_json))
	buffer := make([]byte, 2048)
	length, err := conn.Read(buffer)
	if nil != err { log.Debugf("%s", err); return err }
	if 0 >= length {
		log.Errorf("strange length received: %d", length)
	}
	conn.Close()
	err = json.Unmarshal(buffer[:length], &m)
	if nil != err { log.Debugf("%s", err); return err }
	if "OK" != m.Type {
		log.Debugf("Non ok message received")
		return nil
	}
	return nil
}

/* called when a PUT message is received */
func (c *CloudClient) ProviderHandler(conn net.Conn, message Message) error {
	k := key.B58KeyDecode(message.Key)
	err := c.AddProvider(k, message.ID, message.Addrs)
	if nil != err { log.Debugf("%s", err); return err }
	message.Type = "OK"
	m_json, err := json.Marshal(message)
	if nil != err { log.Debugf("%s", err); return err }
	fmt.Fprintf(conn, string(m_json))
	conn.Close()
	return nil
}

/* add <key, {id: , addrs:[]}> in the datastore */
func (c *CloudClient) AddProvider(k key.Key, ID string, Addrs []string ) error {
	log.Debugf("AddProvider %s", k)

	providers := []Provider{}
	provider :=  Provider{ID, Addrs}

	old_providers_json, err := c.datastore.Get(k.DsKey())
	if nil == err {
		log.Debugf("key found %s", k)
		e := []byte(old_providers_json.([]byte))
		err := json.Unmarshal(e, &providers)
		if nil != err { log.Debugf("%s", err); return err }
	}

	/* check if the provider already exist */
	found := false
	for i := 0; i < len(providers); i++ {
		if providers[i].ID == provider.ID {
			log.Debugf("%s already found for key %s (%s)", provider, k, providers)
			providers[i] = provider
			found = true
			break
		}
	}
	if false == found {
		providers = append(providers, provider)
	}

	if false == found {
		providers_json, err := json.Marshal(providers)
		if nil != err { log.Debugf("%s", err); return err }
		log.Debugf("%s", providers_json)

		err = c.datastore.Put(k.DsKey(), []byte(providers_json))
		if nil != err { log.Debugf("%s", err); return err }
	}

	return nil
}


func (c *CloudClient) PutValue(ctx context.Context, k key.Key, value []byte) error {
	log.Debugf("PutValue")
	return nil
}

var _ routing.IpfsRouting = &CloudClient{}


