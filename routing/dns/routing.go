package dnsrouting

import (
  "fmt"
  "net"
  config "github.com/ipfs/go-ipfs/repo/config"
  context "gx/ipfs/QmZy2y8t9zQH2a1b8q2ZSLKp17ATuJoCNxxyMFG5qFExpt/go-net/context"
  dns "github.com/miekg/dns"
  host "gx/ipfs/QmVCe3SNMjkcPgnpFhZs719dheq6xE7gJwjzV7aWcUM4Ms/go-libp2p/p2p/host"
  key "github.com/ipfs/go-ipfs/blocks/key"
  logging "gx/ipfs/QmNQynaz7qfriSUJkiEZUrm2Wen1u3Kj9goZzWtrPyu7XR/go-log"
  p2phost "gx/ipfs/QmVCe3SNMjkcPgnpFhZs719dheq6xE7gJwjzV7aWcUM4Ms/go-libp2p/p2p/host"
  peer "gx/ipfs/QmRBqJF7hb8ZSpRcMwUt8hNhydWcxGEhtk81HKq6oUwKvs/go-libp2p-peer"
  pstore "gx/ipfs/QmQdnfvZQuhdT93LNc5bos52wAmdr3G2p6G8teLJMEN32P/go-libp2p-peerstore"
  repo "github.com/ipfs/go-ipfs/repo"
  routing "github.com/ipfs/go-ipfs/routing"
)
var log = logging.Logger("routing/dns")
type DNSClient struct {
  self  peer.ID
  host  host.Host
  peerstore  pstore.Peerstore
  datastore  repo.Datastore
  ctx  context.Context

  resolver string
}

func ConstructDNSRouting(ctx context.Context, h p2phost.Host, d repo.Datastore) (routing.IpfsRouting, error) {
  log.Debugf("DNS routing")
  client := new(DNSClient)
  client.self = h.ID()
  client.peerstore = h.Peerstore()
  client.host = h
  client.datastore = d
  client.ctx = ctx

  if "" != Cfg.DNS.Resolver {
    client.resolver = Cfg.DNS.Resolver
  }

  return client, nil
}

func (c *DNSClient) Bootstrap(ctx context.Context) error {
  log.Debugf("Bootstrap")
  return nil
}

func (c *DNSClient) FindPeer(ctx context.Context, _ peer.ID) (pstore.PeerInfo, error) {
  log.Debugf("FindPeer")
  return pstore.PeerInfo{}, nil
}

func (c *DNSClient) FindProvidersAsync(ctx context.Context, k key.Key, count int) <-chan pstore.PeerInfo {
  log.Debugf("FindProvidersAsync")
  out := make(chan pstore.PeerInfo, count)
  go c.FindProvidersAsync_(ctx, k, out)
  return out
}

func (c *DNSClient) GetValue(ctx context.Context, k key.Key) ([]byte, error) {
  log.Debugf("GetValue")
  return nil, nil
}

func (c *DNSClient) GetValues(ctx context.Context, k key.Key, nvals int) ([]routing.RecvdVal, error) {
  log.Debugf("GetValues")
  return nil, nil
}

func (c *DNSClient) Provide(ctx context.Context, k key.Key) error {
  log.Debugf("Provide")
  return nil
}

func (c *DNSClient) PutValue(ctx context.Context, k key.Key, value []byte) error {
  log.Debugf("PutValue")
  return nil
}

func (c *DNSClient) FindProvidersAsync_(ctx context.Context, k key.Key, out chan pstore.PeerInfo) error {
  log.Debugf("FindProvidersAsync_")
  client := new(dns.Client)
  message := new(dns.Msg)
  message.SetQuestion(dns.Fqdn(string(k)), dns.TypeNS)
  message.RecursionDesired = false


  var authority_servers []string // a stack of partial answers
  authority_servers = append(authority_servers, c.resolver) 

  for 0 != len(authority_servers) {
    authority_server := (authority_servers)[len(authority_servers)-1]
    authority_servers = (authority_servers)[0:len(authority_servers)-1]

    r, _, err := client.Exchange(message, net.JoinHostPort(authority_server, "53"))
    if r == nil {
      log.Fatalf("**** error: %s\n", err.Error())
      log.Debugf("**** We need to try another DNS server\n")
      continue
    }
    if r.Rcode != dns.RcodeSuccess {
      log.Fatalf("**** invalid answer name %s after query for %s\n", string(k), string(k))
      log.Fatalf("**** Object does not exist, perhaps try another path in the DNS\n");
      continue
    }
    if 0 == len(r.Extra) {
      log.Fatalf("**** Lookup stalled\n, try another path\n")
      continue
    }
/*
    for _, a := range r.Ns {
      fmt.Printf("%v\n", a)
    }
*/
    for _, a := range r.Extra {
      next_server := string(a.(*dns.A).A)
      authority_servers = append(authority_servers, next_server)
      fmt.Printf("%v\n", next_server)
    }

  }
  ctx.Done()
  return nil

}

var Cfg *config.Config
var _ routing.IpfsRouting = &DNSClient{}
