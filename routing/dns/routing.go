package dnsrouting

import (
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
}

func ConstructDNSRouting(ctx context.Context, h p2phost.Host, d repo.Datastore) (routing.IpfsRouting, error) {
  log.Debugf("DNS routing")
  client := new(DNSClient)
  client.self = h.ID()
  client.peerstore = h.Peerstore()
  client.host = h
  client.datastore = d
  client.ctx = ctx
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
  return nil
}

var Cfg *config.Config
var _ routing.IpfsRouting = &DNSClient{}
