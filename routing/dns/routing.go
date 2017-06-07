package dnsrouting

import (
  "fmt"
  "net"
  "errors"
  "strings"
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

const (
  Bottom2Top = 0
  Top2Bottom = 1
)

type DNSClient struct {
  self  peer.ID
  host  host.Host
  peerstore  pstore.Peerstore
  datastore  repo.Datastore
  ctx  context.Context

  resolver string
  site string
  zone string
  site_fqdn string
  path []string
}

func reverse(ss []string) {
    last := len(ss) - 1
    for i := 0; i < len(ss)/2; i++ {
        ss[i], ss[last-i] = ss[last-i], ss[i]
    }
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
  if "" != Cfg.DNS.Site {
    client.site = Cfg.DNS.Site
  }
  if "" != Cfg.DNS.Zone {
    client.zone = Cfg.DNS.Zone
  }

  client.site_fqdn = fmt.Sprintf("%s.%s.", client.site, client.zone)

  return client, nil
}

func (c *DNSClient) Bootstrap(ctx context.Context) error {
  log.Debugf("Bootstrap")

  _, path, error := c.QueryDNSRecursive(c.site_fqdn, c.QueryTXT, Top2Bottom)
  if nil != error {
    log.Debugf("DNS lookup failed\n")
    ctx.Done()
    return errors.New("DNS lookup failed");
  } else {
    c.path = path
  }
  fmt.Printf("%s\n\n", path)

  // TODO: nssupdate the local dns for *.siteX -> mymultihash

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

func (c *DNSClient) QueryDNS(client *dns.Client, fqdn string, type_ uint16, resolver string) ([]dns.RR, error) {
  message := new(dns.Msg)
  message.SetQuestion(dns.Fqdn(fqdn), type_)
  message.RecursionDesired = false

  rr, _, _ := client.Exchange(message, net.JoinHostPort(resolver, "53"))
  if rr == nil {
    log.Debugf("**** Lookup stalled\n, try another path\n")
    return nil, errors.New("")
  }
  if rr.Rcode != dns.RcodeSuccess {
    log.Debugf("**** Lookup stalled\n, try another path\n")
    return nil, errors.New("")
  }
  if 0 == len(rr.Answer) {
    log.Debugf("Answer is empty for %s\n", fqdn)
    return nil, errors.New("")
  }

  return rr.Answer, nil
}

func (c *DNSClient) QueryTXT(client *dns.Client, fqdn string, resolver string) ([]string, error) {
  rr, error := c.QueryDNS(client, fqdn, dns.TypeTXT, resolver)
  if nil != error {
    return nil, error
  }
  var results []string
  for _, a := range rr {
    res := a.(*dns.TXT).Txt[0]
    results = append(results, res)
  }
  return results, nil
}


func (c *DNSClient) QueryDNSRecursive(fqdn string, callback func(*dns.Client, string, string)([]string, error), direction int) ([]string, []string, error) {
  client := new(dns.Client)

  message := new(dns.Msg)
  message.SetQuestion(dns.Fqdn(fqdn), dns.TypeA)
  message.RecursionDesired = false

  var servers []string // a stack of partial answers
  if Bottom2Top == direction {
    servers = make([]string, len(c.path))
    fmt.Printf("ok1\n")
    copy(servers, c.path)
  } else {
    fmt.Printf("ok2\n")
    servers = append(servers, c.resolver) 
  }
  fmt.Printf("%s\n\n", servers)

  var path []string
  var results []string

  found := false

  for 0 != len(servers) && false == found {
    server := (servers)[len(servers)-1]
    servers = (servers)[0:len(servers)-1]

    path = append(path, server)

    log.Debugf("Query %s\n", server)

    r, _, err := client.Exchange(message, net.JoinHostPort(server, "53"))
    if r == nil {
      log.Debugf("**** error: %s\n", err.Error())
      log.Debugf("**** We need to try another DNS server\n")
      path = (path)[0:len(path)-1]
      continue
    }
    if r.Rcode != dns.RcodeSuccess {
      log.Debugf("**** invalid answer name %s after query for %s\n", fqdn, fqdn)
      log.Debugf("**** Object does not exist, perhaps try another path in the DNS\n");
      path = (path)[0:len(path)-1]
      continue
    }
    if 0 == len(r.Answer) {
      fmt.Printf("no answer from %s\n", server)
      res, error := callback(client, fqdn, server)
      results = res 
      if nil != error {
        path = (path)[0:len(path)-1]
        continue
      } else {
        found = true
        break
      }
    }

/*
    for _, a := range r.Ns {
      fmt.Printf("%v\n", a)
    }
*/

    // TODO: here we have the opportunity to prefer a local server than a remote one, indeed, not needed if we request from bottom to top
    for _, a := range r.Answer {
      next_server := a.(*dns.A).A.String()
      if next_server == server {
        fmt.Printf("--> <--\n")
        res, error := callback(client, fqdn, server)
        results = res
        if nil == error {
          fmt.Printf("ok\n\n")
          found = true
          break
        }
      } else {
        servers = append(servers, next_server)
        fmt.Printf("%v\n", next_server)
      }
    }

  }
  if 0 == len(results) && 0 == len(servers) {
    return nil, nil, errors.New("Value not found in DNS")
  }

  return results, path, nil
}

func (c *DNSClient) FindNodesToUpdate(path []string) []string {
  // the idea is that we need to update all nodes until the "common root" between the path to the object and the 
  common_root := path[0]
  found := false
  var results []string
  for _, el := range c.path {
    if el == common_root {
      found = true
    }
    if true == found {
      results = append(results, el)
    }
  }
  return results
}

func (c *DNSClient) UpdateDNS(fqdn string, servers []string) error {
  client := new(dns.Client)

  last_server := servers[0]
  var prelast_server string
  if len(c.path) >= 2 {
    prelast_server = servers[1]
  } 

  type_ := "TXT"
  value := c.site_fqdn

  for 0 != len(servers) {
    server := (servers)[len(servers)-1]
    servers = (servers)[0:len(servers)-1]
    message := new(dns.Msg)
    a := fmt.Sprintf("%s.", c.zone)
    message.SetUpdate(a)
    record := fmt.Sprintf("%s. 30 IN %s %s", fqdn, type_, value)
    fmt.Printf("%s -> %s\n", record, server)
    rr, _ := dns.NewRR(record)
    rrs := make([]dns.RR, 1)
    rrs[0] = rr
    message.Insert(rrs)
    _, _, err := client.Exchange(message, net.JoinHostPort(server, "53"))
    if nil != err {
      log.Debugf("%s\n", err)
    }
    type_ = "A"
    value = server
  }

  // Extra update (because site1->ip object.site1->other_ip therefore if we request object.site1 we get only other_ip and not the first one)
  if len(c.path) < 2 {
   return nil
  }

  // extract the site from object name
  site := strings.Join(strings.Split(fqdn, ".")[1:], ".")
  fmt.Printf("%s\n", last_server)
  rr, err := c.QueryDNS(client, site, dns.TypeA, last_server)
  if nil != err {
    log.Debugf("Unable to set the path on the last server\n")
    return err
  }
  dir := rr[0].(*dns.A).A.String()  
  fmt.Printf("%s\n", dir)
  if ( dir != prelast_server ) {
    fmt.Printf("Additional update\n")
    message := new(dns.Msg)
    a := fmt.Sprintf("%s.", c.zone)
    message.SetUpdate(a)
    record := fmt.Sprintf("%s. 30 IN A %s", fqdn, dir)
    fmt.Printf("%s -> %s\n", record, last_server)
    rr, _ := dns.NewRR(record)
    rrs := make([]dns.RR, 1)
    rrs[0] = rr
    message.Insert(rrs)
    _, _, err := client.Exchange(message, net.JoinHostPort(last_server, "53"))
    if nil != err {
      log.Debugf("%s\n", err)
    }
  }

  return nil

}


func (c *DNSClient) FindProvidersAsync_(ctx context.Context, k key.Key, out chan pstore.PeerInfo) error {
  log.Debugf("FindProvidersAsync_")
  results, path, error := c.QueryDNSRecursive(string(k), c.QueryTXT, Bottom2Top)
  if nil != error {
    log.Debugf("DNS lookup failed\n")
    ctx.Done()
    return errors.New("DNS lookup failed");
  }
  fmt.Printf("%s\n", results)
  fmt.Printf("%s\n", path)
  fmt.Printf("%s\n\n", c.FindNodesToUpdate(path))
  c.UpdateDNS(string(k), c.FindNodesToUpdate(path))
  ctx.Done()
  return nil

}


var Cfg *config.Config
var _ routing.IpfsRouting = &DNSClient{}
