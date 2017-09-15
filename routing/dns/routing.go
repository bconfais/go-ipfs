package dnsrouting

import (
  "fmt"
  "os"
  "math/rand"
  "net"
  "errors"
  "strings"
  "time"
  "sync"
  config "github.com/ipfs/go-ipfs/repo/config"
  context "gx/ipfs/QmZy2y8t9zQH2a1b8q2ZSLKp17ATuJoCNxxyMFG5qFExpt/go-net/context"
  dns "github.com/miekg/dns"
  host "gx/ipfs/QmVCe3SNMjkcPgnpFhZs719dheq6xE7gJwjzV7aWcUM4Ms/go-libp2p/p2p/host"
  key "github.com/ipfs/go-ipfs/blocks/key"
  logging "gx/ipfs/QmNQynaz7qfriSUJkiEZUrm2Wen1u3Kj9goZzWtrPyu7XR/go-log"
  ma "gx/ipfs/QmYzDkkgAEmrcNzFCiYo6L1dTX4EAG1gZkbtdbd9trL4vd/go-multiaddr"
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
  updates struct{
    sync.RWMutex
    m map[string]string
  }
}

func in_slice(a string, list []string) bool {
    for _, b := range list {
        if b == a {
            return true
        }
    }
    return false
}


func reverse(ss []string) {
  last := len(ss) - 1
  for i := 0; i < len(ss)/2; i++ {
    ss[i], ss[last-i] = ss[last-i], ss[i]
  }
}

func isASCII(s string) bool {
  for _, c := range s {
    if c > 127 {
      return false
    }
  }
 return true
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
  client.updates.m = make(map[string]string)
  return client, nil
}

func (c *DNSClient) Bootstrap(ctx context.Context) error {
  log.Debugf("Bootstrap")

  result, _, err := c.QueryDNSRecursive(c.site_fqdn, dns.TypeTXT, c.dnsTXT, Top2Bottom)
  if nil != err {
    log.Debugf("DNS lookup failed %s\n", err.Error())
    ctx.Done()
    return errors.New("DNS lookup failed");
  }
  c.path = strings.Split(result[0], ",")
  c.UpdateDNS("", c.path)
  fmt.Printf("%s\n\n", c.path)
  return nil
}

func (c *DNSClient) FindPeer(ctx context.Context, _ peer.ID) (pstore.PeerInfo, error) {
  log.Debugf("FindPeer")
  return pstore.PeerInfo{}, nil
}

func (c *DNSClient) FindProvidersAsync(ctx context.Context, k key.Key, count int) <-chan pstore.PeerInfo {
  log.Debugf("FindProvidersAsync")
  out := make(chan pstore.PeerInfo, count)
  if (strings.HasPrefix(string(k), "Qm")) {
   return nil
  }
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
  retry := 0
  start := time.Now()
  if (strings.HasPrefix(string(k), "Qm")) {
    return nil
  }
  log.Debugf("Provide")
  if false == isASCII(string(k)) {
   k = key.Key(key.B58KeyEncode(k))
  }

  if (strings.Contains(string(k), fmt.Sprintf("%s.%s", c.site, c.zone))) {
   /* the objet was written without locating it, no need to update metadata */
   return nil;
  }

  for {
   retry = retry + 1
   if ( "" != c.updates.m[string(k)] ) {
     break;
   }
   time.Sleep(time.Duration(retry)*time.Second)   
  }

  servers := c.FindNodesToUpdate(c.updates.m[string(k)])
  err := c.UpdateDNS(string(k), servers)
  if err != nil {
   log.Debugf("**** error: %s\n", err.Error())
  }


  elapsed := time.Since(start)
  f, _ := os.OpenFile("/tmp/log", os.O_APPEND|os.O_WRONLY, 0644)
  defer f.Close()
  f.WriteString(fmt.Sprintf("provides took %s (%s)\n", elapsed, string(k)))
  return nil
}

func (c *DNSClient) PutValue(ctx context.Context, k key.Key, value []byte) error {
  log.Debugf("PutValue")
  return nil
}

func (c *DNSClient) dnsTXT(rr []dns.RR) ([]string) {
  var results []string
  for _, a := range rr {
    res := a.(*dns.TXT).Txt[0]
    results = append(results, res)
  }
  return results
}

func (c *DNSClient) QueryDNSRecursive(fqdn string, record_type uint16, callback func([]dns.RR)([]string), direction int) ([]string, string, error) {
  client := new(dns.Client)
  client.Timeout = 30*time.Second
  client.ReadTimeout = 30*time.Second
  client.WriteTimeout = 30*time.Second
  client.DialTimeout = 30*time.Second
  client.Net = "tcp"

  message := new(dns.Msg)
  message.SetQuestion(dns.Fqdn(fqdn), record_type)
  message.RecursionDesired = false

  f, _ := os.OpenFile("/tmp/log", os.O_APPEND|os.O_WRONLY, 0644)
  defer f.Close()

  var servers []string // a stack of partial answers
  if Bottom2Top == direction {
    servers = make([]string, len(c.path))
    copy(servers, c.path)
  } else {
    servers = append(servers, c.resolver) 
  }
  fmt.Printf("%s\n\n", servers)

  var results []string
  var trypath []string

  found := false
  nb_hops := 0
  retry := 0
  max_retry := 3
  last_server := ""
  for 0 != len(servers) && false == found {
    server := (servers)[len(servers)-1]
    servers = (servers)[0:len(servers)-1]

    fmt.Printf("Request %s on %s \n", fqdn, server)

    trypath = append(trypath, server)
    nb_hops = nb_hops + 1
    log.Debugf("Query %s\n", server)

    r, _, err := client.Exchange(message, net.JoinHostPort(server, "53"))
    if nil != err {
     log.Debugf("**** error: %s\n", err.Error())
     log.Debugf("**** We need to try another DNS server\n")
     continue
    }

    if r == nil {
      retry = retry +1
      if ( retry < max_retry ) {
       nb_hops = nb_hops - 1
       servers = append(servers, server)
       log.Debugf("*** retry %d for %s\n", retry, fqdn)
//       time.Sleep(time.Duration(retry)*time.Second)
      } else {
       log.Debugf("**** error: %s\n", err.Error())
       log.Debugf("**** We need to try another DNS server\n")
       retry = 0
      }
      continue
    }

    if r.Rcode != dns.RcodeSuccess {
      retry = retry +1
      if ( retry < max_retry ) {
       nb_hops = nb_hops - 1
       servers = append(servers, server)
       log.Debugf("*** retry %d for %s\n", retry, fqdn)
//       time.Sleep(time.Duration(retry)*time.Second)
      } else {
        log.Debugf("**** invalid answer name %s after query for %s\n", fqdn, fqdn)
        log.Debugf("**** Object does not exist, perhaps try another path in the DNS\n");
        retry = 0
      }
      continue
    }
    retry = 0
    if 0 != len(r.Answer) {
      f.WriteString(fmt.Sprintf("found %d answers (%s)\n", len(r.Answer), fqdn))
      fmt.Printf("no answer from %s\n", server)
      results = callback(r.Answer)
      found = true
      last_server = server
      break
    }
  }
  if (found == true) {
    f.WriteString(fmt.Sprintf("lookup %d hops (%s) %s\n", nb_hops, fqdn, trypath))
    return results, last_server, nil
  }
  f.WriteString(fmt.Sprintf("lookup error %d Hops (%s) %s\n", nb_hops, fqdn, trypath))
  return nil, last_server, errors.New("Value not found in DNS")
}

func (c *DNSClient) FindNodesToUpdate(server string) []string {
  tmp := make([]string, len(c.path))
  copy(tmp, c.path)
  reverse(tmp)
  var results []string
  for _, el := range tmp {
    results = append(results, el)
    if el == server {
     break;
    }
  }
  fmt.Printf("nodes to update %s\n\n", results)
  return results;
}

func (c *DNSClient) UpdateQueryDNS(clientc *dns.Client, zone string, record string, server string) error {
  retry := 0
  for (retry < 10) {
  client := new(dns.Client)
  client.Timeout = 5*time.Second
  client.ReadTimeout = 5*time.Second
  client.WriteTimeout = 5*time.Second
  client.DialTimeout = 5*time.Second
  client.Net = "tcp"
    retry = retry + 1
    message := new(dns.Msg)
    message.SetUpdate(zone)
    fmt.Printf("%s -> %s\n", record, server)
    rr, _ := dns.NewRR(record)
    rrs := make([]dns.RR, 1)
    rrs[0] = rr
    message.Insert(rrs)
    r, _, err := client.Exchange(message, net.JoinHostPort(server, "53"))
    if err != nil {
      f, _ := os.OpenFile("/tmp/log", os.O_APPEND|os.O_WRONLY, 0644)
      defer f.Close()
      f.WriteString(fmt.Sprintf("Error updating %s %s %s (error)\n", err, record, server))
      log.Debugf("Error update %s %s %s (error)\n", err, record, server)
      time.Sleep(time.Duration(retry)*time.Second)
      continue
    }
    if r.Rcode != dns.RcodeSuccess {
      f, _ := os.OpenFile("/tmp/log", os.O_APPEND|os.O_WRONLY, 0644)
      defer f.Close()
      f.WriteString(fmt.Sprintf("Error updating %s %s %s (no success)\n", err, record, server))
      log.Debugf("Error update %s %s %s (no success)\n", err, record, server)
      time.Sleep(time.Duration(retry)*time.Second)
      continue
    }
    return nil
  }
  f, _ := os.OpenFile("/tmp/log", os.O_APPEND|os.O_WRONLY, 0644)
  defer f.Close()
  f.WriteString(fmt.Sprintf("Too many errors while updating %s\n",  server))
  return errors.New("Too many errors while updating")
}

func (c *DNSClient) UpdateMultiHash(fqdn string, server string) error {
  client := new(dns.Client)
  client.Timeout = 30*time.Second
  client.ReadTimeout = 30*time.Second
  client.WriteTimeout = 30*time.Second
  client.DialTimeout = 30*time.Second
  client.Net = "tcp"

  zone := fmt.Sprintf("%s.", c.zone)
  for _, addr :=  range c.host.Addrs() {
    if strings.HasPrefix(addr.String(), "/ip6") {
      continue
    }
    if strings.HasPrefix(addr.String(), "/ip4/127") {
      continue
    }
    if strings.HasPrefix(addr.String(), "/ip4/172") { // grid5000 specific
      continue
    }
    if (fqdn == "") {
/*
     record := fmt.Sprintf("%s 3000 IN TXT \"%s/ipfs/%s\"", c.site_fqdn, addr.String(), c.self.Pretty())
     fmt.Printf("%s\n", record)
     err := c.UpdateQueryDNS(client, zone, record, server)
     if nil != err {
      log.Debugf("*** Error\n")
      return err
     }
*/
     record := fmt.Sprintf("*.%s 3000 IN TXT \"%s/ipfs/%s\"", c.site_fqdn, addr.String(), c.self.Pretty())
     fmt.Printf("%s\n", record)
     err := c.UpdateQueryDNS(client, zone, record, server)
     if nil != err {
      log.Debugf("*** Error\n")
      return err
     }
    } else {
     record := fmt.Sprintf("%s 3000 IN TXT \"%s/ipfs/%s\"", fqdn, addr.String(), c.self.Pretty())
     fmt.Printf("%s\n", record)
     err := c.UpdateQueryDNS(client, zone, record, server)
     if nil != err {
      log.Debugf("*** Error\n")
      return err
     }
    }
  }
  return nil

}

func (c *DNSClient) UpdateDNS(fqdn string, servers []string) error {
  f, _ := os.OpenFile("/tmp/log", os.O_APPEND|os.O_WRONLY, 0644)
  defer f.Close()
  f.WriteString(fmt.Sprintf("updating %s %s\n", fqdn, servers))

  client := new(dns.Client)
  client.Timeout = 30*time.Second
  client.ReadTimeout = 30*time.Second
  client.WriteTimeout = 30*time.Second
  client.DialTimeout = 30*time.Second
  client.Net = "tcp"

  nb_hops := 0
  for _, server := range servers {
   nb_hops = nb_hops + 1
   err := c.UpdateMultiHash(fqdn, server)
   if (err != nil) {
    fmt.Printf("%s\n", err)
   }
  }

  f.WriteString(fmt.Sprintf("update %d messages (%s)\n", nb_hops, fqdn))
  return nil
}


func (c *DNSClient) FindProvidersAsync_(ctx context.Context, k key.Key, out chan pstore.PeerInfo) error {
  log.Debugf("FindProvidersAsync_")
  f, _ := os.OpenFile("/tmp/log", os.O_APPEND|os.O_WRONLY, 0644)
  defer f.Close()
  start := time.Now()
  defer close(out)

  results, last_server, err := c.QueryDNSRecursive(string(k), dns.TypeTXT, c.dnsTXT, Bottom2Top)
  log.Debugf("last server %s\n", last_server)
  if nil != err {
    log.Debugf("DNS lookup failed\n")
    f.WriteString(fmt.Sprintf("FindProvidersAsync_ failed (%s)\n", string(k)))
    ctx.Done()
    return errors.New("DNS lookup failed");
  }
  fmt.Printf("result: %s\n", results)
  c.updates.Lock()
  c.updates.m[string(k)] = last_server
  c.updates.Unlock()

  var pp []*pstore.PeerInfo
  for _, ipfsnode := range results {
    if false == strings.HasPrefix(ipfsnode, "/ip4") {
      continue
    }
    ipfsnode_ := strings.Split(ipfsnode, "/")
    id := ipfsnode_[len(ipfsnode_)-1]
    addr := strings.Join(ipfsnode_[:len(ipfsnode_)-2], "/")
    fmt.Printf("%s -> %s %s\n", ipfsnode, id, addr)

    p := new(pstore.PeerInfo)
    paddr, _ := ma.NewMultiaddr(addr)

    p.ID = peer.ID(key.B58KeyDecode(id))
    p.Addrs = append(p.Addrs, paddr)
    pp = append(pp, p)
  }
  s := rand.NewSource(time.Now().Unix())
  r := rand.New(s)
  i :=r.Intn(len(pp))

  c.peerstore.AddAddrs(pp[i].ID, pp[i].Addrs, pstore.TempAddrTTL)
  out <- c.peerstore.PeerInfo(pp[i].ID)

  ctx.Done()
  elapsed := time.Since(start)
  f.WriteString(fmt.Sprintf("findprovidersasync_ took %s (%s)\n", elapsed, string(k)))

  return nil

}


var Cfg *config.Config
var _ routing.IpfsRouting = &DNSClient{}
