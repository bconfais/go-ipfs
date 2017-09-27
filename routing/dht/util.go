package dht

import (
	"sync"
)

// Pool size is the number of nodes used for group find/set RPC calls
var PoolSize = 1

// K is the maximum number of requests to perform before returning failure.
var KValue = 3

// Alpha is the concurrency factor for asynchronous requests.
var AlphaValue = 3000

// A counter for incrementing a variable across multiple threads
type counter struct {
	n   int
	mut sync.Mutex
}

func (c *counter) Increment() {
	c.mut.Lock()
	c.n++
	c.mut.Unlock()
}

func (c *counter) Decrement() {
	c.mut.Lock()
	c.n--
	c.mut.Unlock()
}

func (c *counter) Size() (s int) {
	c.mut.Lock()
	s = c.n
	c.mut.Unlock()
	return
}
