package rpc

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Pool manages a fixed-size pool of RPC connections to the DERO daemon.
// Uses a buffered channel as a lock-free pool -- Get blocks when empty,
// Put returns the connection for reuse.
type Pool struct {
	conns    chan *Client
	endpoint string
	size     int
	closed   atomic.Bool
	mu       sync.Mutex
}

// NewPool creates a connection pool with the given size.
func NewPool(endpoint string, size int) (*Pool, error) {
	p := &Pool{
		conns:    make(chan *Client, size),
		endpoint: endpoint,
		size:     size,
	}

	var wg sync.WaitGroup
	var createErr error
	var errOnce sync.Once
	for i := 0; i < size; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := NewClient(endpoint)
			if err != nil {
				errOnce.Do(func() { createErr = err })
				return
			}
			p.conns <- c
		}()
	}
	wg.Wait()
	if createErr != nil {
		p.Close()
		return nil, fmt.Errorf("pool: %w", createErr)
	}

	logger.Infof("RPC pool created: %d connections to %s (parallel)", size, endpoint)
	return p, nil
}

// Get retrieves a connection from the pool. Blocks if all connections are in use.
func (p *Pool) Get() *Client {
	return <-p.conns
}

// Put returns a connection to the pool.
func (p *Pool) Put(c *Client) {
	if p.closed.Load() {
		c.Close()
		return
	}
	p.conns <- c
}

// Close shuts down all connections in the pool.
func (p *Pool) Close() {
	p.closed.Store(true)
	p.mu.Lock()
	defer p.mu.Unlock()

	close(p.conns)
	for c := range p.conns {
		c.Close()
	}
}

// Size returns the pool size.
func (p *Pool) Size() int {
	return p.size
}

// GetWithTimeout retrieves a connection with a timeout. Returns nil if timeout expires.
func (p *Pool) GetWithTimeout(d time.Duration) *Client {
	select {
	case c := <-p.conns:
		return c
	case <-time.After(d):
		return nil
	}
}

// WithConn executes a function with a pooled connection, automatically returning it.
func (p *Pool) WithConn(fn func(*Client) error) error {
	c := p.Get()
	defer p.Put(c)
	return fn(c)
}
