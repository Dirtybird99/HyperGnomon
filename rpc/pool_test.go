package rpc

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newTestPool builds a Pool with pre-populated dummy clients so tests
// never need a real daemon connection.
func newTestPool(size int) *Pool {
	p := &Pool{
		conns:    make(chan *Client, size),
		endpoint: "test://localhost:0",
		size:     size,
	}
	for i := 0; i < size; i++ {
		p.conns <- &Client{Endpoint: fmt.Sprintf("test-%d", i)}
	}
	return p
}

// ---------------------------------------------------------------------------
// TestPool_Size
// ---------------------------------------------------------------------------

func TestPool_Size(t *testing.T) {
	for _, sz := range []int{1, 4, 16} {
		p := newTestPool(sz)
		if got := p.Size(); got != sz {
			t.Errorf("Size() = %d, want %d", got, sz)
		}
	}
}

// ---------------------------------------------------------------------------
// TestPool_GetPut
// ---------------------------------------------------------------------------

func TestPool_GetPut(t *testing.T) {
	const poolSize = 4
	p := newTestPool(poolSize)

	// Drain every connection from the pool.
	clients := make([]*Client, poolSize)
	for i := 0; i < poolSize; i++ {
		clients[i] = p.Get()
		if clients[i] == nil {
			t.Fatalf("Get() returned nil on call %d", i)
		}
	}

	// Channel should be empty now -- a non-blocking receive must fail.
	select {
	case c := <-p.conns:
		t.Fatalf("expected empty pool, got %v", c)
	default:
		// good
	}

	// Return all connections.
	for _, c := range clients {
		p.Put(c)
	}

	// Channel should be full again.
	if got := len(p.conns); got != poolSize {
		t.Fatalf("pool length after Put = %d, want %d", got, poolSize)
	}
}

func TestPool_GetPut_PreservesIdentity(t *testing.T) {
	p := newTestPool(1)
	c := p.Get()
	endpoint := c.Endpoint
	p.Put(c)

	c2 := p.Get()
	if c2.Endpoint != endpoint {
		t.Errorf("got back different client: %q vs %q", c2.Endpoint, endpoint)
	}
}

func TestPool_GetBlocks(t *testing.T) {
	// Pool of 1 -- second Get must block until the first is Put back.
	p := newTestPool(1)
	first := p.Get()

	done := make(chan *Client, 1)
	go func() {
		done <- p.Get() // should block
	}()

	// Give the goroutine a moment, then verify it hasn't returned.
	select {
	case <-done:
		t.Fatal("second Get() returned before Put()")
	case <-time.After(50 * time.Millisecond):
		// expected
	}

	p.Put(first)

	select {
	case c := <-done:
		if c == nil {
			t.Fatal("second Get() returned nil")
		}
	case <-time.After(time.Second):
		t.Fatal("second Get() still blocked after Put()")
	}
}

// ---------------------------------------------------------------------------
// TestPool_WithConn
// ---------------------------------------------------------------------------

func TestPool_WithConn(t *testing.T) {
	p := newTestPool(2)

	var seen string
	err := p.WithConn(func(c *Client) error {
		seen = c.Endpoint
		return nil
	})
	if err != nil {
		t.Fatalf("WithConn returned error: %v", err)
	}
	if seen == "" {
		t.Fatal("callback was not invoked")
	}

	// Connection should be back in the pool.
	if got := len(p.conns); got != 2 {
		t.Fatalf("pool length after WithConn = %d, want 2", got)
	}
}

func TestPool_WithConn_PropagatesError(t *testing.T) {
	p := newTestPool(1)
	want := errors.New("boom")

	got := p.WithConn(func(_ *Client) error { return want })
	if !errors.Is(got, want) {
		t.Fatalf("WithConn error = %v, want %v", got, want)
	}

	// Even on error the connection must be returned.
	if n := len(p.conns); n != 1 {
		t.Fatalf("pool length after error = %d, want 1", n)
	}
}

func TestPool_WithConn_Concurrent(t *testing.T) {
	const poolSize = 4
	const goroutines = 20
	p := newTestPool(poolSize)

	var wg sync.WaitGroup
	var count atomic.Int64
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_ = p.WithConn(func(_ *Client) error {
				count.Add(1)
				// Simulate brief work so connections are contended.
				time.Sleep(time.Millisecond)
				return nil
			})
		}()
	}

	wg.Wait()

	if got := count.Load(); got != goroutines {
		t.Fatalf("callback invoked %d times, want %d", got, goroutines)
	}
	if n := len(p.conns); n != poolSize {
		t.Fatalf("pool length after concurrent WithConn = %d, want %d", n, poolSize)
	}
}

// ---------------------------------------------------------------------------
// TestPool_Close
// ---------------------------------------------------------------------------

func TestPool_Close(t *testing.T) {
	p := newTestPool(3)
	p.Close()

	// After Close the closed flag must be set.
	if !p.closed.Load() {
		t.Fatal("closed flag not set after Close()")
	}

	// The channel should be closed and drained.
	c, ok := <-p.conns
	if ok {
		t.Fatalf("channel still open after Close(), got %v", c)
	}
}

func TestPool_Put_AfterClose(t *testing.T) {
	// Put on a closed pool should not panic and should not re-insert.
	// The real Put calls c.Close() when pool is closed, which is safe on
	// our dummy clients (nil WS / nil RPC).
	p := newTestPool(1)
	c := p.Get()
	p.Close()

	// Should not panic.
	p.Put(c)
}

// ---------------------------------------------------------------------------
// TestPool_NewPool_ZeroSize (edge case)
// ---------------------------------------------------------------------------

func TestPool_ZeroSize(t *testing.T) {
	p := &Pool{
		conns:    make(chan *Client, 0),
		endpoint: "test",
		size:     0,
	}
	if p.Size() != 0 {
		t.Fatalf("Size() = %d, want 0", p.Size())
	}
	// Close on empty pool must not hang or panic.
	p.Close()
}
