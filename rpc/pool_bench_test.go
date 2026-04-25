package rpc

import (
	"fmt"
	"testing"
)

// The RPC pool is a buffered-channel pool. Get is `<-conns`, Put is
// `conns <- c` with an atomic.Bool guard. These benches measure the
// raw channel-pool primitive cost independently of the `*Client`
// instance they shuttle, so the dummy clients are never connected to
// a daemon. NewPool requires a live daemon; we bypass it and construct
// Pool{} fields directly for that reason.

func newBenchPool(size int) *Pool {
	p := &Pool{
		conns: make(chan *Client, size),
		size:  size,
	}
	for i := 0; i < size; i++ {
		p.conns <- &Client{Endpoint: fmt.Sprintf("bench-%d", i)}
	}
	return p
}

// BenchmarkPool_GetPut measures the single-goroutine round-trip.
// This is the baseline a concurrent workload amortizes against.
func BenchmarkPool_GetPut(b *testing.B) {
	p := newBenchPool(8)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := p.Get()
		p.Put(c)
	}
}

// BenchmarkPool_GetPut_Parallel drives contention across GOMAXPROCS
// workers. The shipped default pool size is 8 (see structures.
// DefaultPoolSize); using that here reflects real operator config.
// If contention ever grows non-linearly, this is the bench that
// surfaces it.
func BenchmarkPool_GetPut_Parallel(b *testing.B) {
	p := newBenchPool(8)
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c := p.Get()
			p.Put(c)
		}
	})
}
