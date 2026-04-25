// Package gnomes_test benches the facade layer's per-tick cost. The
// actual civilware-shape facade (pkg/gnomes/indexer) runs a ~10 Hz
// goroutine that copies atomic.Int64 fields out to plain int64 fields
// so the exported `Indexer.LastIndexedHeight` + `.ChainHeight` are
// readable without callers learning HyperGnomon's atomic type.
//
// This bench measures one tick's CPU cost — the two atomic.Load /
// atomic.Store pairs the goroutine does inside its sleep loop. The
// sleep itself (100 ms) is not bench-relevant; what matters is that
// the tick body stays cheap so the goroutine never becomes a
// scheduling burden on embedders running many Indexer instances.
package gnomes_test

import (
	"sync/atomic"
	"testing"
)

// A scaffold mirroring the exact ops inside startFieldRefresh:
//
//	atomic.StoreInt64(&idx.LastIndexedHeight, idx.inner.LastIndexedHeight.Load())
//	atomic.StoreInt64(&idx.ChainHeight,       idx.inner.ChainHeight.Load())
//
// The real facade is not instantiable in-bench without standing up a
// full hgindexer via New() (opens a bbolt store, dials RPC). Since
// those setup costs aren't what we're measuring, we reproduce the
// exact atomic op shape here. CPU-visible work is identical.
type benchFacadeFields struct {
	LastIndexedHeight int64
	ChainHeight       int64
}

type benchInnerFields struct {
	LastIndexedHeight atomic.Int64
	ChainHeight       atomic.Int64
}

func BenchmarkFacadeFieldRefresh(b *testing.B) {
	facade := &benchFacadeFields{}
	inner := &benchInnerFields{}
	inner.LastIndexedHeight.Store(6_927_000)
	inner.ChainHeight.Store(6_927_005)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		atomic.StoreInt64(&facade.LastIndexedHeight, inner.LastIndexedHeight.Load())
		atomic.StoreInt64(&facade.ChainHeight, inner.ChainHeight.Load())
	}
}
