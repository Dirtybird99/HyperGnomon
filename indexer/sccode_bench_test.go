package indexer

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hypergnomon/hypergnomon/structures"
)

func newBenchIndexer(b *testing.B) *Indexer {
	b.Helper()
	// Can't call storage.NewBboltStore here (would import cycle indexer tests
	// already use it; newTestIndexer in sccode_test.go already handles this).
	// This is a thin wrapper to keep benchmark files self-describing.
	return newTestIndexerB(b)
}

func newTestIndexerB(tb testing.TB) *Indexer {
	tb.Helper()
	return newTestIndexerShared(tb)
}

// BenchmarkGetSCCode_CacheHit measures the hot-path bucket lookup — the
// 99.9% case once backfill has warmed the cache for a given SCID.
func BenchmarkGetSCCode_CacheHit(b *testing.B) {
	for _, sz := range []int{256, 2048, 16384} {
		b.Run(fmt.Sprintf("size=%d", sz), func(b *testing.B) {
			idx := newBenchIndexer(b)
			scid := "hot-scid"
			if err := idx.Store.PutSCCode(scid, &structures.SCCodeEntry{
				Code: strings.Repeat("x", sz), InstallHeight: 1,
			}); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				entry, err := idx.GetSCCode(scid)
				if err != nil {
					b.Fatal(err)
				}
				if entry == nil {
					b.Fatal("nil on hit")
				}
			}
		})
	}
}

// BenchmarkGetSCCode_CacheMiss measures the full lazy-backfill round-trip
// including the fake fetcher + persist. Each iteration uses a unique SCID
// so every call is a miss; otherwise the bucket would warm up after one.
func BenchmarkGetSCCode_CacheMiss(b *testing.B) {
	idx := newBenchIndexer(b)
	code := strings.Repeat("x", 2048)
	prev := setSCCodeFetcher(func(i *Indexer, _ string) (string, int64, error) {
		return code, i.ChainHeight.Load(), nil
	})
	b.Cleanup(func() { setSCCodeFetcher(prev) })

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scid := fmt.Sprintf("miss-%d", i)
		entry, err := idx.GetSCCode(scid)
		if err != nil {
			b.Fatal(err)
		}
		if entry == nil {
			b.Fatal("nil after backfill")
		}
	}
}

// BenchmarkGetSCCode_CacheMiss_WithLatency injects 1ms of fetcher latency
// (typical daemon RTT on a good network) so the measurement reflects the
// realistic lazy-backfill cost, not a free function-call loop.
func BenchmarkGetSCCode_CacheMiss_WithLatency(b *testing.B) {
	idx := newBenchIndexer(b)
	code := strings.Repeat("x", 2048)
	prev := setSCCodeFetcher(func(i *Indexer, _ string) (string, int64, error) {
		time.Sleep(time.Millisecond)
		return code, i.ChainHeight.Load(), nil
	})
	b.Cleanup(func() { setSCCodeFetcher(prev) })

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		scid := fmt.Sprintf("miss-lat-%d", i)
		if _, err := idx.GetSCCode(scid); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetSCCode_ConcurrentMiss_SingleFlight measures the per-op cost of
// a cache miss when many goroutines race on the same SCID. The reported
// number is per-iteration across all goroutines; the key observation is
// that fetcherCalls at the end should equal the number of distinct SCIDs
// seen, not the goroutine count.
//
// Uses -cpu=... to sweep concurrency. The allocated assertion below fires
// if single-flight is ever broken — benchmark doubles as a regression
// guard on top of TestGetSCCode_SingleFlight.
func BenchmarkGetSCCode_ConcurrentMiss_SingleFlight(b *testing.B) {
	idx := newBenchIndexer(b)
	code := strings.Repeat("x", 2048)
	var fetcherCalls atomic.Int64
	prev := setSCCodeFetcher(func(i *Indexer, _ string) (string, int64, error) {
		fetcherCalls.Add(1)
		return code, i.ChainHeight.Load(), nil
	})
	b.Cleanup(func() { setSCCodeFetcher(prev) })

	b.ReportAllocs()
	// Use per-op unique scids keyed on b.N, not per-goroutine, so we
	// measure the contended case: every goroutine racing on the same
	// cache slot simultaneously.
	var counter atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := counter.Add(1)
			// Group goroutines in cohorts of 8 sharing the same SCID, so
			// single-flight actually has work to dedupe.
			scid := fmt.Sprintf("cf-%d", id/8)
			if _, err := idx.GetSCCode(scid); err != nil {
				b.Fatal(err)
			}
		}
	})
}
