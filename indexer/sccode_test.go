package indexer

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// newTestIndexer delegates to newTestIndexerShared; kept as a named alias so
// test readers see consistent naming between sccode_test.go and the other
// test files. testing.T is accepted via the wider testing.TB interface in
// the shared helper.
func newTestIndexer(t *testing.T) *Indexer {
	return newTestIndexerShared(t)
}

func TestGetSCCode_CacheHit(t *testing.T) {
	idx := newTestIndexer(t)
	scid := "abc"
	if err := idx.Store.PutSCCode(scid, &structures.SCCodeEntry{
		Code: "cached", InstallHeight: 7,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Fetcher must not be called on a cache hit.
	prev := setSCCodeFetcher(func(_ *Indexer, _ string) (string, int64, error) {
		t.Fatalf("fetcher called on cache hit")
		return "", 0, nil
	})
	t.Cleanup(func() { setSCCodeFetcher(prev) })

	got, err := idx.GetSCCode(scid)
	if err != nil {
		t.Fatalf("GetSCCode: %v", err)
	}
	if got == nil || got.Code != "cached" || got.InstallHeight != 7 {
		t.Fatalf("cache hit mismatch: %+v", got)
	}
}

func TestGetSCCode_CacheMissBackfill(t *testing.T) {
	idx := newTestIndexer(t)
	scid := "backfill-scid"

	var calls atomic.Int64
	prev := setSCCodeFetcher(func(i *Indexer, s string) (string, int64, error) {
		calls.Add(1)
		if s != scid {
			t.Errorf("fetcher called with %q, want %q", s, scid)
		}
		return "backfilled-code", i.ChainHeight.Load(), nil
	})
	t.Cleanup(func() { setSCCodeFetcher(prev) })

	got, err := idx.GetSCCode(scid)
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	if got == nil || got.Code != "backfilled-code" || got.InstallHeight != 9999 {
		t.Fatalf("backfill entry wrong: %+v", got)
	}

	// Second call must hit the bucket, not the fetcher.
	got2, err := idx.GetSCCode(scid)
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if got2 == nil || got2.Code != "backfilled-code" {
		t.Fatalf("second call bucket miss: %+v", got2)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("fetcher called %d times, want 1 (second call should be bucket hit)", n)
	}
}

func TestGetSCCode_UnknownSCID(t *testing.T) {
	idx := newTestIndexer(t)
	prev := setSCCodeFetcher(func(_ *Indexer, _ string) (string, int64, error) {
		// Daemon signals "doesn't exist" with empty code + nil err.
		return "", 0, nil
	})
	t.Cleanup(func() { setSCCodeFetcher(prev) })

	got, err := idx.GetSCCode("nonexistent")
	if err != nil {
		t.Fatalf("unknown scid returned err: %v", err)
	}
	if got != nil {
		t.Fatalf("unknown scid should return (nil,nil), got %+v", got)
	}
}

// TestGetSCCode_SingleFlight confirms that N concurrent misses against the
// same SCID trigger exactly 1 fetcher call — the single-flight guarantee.
// Uses a blocking fetcher so the goroutines pile up on the sync.Once.
func TestGetSCCode_SingleFlight(t *testing.T) {
	idx := newTestIndexer(t)
	scid := "herd-scid"

	const concurrency = 64
	var calls atomic.Int64
	release := make(chan struct{})
	prev := setSCCodeFetcher(func(i *Indexer, _ string) (string, int64, error) {
		calls.Add(1)
		<-release // block until test releases
		return "herd-code", i.ChainHeight.Load(), nil
	})
	t.Cleanup(func() { setSCCodeFetcher(prev) })

	var wg sync.WaitGroup
	results := make([]*structures.SCCodeEntry, concurrency)
	errs := make([]error, concurrency)
	ready := make(chan struct{})
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func(i int) {
			defer wg.Done()
			<-ready // line up so all goroutines enter GetSCCode roughly together
			results[i], errs[i] = idx.GetSCCode(scid)
		}(i)
	}
	close(ready)
	// Small yield to let goroutines converge on the single-flight before release.
	for i := 0; i < 100 && calls.Load() == 0; i++ {
		// spin — the first goroutine should enter the fetcher almost immediately
	}
	close(release)
	wg.Wait()

	for i := 0; i < concurrency; i++ {
		if errs[i] != nil {
			t.Errorf("goroutine %d: err %v", i, errs[i])
		}
		if results[i] == nil || results[i].Code != "herd-code" {
			t.Errorf("goroutine %d: result %+v", i, results[i])
		}
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("single-flight violated: fetcher called %d times for %d goroutines on same scid", n, concurrency)
	}
}

func TestGetSCCode_EmptyScidError(t *testing.T) {
	idx := newTestIndexer(t)
	_, err := idx.GetSCCode("")
	if err == nil {
		t.Fatal("expected error on empty scid")
	}
}
