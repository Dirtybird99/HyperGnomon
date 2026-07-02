package storage

import (
	"fmt"
	"testing"

	bolt "go.etcd.io/bbolt"

	"github.com/hypergnomon/hypergnomon/structures"
)

// TestFlushBatch_LatestVarHeightPerScidRun pins the semantics behind the
// once-per-scid-run latest-height upsert: a multi-height batch must land the
// run's max in bucketScVarsLatest, and a later flush with a LOWER height must
// not regress it (putLatestSCVarsHeight's existing>=height guard).
func TestFlushBatch_LatestVarHeightPerScidRun(t *testing.T) {
	store := openTestStore(t)
	scid := fmt.Sprintf("%064x", 7)
	vars := []*structures.SCIDVariable{{Key: "k", Value: "v"}}

	batch := NewWriteBatch()
	for _, h := range []int64{100, 300, 200} { // AddVariables order is irrelevant; the flush sorts
		batch.AddVariables(scid, h, vars)
	}
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("flush 1: %v", err)
	}
	latest := func() int64 {
		var got int64
		if err := store.DB.View(func(tx *bolt.Tx) error {
			got = latestSCVarsHeightTx(tx, scid)
			return nil
		}); err != nil {
			t.Fatalf("view: %v", err)
		}
		return got
	}
	if got := latest(); got != 300 {
		t.Fatalf("latest after multi-height flush: got %d want 300", got)
	}

	// A later flush at a lower height must not regress the stored latest.
	batch2 := NewWriteBatch()
	batch2.AddVariables(scid, 250, vars)
	if err := store.FlushBatch(batch2); err != nil {
		t.Fatalf("flush 2: %v", err)
	}
	if got := latest(); got != 300 {
		t.Fatalf("latest regressed by lower-height flush: got %d want 300", got)
	}
	PutWriteBatch(batch)
	PutWriteBatch(batch2)
}

// BenchmarkFlushBatch_VarMultiHeight covers the fixture gap that hid the
// latest-height write amplification: VarSnapshotBurst uses ONE height per
// scid, so a flush that upserts bucketScVarsLatest once per (scid, height)
// instead of once per scid looks identical there. This fixture packs many
// ascending heights per scid — the shape of segment replay and multi-block
// batches for active contracts — where the sorted flush order makes
// putLatestSCVarsHeight's existing>=height skip never fire.
func BenchmarkFlushBatch_VarMultiHeight(b *testing.B) {
	const nSCIDs = 64
	const nHeights = 16 // 1024 snapshots per flush, 16 per scid run

	scids := make([]string, nSCIDs)
	for i := range scids {
		scids[i] = fmt.Sprintf("%016x%048d", i*2654435761, i)
	}
	mkVars := func(scid string, h int64) []*structures.SCIDVariable {
		return []*structures.SCIDVariable{
			{Key: "var_header_name", Value: "App " + scid[:8]},
			{Key: "likes", Value: uint64(h)},
			{Key: "dURL", Value: scid[:12] + ".tela"},
		}
	}

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		b.StopTimer()
		store := newTestStore(b)
		batch := NewWriteBatch()
		for _, scid := range scids {
			for h := int64(0); h < nHeights; h++ {
				batch.AddVariables(scid, 7_180_000+h, mkVars(scid, h))
			}
		}
		b.StartTimer()
		if err := store.FlushBatch(batch); err != nil {
			b.Fatalf("flush: %v", err)
		}
		b.StopTimer()
		PutWriteBatch(batch)
		_ = store.Close()
		b.StartTimer()
	}
	b.ReportMetric(float64(nSCIDs*nHeights), "snapshots")
}
