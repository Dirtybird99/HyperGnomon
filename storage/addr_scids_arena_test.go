package storage

import (
	"fmt"
	"testing"
)

// scidForIdx returns a deterministic, distinct 64-hex-char SCID for index i.
func scidForIdx(i int) string { return fmt.Sprintf("%064x", i) }

// expAddrSCID mirrors the persisted addr_scids semantics (min/max/sum) so the
// arena tests can simulate expected state in lockstep with every AddAddrSCID
// call, then assert it against GetAddressSCIDs exactly.
type expAddrSCID struct {
	first, last int64
	count       int64
}

func applyExp(exp map[string]*expAddrSCID, scid string, h int64) {
	e := exp[scid]
	if e == nil {
		exp[scid] = &expAddrSCID{first: h, last: h, count: 1}
		return
	}
	if h < e.first {
		e.first = h
	}
	if h > e.last {
		e.last = h
	}
	e.count++
}

func assertAddrSCIDs(t *testing.T, store *BboltStore, addr string, exp map[string]*expAddrSCID) {
	t.Helper()
	got, err := store.GetAddressSCIDs(addr)
	if err != nil {
		t.Fatalf("GetAddressSCIDs: %v", err)
	}
	if len(got) != len(exp) {
		t.Fatalf("entry count: got %d want %d", len(got), len(exp))
	}
	for scid, want := range exp {
		g := got[scid]
		if g == nil {
			t.Fatalf("missing entry for scid %s", scid)
		}
		if g.FirstHeight != want.first || g.LastHeight != want.last || g.Count != want.count {
			t.Fatalf("scid %s: got {%d %d %d} want {%d %d %d}",
				scid, g.FirstHeight, g.LastHeight, g.Count,
				want.first, want.last, want.count)
		}
	}
}

// TestAddrSCIDs_ArenaWithinBatchRealloc forces the batch-owned AddrSCIDEntry
// arena to reallocate multiple times inside a SINGLE batch while re-touching
// pairs that were inserted BEFORE the first realloc. If AddAddrSCID ever
// re-indexed the arena (instead of mutating through the canonical map pointer),
// the early pairs' min/max/Count would be corrupted by a realloc. The test
// reads every pair back via GetAddressSCIDs and asserts exact values.
//
// newEmptyBatch seeds addrSCIDArena with cap 1024, so adding 3000 distinct
// pairs guarantees >=2 reallocs. The test bypasses the pool via newEmptyBatch
// for determinism. (Also passes on the pre-change code where each entry is its
// own heap alloc — it is a behavior gate, green before and after.)
func TestAddrSCIDs_ArenaWithinBatchRealloc(t *testing.T) {
	store := openTestStore(t)
	const addr = "dero1qarenawithinbatch"
	const n = 3000
	const early = 200

	exp := make(map[string]*expAddrSCID, n)
	batch := newEmptyBatch()
	batch.LastHeight = 1

	touch := func(scid string, h int64) {
		batch.AddAddrSCID(addr, scid, h)
		applyExp(exp, scid, h)
	}

	// Phase 1: insert n distinct pairs. Pairs 0..early are inserted while the
	// arena still holds its initial backing array (before any realloc).
	for i := 0; i < n; i++ {
		touch(scidForIdx(i), int64(1_000_000+i))
		// Interleave: once well past the first realloc, re-touch the early
		// pairs mid-stream with a very low height (drives FirstHeight down).
		if i == 1500 {
			for j := 0; j < early; j++ {
				touch(scidForIdx(j), int64(7))
			}
		}
	}

	// Phase 2: after all reallocs, re-touch the early pairs again with a high
	// height (drives LastHeight up and Count further) through the map pointer.
	for j := 0; j < early; j++ {
		touch(scidForIdx(j), int64(50_000_000+j))
	}

	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}
	assertAddrSCIDs(t, store, addr, exp)
}

// TestAddrSCIDs_ArenaCrossBatchReuse exercises arena slot reuse across a Reset
// boundary on the SAME batch object: batch-1 pairs are flushed, the batch is
// Reset (arena truncated to [:0], AddrSCIDs cleared), then batch-2 reuses the
// same backing array for new pairs while re-touching some batch-1 pairs with
// new heights. Untouched batch-1 pairs must stay unchanged, re-touched ones
// must merge (min/max/sum) across batches, and new batch-2 pairs must be exact.
func TestAddrSCIDs_ArenaCrossBatchReuse(t *testing.T) {
	store := openTestStore(t)
	const addr = "dero1qarenacrossbatch"

	exp := make(map[string]*expAddrSCID)
	batch := newEmptyBatch()

	touch := func(scid string, h int64) {
		batch.AddAddrSCID(addr, scid, h)
		applyExp(exp, scid, h)
	}

	keep := scidForIdx(1)
	touched := scidForIdx(2)

	// Batch 1.
	batch.LastHeight = 200
	touch(keep, 100)
	touch(touched, 200)
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("flush b1: %v", err)
	}

	// Reset reuses the arena backing array for the next batch and clears the
	// map (dropping all batch-1 pointers).
	batch.Reset()

	// Batch 2: re-touch batch-1 'touched' with new extreme heights, add a
	// fresh batch-2 pair. 'keep' is left untouched.
	p2 := scidForIdx(3)
	batch.LastHeight = 400
	touch(touched, 50)  // drives FirstHeight down across batches
	touch(touched, 300) // drives LastHeight up across batches
	touch(p2, 400)
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("flush b2: %v", err)
	}
	assertAddrSCIDs(t, store, addr, exp)
}
