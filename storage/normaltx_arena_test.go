package storage

import (
	"fmt"
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// normTxKey identifies a NormalTx record uniquely within an addr for set
// comparison on readback (the composite bucket key is addr+height+txid+scid).
type normTxKey struct {
	height int64
	txid   string
	scid   string
}

func normTxKeyOf(r *structures.NormalTXWithSCIDParse) normTxKey {
	return normTxKey{height: r.Height, txid: r.Txid, scid: r.Scid}
}

// TestNormalTxArena_WithinBatchRealloc forces the batch-owned normalTxArena to
// reallocate multiple times inside a SINGLE batch while re-touching addrs that
// received records BEFORE the first realloc. AddNormalTx carves each record
// from the arena and publishes &arena[len-1] into NormalTxs[addr]; if a realloc
// ever invalidated an already-published pointer (e.g. by re-indexing the arena),
// the early records' Height/Txid/Scid would corrupt and their composite bucket
// keys would drift. The test reads every record back via GetNormalTxWithSCIDByAddr
// and asserts the exact per-addr multiset. Green before AND after the arena
// change (a behavior gate). -race cannot catch this — the bug is single-threaded
// pointer invalidation, not a data race.
//
// newEmptyBatch seeds normalTxArena with cap 512, so >512 distinct records force
// >=1 realloc; the re-touch phase adds more after the realloc so an early addr's
// slice holds both pre- and post-realloc pointers at once.
func TestNormalTxArena_WithinBatchRealloc(t *testing.T) {
	store := openTestStore(t)

	const nAddrs = 400
	const perAddr = 2 // 800 records total -> forces realloc past cap 512
	const early = 40

	addrOf := func(i int) string { return fmt.Sprintf("deroaddr%056d", i) }
	scidOf := func(i int) string { return fmt.Sprintf("%064x", i) }
	txidOf := func(a, n int) string { return fmt.Sprintf("%032x%032x", a, n) }

	exp := make(map[string]map[normTxKey]*structures.NormalTXWithSCIDParse, nAddrs)
	batch := newEmptyBatch()

	touch := func(addrIdx int, height int64, n int) {
		addr := addrOf(addrIdx)
		rec := &structures.NormalTXWithSCIDParse{
			Txid:   txidOf(addrIdx, n),
			Scid:   scidOf(addrIdx),
			Fees:   uint64(100 + addrIdx),
			Height: height,
		}
		batch.AddNormalTx(addr, rec.Txid, rec.Scid, rec.Fees, rec.Height)
		if exp[addr] == nil {
			exp[addr] = make(map[normTxKey]*structures.NormalTXWithSCIDParse)
		}
		exp[addr][normTxKeyOf(rec)] = rec
	}

	// Phase 1: perAddr records for each addr. Addrs 0..early receive theirs while
	// the arena still holds its initial backing array (before any realloc).
	for i := 0; i < nAddrs; i++ {
		for n := 0; n < perAddr; n++ {
			touch(i, int64(1_000_000+i*10+n), n)
		}
		// Once well past the first realloc, re-touch the early addrs mid-stream
		// with new records (new arena slots in the post-realloc backing array,
		// appended to slices that already hold pre-realloc pointers).
		if i == 300 {
			for j := 0; j < early; j++ {
				touch(j, int64(7_000_000+j), perAddr+0)
			}
		}
	}

	// In-memory arena-stability check: every published pointer still holds its
	// original fields (catches realloc corruption independent of storage keying).
	for addr, want := range exp {
		got := batch.NormalTxs[addr]
		if len(got) != len(want) {
			t.Fatalf("in-mem count for %s: got %d want %d", addr, len(got), len(want))
		}
		for _, p := range got {
			w := want[normTxKeyOf(p)]
			if w == nil {
				t.Fatalf("in-mem stray record %+v for %s", *p, addr)
			}
			if p.Fees != w.Fees {
				t.Fatalf("in-mem fees drift for %s @%d: got %d want %d", addr, p.Height, p.Fees, w.Fees)
			}
		}
	}

	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}

	// End-to-end: read every addr back and assert the exact record multiset.
	for addr, want := range exp {
		recs, err := store.GetNormalTxWithSCIDByAddr(addr)
		if err != nil {
			t.Fatalf("GetNormalTxWithSCIDByAddr(%s): %v", addr, err)
		}
		if len(recs) != len(want) {
			t.Fatalf("readback count for %s: got %d want %d", addr, len(recs), len(want))
		}
		for _, r := range recs {
			w := want[normTxKeyOf(r)]
			if w == nil {
				t.Fatalf("readback stray record %+v for %s", *r, addr)
			}
			if r.Fees != w.Fees || r.Scid != w.Scid || r.Txid != w.Txid || r.Height != w.Height {
				t.Fatalf("readback drift for %s: got %+v want %+v", addr, *r, *w)
			}
		}
	}
}

// TestNormalTxArena_CrossBatchReuse exercises arena slot reuse across a Reset
// boundary on the SAME batch object: batch-1 records are flushed, the batch is
// Reset (arena truncated to [:0], NormalTxs cleared), then batch-2 reuses the
// same backing array for new records while an addr from batch-1 receives fresh
// records too. Batch-1 records must persist, batch-2 records must be exact, and
// the arena reuse must not resurrect stale batch-1 pointers.
func TestNormalTxArena_CrossBatchReuse(t *testing.T) {
	store := openTestStore(t)

	addr := "deroaddrcrossbatch000000000000000000000000000000000000000000000"
	scid := fmt.Sprintf("%064x", 1)

	batch := newEmptyBatch()

	// Batch 1: two records.
	batch.AddNormalTx(addr, fmt.Sprintf("%064x", 11), scid, 111, 100)
	batch.AddNormalTx(addr, fmt.Sprintf("%064x", 12), scid, 112, 200)
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("flush b1: %v", err)
	}

	// Reset reuses the arena backing array and clears NormalTxs.
	batch.Reset()

	// Batch 2: a fresh record for the same addr (arena slot 0 reused).
	batch.AddNormalTx(addr, fmt.Sprintf("%064x", 21), scid, 221, 300)
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("flush b2: %v", err)
	}

	recs, err := store.GetNormalTxWithSCIDByAddr(addr)
	if err != nil {
		t.Fatalf("GetNormalTxWithSCIDByAddr: %v", err)
	}
	// Expect all three records (b1's two + b2's one) — heights 100, 200, 300.
	wantHeights := map[int64]uint64{100: 111, 200: 112, 300: 221}
	if len(recs) != len(wantHeights) {
		t.Fatalf("cross-batch count: got %d want %d", len(recs), len(wantHeights))
	}
	for _, r := range recs {
		wantFees, ok := wantHeights[r.Height]
		if !ok {
			t.Fatalf("cross-batch stray record %+v", *r)
		}
		if r.Fees != wantFees {
			t.Fatalf("cross-batch fees drift @%d: got %d want %d", r.Height, r.Fees, wantFees)
		}
	}
}
