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

// TestNormalTxArena_WithinBatchRealloc grows the flat NormalTxs value slice past
// its initial cap inside a SINGLE batch while re-touching addrs that received
// records before the grow. NormalTxs holds (addr, record) VALUES (no pointer
// arena), so a slice realloc copies entries and cannot corrupt earlier ones —
// this guards that every appended record survives the grow with exact fields and
// reads back correctly via GetNormalTxWithSCIDByAddr (a behavior gate, green
// before and after the flat-slice change).
//
// newEmptyBatch seeds NormalTxs with cap 512, so >512 records force >=1 realloc;
// the re-touch phase adds more after the realloc.
func TestNormalTxArena_WithinBatchRealloc(t *testing.T) {
	store := openTestStore(t)

	const nAddrs = 400
	const perAddr = 2 // 800 records total -> forces realloc past cap 512
	const early = 40

	addrOf := func(i int) string { return fmt.Sprintf("deroaddr%056d", i) }
	txidOf := func(a, n int) string { return fmt.Sprintf("%032x%032x", a, n) }

	exp := make(map[string]map[normTxKey]*structures.NormalTXWithSCIDParse, nAddrs)
	batch := newEmptyBatch()

	touch := func(addrIdx int, height int64, n int) {
		addr := addrOf(addrIdx)
		rec := &structures.NormalTXWithSCIDParse{
			Txid:   txidOf(addrIdx, n),
			Scid:   scidForIdx(addrIdx),
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

	// In-memory check: every appended (addr, record) survives the slice grow with
	// its exact fields (values, not pointers — a realloc copies them).
	inMem := make(map[string]int, len(exp))
	for i := range batch.NormalTxs {
		e := &batch.NormalTxs[i]
		w := exp[e.Addr][normTxKeyOf(&e.Tx)]
		if w == nil {
			t.Fatalf("in-mem stray record %+v for %s", e.Tx, e.Addr)
		}
		if e.Tx.Fees != w.Fees {
			t.Fatalf("in-mem fees drift for %s @%d: got %d want %d", e.Addr, e.Tx.Height, e.Tx.Fees, w.Fees)
		}
		inMem[e.Addr]++
	}
	for addr, want := range exp {
		if inMem[addr] != len(want) {
			t.Fatalf("in-mem count for %s: got %d want %d", addr, inMem[addr], len(want))
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
	scid := scidForIdx(1)

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
