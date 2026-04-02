package storage

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// --- helpers ---

// randHex returns a random hex string of the given byte length (output is 2*n chars).
func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// fakeSCID returns a realistic 64-char hex SCID.
func fakeSCID() string { return randHex(32) }

// fakeAddr returns a DERO-style address (dero1q prefix + 60 hex chars).
func fakeAddr() string { return "dero1q" + randHex(30) }

// fakeTxid returns a 64-char hex transaction ID.
func fakeTxid() string { return randHex(32) }

// openTestStore creates a BboltStore in a temp directory.
func openTestStore(tb testing.TB) *BboltStore {
	tb.Helper()
	store, err := NewBboltStore(tb.TempDir(), "")
	if err != nil {
		tb.Fatalf("NewBboltStore: %v", err)
	}
	tb.Cleanup(func() { store.Close() })
	return store
}

// makeBatch builds a WriteBatch with the given number of owners, invocations,
// and variable snapshots spread across distinct SCIDs.
func makeBatch(nOwners, nInvocations, nVarSnapshots int) *WriteBatch {
	batch := NewWriteBatch()

	// Pre-generate SCIDs so invocations and variables can reference real ones.
	scids := make([]string, nOwners)
	for i := range scids {
		scids[i] = fakeSCID()
		batch.AddOwner(scids[i], fakeAddr())
	}

	for i := 0; i < nInvocations; i++ {
		scid := scids[i%len(scids)]
		txid := fakeTxid()
		batch.AddInvocation(structures.InvokeRecord{
			Scid:       scid,
			Sender:     fakeAddr(),
			Entrypoint: "Transfer",
			Height:     int64(1000 + i),
			Details: &structures.SCTXParse{
				Txid:       txid,
				Scid:       scid,
				Entrypoint: "Transfer",
				Method:     structures.MethodInvokeSC,
				Sender:     fakeAddr(),
				Fees:       uint64(100 + i),
				Height:     int64(1000 + i),
			},
		})
	}

	for i := 0; i < nVarSnapshots; i++ {
		scid := scids[i%len(scids)]
		height := int64(2000 + i)
		vars := []*structures.SCIDVariable{
			{Key: "balance", Value: uint64(i * 1000)},
			{Key: "owner", Value: fakeAddr()},
		}
		batch.AddVariables(scid, height, vars)
		batch.AddInteractionHeight(scid, height)
	}

	batch.LastHeight = int64(2000 + nVarSnapshots)
	batch.RegTxCount = int64(nOwners)
	batch.BurnTxCount = int64(nInvocations / 10)
	batch.NormTxCount = int64(nInvocations)

	return batch
}

// --- functional tests ---

func TestBboltStore_BasicOps(t *testing.T) {
	store := openTestStore(t)

	// Default last index height should be 0.
	h, err := store.GetLastIndexHeight()
	if err != nil {
		t.Fatalf("GetLastIndexHeight (initial): %v", err)
	}
	if h != 0 {
		t.Fatalf("expected initial height 0, got %d", h)
	}

	// Store and retrieve last index height.
	if err := store.StoreLastIndexHeight(42000); err != nil {
		t.Fatalf("StoreLastIndexHeight: %v", err)
	}
	h, err = store.GetLastIndexHeight()
	if err != nil {
		t.Fatalf("GetLastIndexHeight: %v", err)
	}
	if h != 42000 {
		t.Fatalf("expected height 42000, got %d", h)
	}

	// Store and retrieve owner.
	scid := fakeSCID()
	owner := fakeAddr()
	if err := store.StoreOwner(scid, owner); err != nil {
		t.Fatalf("StoreOwner: %v", err)
	}
	got, err := store.GetOwner(scid)
	if err != nil {
		t.Fatalf("GetOwner: %v", err)
	}
	if got != owner {
		t.Fatalf("owner mismatch: got %q, want %q", got, owner)
	}

	// Non-existent SCID returns empty string, no error.
	got, err = store.GetOwner(fakeSCID())
	if err != nil {
		t.Fatalf("GetOwner (missing): %v", err)
	}
	if got != "" {
		t.Fatalf("expected empty owner for missing SCID, got %q", got)
	}

	// GetAllSCIDs should contain our SCID.
	scids, err := store.GetAllSCIDs()
	if err != nil {
		t.Fatalf("GetAllSCIDs: %v", err)
	}
	found := false
	for _, s := range scids {
		if s == scid {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("GetAllSCIDs did not contain %s", scid)
	}
}

func TestBboltStore_FlushBatch(t *testing.T) {
	store := openTestStore(t)

	const (
		numOwners      = 100
		numInvocations = 500
		numVarSnaps    = 200
	)

	batch := makeBatch(numOwners, numInvocations, numVarSnaps)

	// Capture SCIDs before flush (map iteration order is random, so snapshot).
	ownerSnapshot := make(map[string]string, len(batch.Owners))
	for k, v := range batch.Owners {
		ownerSnapshot[k] = v
	}

	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}

	// Verify owner count.
	allOwners, err := store.GetAllOwnersAndSCIDs()
	if err != nil {
		t.Fatalf("GetAllOwnersAndSCIDs: %v", err)
	}
	if len(allOwners) != numOwners {
		t.Fatalf("expected %d owners, got %d", numOwners, len(allOwners))
	}

	// Spot-check every owner.
	for scid, wantOwner := range ownerSnapshot {
		gotOwner, err := store.GetOwner(scid)
		if err != nil {
			t.Fatalf("GetOwner(%s): %v", scid[:16], err)
		}
		if gotOwner != wantOwner {
			t.Fatalf("owner mismatch for %s: got %q, want %q", scid[:16], gotOwner, wantOwner)
		}
	}

	// Verify invocation count for a sample SCID.
	// Each SCID gets roughly numInvocations/numOwners invocations.
	sampleSCID := batch.Invocations[0].Scid
	invokes, err := store.GetInvokeDetailsBySCID(sampleSCID)
	if err != nil {
		t.Fatalf("GetInvokeDetailsBySCID: %v", err)
	}
	if len(invokes) == 0 {
		t.Fatalf("expected invocations for SCID %s, got none", sampleSCID[:16])
	}

	// Verify variable snapshots.
	for scid, heightMap := range batch.Variables {
		for height, wantVars := range heightMap {
			gotVars, err := store.GetSCIDVariableDetailsAtHeight(scid, height)
			if err != nil {
				t.Fatalf("GetSCIDVariableDetailsAtHeight(%s, %d): %v", scid[:16], height, err)
			}
			if len(gotVars) != len(wantVars) {
				t.Fatalf("var count mismatch at %s:%d: got %d, want %d",
					scid[:16], height, len(gotVars), len(wantVars))
			}
		}
	}

	// Verify interaction heights.
	for scid, wantHeights := range batch.Heights {
		gotHeights, err := store.GetSCIDInteractionHeights(scid)
		if err != nil {
			t.Fatalf("GetSCIDInteractionHeights(%s): %v", scid[:16], err)
		}
		if len(gotHeights) != len(wantHeights) {
			t.Fatalf("height count mismatch for %s: got %d, want %d",
				scid[:16], len(gotHeights), len(wantHeights))
		}
	}

	// Verify TX counts.
	reg, burn, norm, err := store.GetTxCounts()
	if err != nil {
		t.Fatalf("GetTxCounts: %v", err)
	}
	if reg != batch.RegTxCount || burn != batch.BurnTxCount || norm != batch.NormTxCount {
		t.Fatalf("tx counts mismatch: got (%d,%d,%d), want (%d,%d,%d)",
			reg, burn, norm, batch.RegTxCount, batch.BurnTxCount, batch.NormTxCount)
	}

	// Verify last indexed height.
	h, err := store.GetLastIndexHeight()
	if err != nil {
		t.Fatalf("GetLastIndexHeight: %v", err)
	}
	if h != batch.LastHeight {
		t.Fatalf("last height mismatch: got %d, want %d", h, batch.LastHeight)
	}
}

func TestBboltStore_BatchReset(t *testing.T) {
	batch := makeBatch(50, 200, 100)

	if len(batch.Owners) == 0 || len(batch.Invocations) == 0 || len(batch.Variables) == 0 {
		t.Fatal("batch was not populated before reset")
	}

	batch.Reset()

	if len(batch.Owners) != 0 {
		t.Fatalf("Owners not cleared: %d entries remain", len(batch.Owners))
	}
	if len(batch.Invocations) != 0 {
		t.Fatalf("Invocations not cleared: %d entries remain", len(batch.Invocations))
	}
	if len(batch.Variables) != 0 {
		t.Fatalf("Variables not cleared: %d entries remain", len(batch.Variables))
	}
	if len(batch.Heights) != 0 {
		t.Fatalf("Heights not cleared: %d entries remain", len(batch.Heights))
	}
	if batch.RegTxCount != 0 || batch.BurnTxCount != 0 || batch.NormTxCount != 0 {
		t.Fatalf("TX counts not cleared: (%d, %d, %d)",
			batch.RegTxCount, batch.BurnTxCount, batch.NormTxCount)
	}
	if batch.LastHeight != 0 {
		t.Fatalf("LastHeight not cleared: %d", batch.LastHeight)
	}

	// Reuse: add new data after reset.
	scid := fakeSCID()
	batch.AddOwner(scid, fakeAddr())
	if len(batch.Owners) != 1 {
		t.Fatalf("batch not reusable after reset: expected 1 owner, got %d", len(batch.Owners))
	}

	// Flush the reused batch to a real store.
	store := openTestStore(t)
	txid := fakeTxid()
	batch.AddInvocation(structures.InvokeRecord{
		Scid:       scid,
		Sender:     fakeAddr(),
		Entrypoint: "Initialize",
		Height:     5000,
		Details: &structures.SCTXParse{
			Txid:       txid,
			Scid:       scid,
			Entrypoint: "Initialize",
			Method:     structures.MethodInstallSC,
			Sender:     fakeAddr(),
			Fees:       200,
			Height:     5000,
		},
	})
	batch.LastHeight = 5000

	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch after reset+reuse: %v", err)
	}
	h, err := store.GetLastIndexHeight()
	if err != nil {
		t.Fatalf("GetLastIndexHeight after reuse: %v", err)
	}
	if h != 5000 {
		t.Fatalf("expected height 5000 after reuse, got %d", h)
	}
}

// --- benchmarks ---
// The key comparison: FlushBatch (one transaction for N records) vs
// individual StoreOwner calls (N transactions for N records).
// This proves the arena-style batch write optimization.

func benchmarkFlushBatch(b *testing.B, n int) {
	store := openTestStore(b)

	// Pre-build batch data outside the timer.
	scids := make([]string, n)
	addrs := make([]string, n)
	txids := make([]string, n)
	for i := 0; i < n; i++ {
		scids[i] = fakeSCID()
		addrs[i] = fakeAddr()
		txids[i] = fakeTxid()
	}

	batch := NewWriteBatch()

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		batch.Reset()

		for i := 0; i < n; i++ {
			batch.AddOwner(scids[i], addrs[i])
			batch.AddInvocation(structures.InvokeRecord{
				Scid:       scids[i],
				Sender:     addrs[i],
				Entrypoint: "Transfer",
				Height:     int64(i),
				Details: &structures.SCTXParse{
					Txid:       txids[i],
					Scid:       scids[i],
					Entrypoint: "Transfer",
					Method:     structures.MethodInvokeSC,
					Sender:     addrs[i],
					Fees:       uint64(100 + i),
					Height:     int64(i),
				},
			})
			batch.AddVariables(scids[i], int64(i), []*structures.SCIDVariable{
				{Key: "balance", Value: uint64(i * 1000)},
			})
			batch.AddInteractionHeight(scids[i], int64(i))
		}

		batch.LastHeight = int64(n)
		batch.RegTxCount = int64(n)

		if err := store.FlushBatch(batch); err != nil {
			b.Fatalf("FlushBatch: %v", err)
		}
	}

	b.ReportMetric(float64(n), "records/flush")
}

func BenchmarkFlushBatch_100(b *testing.B)   { benchmarkFlushBatch(b, 100) }
func BenchmarkFlushBatch_1000(b *testing.B)  { benchmarkFlushBatch(b, 1000) }
func BenchmarkFlushBatch_10000(b *testing.B) { benchmarkFlushBatch(b, 10000) }

// BenchmarkIndividualWrites does the same work as BenchmarkFlushBatch but uses
// per-record StoreOwner + StoreInvokeDetails + StoreSCIDVariableDetails calls.
// Each call opens its own BoltDB transaction. Compare against FlushBatch to
// measure the batch optimization speedup.
func BenchmarkIndividualWrites(b *testing.B) {
	const n = 100 // Use 100 to keep wall-clock reasonable; compare vs FlushBatch_100.

	store := openTestStore(b)

	scids := make([]string, n)
	addrs := make([]string, n)
	txids := make([]string, n)
	for i := 0; i < n; i++ {
		scids[i] = fakeSCID()
		addrs[i] = fakeAddr()
		txids[i] = fakeTxid()
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		for i := 0; i < n; i++ {
			if err := store.StoreOwner(scids[i], addrs[i]); err != nil {
				b.Fatalf("StoreOwner: %v", err)
			}

			detail := &structures.SCTXParse{
				Txid:       txids[i],
				Scid:       scids[i],
				Entrypoint: "Transfer",
				Method:     structures.MethodInvokeSC,
				Sender:     addrs[i],
				Fees:       uint64(100 + i),
				Height:     int64(i),
			}
			if err := store.StoreInvokeDetails(scids[i], addrs[i], "Transfer", int64(i), detail); err != nil {
				b.Fatalf("StoreInvokeDetails: %v", err)
			}

			vars := []*structures.SCIDVariable{
				{Key: "balance", Value: uint64(i * 1000)},
			}
			if err := store.StoreSCIDVariableDetails(scids[i], vars, int64(i)); err != nil {
				b.Fatalf("StoreSCIDVariableDetails: %v", err)
			}

			if err := store.StoreSCIDInteractionHeight(scids[i], int64(i)); err != nil {
				b.Fatalf("StoreSCIDInteractionHeight: %v", err)
			}
		}

		if err := store.StoreLastIndexHeight(int64(n)); err != nil {
			b.Fatalf("StoreLastIndexHeight: %v", err)
		}
	}

	b.ReportMetric(float64(n), "records/iter")
}

// BenchmarkFlushBatch_Scaling runs all batch sizes as sub-benchmarks for
// convenient comparison with `go test -bench=Scaling -benchmem`.
func BenchmarkFlushBatch_Scaling(b *testing.B) {
	for _, size := range []int{100, 500, 1000, 5000, 10000} {
		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			benchmarkFlushBatch(b, size)
		})
	}
}
