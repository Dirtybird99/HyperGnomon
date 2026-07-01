package storage

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"

	"github.com/hypergnomon/hypergnomon/structures"
)

// BenchmarkFlushBatch_HeightsAccumulation measures the cost of flushing a
// small height delta against a SCID that already has N accumulated
// interaction heights. The legacy layout (one msgpack blob per SCID) made
// this O(N) per flush — decode the full history, append, re-encode, rewrite —
// which is quadratic over the life of an active contract during initial sync.
// The composite-key layout must stay flat as N grows.
func BenchmarkFlushBatch_HeightsAccumulation(b *testing.B) {
	for _, n := range []int{1000, 10000, 100000} {
		b.Run(fmt.Sprintf("preseeded=%d", n), func(b *testing.B) {
			store := newTestStore(b)
			scid := benchHistScid

			// Pre-seed N heights in big chunks.
			const chunk = 1000
			for at := 0; at < n; at += chunk {
				batch := NewWriteBatch()
				for i := 0; i < chunk && at+i < n; i++ {
					batch.AddInteractionHeight(scid, int64(at+i))
				}
				if err := store.FlushBatch(batch); err != nil {
					b.Fatalf("preseed flush: %v", err)
				}
				PutWriteBatch(batch)
			}

			next := int64(n)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				batch := NewWriteBatch()
				for i := int64(0); i < 100; i++ {
					batch.AddInteractionHeight(scid, next+i)
				}
				if err := store.FlushBatch(batch); err != nil {
					b.Fatalf("flush: %v", err)
				}
				PutWriteBatch(batch)
				next += 100
			}
		})
	}
}

// BenchmarkFlushBatch_NormalTxAccumulation is the same shape for the
// per-address normal-TX history.
func BenchmarkFlushBatch_NormalTxAccumulation(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprintf("preseeded=%d", n), func(b *testing.B) {
			store := newTestStore(b)
			addr := "dero1qyjjxxaabbccddeeff0011223344556677889900aabbccddee00112233445566"

			const chunk = 1000
			for at := 0; at < n; at += chunk {
				batch := NewWriteBatch()
				for i := 0; i < chunk && at+i < n; i++ {
					h := int64(at + i)
					batch.AddNormalTx(addr, fmt.Sprintf("%064d", h), benchHistScid, 100, h)
				}
				if err := store.FlushBatch(batch); err != nil {
					b.Fatalf("preseed flush: %v", err)
				}
				PutWriteBatch(batch)
			}

			next := int64(n)
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				batch := NewWriteBatch()
				for i := int64(0); i < 10; i++ {
					h := next + i
					batch.AddNormalTx(addr, fmt.Sprintf("%064d", h), benchHistScid, 100, h)
				}
				if err := store.FlushBatch(batch); err != nil {
					b.Fatalf("flush: %v", err)
				}
				PutWriteBatch(batch)
				next += 10
			}
		})
	}
}

const benchHistScid = "a05395bb0cf77adc850928b0db00eb5ca7a9ccbafd9a38d021c8d299ad5ce1a4"

// BenchmarkFlushBatch_VarSnapshotBurst mirrors the classify-probe var_flush:
// one transaction writing variable snapshots + class metadata for thousands
// of SCIDs at once. Map iteration hands bbolt keys in random order, which
// maximizes B+tree page splits; the flush path must keep this fast since it
// sits on the fastsync readiness critical path (measured 4.1-4.3s for ~13k
// snapshots on mainnet).
func BenchmarkFlushBatch_VarSnapshotBurst(b *testing.B) {
	const nSCIDs = 2000
	scids := make([]string, nSCIDs)
	for i := range scids {
		scids[i] = fmt.Sprintf("%016x%048d", i*2654435761, i)
	}
	mkVars := func(scid string) []*structures.SCIDVariable {
		return []*structures.SCIDVariable{
			{Key: "var_header_name", Value: "App " + scid[:8]},
			{Key: "var_header_description", Value: "A snapshot fixture with a realistically sized description string for " + scid[:16]},
			{Key: "var_header_icon", Value: "https://example.test/" + scid[:16] + "/icon.png"},
			{Key: "dURL", Value: scid[:12] + ".tela"},
			{Key: "telaVersion", Value: "1.1.1"},
			{Key: "owner", Value: "dero1qyjjxxaabbccddeeff0011223344556677889900aabbccddee00112233445566"},
		}
	}

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		b.StopTimer()
		store := newTestStore(b)
		batch := NewWriteBatch()
		for _, scid := range scids {
			batch.AddVariables(scid, 7180000, mkVars(scid))
			batch.AddClass(scid, &structures.ClassMeta{
				Class: "TELA-INDEX-1", Tags: []string{"all", "tela"},
				Name: "App " + scid[:8], DURL: scid[:12] + ".tela",
				InstallHeight: 7180000, LastHeight: 7180000,
			})
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
}

func newTestStore(tb testing.TB) *BboltStore {
	tb.Helper()
	store, err := NewBboltStore(tb.TempDir(), "")
	if err != nil {
		tb.Fatalf("NewBboltStore: %v", err)
	}
	tb.Cleanup(func() { _ = store.Close() })
	return store
}

func TestInteractionHeights_CompositeRoundTrip(t *testing.T) {
	store := newTestStore(t)
	scid := benchHistScid

	// Two flushes, with a same-height duplicate inside the first batch and a
	// cross-batch duplicate of height 7.
	batch := NewWriteBatch()
	batch.AddInteractionHeight(scid, 5)
	batch.AddInteractionHeight(scid, 7)
	batch.AddInteractionHeight(scid, 7)
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("flush 1: %v", err)
	}
	PutWriteBatch(batch)

	batch = NewWriteBatch()
	batch.AddInteractionHeight(scid, 7)
	batch.AddInteractionHeight(scid, 9)
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("flush 2: %v", err)
	}
	PutWriteBatch(batch)

	heights, err := store.GetSCIDInteractionHeights(scid)
	if err != nil {
		t.Fatalf("GetSCIDInteractionHeights: %v", err)
	}
	want := []int64{5, 7, 7, 7, 9}
	if len(heights) != len(want) {
		t.Fatalf("heights = %v, want %v", heights, want)
	}
	for i := range want {
		if heights[i] != want[i] {
			t.Fatalf("heights = %v, want %v", heights, want)
		}
	}
}

func TestInteractionHeights_LegacyBlobMerge(t *testing.T) {
	store := newTestStore(t)
	scid := benchHistScid

	// Seed a legacy whole-history msgpack blob the way pre-composite code did.
	legacy, err := msgpackMarshalHeights([]int64{1, 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketHeight).Put([]byte(scid), legacy)
	}); err != nil {
		t.Fatalf("seed legacy: %v", err)
	}

	if err := store.StoreSCIDInteractionHeight(scid, 3); err != nil {
		t.Fatalf("StoreSCIDInteractionHeight: %v", err)
	}

	heights, err := store.GetSCIDInteractionHeights(scid)
	if err != nil {
		t.Fatalf("GetSCIDInteractionHeights: %v", err)
	}
	want := []int64{1, 2, 3}
	if len(heights) != len(want) {
		t.Fatalf("heights = %v, want %v", heights, want)
	}
	for i := range want {
		if heights[i] != want[i] {
			t.Fatalf("heights = %v, want %v", heights, want)
		}
	}
}

func TestNormalTx_CompositeRoundTripAndLegacyMerge(t *testing.T) {
	store := newTestStore(t)
	addr := "dero1qyjjxxaabbccddeeff0011223344556677889900aabbccddee00112233445566"

	// Legacy blob with one record.
	legacyRec := []*structures.NormalTXWithSCIDParse{{Txid: "aa", Scid: benchHistScid, Fees: 1, Height: 10}}
	legacy, err := msgpackMarshalNormalTxs(legacyRec)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketNormTx).Put([]byte(addr), legacy)
	}); err != nil {
		t.Fatalf("seed legacy: %v", err)
	}

	// Composite records via batch flush and via the single-record path.
	batch := NewWriteBatch()
	batch.AddNormalTx(addr, "bb", benchHistScid, 2, 20)
	batch.AddNormalTx(addr, "cc", benchHistScid, 3, 30)
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("flush: %v", err)
	}
	PutWriteBatch(batch)
	if err := store.StoreNormalTxWithSCIDByAddr(addr, &structures.NormalTXWithSCIDParse{
		Txid: "dd", Scid: benchHistScid, Fees: 4, Height: 40,
	}); err != nil {
		t.Fatalf("StoreNormalTxWithSCIDByAddr: %v", err)
	}

	txs, err := store.GetNormalTxWithSCIDByAddr(addr)
	if err != nil {
		t.Fatalf("GetNormalTxWithSCIDByAddr: %v", err)
	}
	if len(txs) != 4 {
		t.Fatalf("got %d txs, want 4: %+v", len(txs), txs)
	}
	wantTxids := []string{"aa", "bb", "cc", "dd"}
	for i, want := range wantTxids {
		if txs[i] == nil || txs[i].Txid != want {
			t.Fatalf("txs[%d] = %+v, want Txid=%s", i, txs[i], want)
		}
	}
	if txs[3].Fees != 4 || txs[3].Height != 40 || txs[3].Scid != benchHistScid {
		t.Fatalf("composite record fields lost: %+v", txs[3])
	}

	// An unrelated address sharing no prefix must see nothing.
	other, err := store.GetNormalTxWithSCIDByAddr("dero1qzzzz")
	if err != nil || len(other) != 0 {
		t.Fatalf("unrelated addr: txs=%v err=%v", other, err)
	}
}

// TestNormalTx_MultiPayloadDistinctSCIDs is the merge-gate regression: one
// DERO tx with two asset payloads emits two records sharing (addr, height,
// txid) but with different SCIDs. The first composite-key layout omitted the
// SCID from the key, so the second Put silently destroyed the first record's
// SCID association — data main preserved.
func TestNormalTx_MultiPayloadDistinctSCIDs(t *testing.T) {
	store := newTestStore(t)
	addr := "dero1qyjjxxaabbccddeeff0011223344556677889900aabbccddee00112233445566"
	txid := "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
	scidA := "1111111111111111111111111111111111111111111111111111111111111111"
	scidB := "2222222222222222222222222222222222222222222222222222222222222222"

	batch := NewWriteBatch()
	batch.AddNormalTx(addr, txid, scidA, 1, 42)
	batch.AddNormalTx(addr, txid, scidB, 1, 42)
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("flush: %v", err)
	}
	PutWriteBatch(batch)

	txs, err := store.GetNormalTxWithSCIDByAddr(addr)
	if err != nil {
		t.Fatalf("GetNormalTxWithSCIDByAddr: %v", err)
	}
	if len(txs) != 2 {
		t.Fatalf("multi-payload records collapsed: got %d, want 2: %+v", len(txs), txs)
	}
	scids := map[string]bool{}
	for _, rec := range txs {
		scids[rec.Scid] = true
	}
	if !scids[scidA] || !scids[scidB] {
		t.Fatalf("SCID association lost: %+v", txs)
	}
}

// One malformed empty-scid invocation must not abort the whole atomic batch.
func TestFlushBatch_EmptyScidInvocationSkipped(t *testing.T) {
	store := newTestStore(t)

	batch := NewWriteBatch()
	batch.AddInvocation(structures.InvokeRecord{
		Scid: "", Sender: "dero1qbad", Entrypoint: "X", Height: 5,
		Details: &structures.SCTXParse{Txid: "junk", Height: 5},
	})
	batch.AddInvocation(structures.InvokeRecord{
		Scid: benchHistScid, Sender: "dero1qgood", Entrypoint: "InputStr", Height: 5,
		Details: &structures.SCTXParse{Txid: "goodtx", Scid: benchHistScid, Height: 5},
	})
	batch.AddInteractionHeight(benchHistScid, 5)
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("flush with empty-scid invocation must not fail: %v", err)
	}
	PutWriteBatch(batch)

	got, err := store.GetInvokeDetailsBySCID(benchHistScid)
	if err != nil || len(got) != 1 || got[0].Txid != "goodtx" {
		t.Fatalf("good record lost alongside skipped junk: %+v err=%v", got, err)
	}
}

func TestDecodeHeightEntry_CorruptCountBounded(t *testing.T) {
	key := appendHeightKey(nil, benchHistScid, 42)
	corrupt := make([]byte, binary.MaxVarintLen64)
	binary.PutUvarint(corrupt, 1<<60)
	_, _, err := DecodeHeightEntry(key, corrupt)
	if err == nil {
		t.Fatal("corrupt huge count must error, not allocate")
	}
}

func TestDecodeHeightEntry_BothLayouts(t *testing.T) {
	// Composite with count=3.
	key := appendHeightKey(nil, benchHistScid, 42)
	val := []byte{3}
	scid, heights, err := DecodeHeightEntry(key, val)
	if err != nil || scid != benchHistScid {
		t.Fatalf("composite: scid=%q err=%v", scid, err)
	}
	if len(heights) != 3 || heights[0] != 42 {
		t.Fatalf("composite heights = %v, want [42 42 42]", heights)
	}

	// Composite with empty value defaults to count=1.
	_, heights, err = DecodeHeightEntry(key, nil)
	if err != nil || len(heights) != 1 || heights[0] != 42 {
		t.Fatalf("empty-value composite heights = %v err=%v", heights, err)
	}

	// Legacy blob.
	legacy, err := msgpackMarshalHeights([]int64{7, 8})
	if err != nil {
		t.Fatal(err)
	}
	scid, heights, err = DecodeHeightEntry([]byte(benchHistScid), legacy)
	if err != nil || scid != benchHistScid || len(heights) != 2 {
		t.Fatalf("legacy: scid=%q heights=%v err=%v", scid, heights, err)
	}
}

func msgpackMarshalHeights(h []int64) ([]byte, error) {
	return msgpack.Marshal(h)
}

func msgpackMarshalNormalTxs(txs []*structures.NormalTXWithSCIDParse) ([]byte, error) {
	return msgpack.Marshal(txs)
}
