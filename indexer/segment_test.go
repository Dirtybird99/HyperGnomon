package indexer

import (
	"path/filepath"
	"testing"

	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

// TestSegmentMerge_RoundTripsData ensures SegmentSync.mergeSegmentDB decodes
// data with the same codec storage.FlushBatch encodes with (msgpack).
// Regression: this previously used json.Unmarshal on msgpack-encoded blobs
// and silently dropped every row.
func TestSegmentMerge_RoundTripsData(t *testing.T) {
	// --- Build a "segment" store with real data via the normal write path.
	segDir := t.TempDir()
	segStore, err := storage.NewBboltStore(segDir, "")
	if err != nil {
		t.Fatalf("open segment store: %v", err)
	}

	scid := "a05395bb0cf77adc850928b0db00eb5ca7a9ccbafd9a38d021c8d299ad5ce1a4"
	addr := "dero1qyjj8ux7r5hvmmgce2pwu4g50s3mgpdepmffvfug4qmvv6pzr3e9jqg4ma0ch"

	segBatch := storage.NewWriteBatch()
	segBatch.AddOwner(scid, addr)
	segBatch.AddVariables(scid, 1000, []*structures.SCIDVariable{
		{Key: "telaVersion", Value: "1.0.0"},
		{Key: "nameHdr", Value: "MyTELA"},
	})
	segBatch.AddInteractionHeight(scid, 1000)
	segBatch.AddInvocation(structures.InvokeRecord{
		Scid:       scid,
		Sender:     addr,
		Entrypoint: "InputStr",
		Height:     1001,
		Details: &structures.SCTXParse{
			Txid:       "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
			Scid:       scid,
			Entrypoint: "InputStr",
			Method:     structures.MethodInvokeSC,
			Sender:     addr,
			Fees:       500,
			Height:     1001,
		},
	})
	segBatch.AddNormalTx(addr, "feed0001feed0001feed0001feed0001feed0001feed0001feed0001feed0001", scid, 10, 1002)
	segBatch.InvalidSCIDs["badscidbadscidbadscidbadscidbadscidbadscidbadscidbadscidbadscid0"] = 42
	segBatch.RegTxCount = 5
	segBatch.BurnTxCount = 3
	segBatch.NormTxCount = 7
	segBatch.LastHeight = 1002

	if err := segStore.FlushBatch(segBatch); err != nil {
		t.Fatalf("segment FlushBatch: %v", err)
	}
	// Keep the DB open-readable but close our writer so merge can open read-only.
	segStore.Close()

	// --- Build a main store and merge the segment DB into it.
	mainDir := t.TempDir()
	mainStore, err := storage.NewBboltStore(mainDir, "")
	if err != nil {
		t.Fatalf("open main store: %v", err)
	}
	t.Cleanup(func() { mainStore.Close() })

	ss := &SegmentSync{MainStore: mainStore}
	if err := ss.mergeSegmentDB(filepath.Join(segDir, "HYPERGNOMON.db")); err != nil {
		t.Fatalf("mergeSegmentDB: %v", err)
	}

	// --- Verify every category round-tripped.
	owner, err := mainStore.GetOwner(scid)
	if err != nil || owner != addr {
		t.Fatalf("owner round-trip: got (%q, %v), want (%q, nil)", owner, err, addr)
	}

	vars, err := mainStore.GetSCIDVariableDetailsAtHeight(scid, 1000)
	if err != nil {
		t.Fatalf("variables: %v", err)
	}
	if len(vars) != 2 {
		t.Fatalf("expected 2 variables after merge, got %d — msgpack decode likely failing silently", len(vars))
	}

	heights, err := mainStore.GetSCIDInteractionHeights(scid)
	if err != nil {
		t.Fatalf("heights: %v", err)
	}
	if len(heights) == 0 || heights[0] != 1000 {
		t.Fatalf("expected height 1000 after merge, got %v", heights)
	}

	invokes, err := mainStore.GetInvokeDetailsBySCID(scid)
	if err != nil {
		t.Fatalf("invokes: %v", err)
	}
	if len(invokes) != 1 {
		t.Fatalf("expected 1 invoke after merge, got %d — msgpack decode likely failing silently", len(invokes))
	}
	if invokes[0].Entrypoint != "InputStr" {
		t.Fatalf("invoke entrypoint: got %q, want %q", invokes[0].Entrypoint, "InputStr")
	}

	normTxs, err := mainStore.GetNormalTxWithSCIDByAddr(addr)
	if err != nil {
		t.Fatalf("normal txs: %v", err)
	}
	if len(normTxs) != 1 {
		t.Fatalf("expected 1 normal tx after merge, got %d", len(normTxs))
	}

	invalid, err := mainStore.GetInvalidSCIDDeploys()
	if err != nil {
		t.Fatalf("invalid: %v", err)
	}
	if len(invalid) != 1 {
		t.Fatalf("expected 1 invalid scid after merge, got %d", len(invalid))
	}

	reg, burn, norm, err := mainStore.GetTxCounts()
	if err != nil {
		t.Fatalf("tx counts: %v", err)
	}
	if reg != 5 || burn != 3 || norm != 7 {
		t.Fatalf("tx counts: got (%d,%d,%d), want (5,3,7)", reg, burn, norm)
	}
}
