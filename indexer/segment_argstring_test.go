package indexer

import (
	"fmt"
	"testing"

	"github.com/deroproject/derohe/rpc"
	"github.com/deroproject/derohe/transaction"
)

// TestSegmentArgStringMatchesSprintf covers the three sites the hash-only
// TestSCIDArgStringMatchesSprintfForHash does not: the string entrypoint,
// the uint64 SC_ACTION, and the string SC_CODE. For a present arg, the typed
// argString must produce byte-identical output to the old fmt.Sprintf("%v",...).
func TestSegmentArgStringMatchesSprintf(t *testing.T) {
	args := rpc.Arguments{
		{Name: "entrypoint", DataType: rpc.DataString, Value: "Rate"},
		{Name: "SC_ACTION", DataType: rpc.DataUint64, Value: uint64(1)},
		{Name: "SC_CODE", DataType: rpc.DataString, Value: "Function Initialize() Uint64\n10 RETURN 0\nEnd Function"},
	}
	cases := []struct {
		name  string
		dtype rpc.DataType
	}{
		{"entrypoint", rpc.DataString},
		{"SC_ACTION", rpc.DataUint64},
		{"SC_CODE", rpc.DataString},
	}
	for _, c := range cases {
		want := fmt.Sprintf("%v", args.Value(c.name, c.dtype))
		if got := argString(args, c.name, c.dtype); got != want {
			t.Fatalf("argString(%s) = %q, want %q", c.name, got, want)
		}
	}
}

// TestSegmentProcessSCTxEmptySCIDReturnsEarly verifies the ported empty-SC_ID
// guard. An invoke-style SC_TX with no SC_ID yields scid=="" via scidArgString;
// the guard must return before any client/batch use. The test passes nil for
// the client and batch: with the guard present, processSCTx returns without
// touching them; without the guard, an empty interned scid reaches
// handleInvokeSC, which dereferences the nil client and panics.
func TestSegmentProcessSCTxEmptySCIDReturnsEarly(t *testing.T) {
	ss := &SegmentSync{}
	var tx transaction.Transaction
	tx.SCDATA = rpc.Arguments{
		{Name: "entrypoint", DataType: rpc.DataString, Value: "Rate"},
		{Name: "SC_ACTION", DataType: rpc.DataUint64, Value: uint64(0)}, // not an install
		// SC_ID intentionally absent
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("processSCTx panicked on missing SC_ID (guard not effective): %v", r)
		}
	}()

	ss.processSCTx(nil, &tx, rpc.Tx_Related_Info{}, "txidplaceholder", 100, nil, nil)

	// Pin the guard condition the early return depends on.
	if got := scidArgString(tx.SCDATA.Value("SC_ID", "H")); got != "" {
		t.Fatalf("scidArgString(missing SC_ID) = %q, want empty", got)
	}
}
