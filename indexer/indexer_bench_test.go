package indexer

import (
	"fmt"
	"testing"

	"github.com/deroproject/derohe/cryptography/crypto"
	"github.com/deroproject/derohe/rpc"

	"github.com/hypergnomon/hypergnomon/structures"
)

// Package-level sinks: assigning bench results here defeats dead-code
// elimination when the function under test inlines (project rule: no fake
// sub-ns numbers).
var (
	benchStrSink      string
	benchVarsSink     []*structures.SCIDVariable
	benchHashStrsSink []string
	benchRegCountSink int64
)

// benchGetSCResult carries a realistic mainnet variable set: ~30 vars
// (20 string-keyed + 10 uint64-keyed), the shape audit #11 sized the
// parseSCVariables arena for.
var benchGetSCResult = func() *rpc.GetSC_Result {
	r := &rpc.GetSC_Result{
		VariableStringKeys: make(map[string]interface{}, 20),
		VariableUint64Keys: make(map[uint64]interface{}, 10),
	}
	r.VariableStringKeys["var_header_name"] = "BenchApp"
	r.VariableStringKeys["var_header_description"] = "A parse-bench fixture"
	r.VariableStringKeys["dURL"] = "benchapp.tela"
	r.VariableStringKeys["likes"] = uint64(412)
	r.VariableStringKeys["dislikes"] = uint64(3)
	for i := 0; i < 15; i++ {
		r.VariableStringKeys[fmt.Sprintf("dero1qy%058d", i)] = uint64(i)
	}
	for i := 0; i < 10; i++ {
		r.VariableUint64Keys[uint64(i)] = fmt.Sprintf("value-%d", i)
	}
	return r
}()

// BenchmarkParseSCVariables is the audit #11 before/after gauge: pre-fix
// one heap object per variable, post-fix a single arena allocation.
func BenchmarkParseSCVariables(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		benchVarsSink = parseSCVariables(benchGetSCResult)
	}
}

// benchSCIDArgs holds an invoke-style SCDATA argument list with a typed
// crypto.Hash SC_ID, exactly what processSCTx sees per invoke TX.
var benchSCIDArgs = func() rpc.Arguments {
	var h crypto.Hash
	for i := range h {
		h[i] = byte(i + 1)
	}
	return rpc.Arguments{
		{Name: "entrypoint", DataType: rpc.DataString, Value: "Rate"},
		{Name: "SC_ID", DataType: rpc.DataHash, Value: h},
	}
}()

// BenchmarkSCIDExtract_Sprintf is the pre-fix processSCTx SC_ID extraction
// (audit #10 before): fmt reflection over a crypto.Hash on every invoke TX.
func BenchmarkSCIDExtract_Sprintf(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		benchStrSink = fmt.Sprintf("%v", benchSCIDArgs.Value("SC_ID", "H"))
	}
}

// BenchmarkSCIDExtract_Typed is the post-fix form (audit #10 after): a type
// switch that calls crypto.Hash.String() directly.
func BenchmarkSCIDExtract_Typed(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		benchStrSink = scidArgString(benchSCIDArgs.Value("SC_ID", "H"))
	}
}

func TestSCIDArgStringMatchesSprintfForHash(t *testing.T) {
	v := benchSCIDArgs.Value("SC_ID", "H")
	want := fmt.Sprintf("%v", v)
	if got := scidArgString(v); got != want {
		t.Fatalf("scidArgString(hash) = %q, want %q", got, want)
	}
	if got := scidArgString(nil); got != "" {
		t.Fatalf("scidArgString(nil) = %q, want empty (not %q)", got, fmt.Sprintf("%v", nil))
	}
	if got := scidArgString("abc"); got != "abc" {
		t.Fatalf("scidArgString(string) = %q, want abc", got)
	}
}

// benchTxHashes mixes ~25% registration-style hashes (3-zero-byte prefix)
// into a block's TX hash list, mirroring early-chain blocks where the
// audit #9 waste was largest.
var benchTxHashes = func() []crypto.Hash {
	hashes := make([]crypto.Hash, 64)
	for i := range hashes {
		if i%4 == 0 {
			// Registration-style: leading bytes stay zero; vary the tail so
			// hashes are distinct.
			hashes[i][31] = byte(i)
			continue
		}
		for j := range hashes[i] {
			hashes[i][j] = byte(i + j + 1)
		}
	}
	return hashes
}()

// BenchmarkTxHashFilter_StringFirst replicates the pre-fix fetcher loop
// (audit #9 before): h.String() hex-encodes EVERY hash, including the
// registration TXs that are skipped one line later.
func BenchmarkTxHashFilter_StringFirst(b *testing.B) {
	out := make([]string, 0, len(benchTxHashes))
	b.ReportAllocs()
	for b.Loop() {
		out = out[:0]
		var reg int64
		for _, h := range benchTxHashes {
			hashStr := h.String()
			hashBytes := h[:]
			if len(hashBytes) >= 3 && hashBytes[0] == 0 && hashBytes[1] == 0 && hashBytes[2] == 0 {
				reg++
				continue
			}
			out = append(out, hashStr)
		}
		benchRegCountSink = reg
		benchHashStrsSink = out
	}
}

// BenchmarkTxHashFilter_PrefixFirst is the post-fix loop shape (audit #9
// after): the 3-zero-byte prefix check runs first, so registration TXs never
// pay for hex encoding.
func BenchmarkTxHashFilter_PrefixFirst(b *testing.B) {
	out := make([]string, 0, len(benchTxHashes))
	b.ReportAllocs()
	for b.Loop() {
		out = out[:0]
		var reg int64
		for _, h := range benchTxHashes {
			if isRegistrationTxHash(h) {
				reg++
				continue
			}
			out = append(out, h.String())
		}
		benchRegCountSink = reg
		benchHashStrsSink = out
	}
}

func TestTxHashFilterShapesAgree(t *testing.T) {
	var regOld, regNew int64
	var oldKept, newKept []string
	for _, h := range benchTxHashes {
		hashStr := h.String()
		hb := h[:]
		if len(hb) >= 3 && hb[0] == 0 && hb[1] == 0 && hb[2] == 0 {
			regOld++
		} else {
			oldKept = append(oldKept, hashStr)
		}
		if isRegistrationTxHash(h) {
			regNew++
		} else {
			newKept = append(newKept, h.String())
		}
	}
	if regOld != regNew {
		t.Fatalf("registration counts differ: old=%d new=%d", regOld, regNew)
	}
	if len(oldKept) != len(newKept) {
		t.Fatalf("kept counts differ: old=%d new=%d", len(oldKept), len(newKept))
	}
	for i := range oldKept {
		if oldKept[i] != newKept[i] {
			t.Fatalf("kept[%d] differs: old=%q new=%q", i, oldKept[i], newKept[i])
		}
	}
}
