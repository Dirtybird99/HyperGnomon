package structures

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestSCIDVariables_RoundTrip_TELAIndexShape mimics a real TELA-INDEX-1
// record as produced by indexer.parseSCVariables from the DERO daemon:
//   - 14 string-keyed variables (dURL, dislikes, docVersion, ..., C)
//     where "C" holds a 6000-byte hex string (SC code blob)
//   - 1 uint64-keyed variable
//
// The marshaler must round-trip this cleanly; a decode failure or a
// slice-length mismatch reproduces the on-disk corruption pattern seen
// on 34/92 INDEX records in HYPERGNOMON.db.
func TestSCIDVariables_RoundTrip_TELAIndexShape(t *testing.T) {
	// 6000-byte hex string — matches what derod emits for "C" when a
	// SC code's raw bytes are hex-encoded via fmt.Sprintf("%x", ...).
	bigHex := strings.Repeat("ab", 3000)
	if len(bigHex) != 6000 {
		t.Fatalf("test fixture: bigHex len %d want 6000", len(bigHex))
	}

	in := []*SCIDVariable{
		{Key: "dURL", Value: "telaapp.example"},
		{Key: "dislikes", Value: uint64(3)},
		{Key: "docVersion", Value: "1.2.3"},
		{Key: "fileCheckS", Value: strings.Repeat("0f", 32)},
		{Key: "hash", Value: strings.Repeat("de", 32)},
		{Key: "likes", Value: uint64(17)},
		{Key: "nameHdr", Value: "Example TELA App"},
		{Key: "descrHdr", Value: "A description with embedded: separators and newlines\n"},
		{Key: "docType", Value: "TELA-JS-1"},
		{Key: "fileCheckC", Value: strings.Repeat("ad", 32)},
		{Key: "iconURLHdr", Value: "https://example.com/icon.png"},
		{Key: "authorHdr", Value: "dero1qy..."},
		{Key: "subDirHdr", Value: ""}, // deliberate empty string
		{Key: "C", Value: bigHex},     // 6000-byte hex — the SC code
		// uint64-keyed var (mirrors VariableUint64Keys map entry)
		{Key: uint64(0), Value: "root-entry"},
	}

	buf := MarshalSCIDVariablesTypedAppend(nil, in)
	if buf[0] != TagSCIDVariablesV1 {
		t.Fatalf("tag: got %#x want %#x", buf[0], TagSCIDVariablesV1)
	}

	out, err := UnmarshalSCIDVariablesTyped(buf)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v\nbuf len=%d tag=%#x count-header=%x",
			err, len(buf), buf[0], buf[1:5])
	}
	if len(out) != len(in) {
		t.Fatalf("count: got %d want %d (encoded len %d)", len(out), len(in), len(buf))
	}
	for i, v := range in {
		if out[i].Key != v.Key {
			t.Fatalf("[%d] Key drift: got %T(%v) want %T(%v)",
				i, out[i].Key, out[i].Key, v.Key, v.Key)
		}
		// Value comparison: for string we compare directly; for uint64
		// we compare directly; interface equality handles both.
		if out[i].Value != v.Value {
			// Print abridged for the large "C" blob so failure output stays sane.
			sIn := valAbridged(v.Value)
			sOut := valAbridged(out[i].Value)
			t.Fatalf("[%d] Value drift (key=%v): got %T(%s) want %T(%s)",
				i, v.Key, out[i].Value, sOut, v.Value, sIn)
		}
	}
}

// TestSCIDVariables_RoundTrip_JSONInterfaceTypes exercises the value
// types that actually come out of jrpc2's default JSON unmarshal into
// a map[string]interface{} — json.Number, float64, []interface{},
// nested map[string]interface{}, nil, and []byte.
//
// These are the candidates for "latent marshaler bug" (hypothesis A).
func TestSCIDVariables_RoundTrip_JSONInterfaceTypes(t *testing.T) {
	cases := []struct {
		name string
		vars []*SCIDVariable
	}{
		{
			name: "json.Number (integer)",
			vars: []*SCIDVariable{
				{Key: "height", Value: json.Number("6927000")},
			},
		},
		{
			name: "json.Number (negative)",
			vars: []*SCIDVariable{
				{Key: "delta", Value: json.Number("-42")},
			},
		},
		{
			name: "float64 with integer value",
			vars: []*SCIDVariable{
				{Key: "count", Value: float64(12345)},
			},
		},
		{
			name: "[]interface{} slice",
			vars: []*SCIDVariable{
				{Key: "list", Value: []interface{}{"a", "b", "c"}},
			},
		},
		{
			name: "nested map[string]interface{}",
			vars: []*SCIDVariable{
				{Key: "obj", Value: map[string]interface{}{"x": "y"}},
			},
		},
		{
			name: "nil value",
			vars: []*SCIDVariable{
				{Key: "missing", Value: nil},
			},
		},
		{
			name: "[]byte value",
			vars: []*SCIDVariable{
				{Key: "raw", Value: []byte{0xde, 0xad, 0xbe, 0xef}},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			buf := MarshalSCIDVariablesTypedAppend(nil, tc.vars)
			out, err := UnmarshalSCIDVariablesTyped(buf)
			if err != nil {
				t.Fatalf("Unmarshal failed: %v; buf=%x", err, buf)
			}
			if len(out) != len(tc.vars) {
				t.Fatalf("count: got %d want %d (buf=%x)", len(out), len(tc.vars), buf)
			}
			// We don't assert value equality — the marshaler is explicitly
			// lossy for unknown types (doc'd as "fallback to stringified").
			// The important invariants are: (1) no error, (2) slice length
			// preserved. That rules out corruption/shift of downstream vars.
			t.Logf("key=%v value-out=%T(%v)", out[0].Key, out[0].Value, out[0].Value)
		})
	}
}

// TestSCIDVariables_RoundTrip_MixedTELAPlusJSONTypes combines the realistic
// TELA-JS-1 shape with one json.Number and one []interface{} thrown in —
// reproducing the common scenario where a filter-bypass SC leaks a float/list
// into the variable map.
func TestSCIDVariables_RoundTrip_MixedTELAPlusJSONTypes(t *testing.T) {
	bigHex := strings.Repeat("ab", 3000)
	in := []*SCIDVariable{
		{Key: "nameHdr", Value: "Mixed TELA App"},
		{Key: "height", Value: json.Number("12345")},
		{Key: "list", Value: []interface{}{"a", 1, true}},
		{Key: "count", Value: float64(42)},
		{Key: "nested", Value: map[string]interface{}{"k": "v"}},
		{Key: "empty", Value: nil},
		{Key: "bytes", Value: []byte("hello")},
		{Key: "C", Value: bigHex},
		{Key: "likes", Value: uint64(100)},
		{Key: uint64(7), Value: "seven"},
	}
	buf := MarshalSCIDVariablesTypedAppend(nil, in)
	out, err := UnmarshalSCIDVariablesTyped(buf)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v; buf len=%d", err, len(buf))
	}
	if len(out) != len(in) {
		t.Fatalf("count: got %d want %d (encoded len %d)", len(out), len(in), len(buf))
	}
	// Spot-check known-good vars survive their neighbors.
	if out[0].Key != "nameHdr" || out[0].Value != "Mixed TELA App" {
		t.Fatalf("[0] corrupted: %+v", out[0])
	}
	if out[7].Key != "C" || out[7].Value != bigHex {
		t.Fatalf("[7] big-hex C var corrupted: key=%v valueLen=%d wantLen=%d",
			out[7].Key, valLen(out[7].Value), len(bigHex))
	}
	if out[8].Key != "likes" || out[8].Value != uint64(100) {
		t.Fatalf("[8] likes corrupted: %+v", out[8])
	}
	if out[9].Key != uint64(7) || out[9].Value != "seven" {
		t.Fatalf("[9] uint64-keyed var corrupted: %+v", out[9])
	}
}

func valAbridged(v interface{}) string {
	if s, ok := v.(string); ok && len(s) > 40 {
		return "string len=" + itoaBase10(int64(len(s))) + " head=" + s[:40] + "..."
	}
	switch x := v.(type) {
	case string:
		return x
	case uint64:
		return itoaBase10(int64(x))
	default:
		return "?"
	}
}

func valLen(v interface{}) int {
	if s, ok := v.(string); ok {
		return len(s)
	}
	return -1
}
