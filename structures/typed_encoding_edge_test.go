package structures

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/deroproject/derohe/rpc"
	"github.com/deroproject/derohe/transaction"
)

// Byte-level edge cases for the typed encoders: InstallRecord (tag 0x05),
// SCTXParse turbo (tag 0x06), and the SCIDVariables codec (tag 0x02).
// Covers multi-byte uvarint lengths, zero-value structs, unicode payloads,
// extreme Method bytes, exhaustive truncation, and corrupt length prefixes.

// long300 is exactly 300 bytes — past the 127-byte single-byte uvarint
// boundary, so its length prefix encodes as 2 bytes.
var long300 = strings.Repeat("x", 300)

func TestInstallRecord_TypedEdgeCases(t *testing.T) {
	if len(long300) != 300 {
		t.Fatalf("fixture: long300 len %d want 300", len(long300))
	}
	cases := []struct {
		name string
		rec  InstallRecord
	}{
		{"zero value", InstallRecord{}},
		{"300-byte owner and entrypoint (multi-byte uvarint)", InstallRecord{Owner: long300, Entrypoint: long300, Fees: 1}},
		{"unicode owner", InstallRecord{Owner: "дero-владелец✓", Entrypoint: "Initialize", Fees: 77}},
		{"max fees", InstallRecord{Owner: "o", Fees: ^uint64(0)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			blob := tc.rec.MarshalTyped()
			if !IsInstallRecordTyped(blob) {
				t.Fatalf("typed blob not detected: % x", blob[:min(len(blob), 16)])
			}
			var got InstallRecord
			if err := got.UnmarshalTyped(blob); err != nil {
				t.Fatalf("UnmarshalTyped: %v", err)
			}
			if got != tc.rec {
				t.Fatalf("round-trip drift:\n got=%+v\nwant=%+v", got, tc.rec)
			}
		})
	}
}

func TestSCTXParse_TurboTypedEdgeCases(t *testing.T) {
	cases := []struct {
		name string
		in   SCTXParse
	}{
		{"zero value", SCTXParse{}},
		{"method 0xFF", SCTXParse{Txid: "t", Scid: "s", Method: 0xFF, Height: 1}},
		{"300-byte entrypoint and sender (multi-byte uvarint)", SCTXParse{Entrypoint: long300, Sender: long300, Fees: 2, Height: 3}},
		{"unicode sender", SCTXParse{Txid: "tx", Sender: "дero-владелец✓", Method: MethodInvokeSC}},
		{"negative height", SCTXParse{Txid: "tx", Height: -9}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.in.CanMarshalTurboTyped() {
				t.Fatal("fixture should be turbo-marshalable (nil ScArgs/Payloads)")
			}
			blob := tc.in.MarshalTurboTyped()
			if !IsSCTXParseTurboTyped(blob) {
				t.Fatalf("typed blob not detected: % x", blob[:min(len(blob), 16)])
			}
			var got SCTXParse
			if err := got.UnmarshalTurboTyped(blob); err != nil {
				t.Fatalf("UnmarshalTurboTyped: %v", err)
			}
			if got.Txid != tc.in.Txid || got.Scid != tc.in.Scid ||
				got.Entrypoint != tc.in.Entrypoint || got.Method != tc.in.Method ||
				got.Sender != tc.in.Sender || got.Fees != tc.in.Fees ||
				got.Height != tc.in.Height {
				t.Fatalf("round-trip drift:\n got=%+v\nwant=%+v", got, tc.in)
			}
			if got.ScArgs != nil || got.Payloads != nil {
				t.Fatalf("turbo decode populated non-turbo fields: %+v", got)
			}
		})
	}
}

func TestSCIDVariables_TypedEdgeCases(t *testing.T) {
	cases := []struct {
		name string
		vars []*SCIDVariable
	}{
		{"300-byte key and value (multi-byte uvarint)", []*SCIDVariable{{Key: long300, Value: long300}}},
		{"unicode value", []*SCIDVariable{{Key: "owner", Value: "дero-владелец✓"}}},
		{"max uint64", []*SCIDVariable{{Key: uint64(0), Value: ^uint64(0)}}},
		{"empty strings", []*SCIDVariable{{Key: "", Value: ""}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			blob := MarshalSCIDVariablesTyped(tc.vars)
			out, err := UnmarshalSCIDVariablesTyped(blob)
			if err != nil {
				t.Fatalf("UnmarshalSCIDVariablesTyped: %v", err)
			}
			if len(out) != len(tc.vars) {
				t.Fatalf("count: got %d want %d", len(out), len(tc.vars))
			}
			for i, v := range tc.vars {
				if out[i].Key != v.Key || out[i].Value != v.Value {
					t.Fatalf("[%d] drift: got %+v want %+v", i, *out[i], *v)
				}
			}
		})
	}
}

// ---------- error paths: exhaustive truncation ----------

// Every strict prefix of a valid blob must produce an error (and must not
// panic). Each Marshal consumes exactly all bytes on decode, so any
// truncation lands mid-header, mid-uvarint, or mid-payload.

func TestInstallRecord_UnmarshalTruncated(t *testing.T) {
	rec := InstallRecord{Owner: long300, Entrypoint: "Initialize", Fees: 42}
	blob := rec.MarshalTyped()
	for trunc := 0; trunc < len(blob); trunc++ {
		var got InstallRecord
		if err := got.UnmarshalTyped(blob[:trunc]); err == nil {
			t.Fatalf("truncation at %d/%d bytes decoded without error", trunc, len(blob))
		}
	}
}

func TestSCTXParse_UnmarshalTurboTruncated(t *testing.T) {
	in := SCTXParse{Txid: long300, Scid: "scid", Entrypoint: "E", Method: 0xFF, Sender: "sender", Fees: 7, Height: 8}
	blob := in.MarshalTurboTyped()
	for trunc := 0; trunc < len(blob); trunc++ {
		var got SCTXParse
		if err := got.UnmarshalTurboTyped(blob[:trunc]); err == nil {
			t.Fatalf("truncation at %d/%d bytes decoded without error", trunc, len(blob))
		}
	}
}

func TestSCIDVariables_UnmarshalTruncated(t *testing.T) {
	vars := []*SCIDVariable{
		{Key: "nameHdr", Value: "MyTELA"},
		{Key: "big", Value: long300},
		{Key: "balance", Value: uint64(12345)},
	}
	blob := MarshalSCIDVariablesTyped(vars)
	for trunc := 0; trunc < len(blob); trunc++ {
		if _, err := UnmarshalSCIDVariablesTyped(blob[:trunc]); err == nil {
			t.Fatalf("truncation at %d/%d bytes decoded without error", trunc, len(blob))
		}
	}
}

// ---------- error paths: declared length exceeds remaining bytes ----------

func TestInstallRecord_UnmarshalOversizedLength(t *testing.T) {
	blob := []byte{TagInstallRecordV1}
	blob = binary.BigEndian.AppendUint64(blob, 42)    // Fees
	blob = binary.AppendUvarint(blob, 1000)           // Owner claims 1000 bytes...
	blob = append(blob, "only-a-few-bytes-follow"...) // ...but far fewer remain
	var got InstallRecord
	if err := got.UnmarshalTyped(blob); err == nil {
		t.Fatal("oversized Owner length decoded without error")
	}
}

func TestSCTXParse_UnmarshalTurboOversizedLength(t *testing.T) {
	blob := []byte{TagSCTXParseTurboV1, MethodInvokeSC}
	blob = binary.BigEndian.AppendUint64(blob, 1) // Fees
	blob = binary.BigEndian.AppendUint64(blob, 2) // Height
	blob = binary.AppendUvarint(blob, 1<<20)      // Txid claims 1 MiB
	blob = append(blob, "stub"...)
	var got SCTXParse
	if err := got.UnmarshalTurboTyped(blob); err == nil {
		t.Fatal("oversized Txid length decoded without error")
	}
}

func TestSCIDVariables_UnmarshalOversizedLength(t *testing.T) {
	blob := []byte{TagSCIDVariablesV1}
	blob = binary.BigEndian.AppendUint32(blob, 1) // one variable
	blob = append(blob, varKindString)            // key kind
	blob = binary.AppendUvarint(blob, 1000)       // key claims 1000 bytes
	blob = append(blob, "abc"...)                 // 3 remain
	if _, err := UnmarshalSCIDVariablesTyped(blob); err == nil {
		t.Fatal("oversized string length decoded without error")
	}
}

func TestSCIDVariables_UnmarshalOversizedCount(t *testing.T) {
	// Declared count says 2^31 variables but almost no payload follows.
	// Must error up front instead of attempting a multi-GB arena.
	blob := []byte{TagSCIDVariablesV1}
	blob = binary.BigEndian.AppendUint32(blob, 1<<31)
	blob = append(blob, varKindString, 0x00) // a single empty-string field
	if _, err := UnmarshalSCIDVariablesTyped(blob); err == nil {
		t.Fatal("oversized count decoded without error")
	}
}

// ---------- sized marshal equivalence ----------

// TestSCIDVariables_SizedMatchesAppend proves MarshalSCIDVariablesTyped is
// byte-identical to the nil-append pattern it replaces, and that the sizing
// pass is exact (cap == len → the single allocation never re-grew).
func TestSCIDVariables_SizedMatchesAppend(t *testing.T) {
	cases := []struct {
		name string
		vars []*SCIDVariable
	}{
		{"nil slice", nil},
		{"mixed realistic", benchVars30},
		{"multi-byte uvarint", []*SCIDVariable{{Key: long300, Value: long300}}},
		{"unknown-type fallback", []*SCIDVariable{{Key: "n", Value: int64(-42)}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sized := MarshalSCIDVariablesTyped(tc.vars)
			appended := MarshalSCIDVariablesTypedAppend(nil, tc.vars)
			if string(sized) != string(appended) {
				t.Fatalf("wire bytes differ:\n sized=%x\nappend=%x", sized, appended)
			}
			if cap(sized) != len(sized) {
				t.Fatalf("sizing pass inexact: len=%d cap=%d", len(sized), cap(sized))
			}
		})
	}
}

// TestSCIDVariables_UnmarshalArenaPointersStable guards the arena-backed
// decode: every returned pointer must remain valid and distinct, with no
// aliasing between elements.
func TestSCIDVariables_UnmarshalArenaPointersStable(t *testing.T) {
	blob := MarshalSCIDVariablesTyped(benchVars30)
	out, err := UnmarshalSCIDVariablesTyped(blob)
	if err != nil {
		t.Fatal(err)
	}
	seen := make(map[*SCIDVariable]bool, len(out))
	for i, v := range out {
		if v == nil {
			t.Fatalf("[%d] nil element", i)
		}
		if seen[v] {
			t.Fatalf("[%d] aliased pointer %p", i, v)
		}
		seen[v] = true
		if v.Key != benchVars30[i].Key || v.Value != benchVars30[i].Value {
			t.Fatalf("[%d] drift: got %+v want %+v", i, *v, *benchVars30[i])
		}
	}
}

// ---------- CanMarshalTurboTyped ----------

func TestSCTXParse_CanMarshalTurboTyped(t *testing.T) {
	cases := []struct {
		name string
		s    *SCTXParse
		want bool
	}{
		{"nil receiver", nil, false},
		{"empty ScArgs and Payloads", &SCTXParse{Txid: "tx", Method: MethodInvokeSC}, true},
		{"zero value", &SCTXParse{}, true},
		{"non-empty ScArgs", &SCTXParse{ScArgs: rpc.Arguments{{}}}, false},
		{"non-empty Payloads", &SCTXParse{Payloads: []transaction.AssetPayload{{}}}, false},
		{"both non-empty", &SCTXParse{ScArgs: rpc.Arguments{{}}, Payloads: []transaction.AssetPayload{{}}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.s.CanMarshalTurboTyped(); got != tc.want {
				t.Fatalf("CanMarshalTurboTyped() = %v, want %v", got, tc.want)
			}
		})
	}
}

// ---------- benchmarks ----------

// benchVars30 mirrors a realistic TELA-style snapshot: ~30 variables,
// two-thirds string values (headers, URLs, hashes), one-third uint64
// counters. Total encoded size ~1.5 KiB, so the nil-append pattern pays
// ~8 grow+copy cycles that the sized variant avoids.
var benchVars30 = func() []*SCIDVariable {
	vars := make([]*SCIDVariable, 0, 30)
	for i := 0; i < 30; i++ {
		n := itoaBase10(int64(i))
		if i%3 == 2 {
			vars = append(vars, &SCIDVariable{Key: "counter_" + n, Value: uint64(i) * 1000003})
		} else {
			vars = append(vars, &SCIDVariable{
				Key:   "varHdr_" + n,
				Value: "https://example.com/tela/asset/" + n + "/payload-with-moderate-length",
			})
		}
	}
	return vars
}()

// BenchmarkSCIDVariables_Marshal_TypedNilAppend is the caller pattern the
// sized variant replaces: MarshalSCIDVariablesTypedAppend(nil, vars) grows
// the destination log2(size) times per snapshot.
func BenchmarkSCIDVariables_Marshal_TypedNilAppend(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		benchSinkBytes = MarshalSCIDVariablesTypedAppend(nil, benchVars30)
	}
}

// BenchmarkSCIDVariables_Marshal_TypedSized is the candidate: one sizing
// pass, one exact allocation, same wire bytes.
func BenchmarkSCIDVariables_Marshal_TypedSized(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		benchSinkBytes = MarshalSCIDVariablesTyped(benchVars30)
	}
}

// benchSinkVars keeps the arena-backed decode observable (same DCE
// rationale as benchSinkBytes in typed_encoding_test.go).
var benchSinkVars []*SCIDVariable

// BenchmarkSCIDVariables_Unmarshal_Typed30 measures decode on the same
// 30-var fixture the marshal benchmarks use, sized to match the production
// snapshot shape (the 6-var BenchmarkSCIDVariables_Unmarshal_Typed in
// scidvars_encoding_test.go remains the historical baseline).
func BenchmarkSCIDVariables_Unmarshal_Typed30(b *testing.B) {
	blob := MarshalSCIDVariablesTyped(benchVars30)
	b.ReportAllocs()
	for b.Loop() {
		out, err := UnmarshalSCIDVariablesTyped(blob)
		if err != nil {
			b.Fatal(err)
		}
		benchSinkVars = out
	}
}
