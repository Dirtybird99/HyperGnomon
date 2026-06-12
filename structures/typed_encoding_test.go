package structures

import (
	"bytes"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// TestAddrSCIDEntry_TypedRoundTrip asserts Marshal → Unmarshal preserves
// all three fields exactly.
func TestAddrSCIDEntry_TypedRoundTrip(t *testing.T) {
	cases := []AddrSCIDEntry{
		{FirstHeight: 0, LastHeight: 0, Count: 0},
		{FirstHeight: 1, LastHeight: 1, Count: 1},
		{FirstHeight: -1, LastHeight: 1<<62 - 1, Count: 42},
		{FirstHeight: 6927000, LastHeight: 6927500, Count: 3381},
	}
	for _, c := range cases {
		b := c.MarshalTyped()
		if len(b) != EncodedAddrSCIDEntrySize {
			t.Fatalf("size: got %d want %d", len(b), EncodedAddrSCIDEntrySize)
		}
		var got AddrSCIDEntry
		if err := got.UnmarshalTyped(b); err != nil {
			t.Fatalf("UnmarshalTyped(%+v): %v", c, err)
		}
		if got != c {
			t.Fatalf("round-trip drift: got %+v want %+v", got, c)
		}
	}
}

// TestAddrSCIDEntry_TagRejectsMsgpack asserts that a msgpack-encoded blob
// does NOT decode via the typed path — the tag byte check is the
// discriminator for the v0/v1 reader dispatch.
func TestAddrSCIDEntry_TagRejectsMsgpack(t *testing.T) {
	e := AddrSCIDEntry{FirstHeight: 1, LastHeight: 2, Count: 3}
	mp, err := msgpack.Marshal(&e)
	if err != nil {
		t.Fatal(err)
	}
	if IsAddrSCIDEntryTyped(mp) {
		t.Fatalf("msgpack blob mis-identified as typed: first byte = 0x%02x", mp[0])
	}
	var decoded AddrSCIDEntry
	if err := decoded.UnmarshalTyped(mp); err == nil {
		t.Fatalf("UnmarshalTyped accepted msgpack bytes (first byte 0x%02x)", mp[0])
	}
}

// TestAddrSCIDEntry_TagRangeSanity confirms msgpack fixmap headers are all
// outside our typed tag, so dispatch by byte[0] is unambiguous.
func TestAddrSCIDEntry_TagRangeSanity(t *testing.T) {
	// The struct has 3 exported fields → msgpack encodes as a 3-entry fixmap
	// which starts with 0x83.
	e := AddrSCIDEntry{FirstHeight: 1, LastHeight: 2, Count: 3}
	mp, _ := msgpack.Marshal(&e)
	if mp[0] < 0x80 || mp[0] > 0x8f {
		t.Fatalf("msgpack first byte %#x outside fixmap range — dispatch may be ambiguous", mp[0])
	}
	if TagAddrSCIDEntryV1 >= 0x80 && TagAddrSCIDEntryV1 <= 0x8f {
		t.Fatal("typed tag overlaps msgpack fixmap range")
	}
}

// TestAddrSCIDEntry_AppendMatchesMarshal proves the append-style encoder
// produces identical bytes to the allocating Marshal.
func TestAddrSCIDEntry_AppendMatchesMarshal(t *testing.T) {
	e := AddrSCIDEntry{FirstHeight: 1000, LastHeight: 2000, Count: 12345}
	a := e.MarshalTyped()
	b := e.MarshalTypedAppend(nil)
	if !bytes.Equal(a, b) {
		t.Fatalf("MarshalTyped vs MarshalTypedAppend differ:\n  Marshal:       %x\n  AppendAppend:  %x", a, b)
	}
}

func TestInstallRecord_TypedRoundTrip(t *testing.T) {
	cases := []InstallRecord{
		{},
		{Owner: "dero1qyjjxxaabbccddeeff0011223344556677889900aabbccddee00112233445566"},
		{Owner: "owner", Entrypoint: "Initialize", Fees: 12345},
	}
	for _, c := range cases {
		b := c.MarshalTyped()
		if !IsInstallRecordTyped(b) {
			t.Fatalf("typed install record not detected: %x", b)
		}
		var got InstallRecord
		if err := got.UnmarshalTyped(b); err != nil {
			t.Fatalf("UnmarshalTyped(%+v): %v", c, err)
		}
		if got != c {
			t.Fatalf("round-trip drift: got %+v want %+v", got, c)
		}
	}
}

func TestInstallRecord_AppendMatchesMarshal(t *testing.T) {
	r := InstallRecord{Owner: "owner", Entrypoint: "Initialize", Fees: 99}
	a := r.MarshalTyped()
	b := r.MarshalTypedAppend(nil)
	if !bytes.Equal(a, b) {
		t.Fatalf("MarshalTyped vs MarshalTypedAppend differ:\n  Marshal:       %x\n  AppendAppend:  %x", a, b)
	}
}

func TestInstallRecord_TagRejectsMsgpack(t *testing.T) {
	r := InstallRecord{Owner: "owner", Entrypoint: "Initialize", Fees: 99}
	mp, err := msgpack.Marshal(&r)
	if err != nil {
		t.Fatal(err)
	}
	if IsInstallRecordTyped(mp) {
		t.Fatalf("msgpack blob mis-identified as typed: first byte = 0x%02x", mp[0])
	}
	var decoded InstallRecord
	if err := decoded.UnmarshalTyped(mp); err == nil {
		t.Fatalf("UnmarshalTyped accepted msgpack bytes (first byte 0x%02x)", mp[0])
	}
}

func TestSCTXParse_TurboTypedRoundTrip(t *testing.T) {
	in := SCTXParse{
		Txid:       "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
		Scid:       "a05395bb0cf77adc850928b0db00eb5ca7a9ccbafd9a38d021c8d299ad5ce1a4",
		Entrypoint: "InputStr",
		Method:     MethodInvokeSC,
		Sender:     "dero1qyjjxxaabbccddeeff0011223344556677889900aabbccddee00112233445566",
		Fees:       12345,
		Height:     6927400,
	}
	blob := in.MarshalTurboTyped()
	if !IsSCTXParseTurboTyped(blob) {
		t.Fatalf("typed SCTXParse not detected: %x", blob)
	}
	var got SCTXParse
	if err := got.UnmarshalTurboTyped(blob); err != nil {
		t.Fatalf("UnmarshalTurboTyped: %v", err)
	}
	if got.Txid != in.Txid || got.Scid != in.Scid || got.Entrypoint != in.Entrypoint ||
		got.Method != in.Method || got.Sender != in.Sender || got.Fees != in.Fees ||
		got.Height != in.Height {
		t.Fatalf("round-trip drift:\n got=%+v\nwant=%+v", got, in)
	}
	if got.ScArgs != nil || got.Payloads != nil {
		t.Fatalf("turbo decode populated non-turbo fields: %+v", got)
	}
}

func TestSCTXParse_TurboAppendMatchesMarshal(t *testing.T) {
	in := SCTXParse{Txid: "tx", Scid: "scid", Entrypoint: "E", Method: MethodInstallSC, Sender: "sender", Fees: 7, Height: 8}
	a := in.MarshalTurboTyped()
	b := in.MarshalTurboTypedAppend(nil)
	if !bytes.Equal(a, b) {
		t.Fatalf("MarshalTurboTyped vs append differ:\n  Marshal: %x\n  Append:  %x", a, b)
	}
}

func TestSCTXParse_TurboTagRejectsMsgpack(t *testing.T) {
	in := SCTXParse{Txid: "tx", Scid: "scid", Entrypoint: "E", Method: MethodInstallSC}
	mp, err := msgpack.Marshal(&in)
	if err != nil {
		t.Fatal(err)
	}
	if IsSCTXParseTurboTyped(mp) {
		t.Fatalf("msgpack blob mis-identified as typed: first byte = 0x%02x", mp[0])
	}
	var got SCTXParse
	if err := got.UnmarshalTurboTyped(mp); err == nil {
		t.Fatalf("UnmarshalTurboTyped accepted msgpack bytes (first byte 0x%02x)", mp[0])
	}
}

// ---------- benchmarks ----------

var benchEntry = AddrSCIDEntry{FirstHeight: 6927000, LastHeight: 6927500, Count: 3381}

// Package-level sinks: the fixed-size typed codecs inline completely, so even
// inside b.Loop a discarded result stack-allocates and the body folds to a
// no-op (~0.1 ns/op). Storing to a global keeps the measured work real.
var (
	benchSinkBytes []byte
	benchSinkEntry AddrSCIDEntry
)

// BenchmarkAddrSCIDEntry_Marshal_Msgpack is the current baseline.
func BenchmarkAddrSCIDEntry_Marshal_Msgpack(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		_, err := msgpack.Marshal(&benchEntry)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAddrSCIDEntry_Marshal_Typed is the candidate.
func BenchmarkAddrSCIDEntry_Marshal_Typed(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		benchSinkBytes = benchEntry.MarshalTyped()
	}
}

// BenchmarkAddrSCIDEntry_Marshal_TypedAppend uses a reusable buffer
// (the shape FlushBatch will actually use).
func BenchmarkAddrSCIDEntry_Marshal_TypedAppend(b *testing.B) {
	buf := make([]byte, 0, EncodedAddrSCIDEntrySize)
	b.ReportAllocs()
	for b.Loop() {
		buf = benchEntry.MarshalTypedAppend(buf[:0])
	}
}

func BenchmarkAddrSCIDEntry_Unmarshal_Msgpack(b *testing.B) {
	blob, _ := msgpack.Marshal(&benchEntry)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		var e AddrSCIDEntry
		if err := msgpack.Unmarshal(blob, &e); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAddrSCIDEntry_Unmarshal_Typed(b *testing.B) {
	blob := benchEntry.MarshalTyped()
	b.ReportAllocs()
	for b.Loop() {
		var e AddrSCIDEntry
		if err := e.UnmarshalTyped(blob); err != nil {
			b.Fatal(err)
		}
		benchSinkEntry = e
	}
}
