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

// ---------- benchmarks ----------

var benchEntry = AddrSCIDEntry{FirstHeight: 6927000, LastHeight: 6927500, Count: 3381}

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
	for range b.N {
		_ = benchEntry.MarshalTyped()
	}
}

// BenchmarkAddrSCIDEntry_Marshal_TypedAppend uses a reusable buffer
// (the shape FlushBatch will actually use).
func BenchmarkAddrSCIDEntry_Marshal_TypedAppend(b *testing.B) {
	buf := make([]byte, 0, EncodedAddrSCIDEntrySize)
	b.ReportAllocs()
	for range b.N {
		buf = buf[:0]
		buf = benchEntry.MarshalTypedAppend(buf)
	}
	_ = buf
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
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		var e AddrSCIDEntry
		if err := e.UnmarshalTyped(blob); err != nil {
			b.Fatal(err)
		}
	}
}
