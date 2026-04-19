package structures

import (
	"bytes"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestSCIDVariables_TypedRoundTrip(t *testing.T) {
	in := []*SCIDVariable{
		{Key: "nameHdr", Value: "MyTELA"},
		{Key: "balance", Value: uint64(12345)},
		{Key: "owner", Value: "dero1qy..."},
		{Key: "height", Value: uint64(6927000)},
		{Key: "", Value: ""},
	}
	buf := MarshalSCIDVariablesTypedAppend(nil, in)
	if buf[0] != TagSCIDVariablesV1 {
		t.Fatalf("tag: got %#x want %#x", buf[0], TagSCIDVariablesV1)
	}
	out, err := UnmarshalSCIDVariablesTyped(buf)
	if err != nil {
		t.Fatalf("UnmarshalSCIDVariablesTyped: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("count: got %d want %d", len(out), len(in))
	}
	for i, v := range in {
		if out[i].Key != v.Key {
			t.Fatalf("[%d] Key: got %v want %v", i, out[i].Key, v.Key)
		}
		if out[i].Value != v.Value {
			t.Fatalf("[%d] Value: got %v want %v", i, out[i].Value, v.Value)
		}
	}
}

func TestSCIDVariables_TagNoOverlapMsgpack(t *testing.T) {
	// msgpack encodes a slice as an array — first byte 0x90-0x9f (fixarray)
	// or 0xdc/0xdd (array 16/32). Tag 0x02 doesn't overlap any of those.
	in := []*SCIDVariable{{Key: "k", Value: "v"}}
	mp, err := msgpack.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if IsSCIDVariablesTyped(mp) {
		t.Fatalf("msgpack mis-identified as typed: first byte 0x%02x", mp[0])
	}
	if TagSCIDVariablesV1 == mp[0] {
		t.Fatalf("tag collides with msgpack array header 0x%02x", mp[0])
	}
}

func TestSCIDVariables_RejectsMsgpack(t *testing.T) {
	in := []*SCIDVariable{{Key: "k", Value: "v"}}
	mp, _ := msgpack.Marshal(in)
	if _, err := UnmarshalSCIDVariablesTyped(mp); err == nil {
		t.Fatal("UnmarshalSCIDVariablesTyped accepted msgpack bytes")
	}
}

func TestSCIDVariables_EmptySlice(t *testing.T) {
	buf := MarshalSCIDVariablesTypedAppend(nil, nil)
	out, err := UnmarshalSCIDVariablesTyped(buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 0 {
		t.Fatalf("expected empty slice, got len %d", len(out))
	}
}

func TestSCIDVariables_UnknownTypeFallback(t *testing.T) {
	// int64 should get stringified into a string field, preserving slice shape.
	in := []*SCIDVariable{
		{Key: "count", Value: int64(42)},
	}
	buf := MarshalSCIDVariablesTypedAppend(nil, in)
	out, err := UnmarshalSCIDVariablesTyped(buf)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("len: got %d want 1", len(out))
	}
	if out[0].Value != "42" {
		t.Fatalf("fallback encoding: got %v want '42'", out[0].Value)
	}
}

func TestSCIDVariables_AppendPreservesPrefix(t *testing.T) {
	prefix := []byte("PFX")
	buf := MarshalSCIDVariablesTypedAppend(prefix, []*SCIDVariable{{Key: "k", Value: "v"}})
	if !bytes.HasPrefix(buf, prefix) {
		t.Fatalf("prefix not preserved: %q", buf)
	}
}

// ------- Benchmarks -------

var benchVars = []*SCIDVariable{
	{Key: "nameHdr", Value: "TELA-INDEX-1 Test App"},
	{Key: "descrHdr", Value: "A longer description field used by TELA INDEX apps"},
	{Key: "iconURLHdr", Value: "https://example.com/icon.png"},
	{Key: "telaVersion", Value: "1.0.0"},
	{Key: "balance", Value: uint64(1000000)},
	{Key: "owner", Value: "dero1qyjjxxaabbccddeeff0011223344"},
}

func BenchmarkSCIDVariables_Marshal_Msgpack(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		_, err := msgpack.Marshal(benchVars)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSCIDVariables_Marshal_Typed(b *testing.B) {
	buf := make([]byte, 0, 256)
	b.ReportAllocs()
	for range b.N {
		buf = buf[:0]
		buf = MarshalSCIDVariablesTypedAppend(buf, benchVars)
	}
	_ = buf
}

func BenchmarkSCIDVariables_Unmarshal_Msgpack(b *testing.B) {
	blob, _ := msgpack.Marshal(benchVars)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		var vars []*SCIDVariable
		if err := msgpack.Unmarshal(blob, &vars); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSCIDVariables_Unmarshal_Typed(b *testing.B) {
	blob := MarshalSCIDVariablesTypedAppend(nil, benchVars)
	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		if _, err := UnmarshalSCIDVariablesTyped(blob); err != nil {
			b.Fatal(err)
		}
	}
}
