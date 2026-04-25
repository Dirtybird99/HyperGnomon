package structures

import (
	"reflect"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

func TestClassMeta_MsgpackRoundtrip_WithTELAFields(t *testing.T) {
	in := ClassMeta{
		Class:         "TELA-INDEX-1",
		Tags:          []string{"all", "tela"},
		Name:          "Hypergnomon",
		Desc:          "root",
		IconURL:       "https://x.y/icon.png",
		DURL:          "hypergnomon.tela",
		Version:       "1.2.3",
		InstallHeight: 100,
		LastHeight:    2000,
	}
	buf, err := msgpack.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out ClassMeta
	if err := msgpack.Unmarshal(buf, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !reflect.DeepEqual(out, in) {
		t.Fatalf("mismatch:\n got: %+v\nwant: %+v", out, in)
	}
}

func TestClassMeta_TypedRoundTrip(t *testing.T) {
	in := ClassMeta{
		Class:         "TELA-INDEX-1",
		Tags:          []string{"all", "tela"},
		Name:          "Hypergnomon",
		Desc:          "Arena-accelerated indexer",
		IconURL:       "https://example.test/icon.png",
		DURL:          "hypergnomon.tela",
		Version:       "1.2.3",
		InstallHeight: 100,
		LastHeight:    2000,
	}
	blob := in.MarshalTyped()
	if !IsClassMetaTyped(blob) {
		t.Fatalf("IsClassMetaTyped returned false for freshly marshaled record")
	}
	var out ClassMeta
	if err := out.UnmarshalTyped(blob); err != nil {
		t.Fatalf("UnmarshalTyped: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round-trip mismatch:\n got: %+v\nwant: %+v", out, in)
	}
}

func TestClassMeta_TypedRoundTrip_OmitEmpty(t *testing.T) {
	// TELA-DOC-1 records commonly have empty DURL + Version; confirm the
	// uvarint-0-bytes encoding of empty strings decodes back to "".
	in := ClassMeta{
		Class:         "TELA-DOC-1",
		Tags:          []string{"all", "tela"},
		InstallHeight: 10,
		LastHeight:    11,
	}
	blob := in.MarshalTyped()
	var out ClassMeta
	if err := out.UnmarshalTyped(blob); err != nil {
		t.Fatalf("UnmarshalTyped: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("omit-empty round-trip mismatch:\n got: %+v\nwant: %+v", out, in)
	}
	if out.DURL != "" || out.Version != "" || out.Name != "" {
		t.Fatalf("empty strings did not round-trip as empty: %+v", out)
	}
}

func TestClassMeta_TypedRejectsShortBuffer(t *testing.T) {
	var out ClassMeta
	// Tag byte only, no heights — must fail.
	if err := out.UnmarshalTyped([]byte{TagClassMetaV1}); err == nil {
		t.Fatal("expected error on truncated buffer")
	}
	// Wrong tag — must fail.
	if err := out.UnmarshalTyped([]byte{0x80, 0, 0, 0, 0, 0, 0, 0, 0}); err == nil {
		t.Fatal("expected error on non-typed tag")
	}
	// Zero-length input — must fail.
	if err := out.UnmarshalTyped(nil); err == nil {
		t.Fatal("expected error on nil input")
	}
}

func TestClassMeta_TagDispatch_DecodesLegacyMsgpack(t *testing.T) {
	// Simulate the bucket-dispatch path in storage/bbolt.go: given a legacy
	// msgpack-encoded ClassMeta, IsClassMetaTyped must return false and a
	// msgpack.Unmarshal must succeed.
	legacy := ClassMeta{
		Class:         "NFA",
		Tags:          []string{"all", "nfa"},
		Name:          "Old record",
		InstallHeight: 42,
		LastHeight:    43,
	}
	blob, err := msgpack.Marshal(&legacy)
	if err != nil {
		t.Fatalf("msgpack marshal: %v", err)
	}
	if IsClassMetaTyped(blob) {
		t.Fatalf("IsClassMetaTyped returned true for msgpack record (tag byte 0x%02x)", blob[0])
	}
	var out ClassMeta
	if err := msgpack.Unmarshal(blob, &out); err != nil {
		t.Fatalf("msgpack unmarshal: %v", err)
	}
	if !reflect.DeepEqual(legacy, out) {
		t.Fatalf("legacy round-trip mismatch:\n got: %+v\nwant: %+v", out, legacy)
	}
}

func TestClassMeta_MsgpackRoundtrip_EmptyTELAFields(t *testing.T) {
	// Previously-written ClassMeta without DURL/Version must still decode.
	// omitempty on both new fields means old payloads have no entries for
	// them, so the zero value should remain intact.
	in := ClassMeta{
		Class:         "NFA",
		Tags:          []string{"all", "nfa"},
		Name:          "Test NFA",
		InstallHeight: 10,
		LastHeight:    11,
	}
	buf, err := msgpack.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out ClassMeta
	if err := msgpack.Unmarshal(buf, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.DURL != "" || out.Version != "" {
		t.Fatalf("omitempty violated: DURL=%q Version=%q", out.DURL, out.Version)
	}
	if out.Class != in.Class || out.Name != in.Name {
		t.Fatalf("non-TELA fields mangled: %+v", out)
	}
}
