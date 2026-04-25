package structures

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// TestCryptoHash_TypedRoundTrip is the audit for civilware/Gnomon issue #2:
// a raw 32-byte TXID stored as an SC variable value round-tripped through
// the typed SCIDVariables encoder must return byte-identical bytes.
//
// Go strings are byte sequences without UTF-8 validation at the language
// level, so `append(dst, s...)` + `string(b[pos:end])` should preserve any
// byte pattern — including 0xef 0xbf 0xbd (the UTF-8 replacement) or
// genuinely invalid UTF-8 continuation sequences. This test exists so a
// future regression that adds utf8.Valid() or normalization breaks here
// instead of quietly corrupting mainnet data.
func TestCryptoHash_TypedRoundTrip(t *testing.T) {
	var hash [32]byte
	if _, err := rand.Read(hash[:]); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	// Force at least one byte to be > 0x7f so we're actually testing
	// non-ASCII/non-UTF-8 territory.
	hash[0] = 0xff
	hash[1] = 0x80

	in := []*SCIDVariable{
		{Key: "p_txid", Value: string(hash[:])},
	}
	blob := MarshalSCIDVariablesTypedAppend(nil, in)
	out, err := UnmarshalSCIDVariablesTyped(blob)
	if err != nil {
		t.Fatalf("UnmarshalSCIDVariablesTyped: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("slice len = %d, want 1", len(out))
	}
	gotStr, ok := out[0].Value.(string)
	if !ok {
		t.Fatalf("value type = %T, want string", out[0].Value)
	}
	got := []byte(gotStr)
	if !bytes.Equal(got, hash[:]) {
		t.Fatalf("round-trip mismatch:\n got %x\nwant %x", got, hash[:])
	}
}

// TestCryptoHash_CompositeKey covers the civilware #2 composite-key case:
// a key built from a constant prefix + TXID() that PR #20 left broken.
// Our storage keys are opaque byte strings so the compound works as long as
// the key encoder doesn't coerce to UTF-8.
func TestCryptoHash_CompositeKey(t *testing.T) {
	prefix := "p_"
	var hash [32]byte
	for i := range hash {
		hash[i] = byte(0x80 + i) // guaranteed non-ASCII
	}
	composite := prefix + string(hash[:])

	in := []*SCIDVariable{
		{Key: composite, Value: uint64(42)},
	}
	blob := MarshalSCIDVariablesTypedAppend(nil, in)
	out, err := UnmarshalSCIDVariablesTyped(blob)
	if err != nil {
		t.Fatalf("UnmarshalSCIDVariablesTyped: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("slice len = %d, want 1", len(out))
	}
	gotKey, ok := out[0].Key.(string)
	if !ok {
		t.Fatalf("key type = %T, want string", out[0].Key)
	}
	if gotKey != composite {
		t.Fatalf("composite key mismatch:\n got %q\nwant %q", []byte(gotKey), []byte(composite))
	}
}

// TestCryptoHash_MsgpackRoundTrip documents how msgpack handles the same
// bytes — as a reference point only. msgpack-v5 may or may not preserve
// non-UTF-8 depending on EncodeStringAsArray/DisallowUnknownFields tuning.
// We're not in that path on the hot write (typed encoding ships first) but
// if the test flips from PASS to FAIL on a future dep bump, we know to
// audit read-side legacy fallbacks.
func TestCryptoHash_MsgpackRoundTrip(t *testing.T) {
	var hash [32]byte
	for i := range hash {
		hash[i] = byte(i * 7) // deterministic mix of ASCII + high-bit
	}
	in := []*SCIDVariable{
		{Key: "raw_bytes", Value: string(hash[:])},
	}
	blob, err := msgpack.Marshal(in)
	if err != nil {
		t.Fatalf("msgpack.Marshal: %v", err)
	}
	var out []*SCIDVariable
	if err := msgpack.Unmarshal(blob, &out); err != nil {
		t.Fatalf("msgpack.Unmarshal: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("slice len = %d", len(out))
	}
	gotStr, _ := out[0].Value.(string)
	if gotStr != string(hash[:]) {
		t.Logf("msgpack round-trip lost bytes (documenting, not failing):\n got %x\nwant %x", []byte(gotStr), hash[:])
	}
}
