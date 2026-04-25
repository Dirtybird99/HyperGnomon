package api

import (
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// TestReadTELASigFields_Present covers the canonical DOC with both
// fileCheckC and fileCheckS stored as hex-encoded hex strings. The
// daemon's RPC layer returns STORE strings as hex; our extractor
// transparently hex-decodes if the inner bytes are printable-ASCII hex.
func TestReadTELASigFields_Present(t *testing.T) {
	vars := []*structures.SCIDVariable{
		// Owner address (hex-encoded ASCII of "dero1qyjjx...").
		{Key: "owner", Value: hexEncode("dero1qyjjxxaabbccddeeff0011223344556677889900aabbccddee00112233445566")},
		{Key: "fileCheckC", Value: "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"},
		{Key: "fileCheckS", Value: "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"},
		{Key: "var_header_name", Value: hexEncode("index.html")},
	}
	got := readTELASigFields(vars)
	if !got.Present {
		t.Fatalf("expected Present=true, got %+v", got)
	}
	if got.CHex != "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789" {
		t.Errorf("CHex mismatch: %q", got.CHex)
	}
	if got.SHex != "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210" {
		t.Errorf("SHex mismatch: %q", got.SHex)
	}
	if got.Owner != "dero1qyjjxxaabbccddeeff0011223344556677889900aabbccddee00112233445566" {
		t.Errorf("Owner mismatch: %q", got.Owner)
	}
}

// TestReadTELASigFields_Absent: DOC without signature fields — Present=false.
func TestReadTELASigFields_Absent(t *testing.T) {
	vars := []*structures.SCIDVariable{
		{Key: "var_header_name", Value: hexEncode("plain.html")},
		{Key: "docType", Value: hexEncode("TELA-HTML-1")},
	}
	got := readTELASigFields(vars)
	if got.Present {
		t.Fatalf("expected Present=false, got %+v", got)
	}
}

// TestReadTELASigFields_ShortS verifies we accept sub-64-nibble S values,
// which Agent B's live fixture produces (63 nibbles). DERO big.Int hex
// prints via %x, which strips leading zero nibbles.
func TestReadTELASigFields_ShortS(t *testing.T) {
	vars := []*structures.SCIDVariable{
		{Key: "fileCheckC", Value: "1234567890123456789012345678901234567890123456789012345678901234"},
		{Key: "fileCheckS", Value: "123456789012345678901234567890123456789012345678901234567890123"}, // 63 nibbles
		{Key: "owner", Value: hexEncode("dero1qyshort000000000000000000000000000000000000000000000000000000a")},
	}
	got := readTELASigFields(vars)
	if !got.Present {
		t.Fatalf("short S rejected as absent: %+v", got)
	}
	if len(got.SHex) != 63 {
		t.Fatalf("SHex length = %d, want 63", len(got.SHex))
	}
}

// TestVerifyTELASignature_Stub documents the v1.0 contract: returns
// (false, nil) for any signature. Ensures callers surface as
// `signed-unverified` not `failed`.
func TestVerifyTELASignature_Stub(t *testing.T) {
	ok, err := verifyTELASignature([]byte("body"), telaSigFields{
		CHex:    "abc",
		SHex:    "def",
		Owner:   "dero1qy...",
		Present: true,
	})
	if err != nil {
		t.Fatalf("stub returned error: %v", err)
	}
	if ok {
		t.Fatal("stub returned ok=true; v1.0 must always return false")
	}
}

// hexEncode mimics derod's RPC-layer transformation of DVM string STORE
// values: every STORE string is returned as fmt.Sprintf("%x", bytes).
// Used to set up realistic var fixtures.
func hexEncode(s string) string {
	const hexdigits = "0123456789abcdef"
	out := make([]byte, 0, 2*len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		out = append(out, hexdigits[c>>4], hexdigits[c&0x0f])
	}
	return string(out)
}
