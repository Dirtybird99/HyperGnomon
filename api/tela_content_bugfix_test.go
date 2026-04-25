package api

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"testing"
)

// TestDecompressTELAGzip covers the canonical civilware/tela compression.go
// on-wire format: base64(gzip(raw)). The body stored inside a TELA-DOC-1
// with `.gz` nameHdr is the base64 string; we must base64-decode then
// gunzip to recover the original bytes.
func TestDecompressTELAGzip(t *testing.T) {
	original := []byte("<html><body>tela body with padding\nand newlines</body></html>")

	// Build the on-wire payload: gzip, then base64.
	var gzBuf bytes.Buffer
	gw, err := gzip.NewWriterLevel(&gzBuf, gzip.BestCompression)
	if err != nil {
		t.Fatalf("gzip writer: %v", err)
	}
	if _, err := gw.Write(original); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	b64 := base64.StdEncoding.EncodeToString(gzBuf.Bytes())

	got, err := decompressTELAGzip([]byte(b64))
	if err != nil {
		t.Fatalf("decompressTELAGzip: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("round-trip mismatch:\n got %q\nwant %q", got, original)
	}
}

// TestDecompressTELAGzip_LeadingWhitespace covers the case where the
// /* … */ comment framing left leading whitespace inside the body. The
// base64 decoder rejects whitespace by default; our wrapper must tolerate.
func TestDecompressTELAGzip_LeadingWhitespace(t *testing.T) {
	original := []byte("hello tela")
	var gzBuf bytes.Buffer
	gw, _ := gzip.NewWriterLevel(&gzBuf, gzip.BestCompression)
	gw.Write(original)
	gw.Close()
	b64 := base64.StdEncoding.EncodeToString(gzBuf.Bytes())
	padded := "\n  " + b64 + "\n"

	got, err := decompressTELAGzip([]byte(padded))
	if err != nil {
		t.Fatalf("decompressTELAGzip with padding: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("mismatch: got %q, want %q", got, original)
	}
}

// TestDecompressTELAGzip_RawFallback: some fixtures or non-standard deploys
// may store raw gzip bytes without the base64 layer. Our wrapper should
// fall back to direct gunzip on base64-decode failure.
func TestDecompressTELAGzip_RawFallback(t *testing.T) {
	original := []byte("raw gzip fallback path")
	var gzBuf bytes.Buffer
	gw, _ := gzip.NewWriterLevel(&gzBuf, gzip.BestCompression)
	gw.Write(original)
	gw.Close()

	// Pass gzip bytes directly (not base64-wrapped). They contain bytes
	// outside the base64 alphabet, so base64.DecodeString will fail,
	// and the fallback gunzip path should recover the original.
	got, err := decompressTELAGzip(gzBuf.Bytes())
	if err != nil {
		t.Fatalf("decompressTELAGzip raw fallback: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("raw fallback mismatch: got %q, want %q", got, original)
	}
}

// TestExtractDocShardBodyFromSource verifies the strict DocShard framing
// per civilware/tela/tela.go:parseDocShardCode — `code[start+3:]` (skip
// `/*\n`) then `TrimSuffix("\n*/")`, no TrimSpace.
func TestExtractDocShardBodyFromSource(t *testing.T) {
	// DocShard source has exactly `/*\n<body>\n*/` framing. Body may
	// contain leading/trailing whitespace that must be preserved — shard
	// reassembly depends on exact byte-identity.
	src := "Function X() Uint64\n10 RETURN 0\nEnd Function\n/*\n  body with leading spaces  \n*/"
	got, err := extractDocShardBodyFromSource(src)
	if err != nil {
		t.Fatalf("extractDocShardBodyFromSource: %v", err)
	}
	want := "  body with leading spaces  "
	if string(got) != want {
		t.Fatalf("DocShard body = %q, want %q", got, want)
	}
}

// TestExtractDocShardBodyFromSource_NoMarker rejects DocShard source
// without the literal /*\n opener (vs extractDOCBodyFromSource which
// tolerates /* alone).
func TestExtractDocShardBodyFromSource_NoMarker(t *testing.T) {
	if _, err := extractDocShardBodyFromSource("Function X() Uint64 End Function"); err == nil {
		t.Fatal("expected error on missing /*\\n opener")
	}
}
