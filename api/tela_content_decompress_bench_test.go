package api

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"strings"
	"testing"
)

// These benches bound the per-request cost of the TELA content-
// server hot paths shipped in v1.0's canonical-spec correctness pass
// (see api/tela_content.go). Each is called once per /tela/{scid}/…
// request on a cache miss — microsecond-scale work dominates until
// cache-hit wins.

// buildTELAGzipFixture produces a realistic on-wire TELA `.gz`
// payload: the original body is gzipped, then base64-encoded. Matches
// civilware/tela/compression.go encode contract.
func buildTELAGzipFixture(body []byte) []byte {
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	_, _ = zw.Write(body)
	_ = zw.Close()
	return []byte(base64.StdEncoding.EncodeToString(gz.Bytes()))
}

var benchTELAGzipPayload = buildTELAGzipFixture(
	[]byte(strings.Repeat("abcdefghij", 512)), // ~5 KiB synthetic HTML body
)

func BenchmarkDecompressTELAGzip(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(benchTELAGzipPayload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := decompressTELAGzip(benchTELAGzipPayload)
		if err != nil || len(out) == 0 {
			b.Fatalf("decompress: out=%d err=%v", len(out), err)
		}
	}
}

// Canonical-shape DOC source: leading DVM prelude, trailing
// `/* ... */` block wrapping the payload body, TrimSpace applied per
// civilware/tela/parseDocCode. Body is ~3.7 KiB to match the live
// algo4.html fixture size.
var benchDOCSource = "Function InitializePrivate()\n\treturn 0\nEnd Function\n/*\n" +
	strings.Repeat("<p>benchmark body line</p>\n", 128) + "*/\n"

func BenchmarkExtractDOCBodyFromSource(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(benchDOCSource)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := extractDOCBodyFromSource(benchDOCSource)
		if err != nil || len(out) == 0 {
			b.Fatalf("extractDOC: out=%d err=%v", len(out), err)
		}
	}
}

// DocShard source uses strict `/*\n…\n*/` framing with NO TrimSpace
// (per civilware/tela/parseDocShardCode). The fixture reflects that
// exact framing so the bench measures the canonical path.
var benchDocShardSource = "Function InitializePrivate()\n\treturn 0\nEnd Function\n/*\n" +
	strings.Repeat("shard-payload-line-data\n", 192) + "*/"

func BenchmarkExtractDocShardBodyFromSource(b *testing.B) {
	b.ReportAllocs()
	b.SetBytes(int64(len(benchDocShardSource)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out, err := extractDocShardBodyFromSource(benchDocShardSource)
		if err != nil || len(out) == 0 {
			b.Fatalf("extractDocShard: out=%d err=%v", len(out), err)
		}
	}
}

// Two shapes for the hex-unwrap: one that's valid printable-ASCII hex
// (expected to decode) and one that looks like hex but would produce
// non-printable bytes (falls through unchanged). The latter is the
// common case for Schnorr signature components.

var (
	benchHexPrintable    = "48656c6c6f20576f726c64" // "Hello World"
	benchHexNonPrintable = strings.Repeat("ab", 32) // decodes to bytes 0xAB — non-printable
)

func BenchmarkDecodeHexIfPrintableASCII_Printable(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = decodeHexIfPrintableASCII(benchHexPrintable)
	}
}

func BenchmarkDecodeHexIfPrintableASCII_Passthrough(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = decodeHexIfPrintableASCII(benchHexNonPrintable)
	}
}
