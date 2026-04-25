package indexer

import (
	"strings"
	"testing"
)

// ClassifySC is called once per new SC install. It walks a small rule
// table (~15 substring matchers) and optionally parses a G45 JSON
// metadata blob. These benches bound the worst-case cost per install
// so a future rule-table regression is visible immediately.
//
// Four sub-benchmarks cover the realistic classification outcomes:
// TELA-INDEX (hit near the middle of the rule table), TELA-DOC
// (similar), G45-NFA (hit near the top), and Miss (full table walk,
// no branded rule matches, triggers the DERO-ASSET fallback check).

func benchCodeWith(header string) string {
	// 4 KiB of boilerplate DVM code surrounding the class header keeps
	// the rule walker's substring search honest — rules use
	// strings.Contains, so a too-small fixture would unfairly short-
	// circuit the scan.
	filler := strings.Repeat("// boilerplate DVM code line\n", 128)
	return filler + header + "\n" + filler
}

var (
	classifyBenchCodeTELAIndex = benchCodeWith(`STORE("telaVersion", "1.1.0")`)
	classifyBenchCodeTELADoc   = benchCodeWith(`STORE("docVersion", "1.1.0")`)
	classifyBenchCodeG45NFA    = benchCodeWith(`"scType": "ART-NFA-MS1"`)
	classifyBenchCodeMiss      = strings.Repeat("// just comments\n", 256) +
		`InitializePrivate()` + "\n" +
		strings.Repeat("// more comments\n", 256)
)

var classifyBenchVars = map[string]interface{}{
	"nameHdr":        "BenchApp",
	"descrHdr":       "A classify-bench fixture",
	"iconURLHdr":     "https://example.com/icon.png",
	"var_header_url": "https://bench.tela",
}

func benchClassify(b *testing.B, code string) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ClassifySC("deadbeef0001", code, classifyBenchVars)
	}
}

func BenchmarkClassifySC_TELAIndex(b *testing.B) {
	benchClassify(b, classifyBenchCodeTELAIndex)
}

func BenchmarkClassifySC_TELADoc(b *testing.B) {
	benchClassify(b, classifyBenchCodeTELADoc)
}

func BenchmarkClassifySC_G45NFA(b *testing.B) {
	benchClassify(b, classifyBenchCodeG45NFA)
}

// BenchmarkClassifySC_Miss is the worst case: the rule walker
// traverses every rule before giving up, then the DERO-ASSET fallback
// runs two more strings.Contains calls. If per-install latency ever
// regresses, this is the bench that will show it first.
func BenchmarkClassifySC_Miss(b *testing.B) {
	benchClassify(b, classifyBenchCodeMiss)
}
