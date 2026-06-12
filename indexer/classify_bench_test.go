package indexer

import (
	"fmt"
	"strings"
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// classifyBenchSink is a package-level sink: assigning bench results here
// defeats dead-code elimination when the function under test inlines
// (project rule: no fake sub-ns numbers).
var classifyBenchSink SCClass

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

var classifyBenchVarSlice = []*structures.SCIDVariable{
	{Key: "nameHdr", Value: "BenchApp"},
	{Key: "descrHdr", Value: "A classify-bench fixture"},
	{Key: "iconURLHdr", Value: "https://example.com/icon.png"},
	{Key: "var_header_url", Value: "https://bench.tela"},
}

// classifyBenchVarSliceNoisy mirrors a real mainnet TELA-INDEX-1 variable
// set: a handful of keys extractClassVars actually cares about, drowned in
// the noise every live INDEX carries — likes/dislikes counters, per-address
// rating entries (uint64 values), and DOC pointer SCIDs. The original
// 4-string fixture could not see the cost of eagerly stringifying every
// value (audit #7); this one can.
var classifyBenchVarSliceNoisy = func() []*structures.SCIDVariable {
	vars := []*structures.SCIDVariable{
		{Key: "var_header_name", Value: "BenchApp"},
		{Key: "var_header_description", Value: "A classify-bench fixture"},
		{Key: "var_header_icon", Value: "https://example.com/icon.png"},
		{Key: "dURL", Value: "benchapp.tela"},
		{Key: "mods", Value: "VSOO, TXDWA"},
		{Key: "likes", Value: uint64(412)},
		{Key: "dislikes", Value: uint64(3)},
	}
	for i := 0; i < 12; i++ {
		vars = append(vars, &structures.SCIDVariable{
			Key:   fmt.Sprintf("dero1qy%058d", i),
			Value: uint64(i % 100),
		})
	}
	for i := 1; i <= 6; i++ {
		vars = append(vars, &structures.SCIDVariable{
			Key:   fmt.Sprintf("DOC%d", i),
			Value: strings.Repeat("ab", 32),
		})
	}
	return vars
}()

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

func BenchmarkClassifySCVars_TELAIndex(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ClassifySCVars("deadbeef0001", classifyBenchCodeTELAIndex, classifyBenchVarSlice)
	}
}

func BenchmarkClassifySC_MapBuild_TELAIndex(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ClassifySC("deadbeef0001", classifyBenchCodeTELAIndex, varsToMap(classifyBenchVarSlice))
	}
}

// BenchmarkClassifySCVars_NoisyTELAIndex is the audit #7 before/after
// gauge: a realistic 25-var INDEX where only ~5 keys match the extractor's
// switch. Pre-fix, every uint64 noise value paid an fmt.Sprintf.
func BenchmarkClassifySCVars_NoisyTELAIndex(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		classifyBenchSink = ClassifySCVars("deadbeef0001", classifyBenchCodeTELAIndex, classifyBenchVarSliceNoisy)
	}
}

// BenchmarkClassifyPhase2PerSCID_Legacy replicates the pre-fix fastsync
// phase-2 / tela_refresher per-SCID cost: a code-less ClassifySCVars walk
// PLUS a second+third walk inside telaFieldsForClassVars (audit #8 before).
func BenchmarkClassifyPhase2PerSCID_Legacy(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		sc := ClassifySCVars("deadbeef0001", "", classifyBenchVarSliceNoisy)
		durl, version := telaFieldsForClassVars("TELA-INDEX-1", classifyBenchVarSliceNoisy)
		sc.DURL, sc.Version = durl, version
		classifyBenchSink = sc
	}
}

// BenchmarkClassifyPhase2PerSCID_WithClass is the post-fix per-SCID cost
// (audit #8 after): one ClassifySCVarsWithClass call, one variable walk.
func BenchmarkClassifyPhase2PerSCID_WithClass(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		classifyBenchSink = ClassifySCVarsWithClass("deadbeef0001", "TELA-INDEX-1", classifyBenchVarSliceNoisy)
	}
}
