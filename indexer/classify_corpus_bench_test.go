package indexer

import "testing"

// Corpus benchmarks — part of the classify-corpus measurement apparatus
// (IMMUTABLE to optimization experiments, like the loader/gates in
// classify_corpus_test.go).
//
// Each b.Loop() iteration classifies the ENTIRE corpus, so allocs/op is the
// exact allocation total of one full pass — deterministic and immune to
// per-op rounding (same shape as BenchmarkProcessNormalTx). The optimize
// loop keys keep/revert off Full only; the rest attribute wins:
//
//   - NFTs / Collections: which corpus half moved.
//   - CodeOnly: ClassifySC(scid, code, nil) — the turbo-install / fastsync
//     probe shape (indexer.go:982) where cost is the rules scan + Tags
//     alloc, no metadata parse. Scan-path changes show up here.

func benchClassifyCorpus(b *testing.B, entries []corpusEntry) {
	b.ReportAllocs()
	for b.Loop() {
		for i := range entries {
			e := &entries[i]
			classifyBenchSink = ClassifySCVars(e.SCID, e.Code, e.Vars)
		}
	}
	b.ReportMetric(float64(len(entries)), "scs")
}

func BenchmarkClassifyCorpus(b *testing.B) {
	cols, nfts := mustCorpus(b)
	all := corpusAll(b)

	b.Run("Full", func(b *testing.B) { benchClassifyCorpus(b, all) })
	b.Run("NFTs", func(b *testing.B) { benchClassifyCorpus(b, nfts) })
	b.Run("Collections", func(b *testing.B) { benchClassifyCorpus(b, cols) })
	b.Run("CodeOnly", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			for i := range all {
				e := &all[i]
				classifyBenchSink = ClassifySC(e.SCID, e.Code, nil)
			}
		}
		b.ReportMetric(float64(len(all)), "scs")
	})
}
