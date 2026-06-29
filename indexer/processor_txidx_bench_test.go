package indexer

import "testing"

// benchTxIdxSink defeats dead-code elimination of the dispatch accumulator.
var benchTxIdxSink int64

// benchTraverseMap isolates the PRE-change map-build + index-lookup cost (no
// per-TX decode/deserialize/dispatch, which this change does not touch).
// Mirrors traverseMap but accumulates an int instead of building a slice, so
// the only heap traffic is the presized map[string]int (~2 allocs/op).
func benchTraverseMap(fb *fetchedBatch) int64 {
	var acc int64
	txMap := make(map[string]int, len(fb.allTxHashes))
	for i, h := range fb.allTxHashes {
		txMap[h] = i
	}
	for _, bi := range fb.blocks {
		for _, hashStr := range bi.txHashes {
			txIdx, ok := txMap[hashStr]
			if !ok || txIdx >= len(fb.txResult.Txs_as_hex) {
				continue
			}
			acc += int64(txIdx) + bi.height + int64(len(hashStr))
		}
	}
	return acc
}

// benchTraverseCounter is the POST-change shape: a running counter, zero heap.
func benchTraverseCounter(fb *fetchedBatch) int64 {
	var acc int64
	txIdx := 0
	for _, bi := range fb.blocks {
		for _, hashStr := range bi.txHashes {
			ti := txIdx
			txIdx++
			if ti >= len(fb.txResult.Txs_as_hex) {
				continue
			}
			acc += int64(ti) + bi.height + int64(len(hashStr))
		}
	}
	return acc
}

// BenchmarkProcessorTxIdx_Map is the before gauge: the presized map[string]int
// over allTxHashes costs ~2 allocs/op (hmap header + bucket array), constant in
// N. buildTxIdxBatch lives in processor_txidx_test.go (same package).
func BenchmarkProcessorTxIdx_Map(b *testing.B) {
	fb := buildTxIdxBatch(8, 8, 64)
	b.ReportAllocs()
	for b.Loop() {
		benchTxIdxSink = benchTraverseMap(fb)
	}
}

// BenchmarkProcessorTxIdx_Counter is the after gauge: pure int arithmetic,
// 0 allocs/op.
func BenchmarkProcessorTxIdx_Counter(b *testing.B) {
	fb := buildTxIdxBatch(8, 8, 64)
	b.ReportAllocs()
	for b.Loop() {
		benchTxIdxSink = benchTraverseCounter(fb)
	}
}
