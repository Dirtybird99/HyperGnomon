package dbbench

import (
	"bytes"
	"testing"
)

// Workload sizes. Kept modest so the suite runs in seconds on a dev box while
// still making the range-scan disparity stark (graviton touches all readM keys
// to answer a rangeW-wide window).
const (
	writeBatchN = 1000  // pairs committed per batchWrite (FlushBatch unit)
	readM       = 50000 // keys pre-loaded for the read benchmarks
	rangeW      = 500   // width of the height window a RangeScan selects
)

// populate writes pairs into e in chunked batches (mirrors batched flushing).
func populate(tb testing.TB, e engine, pairs []kv, chunk int) {
	tb.Helper()
	for i := 0; i < len(pairs); i += chunk {
		end := i + chunk
		if end > len(pairs) {
			end = len(pairs)
		}
		if err := e.batchWrite(pairs[i:end]); err != nil {
			tb.Fatalf("populate: %v", err)
		}
	}
}

// openPopulated opens a fresh engine, loads pairs, then CLOSES and REOPENS it so
// every read benchmark times against a cold, committed-on-disk store — uniform
// across engines and free of any read-after-commit caching quirk.
func openPopulated(b *testing.B, f engineFactory, pairs []kv) (engine, func()) {
	b.Helper()
	dir := b.TempDir()
	e, err := f.open(dir)
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	populate(b, e, pairs, 10000)
	if err := e.close(); err != nil {
		b.Fatalf("close after populate: %v", err)
	}
	e, err = f.open(dir)
	if err != nil {
		b.Fatalf("reopen: %v", err)
	}
	return e, func() { _ = e.close() }
}

// TestEnginesRoundTrip is the fairness gate: every engine must persist and return
// byte-identical data for the same workload, or the benchmark numbers are
// meaningless (a no-op store would look infinitely fast). The benchmarks below are
// only trustworthy if this passes.
func TestEnginesRoundTrip(t *testing.T) {
	const n = 500
	pairs := heightPairs(n)
	const lo, hi = 100, 200
	const wantRange = hi - lo // 100 entries: heights 100..199

	type snap struct {
		getVal []byte
		rng    int
		all    int
	}
	got := map[string]snap{}

	for _, f := range engineFactories() {
		t.Run(f.name, func(t *testing.T) {
			dir := t.TempDir()
			e, err := f.open(dir)
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			if err := e.batchWrite(pairs); err != nil {
				t.Fatalf("batchWrite: %v", err)
			}
			if err := e.close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			e, err = f.open(dir) // reopen for a clean read view
			if err != nil {
				t.Fatalf("reopen: %v", err)
			}
			defer e.close()

			// present key returns the exact bytes written
			v, err := e.get(heightKey(250))
			if err != nil {
				t.Fatalf("get: %v", err)
			}
			if !bytes.Equal(v, makeValue(250, 64)) {
				t.Fatalf("get(heightKey(250)) wrong/empty value (len=%d)", len(v))
			}
			// absent key returns (nil, nil)
			if mv, err := e.get(scidKey(999999)); err != nil || mv != nil {
				t.Fatalf("get(absent) = %v, %v; want nil, nil", mv, err)
			}
			// ordered window selects exactly wantRange entries
			rng, err := e.rangeScan(heightKey(lo), heightKey(hi))
			if err != nil {
				t.Fatalf("rangeScan: %v", err)
			}
			if rng != wantRange {
				t.Errorf("rangeScan = %d; want %d", rng, wantRange)
			}
			// full scan sees every entry
			all, err := e.scanAll()
			if err != nil {
				t.Fatalf("scanAll: %v", err)
			}
			if all != n {
				t.Errorf("scanAll = %d; want %d", all, n)
			}
			got[f.name] = snap{getVal: v, rng: rng, all: all}
		})
	}

	// cross-engine agreement: all engines must return identical results.
	var refName string
	var ref snap
	for name, s := range got {
		if refName == "" {
			refName, ref = name, s
			continue
		}
		if !bytes.Equal(s.getVal, ref.getVal) || s.rng != ref.rng || s.all != ref.all {
			t.Errorf("engine %q disagrees with %q: get/rng/all = (len %d, %d, %d) vs (len %d, %d, %d)",
				name, refName, len(s.getVal), s.rng, s.all, len(ref.getVal), ref.rng, ref.all)
		}
	}
}

// BenchmarkBatchWrite — the FlushBatch hot path: commit writeBatchN pairs in one
// transaction. Two value widths: 256 B (mixed records) and 2 KB (variable
// snapshots, the write-byte dominator). Rotating 8 distinct batches keeps every
// commit doing real work without growing the working set.
func BenchmarkBatchWrite(b *testing.B) {
	cases := []struct {
		name    string
		valSize int
	}{
		{"small_256B", 256},
		{"large_2KB", 2048},
	}
	for _, tc := range cases {
		for _, f := range engineFactories() {
			b.Run(tc.name+"/"+f.name, func(b *testing.B) {
				batches := rotatingBatches(8, writeBatchN, tc.valSize)
				e, err := f.open(b.TempDir())
				if err != nil {
					b.Fatalf("open: %v", err)
				}
				defer e.close()
				if err := e.batchWrite(batches[0]); err != nil { // warm
					b.Fatalf("warm batchWrite: %v", err)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if err := e.batchWrite(batches[i&7]); err != nil {
						b.Fatalf("batchWrite: %v", err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(writeBatchN), "keys/op")
			})
		}
	}
}

// BenchmarkPointRead — random point lookup over readM keys (GetOwner/GetSCIDClass).
func BenchmarkPointRead(b *testing.B) {
	pairs := pointPairs(readM, 80)
	for _, f := range engineFactories() {
		b.Run(f.name, func(b *testing.B) {
			e, done := openPopulated(b, f, pairs)
			defer done()
			if v, err := e.get(scidKey(0)); err != nil || v == nil {
				b.Fatalf("setup check: get(scidKey(0)) = %v, %v", v, err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				v, err := e.get(scidKey(i % readM))
				if err != nil {
					b.Fatalf("get: %v", err)
				}
				sink += len(v)
			}
		})
	}
}

// BenchmarkRangeScan — the decisive op. bbolt/sqlite seek the window in O(rangeW);
// graviton has no ordered range so it traverses all readM keys and filters — O(N).
func BenchmarkRangeScan(b *testing.B) {
	pairs := heightPairs(readM)
	for _, f := range engineFactories() {
		b.Run(f.name, func(b *testing.B) {
			e, done := openPopulated(b, f, pairs)
			defer done()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				h := (i * 97) % (readM - rangeW)
				n, err := e.rangeScan(heightKey(h), heightKey(h+rangeW))
				if err != nil {
					b.Fatalf("rangeScan: %v", err)
				}
				if n != rangeW {
					b.Fatalf("rangeScan = %d; want %d", n, rangeW)
				}
			}
			b.ReportMetric(float64(rangeW), "rows/op")
		})
	}
}

// BenchmarkScanAll — full iteration over readM keys (GetAllSCIDs).
func BenchmarkScanAll(b *testing.B) {
	pairs := heightPairs(readM)
	for _, f := range engineFactories() {
		b.Run(f.name, func(b *testing.B) {
			e, done := openPopulated(b, f, pairs)
			defer done()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				n, err := e.scanAll()
				if err != nil {
					b.Fatalf("scanAll: %v", err)
				}
				if n != readM {
					b.Fatalf("scanAll = %d; want %d", n, readM)
				}
			}
			b.ReportMetric(float64(readM), "rows/op")
		})
	}
}
