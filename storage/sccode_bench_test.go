package storage

import (
	"fmt"
	"strings"
	"testing"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/hypergnomon/hypergnomon/structures"
)

// benchSCCodeSizes is the parameter sweep for code-size-sensitive benchmarks.
// 256 B: tiny NFA/nameservice contract. 2 KB: median DVM contract.
// 16 KB: fat TELA-INDEX-1 app. 128 KB: pathological TELA-DOC-1 (embedded JS).
var benchSCCodeSizes = []int{256, 2 * 1024, 16 * 1024, 128 * 1024}

func makeSCCodeEntry(size int) *structures.SCCodeEntry {
	return &structures.SCCodeEntry{
		Code:          strings.Repeat("x", size),
		InstallHeight: 1_234_567,
	}
}

// Msgpack-baseline benchmarks are kept around so any future revert from the
// typed encoding is measurable against the same harness. The production
// path uses MarshalTyped / UnmarshalTyped.
func BenchmarkSCCode_Marshal_Msgpack(b *testing.B) {
	for _, sz := range benchSCCodeSizes {
		b.Run(fmt.Sprintf("size=%d", sz), func(b *testing.B) {
			entry := makeSCCodeEntry(sz)
			b.ReportAllocs()
			b.SetBytes(int64(sz))
			for i := 0; i < b.N; i++ {
				if _, err := msgpack.Marshal(entry); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkSCCode_Unmarshal_Msgpack(b *testing.B) {
	for _, sz := range benchSCCodeSizes {
		b.Run(fmt.Sprintf("size=%d", sz), func(b *testing.B) {
			entry := makeSCCodeEntry(sz)
			blob, err := msgpack.Marshal(entry)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(sz))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var out structures.SCCodeEntry
				if err := msgpack.Unmarshal(blob, &out); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSCCode_Marshal_Typed is the production path. Should be strictly
// faster + fewer allocs than the msgpack baseline — especially at small
// sizes where msgpack's framing dominates.
func BenchmarkSCCode_Marshal_Typed(b *testing.B) {
	for _, sz := range benchSCCodeSizes {
		b.Run(fmt.Sprintf("size=%d", sz), func(b *testing.B) {
			entry := makeSCCodeEntry(sz)
			b.ReportAllocs()
			b.SetBytes(int64(sz))
			for i := 0; i < b.N; i++ {
				_ = entry.MarshalTyped()
			}
		})
	}
}

func BenchmarkSCCode_Unmarshal_Typed(b *testing.B) {
	for _, sz := range benchSCCodeSizes {
		b.Run(fmt.Sprintf("size=%d", sz), func(b *testing.B) {
			entry := makeSCCodeEntry(sz)
			blob := entry.MarshalTyped()
			b.ReportAllocs()
			b.SetBytes(int64(sz))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var out structures.SCCodeEntry
				if err := out.UnmarshalTyped(blob); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkBboltStore_PutSCCode(b *testing.B) {
	for _, sz := range benchSCCodeSizes {
		b.Run(fmt.Sprintf("size=%d", sz), func(b *testing.B) {
			store := openTestStore(b)
			entry := makeSCCodeEntry(sz)
			scid := fakeSCID()
			b.ReportAllocs()
			b.SetBytes(int64(sz))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := store.PutSCCode(scid, entry); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkBboltStore_GetSCCode(b *testing.B) {
	for _, sz := range benchSCCodeSizes {
		b.Run(fmt.Sprintf("size=%d", sz), func(b *testing.B) {
			store := openTestStore(b)
			scid := fakeSCID()
			if err := store.PutSCCode(scid, makeSCCodeEntry(sz)); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(sz))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				entry, err := store.GetSCCode(scid)
				if err != nil {
					b.Fatal(err)
				}
				if entry == nil {
					b.Fatal("nil on hit")
				}
			}
		})
	}
}

// BenchmarkWriteBatch_AddSCCode measures the per-install cost of feeding a
// code snapshot into the pooled WriteBatch. This is what the hot install
// path pays at each of the four populate sites.
func BenchmarkWriteBatch_AddSCCode(b *testing.B) {
	code := strings.Repeat("x", 2048)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		batch := NewWriteBatch()
		batch.AddSCCode("scid", 123, code)
		PutWriteBatch(batch)
	}
}

// BenchmarkFlushBatch_WithSCCodes parameterized by how many SC codes land in
// one batch. Proves the atomic-commit cost scales linearly with N and
// establishes the per-code baseline for the commit path.
func BenchmarkFlushBatch_WithSCCodes(b *testing.B) {
	for _, n := range []int{1, 16, 128} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			store := openTestStore(b)
			code := strings.Repeat("x", 2048)
			// Pre-generate scids so the inner loop measures the flush, not
			// the fakeSCID helper.
			scids := make([]string, n)
			for i := range scids {
				scids[i] = fakeSCID()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := NewWriteBatch()
				for _, s := range scids {
					batch.AddSCCode(s, 1000, code)
				}
				batch.LastHeight = 1000
				if err := store.FlushBatch(batch); err != nil {
					b.Fatal(err)
				}
				PutWriteBatch(batch)
			}
		})
	}
}
