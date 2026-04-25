package api

import (
	"fmt"
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// fillTELACache pre-populates a cache with n distinct (scid, path) entries,
// split across 4 scids so invalidate-prefix benches have meaningful shapes.
func fillTELACache(c *telaContentCache, n int) {
	for i := 0; i < n; i++ {
		scid := fmt.Sprintf("scid%02d", i%4)
		path := fmt.Sprintf("p%d", i)
		c.Put(scid+"|"+path, &structures.TELAContentEntry{
			Body: make([]byte, 512),
		})
	}
}

// BenchmarkTELAContentCache_Get — hot-path cache read. Parameterized by
// fill size so we see whether LRU overhead grows with cache depth.
func BenchmarkTELAContentCache_Get(b *testing.B) {
	for _, fill := range []int{64, 1024, 8192} {
		b.Run(fmt.Sprintf("fill=%d", fill), func(b *testing.B) {
			c := newTELAContentCache(int64(fill * 2048))
			fillTELACache(c, fill)
			key := fmt.Sprintf("scid%02d|p%d", 0, fill/2)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if c.Get(key) == nil {
					b.Fatal("miss on expected hit")
				}
			}
		})
	}
}

// BenchmarkTELAContentCache_Put — write path including LRU insert and (if
// over cap) eviction. No-eviction run keeps cap generous; eviction run
// forces one eviction per Put.
func BenchmarkTELAContentCache_Put(b *testing.B) {
	b.Run("no-evict", func(b *testing.B) {
		c := newTELAContentCache(1 << 30) // effectively unbounded
		entry := &structures.TELAContentEntry{Body: make([]byte, 512)}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("k%d", i)
			c.Put(key, entry)
		}
	})
	b.Run("with-evict", func(b *testing.B) {
		c := newTELAContentCache(512 * 64) // 64 entries worth
		entry := &structures.TELAContentEntry{Body: make([]byte, 512)}
		fillTELACache(c, 64)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := fmt.Sprintf("evict%d", i)
			c.Put(key, entry)
		}
	})
}

// BenchmarkTELAContentCache_InvalidatePrefix — the invalidation path the
// event-bus hits on every EventInstall/EventVarChange. After the byScid
// index fix, cost is O(|entries for this scid|) independent of total fill,
// not O(fill). Benchmark proves that by sweeping fill at a fixed target
// size (2 entries per target scid = typical TELA INDEX + one child DOC).
//
// The timed loop refills two target entries and invalidates them. This is
// intentionally not hidden behind StopTimer: the benchmark harness may pick
// a huge b.N, and unbounded per-iteration setup outside the timer can make
// full-suite benchmark runs take minutes while reporting tiny ns/op values.
func BenchmarkTELAContentCache_InvalidatePrefix(b *testing.B) {
	for _, fill := range []int{64, 1024, 8192} {
		b.Run(fmt.Sprintf("fill=%d", fill), func(b *testing.B) {
			// Generous cap so background fill doesn't trigger eviction
			// mid-benchmark and confuse the measurement.
			c := newTELAContentCache(int64((fill + 2048) * 1024))
			fillTELACache(c, fill)
			entry := &structures.TELAContentEntry{Body: make([]byte, 512)}
			const targetCount = 1024
			targets := make([]string, targetCount)
			keysA := make([]string, targetCount)
			keysB := make([]string, targetCount)
			for i := 0; i < targetCount; i++ {
				targets[i] = fmt.Sprintf("target%d", i)
				keysA[i] = targets[i] + "|a"
				keysB[i] = targets[i] + "|b"
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				idx := i & (targetCount - 1)
				c.Put(keysA[idx], entry)
				c.Put(keysB[idx], entry)
				c.InvalidatePrefix(targets[idx])
			}
		})
	}
}
