package storage

import (
	"fmt"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"

	"github.com/hypergnomon/hypergnomon/structures"
)

// benchClassMetaSample is shared by the storage-level benches so the
// encoding comparison isn't muddied by field-count variance.
var benchClassMetaSample = &structures.ClassMeta{
	Class:         "TELA-INDEX-1",
	Tags:          []string{"all", "tela"},
	Name:          "Benchmark App",
	Desc:          "A standard-shape TELA record",
	IconURL:       "https://example.com/icon.png",
	DURL:          "bench.tela",
	Version:       "1.0.0",
	InstallHeight: 6_927_000,
	LastHeight:    6_927_500,
}

// BenchmarkBboltStore_GetSCIDClass_Typed measures the production read path
// (typed record on disk, typed decoder). This is what every API handler
// pays on a ClassMeta lookup after the upgrade window closes.
func BenchmarkBboltStore_GetSCIDClass_Typed(b *testing.B) {
	store := openTestStore(b)
	scid := fakeSCID()
	err := store.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketClassIdx).Put([]byte(scid), benchClassMetaSample.MarshalTyped())
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := store.GetSCIDClass(scid)
		if err != nil {
			b.Fatal(err)
		}
		if got == nil {
			b.Fatal("nil on hit")
		}
	}
}

// BenchmarkBboltStore_GetSCIDClass_LegacyMsgpack measures the fallback
// read path for legacy records. Matters during the upgrade window; a
// regression here would slow down every first read on a legacy DB.
func BenchmarkBboltStore_GetSCIDClass_LegacyMsgpack(b *testing.B) {
	store := openTestStore(b)
	scid := fakeSCID()
	blob, err := msgpack.Marshal(benchClassMetaSample)
	if err != nil {
		b.Fatal(err)
	}
	err = store.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketClassIdx).Put([]byte(scid), blob)
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := store.GetSCIDClass(scid)
		if err != nil {
			b.Fatal(err)
		}
		if got == nil {
			b.Fatal("nil on hit")
		}
	}
}

// BenchmarkFlushBatch_WithClassMeta parameterized by batch size. Measures
// the combined cost of classIdx + classPrefix writes under typed encoding
// at sizes that bracket realistic scanner flushes (1=solo install,
// 16=small batch, 128=dense fastsync flush).
func BenchmarkFlushBatch_WithClassMeta(b *testing.B) {
	for _, n := range []int{1, 16, 128} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			store := openTestStore(b)
			scids := make([]string, n)
			for i := range scids {
				scids[i] = fakeSCID()
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := NewWriteBatch()
				for j, s := range scids {
					batch.AddClass(s, &structures.ClassMeta{
						Class:         "TELA-INDEX-1",
						Tags:          []string{"all", "tela"},
						Name:          fmt.Sprintf("App %d", j),
						InstallHeight: int64(1000 + j),
						LastHeight:    int64(1000 + j),
					})
				}
				batch.LastHeight = int64(1000 + n)
				if err := store.FlushBatch(batch); err != nil {
					b.Fatal(err)
				}
				PutWriteBatch(batch)
			}
		})
	}
}
