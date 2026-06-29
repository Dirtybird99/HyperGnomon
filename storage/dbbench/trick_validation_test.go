package dbbench

// Trick-validation bench (adapted from Loop Library 023, "self-improving champion
// loop"). Champion = HyperGnomon's per-row MERGE-ON-WRITE; challenger = siteraiser's
// APPEND-ONLY + AGGREGATE-ON-READ (DOCS/ECOSYSTEM_GNOMON.md §4 trick #2). We model
// the AddrSCID interaction index: each (addr,scid) touch at a height must yield
// (FirstHeight=min, LastHeight=max, Count=n). A trick is LEGIT only if it beats the
// champion on the write hot path without failing correctness or an unacceptable read
// regression. TestTrickCompaction validates steal-trick #1 (bbolt.Compact()).

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

var (
	champBucket = []byte("champ_addr_scids")  // merge-on-write: one row per (addr,scid)
	challBucket = []byte("chall_addr_events") // append-only: one row per interaction
)

type aggregate struct{ first, last, count uint64 }

func mvKey(addr, scid string) []byte { return []byte(addr + "|" + scid + "|") }

// --- champion: merge-on-write (read-modify-write min/max/sum) ---

func championWrite(b *bolt.Bucket, addr, scid string, height uint64) error {
	k := mvKey(addr, scid)
	agg := aggregate{first: height, last: height, count: 1}
	if v := b.Get(k); v != nil { // read-back + merge
		agg.first = binary.BigEndian.Uint64(v[0:8])
		agg.last = binary.BigEndian.Uint64(v[8:16])
		agg.count = binary.BigEndian.Uint64(v[16:24])
		if height < agg.first {
			agg.first = height
		}
		if height > agg.last {
			agg.last = height
		}
		agg.count++
	}
	var buf [24]byte
	binary.BigEndian.PutUint64(buf[0:8], agg.first)
	binary.BigEndian.PutUint64(buf[8:16], agg.last)
	binary.BigEndian.PutUint64(buf[16:24], agg.count)
	return b.Put(k, buf[:])
}

func championRead(b *bolt.Bucket, addr, scid string) (aggregate, bool) {
	v := b.Get(mvKey(addr, scid))
	if v == nil {
		return aggregate{}, false
	}
	return aggregate{
		first: binary.BigEndian.Uint64(v[0:8]),
		last:  binary.BigEndian.Uint64(v[8:16]),
		count: binary.BigEndian.Uint64(v[16:24]),
	}, true
}

// --- challenger: append-only (one immutable row per event), fold at read time ---

func challengerWrite(b *bolt.Bucket, addr, scid string, height, seq uint64) error {
	// key = addr|scid|<be8 seq> (unique); value = be8(height). No read-back.
	var sk [8]byte
	binary.BigEndian.PutUint64(sk[:], seq)
	k := append(mvKey(addr, scid), sk[:]...)
	var hv [8]byte
	binary.BigEndian.PutUint64(hv[:], height)
	return b.Put(k, hv[:])
}

func challengerRead(b *bolt.Bucket, addr, scid string) (aggregate, bool) {
	prefix := mvKey(addr, scid)
	c := b.Cursor()
	agg := aggregate{first: ^uint64(0)}
	found := false
	for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
		h := binary.BigEndian.Uint64(v)
		if h < agg.first {
			agg.first = h
		}
		if h > agg.last {
			agg.last = h
		}
		agg.count++
		found = true
	}
	if !found {
		return aggregate{}, false
	}
	return agg, true
}

// --- correctness gate: both strategies must agree, exactly ---

func TestTrickMergeVsAppendCorrectness(t *testing.T) {
	db, err := bolt.Open(filepath.Join(t.TempDir(), "tv.db"), 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists(champBucket)
		tx.CreateBucketIfNotExists(challBucket)
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Deterministic interaction stream: 3 addr-scid pairs, varied heights/counts.
	type ev struct {
		addr, scid string
		height     uint64
	}
	events := []ev{}
	pairs := [][2]string{{"addrA", "scid1"}, {"addrB", "scid1"}, {"addrA", "scid2"}}
	for i, p := range pairs {
		for j := 0; j < 5+i*7; j++ { // different depths per pair
			events = append(events, ev{p[0], p[1], uint64(100 + (j*13+i*5)%97)})
		}
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		cb, ab := tx.Bucket(champBucket), tx.Bucket(challBucket)
		for seq, e := range events {
			if err := championWrite(cb, e.addr, e.scid, e.height); err != nil {
				return err
			}
			if err := challengerWrite(ab, e.addr, e.scid, e.height, uint64(seq)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if err := db.View(func(tx *bolt.Tx) error {
		cb, ab := tx.Bucket(champBucket), tx.Bucket(challBucket)
		for _, p := range pairs {
			c, okc := championRead(cb, p[0], p[1])
			a, oka := challengerRead(ab, p[0], p[1])
			if okc != oka || c != a {
				t.Errorf("%s/%s disagree: champion=%+v(%v) challenger=%+v(%v)", p[0], p[1], c, okc, a, oka)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// --- write hot path: apply a batch of B interactions across K keys in one txn ---

const (
	tvBatch = 100 // interactions per txn (FlushBatch-shaped)
	tvKeys  = 20  // distinct (addr,scid) pairs the batch spreads across
)

func tvAddr(i int) string { return fmt.Sprintf("addr%012d", i) }
func tvScid(i int) string { return fmt.Sprintf("scid%012d", i) }

func openTV(b *testing.B) *bolt.DB {
	db, err := bolt.Open(filepath.Join(b.TempDir(), "tv.db"), 0o600, nil)
	if err != nil {
		b.Fatal(err)
	}
	db.NoSync = true // bulk profile, matches the indexer hot path
	if err := db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists(champBucket)
		tx.CreateBucketIfNotExists(challBucket)
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	return db
}

func BenchmarkAddrIndexWrite(b *testing.B) {
	b.Run("champion_merge", func(b *testing.B) {
		db := openTV(b)
		defer db.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := db.Update(func(tx *bolt.Tx) error {
				cb := tx.Bucket(champBucket)
				for j := 0; j < tvBatch; j++ {
					if err := championWrite(cb, tvAddr(j%tvKeys), tvScid(j%tvKeys), uint64(i*tvBatch+j)); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(tvBatch), "events/op")
	})
	b.Run("challenger_append", func(b *testing.B) {
		db := openTV(b)
		defer db.Close()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if err := db.Update(func(tx *bolt.Tx) error {
				ab := tx.Bucket(challBucket)
				for j := 0; j < tvBatch; j++ {
					seq := uint64(i*tvBatch + j)
					if err := challengerWrite(ab, tvAddr(j%tvKeys), tvScid(j%tvKeys), seq, seq); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				b.Fatal(err)
			}
		}
		b.StopTimer()
		b.ReportMetric(float64(tvBatch), "events/op")
	})
}

// --- read: fold the aggregate for one (addr,scid) at varying history depth ---

func BenchmarkAddrIndexRead(b *testing.B) {
	for _, depth := range []int{1, 10, 100, 1000} {
		// One key carrying `depth` interactions in each bucket.
		const addr, scid = "addrR", "scidR"
		db := openTV(b)
		if err := db.Update(func(tx *bolt.Tx) error {
			cb, ab := tx.Bucket(champBucket), tx.Bucket(challBucket)
			for s := 0; s < depth; s++ {
				if err := championWrite(cb, addr, scid, uint64(100+s)); err != nil {
					return err
				}
				if err := challengerWrite(ab, addr, scid, uint64(100+s), uint64(s)); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}

		b.Run(fmt.Sprintf("champion_merge/depth=%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = db.View(func(tx *bolt.Tx) error {
					a, _ := championRead(tx.Bucket(champBucket), addr, scid)
					sink += int(a.count)
					return nil
				})
			}
		})
		b.Run(fmt.Sprintf("challenger_append/depth=%d", depth), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = db.View(func(tx *bolt.Tx) error {
					a, _ := challengerRead(tx.Bucket(challBucket), addr, scid)
					sink += int(a.count)
					return nil
				})
			}
		})
		db.Close()
	}
}

// --- steal-trick #1 (one-shot, not a loop): does bbolt.Compact() reclaim space? ---

func TestTrickCompaction(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "src.db")
	db, err := bolt.Open(srcPath, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	bkt := []byte("churn")
	// Create fragmentation: write a lot, then delete most (frees pages in-place).
	if err := db.Update(func(tx *bolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists(bkt)
		val := make([]byte, 256)
		for i := 0; i < 50000; i++ {
			var k [8]byte
			binary.BigEndian.PutUint64(k[:], uint64(i))
			b.Put(k[:], val)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bkt)
		for i := 0; i < 50000; i++ {
			if i%5 != 0 { // delete 80% -> lots of free pages
				var k [8]byte
				binary.BigEndian.PutUint64(k[:], uint64(i))
				b.Delete(k[:])
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	db.Close()

	srcSize := fileSize(t, srcPath)

	// Compact into a fresh file (the steal-trick: scheduled bbolt.Compact()).
	src, err := bolt.Open(srcPath, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	dstPath := filepath.Join(dir, "compact.db")
	dst, err := bolt.Open(dstPath, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	if err := bolt.Compact(dst, src, 64*1024*1024); err != nil {
		t.Fatal(err)
	}
	dstSize := fileSize(t, dstPath)

	pct := 100 * float64(srcSize-dstSize) / float64(srcSize)
	t.Logf("compaction: src=%d bytes (%.1f MiB) -> dst=%d bytes (%.1f MiB); reclaimed %.1f%%",
		srcSize, float64(srcSize)/1048576, dstSize, float64(dstSize)/1048576, pct)
	if dstSize >= srcSize {
		t.Errorf("Compact did not shrink a fragmented DB (src=%d dst=%d)", srcSize, dstSize)
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return fi.Size()
}
