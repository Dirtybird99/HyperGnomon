package dbbench

import (
	"bytes"
	"path/filepath"

	bolt "go.etcd.io/bbolt"
)

var benchBucket = []byte("bench")

type bboltEngine struct{ db *bolt.DB }

func openBbolt(dir string) (engine, error) {
	// Pre-map a large region up front so the bulk write never pauses to grow and
	// remap the file mid-batch (NoSync already skips fsync; remap was the residual stall).
	db, err := bolt.Open(filepath.Join(dir, "bench.db"), 0o600, &bolt.Options{InitialMmapSize: 256 << 20})
	if err != nil {
		return nil, err
	}
	if durability == "bulk" {
		db.NoSync = true // mirror the indexer's bulk-sync hot path
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(benchBucket)
		return e
	}); err != nil {
		db.Close()
		return nil, err
	}
	return &bboltEngine{db}, nil
}

func (e *bboltEngine) name() string { return "bbolt" }

func (e *bboltEngine) batchWrite(pairs []kv) error {
	return e.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(benchBucket)
		for _, p := range pairs {
			if err := b.Put(p.k, p.v); err != nil {
				return err
			}
		}
		return nil
	})
}

func (e *bboltEngine) get(key []byte) ([]byte, error) {
	var out []byte
	err := e.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(benchBucket).Get(key); v != nil {
			out = append([]byte(nil), v...) // copy: v is only valid in-txn
		}
		return nil
	})
	return out, err
}

func (e *bboltEngine) rangeScan(lo, hi []byte) (int, error) {
	n := 0
	err := e.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(benchBucket).Cursor()
		for k, v := c.Seek(lo); k != nil && bytes.Compare(k, hi) < 0; k, v = c.Next() {
			n++
			sink += len(v)
		}
		return nil
	})
	return n, err
}

func (e *bboltEngine) scanAll() (int, error) {
	n := 0
	err := e.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(benchBucket).Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			n++
			sink += len(v)
		}
		return nil
	})
	return n, err
}

func (e *bboltEngine) close() error { return e.db.Close() }
