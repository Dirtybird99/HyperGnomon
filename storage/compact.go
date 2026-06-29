package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

// CompactResult reports the outcome of CompactBbolt.
type CompactResult struct {
	Before     int64  // source file size before compaction (bytes)
	After      int64  // compacted file size (bytes)
	BackupPath string // the renamed original, kept for operator rollback
}

// CompactBbolt rewrites the main bbolt store under dbDir via bolt.Compact,
// dropping free/fragmented pages to reclaim disk and improve page locality, then
// atomically swaps the compacted file in. The original is kept as a ".bak"
// sibling so an operator can roll back; the caller deletes it once the compacted
// DB is verified.
//
// The store MUST be offline: a running indexer holds bbolt's exclusive lock, so
// this opens the source read-only with a short timeout and fails fast (rather
// than blocking forever) if the DB is still in use. The compacted file is NOT
// pre-extended to the main store's mmap reservation, so on disk it is its true
// size; the next NewBboltStore open re-applies the 256 MiB reservation (on
// Windows that re-extends the file — see NewBboltStoreWithMmap).
func CompactBbolt(dbDir string, txMaxSize int64) (CompactResult, error) {
	var res CompactResult
	src := filepath.Join(dbDir, mainStoreFile)

	fi, err := os.Stat(src)
	if err != nil {
		return res, fmt.Errorf("stat %s: %w", src, err)
	}
	res.Before = fi.Size()

	// Read-only + short timeout: if the indexer is running it holds the lock and
	// we want a clear, fast error instead of hanging.
	srcDB, err := bolt.Open(src, 0600, &bolt.Options{ReadOnly: true, Timeout: 5 * time.Second})
	if err != nil {
		return res, fmt.Errorf("open %s (is the indexer running? stop it first): %w", src, err)
	}

	tmp := src + ".compact-tmp"
	_ = os.Remove(tmp) // clear any stale tmp from a prior aborted run
	dstDB, err := bolt.Open(tmp, 0600, nil)
	if err != nil {
		_ = srcDB.Close()
		return res, fmt.Errorf("create %s: %w", tmp, err)
	}

	if err := bolt.Compact(dstDB, srcDB, txMaxSize); err != nil {
		_ = dstDB.Close()
		_ = srcDB.Close()
		_ = os.Remove(tmp)
		return res, fmt.Errorf("compact: %w", err)
	}
	if err := dstDB.Close(); err != nil {
		_ = srcDB.Close()
		_ = os.Remove(tmp)
		return res, fmt.Errorf("close compacted db: %w", err)
	}
	if err := srcDB.Close(); err != nil {
		_ = os.Remove(tmp)
		return res, fmt.Errorf("close source db: %w", err)
	}

	if fi, err := os.Stat(tmp); err == nil {
		res.After = fi.Size()
	}

	// Atomic swap: src -> .bak, tmp -> src; roll back if the second rename fails.
	bak := src + ".bak"
	_ = os.Remove(bak) // a previous compaction's backup, if any
	if err := os.Rename(src, bak); err != nil {
		_ = os.Remove(tmp)
		return res, fmt.Errorf("back up %s: %w", src, err)
	}
	if err := os.Rename(tmp, src); err != nil {
		_ = os.Rename(bak, src) // roll back to the original
		return res, fmt.Errorf("swap compacted db in: %w", err)
	}
	res.BackupPath = bak
	return res, nil
}
