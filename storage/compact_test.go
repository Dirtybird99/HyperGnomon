package storage

import (
	"fmt"
	"os"
	"testing"
)

// TestCompactBbolt is the gate for the `hypergnomon compact` subcommand: after
// compaction the DB must still open and serve its surviving data, the file must
// not grow, and a rollback backup must be left behind.
func TestCompactBbolt(t *testing.T) {
	dir := t.TempDir()

	// Populate a chunk of data, then drop the index so its pages become free —
	// the fragmentation compaction is meant to reclaim.
	s, err := NewBboltStore(dir, "")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	b := NewWriteBatch()
	for i := 0; i < 2000; i++ {
		b.AddOwner(fmt.Sprintf("%064x", i), "dero1qchurn")
	}
	b.LastHeight = 1
	if err := s.FlushBatch(b); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}
	PutWriteBatch(b)
	if err := s.ResetIndex(); err != nil { // frees the data pages
		t.Fatalf("ResetIndex: %v", err)
	}
	keep := "de" + randHex(31)
	if err := s.StoreOwner(keep, "dero1qkeep"); err != nil { // something must survive
		t.Fatalf("StoreOwner: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	res, err := CompactBbolt(dir, 64<<20)
	if err != nil {
		t.Fatalf("CompactBbolt: %v", err)
	}
	if res.After > res.Before {
		t.Errorf("compacted file grew: before=%d after=%d", res.Before, res.After)
	}
	if res.BackupPath == "" {
		t.Fatal("no backup path returned")
	}
	if _, err := os.Stat(res.BackupPath); err != nil {
		t.Errorf("rollback backup missing: %v", err)
	}

	// The compacted DB must reopen cleanly and still serve the surviving owner.
	s2, err := NewBboltStore(dir, "")
	if err != nil {
		t.Fatalf("reopen compacted: %v", err)
	}
	defer s2.Close()
	if got, err := s2.GetOwner(keep); err != nil || got != "dero1qkeep" {
		t.Errorf("GetOwner after compact = %q, %v; want dero1qkeep", got, err)
	}
}

// TestCompactBbolt_Missing reports a clear error when the store does not exist.
func TestCompactBbolt_Missing(t *testing.T) {
	if _, err := CompactBbolt(t.TempDir(), 64<<20); err == nil {
		t.Fatal("expected an error compacting a non-existent store")
	}
}
