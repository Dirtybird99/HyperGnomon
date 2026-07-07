package indexer

import (
	"testing"

	"github.com/deroproject/derohe/block"
	"github.com/deroproject/derohe/cryptography/crypto"

	"github.com/hypergnomon/hypergnomon/storage"
)

// hexHash returns a valid 64-char lowercase-hex string suitable for use as a
// stored block hash. crypto.Hash.String() renders %064x, so feeding the same
// hex back through HashHexToHash round-trips exactly.
func hexHash(fill byte) string {
	var h crypto.Hash
	for i := range h {
		h[i] = fill
	}
	return h.String()
}

// seedBlockHash persists a single block hash at the given height using the
// normal WriteBatch path (the same one the scan pipeline uses).
func seedBlockHash(t *testing.T, s storage.Storage, height int64, hash string) {
	t.Helper()
	batch := storage.NewWriteBatch()
	defer storage.PutWriteBatch(batch)
	batch.AddBlockHash(height, hash)
	batch.LastHeight = height
	if err := s.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}
}

// TestReorgDetectedCount verifies the exported getter reflects onReorgDetected
// increments. This is the value surfaced in the API /getstats response.
func TestReorgDetectedCount(t *testing.T) {
	idx := &Indexer{}
	if got := idx.ReorgDetectedCount(); got != 0 {
		t.Fatalf("fresh indexer ReorgDetectedCount = %d, want 0", got)
	}
	idx.onReorgDetected(41, 42)
	idx.onReorgDetected(42, 43)
	if got := idx.ReorgDetectedCount(); got != 2 {
		t.Fatalf("ReorgDetectedCount after 2 detections = %d, want 2", got)
	}
}

// TestCheckReorgForBlock exercises the shared detection helper that BOTH the
// catch-up batch path and the live speculative single-block path now call. A
// diverging parent tip must fire onReorgDetected; a matching tip, empty tips,
// and a missing h-1 anchor must not.
func TestCheckReorgForBlock(t *testing.T) {
	s, err := storage.NewBboltStore(t.TempDir(), "")
	if err != nil {
		t.Fatalf("NewBboltStore: %v", err)
	}
	defer s.Close()

	const h = int64(100)
	stored := hexHash(0x11)   // the hash we have committed for height h-1
	diverged := hexHash(0x22) // a different parent → reorg
	seedBlockHash(t, s, h-1, stored)

	idx := &Indexer{Store: s}

	// Matching parent tip → no detection.
	match := block.Block{Tips: []crypto.Hash{crypto.HashHexToHash(stored)}}
	idx.checkReorgForBlock(h, &match)
	if got := idx.ReorgDetectedCount(); got != 0 {
		t.Fatalf("matching parent fired detection: count=%d, want 0", got)
	}

	// Diverging parent tip → exactly one detection.
	mismatch := block.Block{Tips: []crypto.Hash{crypto.HashHexToHash(diverged)}}
	idx.checkReorgForBlock(h, &mismatch)
	if got := idx.ReorgDetectedCount(); got != 1 {
		t.Fatalf("diverging parent: count=%d, want 1", got)
	}

	// Empty Tips (genesis / malformed) → skipped, no change.
	empty := block.Block{}
	idx.checkReorgForBlock(h, &empty)
	if got := idx.ReorgDetectedCount(); got != 1 {
		t.Fatalf("empty tips changed count: count=%d, want 1", got)
	}

	// No stored hash at h-1 (here height 200 has no anchor at 199) →
	// undetermined, treated as ok, no detection.
	idx.checkReorgForBlock(200, &mismatch)
	if got := idx.ReorgDetectedCount(); got != 1 {
		t.Fatalf("missing h-1 anchor fired detection: count=%d, want 1", got)
	}
}
