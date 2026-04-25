package indexer

import (
	"testing"

	"github.com/hypergnomon/hypergnomon/storage"
)

// newTestIndexerShared constructs an Indexer + bbolt store wired to a
// tempdir, usable from both correctness tests and benchmarks. testing.TB is
// the common interface for *testing.T and *testing.B.
func newTestIndexerShared(tb testing.TB) *Indexer {
	tb.Helper()
	store, err := storage.NewBboltStore(tb.TempDir(), "")
	if err != nil {
		tb.Fatalf("NewBboltStore: %v", err)
	}
	tb.Cleanup(func() { store.Close() })
	idx := &Indexer{Store: store}
	idx.ChainHeight.Store(9999)
	return idx
}
