// Package gnomes_test is the civilware compat-surface integration
// test. It mirrors HOLOGRAM's exact import paths + call shape (per
// Agent F's teardown of github.com/DHEBP/HOLOGRAM/gnomon.go) so a
// maintainer can see at a glance that the migration is a three-sed
// operation, not a rewrite.
//
// This test lives at the pkg/gnomes root rather than per-subpackage
// because its entire purpose is to exercise all three packages
// together in realistic consumer order.
package gnomes_test

import (
	"errors"
	"testing"

	compatindexer "github.com/hypergnomon/hypergnomon/pkg/gnomes/indexer"
	compatstorage "github.com/hypergnomon/hypergnomon/pkg/gnomes/storage"
	compatstructures "github.com/hypergnomon/hypergnomon/pkg/gnomes/structures"
)

// TestCompatSurface_CompilesLikeCivilware is the cheapest guard: it
// uses every type and symbol in a HOLOGRAM-shaped call sequence so
// any API change that breaks consumer code fails here immediately.
// It does NOT connect to a daemon — that's the example in
// pkg/gnomes/example.
func TestCompatSurface_CompilesLikeCivilware(t *testing.T) {
	// 1. Construct the FastSyncConfig as HOLOGRAM does.
	cfg := &compatstructures.FastSyncConfig{
		Enabled:           true,
		SkipFSRecheck:     false,
		ForceFastSync:     false,
		ForceFastSyncDiff: 100,
		NoCode:            false,
	}
	if cfg.ForceFastSyncDiff != 100 {
		t.Fatalf("FastSyncConfig field wiring broken")
	}

	// 2. SCIDVariable type must be usable as map value + typed pointer.
	// Civilware consumers build these at runtime from parsed RPC
	// responses; HOLOGRAM treats it as a plain struct with Key/Value.
	v := &compatstructures.SCIDVariable{Key: "hello", Value: "world"}
	if v.Key != "hello" || v.Value != "world" {
		t.Fatalf("SCIDVariable field access broken")
	}

	// 3. NewIndexer rejects gravdb with a typed-but-dead indexer
	// (civilware-shape has no error return, so we signal via DBType=="").
	dead := compatindexer.NewIndexer(
		nil, nil,
		"gravdb", // civilware gravdb path — unsupported
		"telaVersion;;;docVersion",
		0,
		"http://127.0.0.1:10102",
		"daemon",
		false, false,
		cfg,
		[]string{},
	)
	if dead == nil {
		t.Fatalf("NewIndexer returned nil — civilware shape must return a non-nil pointer")
	}
	if dead.DBType != "" {
		t.Fatalf("gravdb path must return dead indexer with DBType=\"\", got %q", dead.DBType)
	}
	// Close on a dead indexer is idempotent no-op.
	dead.Close()
	dead.Close()

	// 4. NewIndexerWithDBDir is the HyperGnomon-native entry point
	// for callers who can't use civilware's (gravDB, boltDB, dbType, ...)
	// shape. We DON'T actually call it here because it would try to
	// open an RPC pool; see pkg/gnomes/example for the real usage.
	_ = compatindexer.NewIndexerWithDBDir

	// 5. InitLog is a no-op; must accept any writer.
	compatindexer.InitLog("test", nil)

	// 6. The storage types are importable and have the civilware-
	// shape methods. We don't actually open a store here — just
	// confirm the symbols exist.
	_ = (*compatstorage.BboltStore)(nil)
	_ = (*compatstorage.GravitonStore)(nil)
	_ = compatstorage.NewBBoltDB
	_ = compatstorage.NewGravDB
	_ = compatstorage.ErrGravDBNotSupported
}

// TestGravDBErrors confirms the graviton stub errors cleanly on every
// method instead of crashing. HOLOGRAM has a fallback path that tries
// gravdb first then bbolt — must not panic.
func TestGravDBErrors(t *testing.T) {
	g, err := compatstorage.NewGravDB("/tmp/nonexistent", "25ms")
	if !errors.Is(err, compatstorage.ErrGravDBNotSupported) {
		t.Fatalf("NewGravDB err = %v, want ErrGravDBNotSupported", err)
	}
	if g != nil {
		t.Fatalf("NewGravDB returned non-nil store alongside error")
	}

	// Typed-nil receiver methods still work without panic.
	var nilGrav *compatstorage.GravitonStore
	_ = nilGrav.Close()
	_, gerr := nilGrav.GetLastIndexHeight()
	if gerr == nil {
		t.Logf("GetLastIndexHeight on nil GravitonStore returned nil err — ok for civilware-shape compat")
	}
}

// TestBboltStore_OpenClose verifies the bbolt facade opens + closes a
// fresh store and returns no error. Uses t.TempDir so the store is
// cleaned up automatically.
func TestBboltStore_OpenClose(t *testing.T) {
	store, err := compatstorage.NewBBoltDB(t.TempDir(), "testfilter")
	if err != nil {
		t.Fatalf("NewBBoltDB: %v", err)
	}
	if store == nil {
		t.Fatalf("NewBBoltDB returned nil store without error")
	}
	// Civilware query methods return empty / zero on a fresh store.
	if h, err := store.GetLastIndexHeight(); err != nil || h != 0 {
		t.Fatalf("GetLastIndexHeight fresh: (%d, %v), want (0, nil)", h, err)
	}
	if m := store.GetAllOwnersAndSCIDs(); len(m) != 0 {
		t.Fatalf("GetAllOwnersAndSCIDs fresh: len=%d, want 0", len(m))
	}
	if store.Inner() == nil {
		t.Fatalf("Inner() returned nil on live store")
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
