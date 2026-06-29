package indexer

import (
	"strings"
	"testing"

	"github.com/hypergnomon/hypergnomon/storage"
)

// TestNew_InjectedStoreBorrowed verifies external-store injection (Config.Store):
// New uses the provided store instead of opening its own, and treats it as
// borrowed — a later failure (here, an unreachable RPC endpoint) must NOT close
// the caller's store. This is the keystone the pkg/gnomes compat shim relies on
// to hand HyperGnomon a consumer's already-open bbolt store.
func TestNew_InjectedStoreBorrowed(t *testing.T) {
	s, err := storage.NewBboltStore(t.TempDir(), "")
	if err != nil {
		t.Fatalf("NewBboltStore: %v", err)
	}
	defer s.Close()

	// Unreachable endpoint → New fails at the RPC pool, AFTER accepting the
	// injected store. The error must therefore be about the pool, not opening a
	// store (proving storage.Open was skipped).
	_, err = New(Config{Store: s, Endpoint: "127.0.0.1:1", PoolSize: 1})
	if err == nil {
		t.Fatal("expected New to fail on an unreachable RPC endpoint")
	}
	if strings.Contains(err.Error(), "open store") {
		t.Fatalf("New opened its own store despite injection: %v", err)
	}

	// The injected store must still be usable — New must not have closed a store
	// it does not own.
	if _, err := s.GetLastIndexHeight(); err != nil {
		t.Fatalf("injected store was closed by New (%v); the caller owns it", err)
	}
}
