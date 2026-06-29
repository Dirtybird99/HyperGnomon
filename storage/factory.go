package storage

import (
	"errors"
	"fmt"
	"strings"
)

// Backend selection errors.
var (
	// ErrUnknownBackend is returned by Open for an unrecognized backend name.
	ErrUnknownBackend = errors.New("unknown storage backend")

	// ErrBackendNotImplemented marks a backend that is planned but not yet
	// shipped (currently: sqlite). The factory recognizes the name so the
	// selector and docs can advertise it, but Open refuses to construct it.
	ErrBackendNotImplemented = errors.New("storage backend not implemented yet")

	// ErrGravitonUnsupported marks the graviton backend as deliberately
	// unsupported. This is a library-API fact, not a tuning result: graviton's
	// own Cursor godoc states it traverses keys "in hash sorted order" and
	// exposes only First/Last/Next/Prev — there is no Seek or ordered Range. That
	// cannot serve HyperGnomon's key-ordered Route B scans (GetClassInstalls,
	// GetInstallsInRange, GetSCIDsByOwner, GetSCIDInteractionHeights,
	// GetAddressSCIDs — all built on BE8 height encoding); each collapses to an
	// O(N) full traversal (~758x slower than bbolt's seek; storage/dbbench).
	// Emulating ordered scans needs per-SCID/per-address index trees, the path
	// behind civilware/Gnomon issue #24 (bloat), which both simple-gnomon forks
	// and HyperGnomon dropped. bbolt's batch-flush also outperforms it
	// (storage/dbbench/RESULTS.md).
	ErrGravitonUnsupported = errors.New("graviton backend unsupported (hash-ordered iteration cannot serve key-ordered prefix/range scans; see civilware/Gnomon #24)")
)

// Open constructs a Storage for the named backend. It is the single seam where
// the backend is chosen — callers (indexer.New, library consumers) depend only
// on the returned Storage interface, never on a concrete store type.
//
// bbolt is the default and only shipped backend; sqlite is planned (returns
// ErrBackendNotImplemented); graviton is unsupported (ErrGravitonUnsupported).
// An empty backend name defaults to bbolt.
func Open(backend, dbDir, searchFilter string) (Storage, error) {
	if err := ValidateBackend(backend); err != nil {
		return nil, err
	}
	// Only bbolt (and its aliases / the empty default) validates to nil today.
	return NewBboltStore(dbDir, searchFilter)
}

// ValidateBackend reports whether Open would accept the backend name, WITHOUT
// opening anything. A CLI can call this right after flag parsing to fail fast
// with a clear message instead of surfacing the error deep inside a
// daemon-connection retry loop. Returns nil for bbolt (and its aliases / the
// empty default), and the matching sentinel otherwise.
func ValidateBackend(backend string) error {
	switch strings.ToLower(strings.TrimSpace(backend)) {
	case "", "bbolt", "bolt", "boltdb", "bbs":
		return nil
	case "sqlite", "sqlite3":
		return fmt.Errorf("%w: sqlite", ErrBackendNotImplemented)
	case "graviton", "gravdb":
		return ErrGravitonUnsupported
	default:
		return fmt.Errorf("%w: %q (valid: %s)", ErrUnknownBackend, backend, strings.Join(SupportedBackends(), ", "))
	}
}

// SupportedBackends lists the selectable backend names (for flag help and docs),
// in order of readiness: shipped, planned, unsupported.
func SupportedBackends() []string {
	return []string{"bbolt", "sqlite", "graviton"}
}
