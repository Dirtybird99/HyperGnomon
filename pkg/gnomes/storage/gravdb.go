package storage

import "errors"

// ErrGravDBNotSupported is returned by every operation on GravitonStore
// when called against a HyperGnomon build. HyperGnomon is bbolt-only
// by design — the graviton backend civilware ships has a known
// unresolved-for-two-years size-bloat bug (civilware/Gnomon #24) and
// both simple-gnomon forks explicitly dropped it.
//
// The type exists only so consumers who import `storage.GravitonStore`
// for typed method signatures (most commonly through the
// `indexer.Indexer.GravDBBackend` field in HOLOGRAM's code) still
// compile. Any call returns this error; consumers that actually
// invoke a method get a clear message, not a crash.
var ErrGravDBNotSupported = errors.New("hypergnomon: graviton backend is not supported; use the bbolt store instead")

// GravitonStore is a civilware-shape type that errors on every method.
// Exposed so source that references the type compiles; any attempt to
// call methods surfaces ErrGravDBNotSupported.
type GravitonStore struct{}

// NewGravDB returns an ErrGravDBNotSupported error immediately. Callers
// who hit this should switch to NewBBoltDB.
func NewGravDB(path, interval string) (*GravitonStore, error) {
	return nil, ErrGravDBNotSupported
}

// Close is a no-op so defer g.Close() compiles and runs without panic.
func (g *GravitonStore) Close() error { return nil }

// GetLastIndexHeight always returns (0, ErrGravDBNotSupported).
func (g *GravitonStore) GetLastIndexHeight() (int64, error) {
	return 0, ErrGravDBNotSupported
}

// GetAllOwnersAndSCIDs returns an empty map (civilware's shape has no
// error return; the "store unusable" signal is the empty result).
func (g *GravitonStore) GetAllOwnersAndSCIDs() map[string]string {
	return map[string]string{}
}

// GetAllSCIDVariableDetails returns nil.
func (g *GravitonStore) GetAllSCIDVariableDetails(scid string) []interface{} {
	return nil
}

// GetSCIDValuesByKey returns empty slices.
func (g *GravitonStore) GetSCIDValuesByKey(scid, key string, height int64, any bool) ([]string, []int64) {
	return nil, nil
}

// GetSCIDKeysByValue returns empty slices.
func (g *GravitonStore) GetSCIDKeysByValue(scid, value string, height int64, any bool) ([]string, []int64) {
	return nil, nil
}

// GetSCIDInteractionHeight returns nil.
func (g *GravitonStore) GetSCIDInteractionHeight(scid string) []int64 {
	return nil
}
