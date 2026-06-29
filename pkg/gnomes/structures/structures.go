// Package structures is the civilware/Gnomon compat surface for shared
// types. Consumers who previously imported
// `github.com/civilware/Gnomon/structures` can rewrite to
// `github.com/hypergnomon/hypergnomon/pkg/gnomes/structures` with no
// additional source changes.
//
// Each exported type either aliases an internal HyperGnomon type (when
// the wire shape matches) or declares a fresh struct with the exact
// civilware shape and provides conversion helpers. Consumers see the
// civilware-documented types; HyperGnomon's internals retain their own
// arena-oriented shape.
//
// v1.0 scope: covers the surface HOLOGRAM imports. Expand as other
// embedders surface missing types.
package structures

import (
	hgstructures "github.com/hypergnomon/hypergnomon/structures"
)

// SCIDVariable is a single key/value pair read from a smart contract's
// STORE. The shape matches civilware's exactly — both fields are
// `interface{}` because DVM STORE values can be either a DERO `Uint64`
// or a `String` (byte sequence, often hex-encoded over RPC).
//
// Type-aliased to hypergnomon/structures.SCIDVariable so a facade call
// path can return the exact same pointer a civilware caller would
// expect. Alias (not new-type) because changing the struct identity
// would break `interface{}` assertions downstream consumers may have
// baked into their code.
type SCIDVariable = hgstructures.SCIDVariable

// FastSyncConfig controls how a newly-created Indexer bootstraps its
// state from the on-chain GnomonSC registry. Mirrors civilware's
// `structures.FastSyncConfig` byte-for-byte so consumers can construct
// one and hand it to `NewIndexer` unchanged.
//
// Field semantics follow civilware's documentation; HyperGnomon's
// internal fastsync path translates to its own Config when NewIndexer
// is called.
type FastSyncConfig struct {
	// Enabled toggles the whole fastsync phase. Off means the indexer
	// scans from height 0 (or StartHeight).
	Enabled bool
	// SkipFSRecheck: when true, skip the per-SCID GetSC revalidation
	// civilware normally runs after registry enumeration. Trusts the
	// GnomonSC registry's data integrity, per civilware/Gnomon #16.
	SkipFSRecheck bool
	// ForceFastSync: runs fastsync even if the indexer already has
	// LastIndexedHeight close to chain tip. Civilware uses this to
	// re-probe for newly-registered SCIDs on request.
	ForceFastSync bool
	// ForceFastSyncDiff: minimum height delta from tip at which
	// fastsync activates. Default 100 per civilware convention.
	ForceFastSyncDiff int64
	// NoCode: when true, the fastsync code probe is skipped and SCs
	// are registered without classification. Civilware wallet+asset
	// runmodes use this to avoid the probe cost.
	NoCode bool
}

// FastSyncImport is one entry handed to (*indexer.Indexer).AddSCIDToIndex to
// inject a specific SCID into the index (civilware's manual-add path, used by
// HOLOGRAM to index a SCID fastsync missed). Mirrors civilware's
// structures.FastSyncImport byte-for-byte so a consumer can build the
// map[string]*FastSyncImport unchanged. HyperGnomon's IndexSingleSCID
// re-extracts the owner itself, so Owner/Height/Headers are advisory here.
type FastSyncImport struct {
	Owner   string
	Height  uint64
	Headers string
}
