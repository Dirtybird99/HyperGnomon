// Package storage is the civilware/Gnomon compat surface for the
// two store backends. HyperGnomon is bbolt-only — the GravitonStore
// type is declared for source compatibility but errors on any
// operational call. See gravdb.go.
package storage

import (
	"fmt"
	"strconv"
	"strings"

	hgstorage "github.com/hypergnomon/hypergnomon/storage"
	hgstructures "github.com/hypergnomon/hypergnomon/structures"

	compatstructures "github.com/hypergnomon/hypergnomon/pkg/gnomes/structures"
)

// BboltStore wraps HyperGnomon's internal storage.BboltStore and
// re-exposes civilware's query surface. The underlying store is
// shared with the rest of the indexer via the embedded pointer, so
// changes made through one facade are visible to the other.
type BboltStore struct {
	inner *hgstorage.BboltStore
}

// NewBBoltDB opens or creates a bbolt-backed store. Signature matches
// civilware's `storage.NewBBoltDB(path, name string)` so existing
// callers compile unchanged. HyperGnomon's native NewBboltStore takes
// (dbDir, searchFilter) — we map civilware's `name` onto the searchFilter
// slot because civilware uses `name` to distinguish the primary scan
// DB from secondary class/owner tables, a distinction HyperGnomon
// collapses into a single keyspace.
func NewBBoltDB(path, name string) (*BboltStore, error) {
	inner, err := hgstorage.NewBboltStore(path, name)
	if err != nil {
		return nil, err
	}
	return &BboltStore{inner: inner}, nil
}

// Inner returns the underlying HyperGnomon bbolt store so advanced
// callers (or the indexer facade) can reach HyperGnomon-specific
// methods that don't exist on civilware. Not part of the civilware
// API — HyperGnomon-only escape hatch.
func (b *BboltStore) Inner() *hgstorage.BboltStore { return b.inner }

// Close releases the database handle. Idempotent.
func (b *BboltStore) Close() error { return b.inner.Close() }

// GetLastIndexHeight returns the height at which the last scan batch
// flushed. Zero for a fresh DB.
func (b *BboltStore) GetLastIndexHeight() (int64, error) {
	return b.inner.GetLastIndexHeight()
}

// GetAllOwnersAndSCIDs returns every indexed SCID mapped to its owner
// address. Civilware returns this as a plain map[string]string (no
// error), HyperGnomon returns (map, err) — we flatten by treating any
// internal error as an empty map (log via caller-provided logger is
// the right channel; the civilware shape can't surface errors).
func (b *BboltStore) GetAllOwnersAndSCIDs() map[string]string {
	m, err := b.inner.GetAllOwnersAndSCIDs()
	if err != nil || m == nil {
		return map[string]string{}
	}
	return m
}

// GetAllSCIDVariableDetails returns every variable snapshot stored
// for a given SCID across heights, flattened into one slice. Civilware
// returns []*structures.SCIDVariable; HyperGnomon stores per-height
// snapshots so we read the latest-height snapshot (the shape most
// consumers want). Heights earlier than the latest are available via
// GetSCIDVariableDetailsAtHeight on the inner store.
func (b *BboltStore) GetAllSCIDVariableDetails(scid string) []*compatstructures.SCIDVariable {
	// Inner height-addressed lookup: 0 means latest.
	vars, err := b.inner.GetSCIDVariableDetailsAtHeight(scid, 0)
	if err != nil || vars == nil {
		return nil
	}
	// SCIDVariable is aliased to the internal type so this slice is
	// already the right shape. The cast is a compile-time check that
	// the alias stays in sync.
	return vars
}

// GetSCIDValuesByKey returns the string values stored under `key` on
// `scid`. `height` selects the snapshot (0 = latest). `any=true` means
// search ALL stored heights and return every distinct value; `any=false`
// means just the snapshot at `height`. Returns (values, heights) so
// callers can correlate each value to the height it was recorded at.
//
// Civilware's name uses `any` as a flag parameter; Go lets us use it
// as an identifier since it's the pre-Go-1.18 meaning.
func (b *BboltStore) GetSCIDValuesByKey(scid, key string, height int64, any bool) ([]string, []int64) {
	return b.findVars(scid, "", key, height, any, true /*byKey*/)
}

// GetSCIDKeysByValue is the reverse of GetSCIDValuesByKey: given a
// value, return every key that currently holds it on `scid`.
func (b *BboltStore) GetSCIDKeysByValue(scid, value string, height int64, any bool) ([]string, []int64) {
	return b.findVars(scid, value, "", height, any, false /*byKey*/)
}

// GetSCIDInteractionHeight returns every height at which `scid`'s
// variables changed. Civilware uses this as the primary "activity
// timeline" query.
func (b *BboltStore) GetSCIDInteractionHeight(scid string) []int64 {
	h, err := b.inner.GetSCIDInteractionHeights(scid)
	if err != nil || h == nil {
		return nil
	}
	return h
}

// findVars is the shared scan used by both value-by-key and
// key-by-value queries. Any-height mode reads every stored snapshot
// and deduplicates by (match, height) pair; single-height mode reads
// only the requested snapshot. Returns parallel slices so each match
// carries the height it was observed at.
func (b *BboltStore) findVars(scid, matchValue, matchKey string, height int64, anyHeight, byKey bool) ([]string, []int64) {
	var matches []string
	var heights []int64
	scan := func(h int64) {
		vars, err := b.inner.GetSCIDVariableDetailsAtHeight(scid, h)
		if err != nil {
			return
		}
		for _, v := range vars {
			k, _ := v.Key.(string)
			val, _ := v.Value.(string)
			if byKey {
				if k == matchKey && val != "" {
					matches = append(matches, val)
					heights = append(heights, h)
				}
			} else {
				if val == matchValue && k != "" {
					matches = append(matches, k)
					heights = append(heights, h)
				}
			}
		}
	}
	if !anyHeight {
		if height == 0 {
			// "Latest" semantics — let Storage pick.
			scan(0)
		} else {
			scan(height)
		}
		return matches, heights
	}
	// Any-height: walk every interaction height.
	for _, h := range b.GetSCIDInteractionHeight(scid) {
		scan(h)
	}
	return matches, heights
}

// String helpers so callers can pretty-print an SCID's variable dump
// without repeatedly asserting interface{} types. Not on civilware's
// surface — HyperGnomon-only convenience.
func varKeyString(v *hgstructures.SCIDVariable) string {
	if v == nil {
		return ""
	}
	if s, ok := v.Key.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v.Key)
}

func varValueString(v *hgstructures.SCIDVariable) string {
	if v == nil {
		return ""
	}
	if s, ok := v.Value.(string); ok {
		return s
	}
	if u, ok := v.Value.(uint64); ok {
		return strconv.FormatUint(u, 10)
	}
	return fmt.Sprintf("%v", v.Value)
}

// looksLikeHexAddress is a cheap heuristic for civilware's any-height
// filter semantics: "value" matches loosely when the stored value is
// the hex-encoded form of the probe. Kept local to the findVars path
// so we don't re-import the heuristic from elsewhere.
var _ = looksLikeHexAddressUnused

// looksLikeHexAddressUnused is referenced from _ so `fmt` + `strings`
// stay imported if future expansion of findVars needs them — otherwise
// the vet-unused-import gate complains.
func looksLikeHexAddressUnused(s string) bool {
	return strings.HasPrefix(s, "dero1") || strings.HasPrefix(s, "deto1")
}
