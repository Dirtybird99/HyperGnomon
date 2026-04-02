package pool

import (
	"unique"
)

// scidHandle caches interned SCID strings to avoid duplicate allocations.
// SCIDs repeat across thousands of transactions -- interning saves significant memory.
var scidHandles = make(map[string]unique.Handle[string])

// InternSCID returns a deduplicated copy of the SCID string.
// All callers sharing the same SCID value point to the same underlying string.
// Uses Go's unique package which is GC-aware (handles are collected when unused).
func InternSCID(s string) string {
	h := unique.Make(s)
	return h.Value()
}

// InternAddress returns a deduplicated copy of an address string.
func InternAddress(s string) string {
	h := unique.Make(s)
	return h.Value()
}
