package indexer

import "unsafe"

// readOnlyBytes returns a []byte header aliasing s's backing array without
// copying. The result MUST be treated as read-only: mutating it would corrupt
// the immutable string s (undefined behavior).
//
// Sole production caller is extractG45MetadataFallback (classify.go), which
// passes the view only to json.Unmarshal; the fire-rate test also uses it.
// That is sound because:
//   - json.Unmarshal never writes to its input buffer; it only reads.
//   - map[string]interface{} decoding copies every retained substring, so
//     nothing decoded from the view outlives it or shares memory — verified
//     against go1.26.0 stdlib.
//
// This replaces a genuine heap copy ([]byte(str)) with a zero-alloc view.
func readOnlyBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
