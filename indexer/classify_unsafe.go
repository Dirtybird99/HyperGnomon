package indexer

import "unsafe"

// readOnlyBytes returns a []byte header aliasing s's backing array without
// copying. The result MUST be treated as read-only: mutating it would corrupt
// the immutable string s (undefined behavior).
//
// Sole caller is extractG45MetadataString, which passes the view only to
// json.Unmarshal. That is sound because:
//   - json.Unmarshal never writes to its input buffer; it only reads.
//   - Decoding into json.RawMessage copies the bytes out
//     (stream.go: *m = append((*m)[0:0], data...)), so no RawMessage aliases
//     the view — verified against go1.26.0 stdlib.
//   - String and map[string]interface{} decoding likewise copy every retained
//     substring, so nothing decoded from the view outlives it or shares memory.
//
// This replaces a genuine heap copy ([]byte(str)) with a zero-alloc view.
func readOnlyBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
