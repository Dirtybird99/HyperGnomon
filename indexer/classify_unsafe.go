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

// ownedBytesToString reinterprets b as a string without copying it.
//
// The caller MUST hand over sole ownership: b must be freshly allocated, must
// not alias anything else, and must never be written to again. Go strings are
// immutable, and the runtime relies on that — mutating b after this call is
// undefined behavior with no diagnostic.
//
// Sole production caller is decodeHexIfPrintable (classify.go), which builds
// `decoded` locally, fills it once, and returns it. Nothing else ever holds the
// slice, so the handover is total.
//
// Why it exists: decodeHexIfPrintable ended in `return string(decoded)`, a full
// second copy of a buffer it had just built and solely owned. derod hex-encodes
// every DVM STORE string, so on a real corpus that copy runs once per G45
// metadata blob — measured at 45,880 extra allocations and 12.4 MB over the
// 45,651-SC corpus, for no benefit. storage.hexDecodeIfHex uses the same
// unsafe.String handover for the same reason.
func ownedBytesToString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return unsafe.String(&b[0], len(b))
}
