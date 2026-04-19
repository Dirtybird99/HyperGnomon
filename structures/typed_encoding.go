package structures

import (
	"encoding/binary"
	"errors"
)

// Typed binary encoding for hot-path structs (DESIGN notes: msgpack
// baseline is ~12 allocs per encode/decode cycle for AddrSCIDEntry; a
// hand-rolled typed encoder should drop that to 0 on encode and ~1 on
// decode).
//
// Format layout for AddrSCIDEntry:
//
//   +------+------+------+------+------+------+------+------+------+
//   | 0x01 |  FirstHeight (BE 8 bytes)                              |
//   +------+------+------+------+------+------+------+------+------+
//   |  LastHeight (BE 8 bytes)                                      |
//   +------+------+------+------+------+------+------+------+------+
//   |  Count (BE 8 bytes)                                           |
//   +------+------+------+------+------+------+------+------+------+
//
// Total: 25 bytes. First byte is a format tag:
//   - 0x01: typed v1 (this format)
//   - 0x80..0x8f: msgpack fixmap (legacy; we'd fall through to msgpack
//     decode if we see one).
//
// The tag never collides with any msgpack encoding of a map-valued struct
// because msgpack's fixmap family starts at 0x80.

const (
	// TagAddrSCIDEntryV1 marks the typed v1 encoding of AddrSCIDEntry.
	TagAddrSCIDEntryV1 byte = 0x01

	// EncodedAddrSCIDEntrySize is the fixed byte length of a v1 record.
	EncodedAddrSCIDEntrySize = 1 + 8 + 8 + 8 // tag + 3× int64
)

// ErrInvalidAddrSCIDEntry is returned when the wire bytes don't match
// either the legacy msgpack shape or the v1 typed shape.
var ErrInvalidAddrSCIDEntry = errors.New("invalid AddrSCIDEntry encoding")

// MarshalTyped encodes the entry into a fresh 25-byte slice. The caller
// can hand the slice straight to bbolt.Put (which copies internally).
func (e *AddrSCIDEntry) MarshalTyped() []byte {
	buf := make([]byte, EncodedAddrSCIDEntrySize)
	buf[0] = TagAddrSCIDEntryV1
	binary.BigEndian.PutUint64(buf[1:9], uint64(e.FirstHeight))
	binary.BigEndian.PutUint64(buf[9:17], uint64(e.LastHeight))
	binary.BigEndian.PutUint64(buf[17:25], uint64(e.Count))
	return buf
}

// MarshalTypedAppend writes into an existing buffer (returning the grown
// slice). Intended for keyBuf-style reuse in FlushBatch — eliminates the
// per-call allocation when called in a tight loop.
func (e *AddrSCIDEntry) MarshalTypedAppend(dst []byte) []byte {
	// Grow once if needed; append semantics preserve existing prefix if caller
	// wants to pack multiple records (they currently don't, but the API is
	// allocation-flexible).
	dst = append(dst, TagAddrSCIDEntryV1)
	dst = binary.BigEndian.AppendUint64(dst, uint64(e.FirstHeight))
	dst = binary.BigEndian.AppendUint64(dst, uint64(e.LastHeight))
	dst = binary.BigEndian.AppendUint64(dst, uint64(e.Count))
	return dst
}

// UnmarshalTyped decodes a v1 typed entry from b. Returns
// ErrInvalidAddrSCIDEntry if the tag or length doesn't match.
func (e *AddrSCIDEntry) UnmarshalTyped(b []byte) error {
	if len(b) != EncodedAddrSCIDEntrySize || b[0] != TagAddrSCIDEntryV1 {
		return ErrInvalidAddrSCIDEntry
	}
	e.FirstHeight = int64(binary.BigEndian.Uint64(b[1:9]))
	e.LastHeight = int64(binary.BigEndian.Uint64(b[9:17]))
	e.Count = int64(binary.BigEndian.Uint64(b[17:25]))
	return nil
}

// IsAddrSCIDEntryTyped reports whether b is v1-typed (as opposed to the
// legacy msgpack encoding). msgpack fixmap headers live in 0x80-0x8f;
// v1 uses 0x01 so the check is a single byte comparison.
func IsAddrSCIDEntryTyped(b []byte) bool {
	return len(b) >= 1 && b[0] == TagAddrSCIDEntryV1
}
