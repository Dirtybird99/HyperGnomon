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

// ============================================================================
// SCIDVariable slice — typed v1 encoding
// ============================================================================
//
// SCIDVariable holds interface{} Key and Value. In practice only two
// concrete types appear from the DERO SDK: string (DVM-BASIC string var)
// and uint64 (DVM-BASIC numeric var). The typed encoder dispatches on
// these two cases per field.
//
// Wire layout:
//
//   byte 0          : tag 0x02
//   bytes 1..4      : count (BE uint32)
//   for each variable:
//     byte         : key kind (0x01 string, 0x02 uint64)
//     string:      : varint len + raw bytes
//     uint64:      : 8 bytes BE
//     byte         : value kind
//     string/uint64: as above
//
// Backward compat: msgpack array header is 0x90-0x9f (fixarray). Tag
// 0x02 does not overlap, so reader dispatch by byte[0] is unambiguous.

const (
	TagSCIDVariablesV1 byte = 0x02

	varKindString byte = 0x01
	varKindUint64 byte = 0x02
)

// ErrInvalidSCIDVariables is returned when the wire bytes don't match
// either a msgpack array or a v1 typed SCIDVariables slice.
var ErrInvalidSCIDVariables = errors.New("invalid SCIDVariable slice encoding")

// MarshalSCIDVariablesTypedAppend writes the slice into dst in v1 typed
// form. Returns the grown slice. Bytes in dst before the call are
// preserved (so callers can pack multiple values if they want; today
// they use this in a "start empty, fill, hand to Put" pattern).
//
// Interface values that aren't string or uint64 get skipped with a
// distinguishable empty-string placeholder (kind=string, len=0). This
// preserves slice length for downstream consumers that index by position.
func MarshalSCIDVariablesTypedAppend(dst []byte, vars []*SCIDVariable) []byte {
	dst = append(dst, TagSCIDVariablesV1)
	// count (BE uint32)
	dst = binary.BigEndian.AppendUint32(dst, uint32(len(vars)))
	for _, v := range vars {
		dst = appendVarField(dst, v.Key)
		dst = appendVarField(dst, v.Value)
	}
	return dst
}

// UnmarshalSCIDVariablesTyped decodes a v1 typed slice from b.
// Returns a freshly-allocated slice; callers own it.
func UnmarshalSCIDVariablesTyped(b []byte) ([]*SCIDVariable, error) {
	if len(b) < 5 || b[0] != TagSCIDVariablesV1 {
		return nil, ErrInvalidSCIDVariables
	}
	n := binary.BigEndian.Uint32(b[1:5])
	pos := 5
	out := make([]*SCIDVariable, n)
	for i := uint32(0); i < n; i++ {
		var v SCIDVariable
		var err error
		v.Key, pos, err = readVarField(b, pos)
		if err != nil {
			return nil, err
		}
		v.Value, pos, err = readVarField(b, pos)
		if err != nil {
			return nil, err
		}
		out[i] = &v
	}
	return out, nil
}

// IsSCIDVariablesTyped reports whether b is the v1 typed encoding.
func IsSCIDVariablesTyped(b []byte) bool {
	return len(b) >= 1 && b[0] == TagSCIDVariablesV1
}

func appendVarField(dst []byte, v interface{}) []byte {
	switch x := v.(type) {
	case string:
		dst = append(dst, varKindString)
		dst = binary.AppendUvarint(dst, uint64(len(x)))
		dst = append(dst, x...)
	case uint64:
		dst = append(dst, varKindUint64)
		dst = binary.BigEndian.AppendUint64(dst, x)
	default:
		// Unknown type — fall back to a stringified form so we preserve
		// *some* data, marked as string. Common case is int64 coming from
		// JSON parsing at upstream; let Sprintf handle it.
		dst = append(dst, varKindString)
		s := toStringBest(v)
		dst = binary.AppendUvarint(dst, uint64(len(s)))
		dst = append(dst, s...)
	}
	return dst
}

func readVarField(b []byte, pos int) (interface{}, int, error) {
	if pos >= len(b) {
		return nil, pos, ErrInvalidSCIDVariables
	}
	kind := b[pos]
	pos++
	switch kind {
	case varKindString:
		n, nLen := binary.Uvarint(b[pos:])
		if nLen <= 0 {
			return nil, pos, ErrInvalidSCIDVariables
		}
		pos += nLen
		end := pos + int(n)
		if end > len(b) {
			return nil, pos, ErrInvalidSCIDVariables
		}
		s := string(b[pos:end])
		return s, end, nil
	case varKindUint64:
		if pos+8 > len(b) {
			return nil, pos, ErrInvalidSCIDVariables
		}
		u := binary.BigEndian.Uint64(b[pos : pos+8])
		return u, pos + 8, nil
	default:
		return nil, pos, ErrInvalidSCIDVariables
	}
}

// toStringBest converts common numeric types to string. Only used for
// unknown-type fallback.
func toStringBest(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case int64:
		return itoaBase10(x)
	case int:
		return itoaBase10(int64(x))
	case float64:
		// Truncate float to int string; SC variables are never true floats.
		return itoaBase10(int64(x))
	default:
		return ""
	}
}

// ============================================================================
// ClassMeta — typed v1 encoding
// ============================================================================
//
// Format layout:
//
//   byte 0           : tag 0x04
//   bytes 1..8       : InstallHeight (BE int64)
//   bytes 9..16      : LastHeight    (BE int64)
//   varint           : len(Tags)
//   for each tag     : varint-len-prefixed bytes
//   varint+bytes     : Class
//   varint+bytes     : Name
//   varint+bytes     : Desc
//   varint+bytes     : IconURL
//   varint+bytes     : DURL
//   varint+bytes     : Version
//
// Backward-compat: existing class/class_scid bucket records are msgpack
// fixmaps (tag 0x80-0x8f) or map16 (0xde). Tag 0x04 does not collide with
// either, so reader dispatch by byte[0] is unambiguous. See IsClassMetaTyped.

const (
	TagClassMetaV1 byte = 0x04

	classMetaMinHeaderSize = 1 + 8 + 8 // tag + InstallHeight + LastHeight
)

// ErrInvalidClassMeta is returned when the wire bytes don't match the v1
// typed ClassMeta layout.
var ErrInvalidClassMeta = errors.New("invalid ClassMeta encoding")

// MarshalTyped encodes m into a fresh byte slice. Hand the result to
// bbolt.Put (which must hold the slice until commit — we never reuse).
func (m *ClassMeta) MarshalTyped() []byte {
	// Rough lower bound: header + 6 zero-length strings (1 byte each) +
	// 1 byte tagCount. Growing amortizes into a single alloc in practice.
	buf := make([]byte, 0, classMetaMinHeaderSize+8+len(m.Class)+len(m.Name)+len(m.Desc)+len(m.IconURL)+len(m.DURL)+len(m.Version))
	return m.MarshalTypedAppend(buf)
}

// MarshalTypedAppend writes into dst and returns the grown slice.
func (m *ClassMeta) MarshalTypedAppend(dst []byte) []byte {
	dst = append(dst, TagClassMetaV1)
	dst = binary.BigEndian.AppendUint64(dst, uint64(m.InstallHeight))
	dst = binary.BigEndian.AppendUint64(dst, uint64(m.LastHeight))
	dst = binary.AppendUvarint(dst, uint64(len(m.Tags)))
	for _, t := range m.Tags {
		dst = binary.AppendUvarint(dst, uint64(len(t)))
		dst = append(dst, t...)
	}
	dst = appendLenString(dst, m.Class)
	dst = appendLenString(dst, m.Name)
	dst = appendLenString(dst, m.Desc)
	dst = appendLenString(dst, m.IconURL)
	dst = appendLenString(dst, m.DURL)
	dst = appendLenString(dst, m.Version)
	return dst
}

// UnmarshalTyped decodes a v1 typed ClassMeta from b. Six string fields
// each get one allocation (unavoidable — they must outlive the bbolt View
// that supplied b). Tags slice gets one allocation for its header plus one
// per element string. Typical ClassMeta with 2 tags: 1+2+6 = 9 allocs,
// vs msgpack's 11.
func (m *ClassMeta) UnmarshalTyped(b []byte) error {
	if len(b) < classMetaMinHeaderSize || b[0] != TagClassMetaV1 {
		return ErrInvalidClassMeta
	}
	m.InstallHeight = int64(binary.BigEndian.Uint64(b[1:9]))
	m.LastHeight = int64(binary.BigEndian.Uint64(b[9:17]))
	pos := 17

	tagCount, nLen := binary.Uvarint(b[pos:])
	if nLen <= 0 {
		return ErrInvalidClassMeta
	}
	pos += nLen
	if tagCount > 0 {
		m.Tags = make([]string, tagCount)
		for i := range m.Tags {
			s, next, err := readLenString(b, pos)
			if err != nil {
				return err
			}
			m.Tags[i] = s
			pos = next
		}
	} else {
		m.Tags = nil
	}
	var err error
	if m.Class, pos, err = readLenString(b, pos); err != nil {
		return err
	}
	if m.Name, pos, err = readLenString(b, pos); err != nil {
		return err
	}
	if m.Desc, pos, err = readLenString(b, pos); err != nil {
		return err
	}
	if m.IconURL, pos, err = readLenString(b, pos); err != nil {
		return err
	}
	if m.DURL, pos, err = readLenString(b, pos); err != nil {
		return err
	}
	if m.Version, pos, err = readLenString(b, pos); err != nil {
		return err
	}
	_ = pos // trailing bytes (forward-compat slack) are ignored
	return nil
}

// IsClassMetaTyped reports whether b is a v1 typed record. The msgpack
// fallback range (0x80-0x8f fixmap, 0xde map16) is disjoint from 0x04.
func IsClassMetaTyped(b []byte) bool {
	return len(b) >= 1 && b[0] == TagClassMetaV1
}

// appendLenString writes a uvarint-prefixed string into dst.
func appendLenString(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

// readLenString reads a uvarint-prefixed string starting at pos. Returns
// the string, next position, and any error. Shared helper for ClassMeta
// and any future typed struct that needs the same primitive.
func readLenString(b []byte, pos int) (string, int, error) {
	if pos >= len(b) {
		return "", pos, ErrInvalidClassMeta
	}
	n, nLen := binary.Uvarint(b[pos:])
	if nLen <= 0 {
		return "", pos, ErrInvalidClassMeta
	}
	pos += nLen
	end := pos + int(n)
	if end > len(b) {
		return "", pos, ErrInvalidClassMeta
	}
	return string(b[pos:end]), end, nil
}

// ============================================================================
// SCCodeEntry — typed v1 encoding
// ============================================================================
//
// Code is the dominant payload; stash it raw after a fixed 9-byte prefix so
// decode is one string allocation (unavoidable — the string must outlive the
// bbolt View closure) and encode is a single append-chain, no allocation
// beyond the eventual grow.
//
// Wire layout:
//
//   +------+------+------+------+------+------+------+------+------+--------+
//   | 0x03 |  InstallHeight (BE 8 bytes)                            | code … |
//   +------+------+------+------+------+------+------+------+------+--------+
//
// Backward compat: no legacy records — sccode is a brand-new bucket, so we
// unconditionally use typed v1.

const (
	TagSCCodeEntryV1 byte = 0x03

	sccodeHeaderSize = 1 + 8 // tag + BE InstallHeight
)

// ErrInvalidSCCodeEntry marks a decode error on a supposed SCCodeEntry.
var ErrInvalidSCCodeEntry = errors.New("invalid SCCodeEntry encoding")

// MarshalTyped encodes e into a freshly-allocated byte slice. Caller
// hands the slice to bbolt.Put, which copies into the page.
func (e *SCCodeEntry) MarshalTyped() []byte {
	buf := make([]byte, sccodeHeaderSize, sccodeHeaderSize+len(e.Code))
	buf[0] = TagSCCodeEntryV1
	binary.BigEndian.PutUint64(buf[1:9], uint64(e.InstallHeight))
	return append(buf, e.Code...)
}

// MarshalTypedAppend writes into dst, returning the grown slice. For
// hot-path callers who recycle a keyBuf-style scratch buffer.
func (e *SCCodeEntry) MarshalTypedAppend(dst []byte) []byte {
	dst = append(dst, TagSCCodeEntryV1)
	dst = binary.BigEndian.AppendUint64(dst, uint64(e.InstallHeight))
	dst = append(dst, e.Code...)
	return dst
}

// UnmarshalTyped populates e from b. One string allocation (the code); no
// header allocations. Returns ErrInvalidSCCodeEntry on tag mismatch or
// truncated buffer.
func (e *SCCodeEntry) UnmarshalTyped(b []byte) error {
	if len(b) < sccodeHeaderSize || b[0] != TagSCCodeEntryV1 {
		return ErrInvalidSCCodeEntry
	}
	e.InstallHeight = int64(binary.BigEndian.Uint64(b[1:9]))
	e.Code = string(b[sccodeHeaderSize:])
	return nil
}

// IsSCCodeEntryTyped reports whether b is the v1 typed encoding. Held for
// symmetry with the other typed detectors; sccode has no legacy records so
// the check is mostly defensive.
func IsSCCodeEntryTyped(b []byte) bool {
	return len(b) >= 1 && b[0] == TagSCCodeEntryV1
}

func itoaBase10(n int64) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
