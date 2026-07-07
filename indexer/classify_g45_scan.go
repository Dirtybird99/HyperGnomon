package indexer

import (
	"bytes"
	"unicode/utf8"
)

// This file adds a zero-alloc top-level scanner tier in front of the G45
// metadata RawMessage/map decode (extractG45MetadataString). It exists because
// the residual ~10 allocs/NFT in the fast path are 100% encoding/json decode
// machinery (decodeState/scanner/reflect/unquote) spent to recover three
// top-level string fields — "name", "description", "icon" — from a blob whose
// bulk (attributes objects, id, image) we then throw away.
//
// CORRECTNESS INVARIANT: the only bug this scanner can introduce is a false
// ok=true. Every ok=false path leaves the SCClass untouched and defers to the
// unchanged extractG45MetadataFallback (struct fast path -> map path), so it is
// correct by construction. A legitimate ok=true — all three target values are
// simple no-escape valid-UTF-8 strings AND the whole object validates cleanly —
// provably matches encoding/json's struct-decode result. The sole failure mode
// is therefore validation that is LOOSER than encoding/json's checkValid
// anywhere, which is why the value/number/string/depth rules below are strict
// (and, where in doubt, conservative: reject -> fall back, which is free
// correctness). See the differential + fuzz gates in classify_g45_scan_test.go.
//
// Equivalence with encoding/json, verified empirically (go1.26.0):
//   - json.Unmarshal runs checkValid over the ENTIRE input first, so trailing
//     garbage, malformed numbers anywhere (e.g. "01"), bad escapes, and raw
//     control chars in any string make it set NOTHING. The scanner replicates
//     that whole-object validation before applying any value.
//   - A no-escape, control-free, valid-UTF-8 string decodes byte-identically to
//     string(b[i:j]); encoding/json's unquote fast path returns the same bytes.
//     Invalid UTF-8, by contrast, is replaced with U+FFFD by the decoder, so
//     any target string that is not utf8.Valid is handed to the fallback.
//   - Duplicate keys are last-wins in both; a target key whose value is ever a
//     non-string (or an escaped/invalid string) makes the scanner bail so the
//     fallback's exact fmt-rendering / replacement semantics apply.

// g45 target-key identifiers. g45KeyFold flags a case/unicode-fold variant of a
// target key (e.g. "Name", "deſcription"): encoding/json's struct decode matches
// struct fields case-insensitively (with the ſ/K Unicode special cases), so such
// a key WOULD populate a field. We cannot reproduce that mapping by hand safely,
// so it forces the fallback.
const (
	g45KeyNone = iota
	g45KeyName
	g45KeyDesc
	g45KeyIcon
	g45KeyFold
)

// Target field names as bytes, for zero-alloc exact and fold comparison.
var (
	g45bName = []byte("name")
	g45bDesc = []byte("description")
	g45bIcon = []byte("icon")
)

// g45MaxDepth caps nesting the scanner will validate. It sits far below
// encoding/json's maxNestingDepth (10000) on purpose: real G45 metadata nests
// ~2 deep, and bailing earlier than encoding/json can only ever route input to
// the fallback (which validates to the true limit) — never a false accept at a
// boundary. See the correctness invariant above.
const g45MaxDepth = 1000

// g45TargetKey maps a raw (no-escape) top-level key to a target id. Exact
// matches fire the fast path; case/unicode-fold variants return g45KeyFold so
// the caller falls back (encoding/json would still map them to the field, and
// bytes.EqualFold is a proven superset of encoding/json's fold rules, so bailing
// on any fold-match is safe). All comparisons are zero-alloc.
func g45TargetKey(key []byte) int {
	if bytes.Equal(key, g45bName) {
		return g45KeyName
	}
	if bytes.Equal(key, g45bDesc) {
		return g45KeyDesc
	}
	if bytes.Equal(key, g45bIcon) {
		return g45KeyIcon
	}
	if bytes.EqualFold(key, g45bName) || bytes.EqualFold(key, g45bDesc) || bytes.EqualFold(key, g45bIcon) {
		return g45KeyFold
	}
	return g45KeyNone
}

// g45ScanMeta walks the top-level JSON object in b, returning the last-seen
// string values of "name"/"description"/"icon" (each defaulting to "") and
// ok=true ONLY when the entire object validated cleanly AND every target value
// encountered was a simple no-escape valid-UTF-8 string. On any deviation it
// returns ok=false, signalling the caller to run the unchanged fallback.
//
// The returned strings are fresh copies (string(b[i:j])) and never alias b
// (which may be an unsafe read-only view of the source string).
func g45ScanMeta(b []byte) (name, desc, icon string, ok bool) {
	i := g45skipWS(b, 0)
	if i >= len(b) || b[i] != '{' {
		return "", "", "", false
	}
	i++
	i = g45skipWS(b, i)
	if i < len(b) && b[i] == '}' { // empty object: valid, fills nothing
		i = g45skipWS(b, i+1)
		if i != len(b) {
			return "", "", "", false
		}
		return "", "", "", true
	}
	for {
		i = g45skipWS(b, i)
		if i >= len(b) || b[i] != '"' {
			return "", "", "", false
		}
		cs, ce, keyEsc, ni, sok := g45scanString(b, i)
		if !sok || keyEsc {
			// A top-level key with any backslash escape decodes to something
			// we don't want to reproduce by hand; defer to the fallback.
			return "", "", "", false
		}
		t := g45TargetKey(b[cs:ce])
		if t == g45KeyFold {
			return "", "", "", false // case-fold variant of a target: fall back
		}
		i = g45skipWS(b, ni)
		if i >= len(b) || b[i] != ':' {
			return "", "", "", false
		}
		i = g45skipWS(b, i+1)
		if i >= len(b) {
			return "", "", "", false
		}
		if t != g45KeyNone {
			if b[i] != '"' {
				return "", "", "", false // non-string target value: fall back
			}
			vcs, vce, vEsc, vni, vok := g45scanString(b, i)
			if !vok || vEsc {
				return "", "", "", false // escaped target string: fall back
			}
			val := b[vcs:vce]
			if !utf8.Valid(val) {
				// encoding/json would replace invalid UTF-8 with U+FFFD; only
				// the fallback reproduces that, so defer.
				return "", "", "", false
			}
			s := string(val) // 1 alloc: copies out of the read-only view
			switch t {
			case g45KeyName:
				name = s
			case g45KeyDesc:
				desc = s
			case g45KeyIcon:
				icon = s
			}
			i = vni
		} else {
			ni2, vok := g45skipValue(b, i, 1)
			if !vok {
				return "", "", "", false
			}
			i = ni2
		}
		i = g45skipWS(b, i)
		if i >= len(b) {
			return "", "", "", false
		}
		switch b[i] {
		case ',':
			i++
		case '}':
			i = g45skipWS(b, i+1)
			if i != len(b) {
				return "", "", "", false // trailing garbage: encoding/json rejects
			}
			return name, desc, icon, true
		default:
			return "", "", "", false
		}
	}
}

// g45skipWS advances past JSON whitespace (space, tab, LF, CR) per the spec.
func g45skipWS(b []byte, i int) int {
	for i < len(b) {
		switch b[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

// g45scanString validates the JSON string starting at b[i] (b[i] == '"') using
// exactly encoding/json's scanner rules and returns the content span [cs:ce)
// (between the quotes), whether it contained any backslash escape, and the
// index just past the closing quote. ok=false on any malformed string.
func g45scanString(b []byte, i int) (cs, ce int, hasEsc bool, next int, ok bool) {
	cs = i + 1
	j := i + 1
	for j < len(b) {
		c := b[j]
		switch {
		case c == '"':
			return cs, j, hasEsc, j + 1, true
		case c == '\\':
			hasEsc = true
			j++
			if j >= len(b) {
				return 0, 0, false, 0, false
			}
			switch b[j] {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				j++
			case 'u':
				if j+4 >= len(b) {
					return 0, 0, false, 0, false
				}
				for k := 1; k <= 4; k++ {
					if !g45isHex(b[j+k]) {
						return 0, 0, false, 0, false
					}
				}
				j += 5
			default:
				return 0, 0, false, 0, false
			}
		case c < 0x20:
			return 0, 0, false, 0, false // unescaped control char: invalid JSON
		default:
			j++
		}
	}
	return 0, 0, false, 0, false // unterminated
}

// g45skipValue validates and skips exactly one JSON value beginning at b[i]
// (already positioned on its first non-whitespace byte), returning the index
// just past it. Validation matches encoding/json's checkValid; depth guards
// against runaway nesting (bailing well before encoding/json's own limit).
func g45skipValue(b []byte, i, depth int) (int, bool) {
	if depth > g45MaxDepth {
		return 0, false
	}
	if i >= len(b) {
		return 0, false
	}
	switch c := b[i]; {
	case c == '"':
		_, _, _, next, ok := g45scanString(b, i)
		return next, ok
	case c == '-' || (c >= '0' && c <= '9'):
		return g45skipNumber(b, i)
	case c == 't':
		if g45matchLit(b, i, "true") {
			return i + 4, true
		}
		return 0, false
	case c == 'f':
		if g45matchLit(b, i, "false") {
			return i + 5, true
		}
		return 0, false
	case c == 'n':
		if g45matchLit(b, i, "null") {
			return i + 4, true
		}
		return 0, false
	case c == '{':
		return g45skipObject(b, i, depth)
	case c == '[':
		return g45skipArray(b, i, depth)
	default:
		return 0, false
	}
}

func g45skipObject(b []byte, i, depth int) (int, bool) {
	i++ // past '{'
	i = g45skipWS(b, i)
	if i < len(b) && b[i] == '}' {
		return i + 1, true
	}
	for {
		i = g45skipWS(b, i)
		if i >= len(b) || b[i] != '"' {
			return 0, false
		}
		_, _, _, ni, ok := g45scanString(b, i)
		if !ok {
			return 0, false
		}
		i = g45skipWS(b, ni)
		if i >= len(b) || b[i] != ':' {
			return 0, false
		}
		i = g45skipWS(b, i+1)
		ni, ok = g45skipValue(b, i, depth+1)
		if !ok {
			return 0, false
		}
		i = g45skipWS(b, ni)
		if i >= len(b) {
			return 0, false
		}
		switch b[i] {
		case ',':
			i++
		case '}':
			return i + 1, true
		default:
			return 0, false
		}
	}
}

func g45skipArray(b []byte, i, depth int) (int, bool) {
	i++ // past '['
	i = g45skipWS(b, i)
	if i < len(b) && b[i] == ']' {
		return i + 1, true
	}
	for {
		i = g45skipWS(b, i)
		ni, ok := g45skipValue(b, i, depth+1)
		if !ok {
			return 0, false
		}
		i = g45skipWS(b, ni)
		if i >= len(b) {
			return 0, false
		}
		switch b[i] {
		case ',':
			i++
		case ']':
			return i + 1, true
		default:
			return 0, false
		}
	}
}

// g45skipNumber validates and skips a JSON number starting at b[i] using the
// strict RFC 8259 grammar encoding/json enforces (no leading zeros, a digit
// required after '.', a digit required after the exponent sign).
func g45skipNumber(b []byte, i int) (int, bool) {
	n := len(b)
	if i < n && b[i] == '-' {
		i++
	}
	if i >= n {
		return 0, false
	}
	switch {
	case b[i] == '0':
		i++
	case b[i] >= '1' && b[i] <= '9':
		i++
		for i < n && b[i] >= '0' && b[i] <= '9' {
			i++
		}
	default:
		return 0, false
	}
	if i < n && b[i] == '.' {
		i++
		if i >= n || b[i] < '0' || b[i] > '9' {
			return 0, false
		}
		for i < n && b[i] >= '0' && b[i] <= '9' {
			i++
		}
	}
	if i < n && (b[i] == 'e' || b[i] == 'E') {
		i++
		if i < n && (b[i] == '+' || b[i] == '-') {
			i++
		}
		if i >= n || b[i] < '0' || b[i] > '9' {
			return 0, false
		}
		for i < n && b[i] >= '0' && b[i] <= '9' {
			i++
		}
	}
	return i, true
}

func g45matchLit(b []byte, i int, lit string) bool {
	if i+len(lit) > len(b) {
		return false
	}
	for k := 0; k < len(lit); k++ {
		if b[i+k] != lit[k] {
			return false
		}
	}
	return true
}

func g45isHex(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}
