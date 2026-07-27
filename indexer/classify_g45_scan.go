package indexer

import (
	"strconv"
	"strings"
	"unicode/utf8"
)

// This file adds a zero-alloc top-level scanner tier in front of the G45
// metadata map decode (extractG45MetadataString). It exists because the
// pre-scanner cost was ~10+ allocs/NFT of encoding/json decode machinery
// (decodeState/scanner/reflect/unquote) spent to recover three top-level
// string fields — "name", "description", "icon" — from a blob whose bulk
// (attributes objects, id, image) we then throw away.
//
// CORRECTNESS INVARIANT: the only bug this scanner can introduce is a wrong
// g45vOK or a wrong g45vNoFields. Every g45vFallback path leaves the SCClass
// untouched and defers to extractG45MetadataFallback — the ORIGINAL
// map[string]interface{} decode, kept as the verbatim oracle — so it is
// correct by construction. A legitimate g45vOK — all three target values are
// simple no-escape valid-UTF-8 strings AND the whole object validates cleanly
// (grammar + float64 number convertibility) — provably matches the original
// map-decode result. The failure modes are therefore validation LOOSER than
// encoding/json's checkValid + convertNumber anywhere, which is why the
// value/number/string/depth rules below are strict (and, where in doubt,
// conservative: defer to the fallback, which is free correctness). See the
// differential + fuzz gates in classify_g45_scan_test.go.
//
// Equivalence with encoding/json, verified empirically (go1.26.0):
//   - json.Unmarshal runs checkValid over the ENTIRE input first, so trailing
//     garbage, malformed numbers anywhere (e.g. "01"), bad escapes, and raw
//     control chars in any string make it set NOTHING; map building then
//     converts EVERY value, so a number outside float64 range anywhere also
//     sets NOTHING. The scanner replicates both whole-object gates before
//     applying any value.
//   - A no-escape, control-free, valid-UTF-8 string decodes byte-identically to
//     string(b[i:j]); encoding/json's unquote fast path returns the same bytes.
//     Invalid UTF-8, by contrast, is replaced with U+FFFD by the decoder, so
//     any target string that is not utf8.Valid is handed to the fallback.
//   - Duplicate keys are last-wins in both; a target key whose value is ever a
//     non-string (or an escaped/invalid string) makes the scanner bail so the
//     fallback's exact fmt-rendering / replacement semantics apply.

// g45 target-key identifiers. g45KeyFold flags a case/unicode-fold variant of a
// target key (e.g. "Name", "deſcription"). The original map decode matches keys
// exactly, so a fold variant sets nothing — but the scanner still defers these
// to the fallback: it is pinned conservative behavior (TestG45ScanFires's
// decline table) dating from when the fallback used a case-folding struct
// decode, and deferring costs nothing on real corpora (zero fold keys).
const (
	g45KeyNone = iota
	g45KeyName
	g45KeyDesc
	g45KeyIcon
	// Media keys. G45 assets never use `icon` — not once in the 45,589-SC
	// mainnet corpus — so these are what actually carries artwork: `image` on
	// NFTs, `backdropImage` on collections, with `alt-` variants of each.
	// g45KeyImagesRaw is `images`, the one media key whose value is an object
	// rather than a string. It is routed to the SKIP branch, not the target
	// branch: a non-string target value forces the whole blob down the
	// fallback map decode, measured at +1,111 allocs and +62 KB over the
	// corpus (a 4x ClassifyCorpus regression) for the 23 blobs carrying it.
	// The skip branch has already walked the value's extent, so capturing the
	// raw text there is free — see the capture in g45ScanMetaVerdict.
	g45KeyImage
	g45KeyBackdropImage
	g45KeyAltImage
	g45KeyAltBackdropImage
	g45KeyAudio
	g45KeyVideo
	g45KeyImagesRaw
	g45KeyFold
)

// g45Fields carries the scanner's extracted values. A struct rather than a
// growing list of named returns: ten strings positionally is a transposition
// bug waiting to happen, and by-value it costs no allocation.
//
// These are RAW per-key values. Precedence between them (image → backdropImage
// and alt-image → alt-backdropImage) is policy and lives in
// extractG45MetadataString, keeping this file a pure JSON reader whose output
// can be compared field-for-field against the map-decode oracle.
type g45Fields struct {
	name, desc, icon           string
	image, backdropImage       string
	altImage, altBackdropImage string
	audio, video               string
	// images is the RAW JSON text of the `images` value (an object), not a
	// decoded string like the fields above. See the skip-branch capture in
	// g45ScanMetaVerdict.
	images string
}

// g45TargetNames are the exact keys g45TargetKey matches, in key-id order
// after g45KeyNone. Used for the fold-variant sweep below.
var g45TargetNames = [...]string{
	"name", "description", "icon",
	"image", "backdropImage", "alt-image", "alt-backdropImage",
	"audio", "video",
}

// g45Verdict is the scanner's routing decision. A distinct type (not a bare
// int) so a verdict can never be confused with a scan position or a key id
// at compile time.
type g45Verdict uint8

// Scanner verdicts. The tri-state exists so the caller can skip the fallback
// decode entirely when it would provably set nothing:
//
//   - g45vOK: the whole object validated (grammar AND float64-convertibility
//     of every number, mirroring the map decode) and every target value was a
//     simple string — the extracted values are authoritative.
//   - g45vNoFields: the fallback map decode provably sets nothing, so the
//     caller skips it. Three proven cases: the input violates JSON grammar in
//     a way encoding/json's checkValid also rejects (the scanner's
//     string/number/literal/structure rules mirror it exactly); its first
//     non-WS byte is not '{' (a valid non-object type-errors into the map —
//     null succeeds but fills nothing); or a skipped number is outside
//     float64 range, which makes the whole map decode error exactly like the
//     original algorithm (strconv.ParseFloat is the same call encoding/json's
//     convertNumber makes).
//   - g45vFallback: not provably either of the above — escaped/fold keys,
//     escaped or non-string or invalid-UTF-8 target values, depth past
//     g45MaxDepth. The fallback MUST run; it may set fields.
//
// The load-bearing distinction is g45vNoFields vs g45vFallback: a wrong
// g45vNoFields silently drops fields the fallback would have set. That is
// exactly what FuzzG45ScanDifferential asserts against (scanner-routed
// extractG45MetadataString vs the original-map-decode oracle), and the golden
// gate pins every real corpus blob.
const (
	g45vOK g45Verdict = iota
	g45vNoFields
	g45vFallback
)

// g45CloneThreshold is the metadata-blob size above which
// extractG45MetadataString clones the scanner's zero-copy substrings before
// storing them in SCClass, so a few-byte Name cannot pin an arbitrarily large
// (potentially hostile) blob inside long-lived ClassMeta holders (eventbus
// queues, the classify seed cache). Real mainnet corpus blobs top out under
// 2KB, so the clone never fires on the benchmark path.
//
// The guard measures len(str) — the cap therefore holds only when str's
// backing array IS the blob, which is true for every retained-classification
// path today (all decode vars from RPC JSON, where each string value gets its
// own allocation). A future caller classifying storage-typed-decoded vars
// (whose strings are views of ONE coalesced per-SC buffer, see
// structures.UnmarshalSCIDVariablesTyped/ownedCut) into a retained ClassMeta
// would need to cap on the backing array instead.
const g45CloneThreshold = 4096

// g45MaxDepth caps nesting the scanner will validate. It sits far below
// encoding/json's maxNestingDepth (10000) on purpose: real G45 metadata nests
// ~2 deep, and bailing earlier than encoding/json can only ever route input to
// the fallback (which validates to the true limit) — never a false accept at a
// boundary. See the correctness invariant above.
const g45MaxDepth = 1000

// g45TargetKey maps a raw (no-escape) top-level key to a target id. Exact
// matches fire the fast path; case/unicode-fold variants return g45KeyFold so
// the caller falls back (encoding/json would still map them to the field, and
// strings.EqualFold — the same simple-fold algorithm as bytes.EqualFold — is a
// proven superset of encoding/json's fold rules, so bailing on any fold-match
// is safe). All comparisons are zero-alloc.
func g45TargetKey(key string) int {
	switch key {
	case "name":
		return g45KeyName
	case "description":
		return g45KeyDesc
	case "icon":
		return g45KeyIcon
	case "image":
		return g45KeyImage
	case "backdropImage":
		return g45KeyBackdropImage
	case "alt-image":
		return g45KeyAltImage
	case "alt-backdropImage":
		return g45KeyAltBackdropImage
	case "audio":
		return g45KeyAudio
	case "video":
		return g45KeyVideo
	case "images":
		return g45KeyImagesRaw
	}
	// Fold sweep. This runs for every NON-target top-level key of every blob —
	// `attributes`, `id`, `score` — so it is the hottest miss path in the
	// classifier, and it grew from 3 targets to 9 with the media keys.
	//
	// The filter is a length check, which is only sound because it is gated on
	// the key being pure ASCII. Simple folding is NOT length-preserving in
	// general — "deſcription" (U+017F, 12 bytes) folds to "description" (11)
	// and is pinned as a must-defer case in TestG45ScanFires — but it IS
	// length-preserving within ASCII, and a fold variant that changes byte
	// length must contain a non-ASCII byte. So: ASCII keys can only fold-match
	// a same-length target; anything else takes the unfiltered sweep.
	ascii := g45IsASCII(key)
	for _, t := range g45TargetNames {
		if ascii && len(t) != len(key) {
			continue
		}
		if strings.EqualFold(key, t) {
			return g45KeyFold
		}
	}
	return g45KeyNone
}

// g45IsASCII reports whether s contains only bytes < 0x80.
func g45IsASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

// g45ImagesKey is the quoted top-level key g45ScanImagesRaw looks for. Used
// verbatim as a Contains pre-filter: a blob with a top-level "images" key
// necessarily contains these bytes, so the filter has no false negatives, and
// a false positive (the sequence appearing inside a nested value) only costs
// one scan that finds nothing.
const g45ImagesKey = `"images"`

// g45ScanImagesRaw returns the raw JSON text of the top-level "images" value —
// a zero-copy substring of s — or ("", false) when there is no such key.
//
// WHY A SEPARATE PASS: "images" is the only media key whose value is an object
// rather than a string. Making it a scanner target would push every blob
// carrying it through the fallback map decode (a non-string target value is an
// automatic g45vFallback), which measured at +1,111 allocs / +62 KB over the
// corpus — a 4x ClassifyCorpus regression bought by 23 blobs. Scanning for it
// separately keeps the main scanner's "targets are simple strings" invariant
// intact and costs a Contains check on blobs that lack the key.
//
// PRECONDITION: s is valid JSON. Both call sites have already established
// that — the g45vOK path because the scanner validated the whole object, the
// fallback path because json.Unmarshal returned nil. This function therefore
// does not re-validate; it only locates. On anything it cannot walk cleanly it
// returns ("", false), so a violated precondition degrades to "absent" rather
// than to a wrong value.
//
// Last-wins on duplicate keys, matching both encoding/json and the main
// scanner. The returned text is verbatim on-chain bytes, NOT re-encoded: key
// order and inner spacing are whatever the minter wrote. That is what makes it
// reproducible from both call sites — canonicalizing would require a decode,
// which is the cost this exists to avoid.
func g45ScanImagesRaw(s string) (string, bool) {
	if !strings.Contains(s, g45ImagesKey) {
		return "", false
	}
	i := g45skipWS(s, 0)
	if i >= len(s) || s[i] != '{' {
		return "", false
	}
	i++
	var out string
	var found bool
	for {
		i = g45skipWS(s, i)
		if i >= len(s) {
			return out, found
		}
		if s[i] == '}' {
			return out, found
		}
		if s[i] != '"' {
			return out, found
		}
		cs, ce, keyEsc, ni, ok := g45scanString(s, i)
		if !ok {
			return out, found
		}
		i = g45skipWS(s, ni)
		if i >= len(s) || s[i] != ':' {
			return out, found
		}
		i = g45skipWS(s, i+1)
		vs := i
		next, verdict := g45skipValue(s, i, 1)
		if verdict != g45vOK {
			return out, found
		}
		// An escaped key never matches: the map decode compares the DECODED
		// key, but "images" decoding to "images" is a shape we decline
		// rather than reproduce by hand — same rule the main scanner applies.
		if !keyEsc && s[cs:ce] == "images" {
			out, found = s[vs:next], true // zero-copy; last occurrence wins
		}
		i = g45skipWS(s, next)
		if i >= len(s) {
			return out, found
		}
		if s[i] != ',' {
			return out, found
		}
		i++
	}
}

// g45ScanMeta is the []byte compatibility entry point (differential and fuzz
// gates drive it). It copies b into a string once so the substrings the core
// scanner returns never alias a caller-mutable byte slice. ok collapses the
// tri-state verdict: true only for a clean fire.
func g45ScanMeta(b []byte) (f g45Fields, ok bool) {
	f, v := g45ScanMetaVerdict(string(b))
	return f, v == g45vOK
}

// g45ScanMetaVerdict walks the top-level JSON object in s, returning the
// last-seen string value of every target key (each defaulting to "") and
// g45vOK ONLY when the entire object validated cleanly AND every target value
// encountered was a simple no-escape valid-UTF-8 string. On any deviation it
// returns g45vFallback (fallback must run) or g45vNoFields (fallback provably
// sets nothing — see the verdict constants).
//
// The returned values are zero-copy substrings of s (s[i:j]) — safe because
// Go strings are immutable. They pin s's backing array for as long as the
// caller retains them; extractG45MetadataString clones them for blobs larger
// than g45CloneThreshold so ClassMeta held in eventbus queues or the seed
// cache never pins more than that per entry.
func g45ScanMetaVerdict(s string) (f g45Fields, verdict g45Verdict) {
	i := g45skipWS(s, 0)
	if i >= len(s) || s[i] != '{' {
		// Not an object. Whether the rest is valid JSON or garbage, neither
		// fallback decode can set a field (see g45vNoFields).
		return g45Fields{}, g45vNoFields
	}
	i++
	i = g45skipWS(s, i)
	if i < len(s) && s[i] == '}' { // empty object: valid, fills nothing
		i = g45skipWS(s, i+1)
		if i != len(s) {
			return g45Fields{}, g45vNoFields // trailing garbage
		}
		return g45Fields{}, g45vOK
	}
	for {
		i = g45skipWS(s, i)
		if i >= len(s) || s[i] != '"' {
			return g45Fields{}, g45vNoFields // object key must be a string
		}
		cs, ce, keyEsc, ni, sok := g45scanString(s, i)
		if !sok {
			return g45Fields{}, g45vNoFields // malformed key string
		}
		if keyEsc {
			// A top-level key with any backslash escape decodes to something
			// we don't want to reproduce by hand; defer to the fallback.
			return g45Fields{}, g45vFallback
		}
		t := g45TargetKey(s[cs:ce])
		if t == g45KeyFold {
			// Case-fold variant of a target: encoding/json would still map it
			// to the field, so the fallback must run.
			return g45Fields{}, g45vFallback
		}
		i = g45skipWS(s, ni)
		if i >= len(s) || s[i] != ':' {
			return g45Fields{}, g45vNoFields
		}
		i = g45skipWS(s, i+1)
		if i >= len(s) {
			return g45Fields{}, g45vNoFields // truncated
		}
		if t != g45KeyNone && t != g45KeyImagesRaw {
			if s[i] != '"' {
				// Non-string target value. If it is valid JSON the map path
				// renders it (fmt semantics), so the fallback must run; we
				// have not validated the rest, so this is NOT provably
				// invalid either.
				return g45Fields{}, g45vFallback
			}
			vcs, vce, vEsc, vni, vok := g45scanString(s, i)
			if !vok {
				return g45Fields{}, g45vNoFields // malformed target string
			}
			if vEsc {
				return g45Fields{}, g45vFallback // escaped target string
			}
			val := s[vcs:vce] // zero-copy substring; no alloc
			if !utf8.ValidString(val) {
				// encoding/json would replace invalid UTF-8 with U+FFFD; only
				// the fallback reproduces that, so defer.
				return g45Fields{}, g45vFallback
			}
			switch t {
			case g45KeyName:
				f.name = val
			case g45KeyDesc:
				f.desc = val
			case g45KeyIcon:
				f.icon = val
			case g45KeyImage:
				f.image = val
			case g45KeyBackdropImage:
				f.backdropImage = val
			case g45KeyAltImage:
				f.altImage = val
			case g45KeyAltBackdropImage:
				f.altBackdropImage = val
			case g45KeyAudio:
				f.audio = val
			case g45KeyVideo:
				f.video = val
			}
			i = vni
		} else {
			ni2, v := g45skipValue(s, i, 1)
			if v != g45vOK {
				return g45Fields{}, v
			}
			// `images` rides the skip branch rather than the target branch:
			// its value is an object, and the target branch would force the
			// whole blob to the fallback. The extent g45skipValue just walked
			// IS the value, so capturing it is free — no second pass, no
			// Contains pre-filter over every blob.
			if t == g45KeyImagesRaw {
				f.images = s[i:ni2] // zero-copy; last occurrence wins
			}
			i = ni2
		}
		i = g45skipWS(s, i)
		if i >= len(s) {
			return g45Fields{}, g45vNoFields // truncated
		}
		switch s[i] {
		case ',':
			i++
		case '}':
			i = g45skipWS(s, i+1)
			if i != len(s) {
				return g45Fields{}, g45vNoFields // trailing garbage: encoding/json rejects
			}
			return f, g45vOK
		default:
			return g45Fields{}, g45vNoFields
		}
	}
}

// g45skipWS advances past JSON whitespace (space, tab, LF, CR) per the spec.
func g45skipWS(s string, i int) int {
	for i < len(s) {
		switch s[i] {
		case ' ', '\t', '\n', '\r':
			i++
		default:
			return i
		}
	}
	return i
}

// g45scanString validates the JSON string starting at s[i] (s[i] == '"') using
// exactly encoding/json's scanner rules and returns the content span [cs:ce)
// (between the quotes), whether it contained any backslash escape, and the
// index just past the closing quote. ok=false on any malformed string.
func g45scanString(s string, i int) (cs, ce int, hasEsc bool, next int, ok bool) {
	cs = i + 1
	j := i + 1
	for j < len(s) {
		c := s[j]
		switch {
		case c == '"':
			return cs, j, hasEsc, j + 1, true
		case c == '\\':
			hasEsc = true
			j++
			if j >= len(s) {
				return 0, 0, false, 0, false
			}
			switch s[j] {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				j++
			case 'u':
				if j+4 >= len(s) {
					return 0, 0, false, 0, false
				}
				for k := 1; k <= 4; k++ {
					if !hexTable[s[j+k]] { // shared hex LUT (fastsync.go)
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

// g45skipValue validates and skips exactly one JSON value beginning at s[i]
// (already positioned on its first non-whitespace byte), returning the index
// just past it plus a verdict. Validation matches encoding/json's checkValid
// plus the map decode's float64 conversion (see g45skipNumber), so
// g45vNoFields means the original map decode provably errors; the ONE
// deliberate divergence is the g45MaxDepth cap (far below encoding/json's
// 10000), which returns g45vFallback because such input may still be valid to
// encoding/json.
func g45skipValue(s string, i, depth int) (int, g45Verdict) {
	if depth > g45MaxDepth {
		return 0, g45vFallback
	}
	if i >= len(s) {
		return 0, g45vNoFields
	}
	switch c := s[i]; {
	case c == '"':
		_, _, _, next, ok := g45scanString(s, i)
		if !ok {
			return 0, g45vNoFields
		}
		return next, g45vOK
	case c == '-' || (c >= '0' && c <= '9'):
		return g45skipNumber(s, i)
	case c == 't':
		if strings.HasPrefix(s[i:], "true") {
			return i + 4, g45vOK
		}
		return 0, g45vNoFields
	case c == 'f':
		if strings.HasPrefix(s[i:], "false") {
			return i + 5, g45vOK
		}
		return 0, g45vNoFields
	case c == 'n':
		if strings.HasPrefix(s[i:], "null") {
			return i + 4, g45vOK
		}
		return 0, g45vNoFields
	case c == '{':
		return g45skipObject(s, i, depth)
	case c == '[':
		return g45skipArray(s, i, depth)
	default:
		return 0, g45vNoFields
	}
}

func g45skipObject(s string, i, depth int) (int, g45Verdict) {
	i++ // past '{'
	i = g45skipWS(s, i)
	if i < len(s) && s[i] == '}' {
		return i + 1, g45vOK
	}
	for {
		i = g45skipWS(s, i)
		if i >= len(s) || s[i] != '"' {
			return 0, g45vNoFields
		}
		_, _, _, ni, ok := g45scanString(s, i)
		if !ok {
			return 0, g45vNoFields
		}
		i = g45skipWS(s, ni)
		if i >= len(s) || s[i] != ':' {
			return 0, g45vNoFields
		}
		i = g45skipWS(s, i+1)
		ni, v := g45skipValue(s, i, depth+1)
		if v != g45vOK {
			return 0, v
		}
		i = g45skipWS(s, ni)
		if i >= len(s) {
			return 0, g45vNoFields
		}
		switch s[i] {
		case ',':
			i++
		case '}':
			return i + 1, g45vOK
		default:
			return 0, g45vNoFields
		}
	}
}

func g45skipArray(s string, i, depth int) (int, g45Verdict) {
	i++ // past '['
	i = g45skipWS(s, i)
	if i < len(s) && s[i] == ']' {
		return i + 1, g45vOK
	}
	for {
		i = g45skipWS(s, i)
		ni, v := g45skipValue(s, i, depth+1)
		if v != g45vOK {
			return 0, v
		}
		i = g45skipWS(s, ni)
		if i >= len(s) {
			return 0, g45vNoFields
		}
		switch s[i] {
		case ',':
			i++
		case ']':
			return i + 1, g45vOK
		default:
			return 0, g45vNoFields
		}
	}
}

// g45skipNumber validates and skips a JSON number starting at s[i] using the
// strict RFC 8259 grammar encoding/json enforces (no leading zeros, a digit
// required after '.', a digit required after the exponent sign), then checks
// float64 convertibility with the same strconv.ParseFloat call encoding/json's
// convertNumber makes. A grammar-valid but unconvertible number (e.g. 1e999)
// makes the original whole-blob map decode error and set nothing, so it
// returns g45vNoFields rather than letting the scanner fire on a blob the
// original algorithm rejected.
func g45skipNumber(s string, i int) (int, g45Verdict) {
	start := i
	n := len(s)
	if i < n && s[i] == '-' {
		i++
	}
	if i >= n {
		return 0, g45vNoFields
	}
	switch {
	case s[i] == '0':
		i++
	case s[i] >= '1' && s[i] <= '9':
		i++
		for i < n && s[i] >= '0' && s[i] <= '9' {
			i++
		}
	default:
		return 0, g45vNoFields
	}
	if i < n && s[i] == '.' {
		i++
		if i >= n || s[i] < '0' || s[i] > '9' {
			return 0, g45vNoFields
		}
		for i < n && s[i] >= '0' && s[i] <= '9' {
			i++
		}
	}
	if i < n && (s[i] == 'e' || s[i] == 'E') {
		i++
		if i < n && (s[i] == '+' || s[i] == '-') {
			i++
		}
		if i >= n || s[i] < '0' || s[i] > '9' {
			return 0, g45vNoFields
		}
		for i < n && s[i] >= '0' && s[i] <= '9' {
			i++
		}
	}
	if _, err := strconv.ParseFloat(s[start:i], 64); err != nil {
		return 0, g45vNoFields // out of float64 range: map decode errors too
	}
	return i, g45vOK
}
