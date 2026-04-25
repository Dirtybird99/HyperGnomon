package indexer

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hypergnomon/hypergnomon/structures"
)

// SCClass represents a classified smart contract type.
type SCClass struct {
	Class   string   // e.g. "G45-NFT", "TELA-DOC-1", "NFA", "NAMESERVICE", "UNKNOWN"
	Tags    []string // e.g. ["g45", "nfa", "tela", "all"]
	Name    string   // from SC variables if available
	Desc    string   // from SC variables if available
	IconURL string   // from SC variables if available
	DURL    string   // TELA app identifier (vars["dURL"])
	Version string   // legacy: telaVersion / docVersion — kept for API compat; canonical TELA has no version STORE key
	// DocType carries the TELA-DOC-1 content-type enum from vars["docType"]:
	// TELA-STATIC-1 / TELA-HTML-1 / TELA-JSON-1 / TELA-CSS-1 / TELA-JS-1 /
	// TELA-MD-1 / TELA-GO-1. Empty for non-DOC classes.
	DocType string
	// Mods lists the TELA-MOD-1 variant tags declared in a TELA-INDEX-1's
	// `mods` STORE variable (comma-separated on-chain). Empty for non-INDEX
	// classes or INDEXes without MODs.
	Mods []string
	// DocShard reports whether this SCID is a TELA DocShard, detected via
	// `dURL` suffix: `.shard` on DOCs, `.shards` on the parent INDEX.
	DocShard bool
}

// classRule maps a code-level pattern to its class and tag.
type classRule struct {
	pattern string // substring to look for in SC code
	class   string
	tag     string
}

// rules are evaluated in order; first match wins. Rule ordering matters when
// patterns overlap — list the more-specific pattern first.
//
// Per civilware/tela canonical spec: TELA contracts embed the literal
// standard-name token (`"TELA-INDEX-1"`, `"TELA-DOC-1"`, `"TELA-MOD-1"`) in
// source. Previous patterns (`"telaVersion"`, `"docVersion"`) worked only
// because those identifiers happened to appear in template source; the
// literal-name match is authoritative and the version-identifier match is
// kept as a defense-in-depth fallback for any off-spec contract that still
// uses those names without the canonical token.
var rules = []classRule{
	// NFA / ART family — highest priority (specific marker).
	{pattern: "ART-NFA-MS1", class: "NFA", tag: "nfa"},
	// G45 family — most-specific first so substring overlap (G45-FAT vs
	// G45-AT) can't produce a wrong match even under accidental source
	// ordering. Within "G45-F*" / "G45-A*" / "G45-C*" / "G45-N*" families,
	// rule order is driven by prefix length, not alphabetic.
	{pattern: "G45-FAT", class: "G45-FAT", tag: "g45"},
	{pattern: "G45-NFT", class: "G45-NFT", tag: "g45"},
	{pattern: "G45-NAME", class: "G45-NAME", tag: "g45"},
	{pattern: "G45-AT", class: "G45-AT", tag: "g45"},
	{pattern: "G45-C", class: "G45-C", tag: "g45"},
	{pattern: "T345", class: "T345", tag: "g45"},
	// TELA family — match canonical standard-name tokens. The MOD token
	// comes before INDEX because a MOD contract may also contain the
	// string "TELA-INDEX-1" in commented helpers.
	{pattern: "TELA-MOD-1", class: "TELA-MOD-1", tag: "tela"},
	{pattern: "TELA-INDEX-1", class: "TELA-INDEX-1", tag: "tela"},
	{pattern: "TELA-DOC-1", class: "TELA-DOC-1", tag: "tela"},
	// Legacy-identifier fallbacks — only match if the canonical tokens
	// above missed. Some older TELA SCs (pre-rename) embedded these
	// identifiers without the standard-name token.
	{pattern: "telaVersion", class: "TELA-INDEX-1", tag: "tela"},
	{pattern: "docVersion", class: "TELA-DOC-1", tag: "tela"},
}

// tagsForClass returns the ClassMeta.Tags slice for a given class name.
// Mirrors the rules table so the fastsync probe can label SCIDs without
// re-running the full pattern match when it already knows the class.
// Always includes "all" as the first tag for the universal-filter convention.
func tagsForClass(class string) []string {
	for _, r := range rules {
		if r.class == class {
			return []string{"all", r.tag}
		}
	}
	switch class {
	case "NAMESERVICE":
		return []string{"all", "nameservice"}
	case "GNOMONSC":
		return []string{"all", "gnomon"}
	case "DERO-ASSET":
		return []string{"all", "asset"}
	}
	return []string{"all"}
}

// classifyDEROAsset is a last-resort fallback that recognises pre-G45 token
// contracts — those using `InitializePrivate` + `SEND_ASSET_TO_ADDRESS`
// without any G45 / NFA / TELA headers. Civilware's classifier carries this
// fallback so the `all` filter shows every SC, not just the branded ones.
func classifyDEROAsset(code string) bool {
	return strings.Contains(code, "InitializePrivate") &&
		strings.Contains(code, "SEND_ASSET_TO_ADDRESS")
}

// ClassifySC determines the class and tags of a smart contract based on its
// SCID, code, and stored variables. Every returned SCClass includes the "all"
// tag so callers can use it as a universal filter.
func ClassifySC(scid string, code string, vars map[string]interface{}) SCClass {
	sc := SCClass{
		Class: "UNKNOWN",
		Tags:  []string{"all"},
	}

	// 1. Well-known SCIDs take priority over code inspection.
	switch scid {
	case structures.NameServiceSCID:
		sc.Class = "NAMESERVICE"
		sc.Tags = append(sc.Tags, "nameservice")
		extractHeaders(&sc, vars)
		return sc
	case structures.GnomonSCID_Mainnet, structures.GnomonSCID_Testnet:
		sc.Class = "GNOMONSC"
		sc.Tags = append(sc.Tags, "gnomon")
		extractHeaders(&sc, vars)
		return sc
	}

	// 2. Pattern-match against SC code (first match wins).
	for _, r := range rules {
		if strings.Contains(code, r.pattern) {
			sc.Class = r.class
			sc.Tags = append(sc.Tags, r.tag)
			break
		}
	}

	// 3. Last-resort: pre-G45 asset fallback. Only triggers when no branded
	// rule matched above.
	if sc.Class == "UNKNOWN" && code != "" && classifyDEROAsset(code) {
		sc.Class = "DERO-ASSET"
		sc.Tags = append(sc.Tags, "asset")
	}

	// 4. Extract human-readable headers from variables.
	extractHeaders(&sc, vars)

	// 5. For G45 family, try to parse the JSON metadata blob.
	if len(sc.Tags) > 1 && sc.Tags[1] == "g45" {
		extractG45Metadata(&sc, vars)
	}

	// 6. TELA-specific dURL + version fields + DocType + Mods + DocShard.
	extractTELAFields(&sc, vars)

	return sc
}

// extractTELAFields pulls dURL, the legacy version key, docType (DOC-1),
// mods (INDEX-1), and the DocShard flag (via dURL suffix) from vars.
// No-op unless the class is a TELA family member.
func extractTELAFields(sc *SCClass, vars map[string]interface{}) {
	if vars == nil {
		return
	}
	durl, version := telaFieldsForClass(sc.Class, vars)
	if durl != "" {
		sc.DURL = durl
	}
	if version != "" {
		sc.Version = version
	}
	// DocType enum (TELA-DOC-1 family only). Hex-decoded if derod returned hex.
	if sc.Class == "TELA-DOC-1" {
		if v, ok := vars["docType"]; ok {
			sc.DocType = decodeHexIfPrintable(fmt.Sprintf("%v", v))
		}
	}
	// Mods list (TELA-INDEX-1 only). Comma-separated tag list, hex-decoded first.
	if sc.Class == "TELA-INDEX-1" {
		if v, ok := vars["mods"]; ok {
			if s := decodeHexIfPrintable(fmt.Sprintf("%v", v)); s != "" {
				for _, m := range strings.Split(s, ",") {
					m = strings.TrimSpace(m)
					if m != "" {
						sc.Mods = append(sc.Mods, m)
					}
				}
			}
		}
	}
	// DocShard detection: `.shard` suffix on DOC dURL, `.shards` on INDEX.
	if sc.DURL != "" {
		if strings.HasSuffix(sc.DURL, ".shard") || strings.HasSuffix(sc.DURL, ".shards") {
			sc.DocShard = true
		}
	}
}

// telaFieldsForClass returns (dURL, version) read from vars for a given class
// label. Used by fastsync + the refresher where the class was proven in a
// prior step (so we don't need ClassifySC's rules to re-match it) but we
// still want the standard TELA field extraction.
//
// Decodes the hex-encoded string values derod returns for STORE strings.
func telaFieldsForClass(class string, vars map[string]interface{}) (string, string) {
	if vars == nil {
		return "", ""
	}
	var durl, version string
	if v, ok := vars["dURL"]; ok {
		durl = decodeHexIfPrintable(fmt.Sprintf("%v", v))
	}
	switch class {
	case "TELA-INDEX-1":
		if v, ok := vars["telaVersion"]; ok {
			version = decodeHexIfPrintable(fmt.Sprintf("%v", v))
		}
	case "TELA-DOC-1":
		if v, ok := vars["docVersion"]; ok {
			version = decodeHexIfPrintable(fmt.Sprintf("%v", v))
		}
	}
	return durl, version
}

// extractHeaders pulls name, description, and iconURL from SC string variables.
func extractHeaders(sc *SCClass, vars map[string]interface{}) {
	if vars == nil {
		return
	}
	// Primary: canonical TELA spec keys (civilware/tela).
	if v, ok := vars["var_header_name"]; ok {
		sc.Name = decodeHexIfPrintable(fmt.Sprintf("%v", v))
	}
	if v, ok := vars["var_header_description"]; ok {
		sc.Desc = decodeHexIfPrintable(fmt.Sprintf("%v", v))
	}
	if v, ok := vars["var_header_icon"]; ok {
		sc.IconURL = decodeHexIfPrintable(fmt.Sprintf("%v", v))
	}
	// Fallback 1: legacy *Hdr keys (older test fixtures / deprecated SCs).
	if sc.Name == "" {
		if v, ok := vars["nameHdr"]; ok {
			sc.Name = decodeHexIfPrintable(fmt.Sprintf("%v", v))
		}
	}
	if sc.Desc == "" {
		if v, ok := vars["descrHdr"]; ok {
			sc.Desc = decodeHexIfPrintable(fmt.Sprintf("%v", v))
		}
	}
	if sc.IconURL == "" {
		if v, ok := vars["iconURLHdr"]; ok {
			sc.IconURL = decodeHexIfPrintable(fmt.Sprintf("%v", v))
		}
	}
	// Fallback 2: free-form name/description (NFA, G45).
	if sc.Name == "" {
		if v, ok := vars["name"]; ok {
			sc.Name = decodeHexIfPrintable(fmt.Sprintf("%v", v))
		}
	}
	if sc.Desc == "" {
		if v, ok := vars["description"]; ok {
			sc.Desc = decodeHexIfPrintable(fmt.Sprintf("%v", v))
		}
	}
}

// decodeHexIfPrintable returns the hex-decoded form of s when s is a valid
// even-length hex string AND the decoded bytes are printable UTF-8. Otherwise
// returns s unchanged.
//
// Why: DERO's derod returns DVM STORE string values hex-encoded on the
// GetSC response (empirical — every var_header_* value on live mainnet
// comes back as hex of the ASCII string). Clients downstream of us expect
// readable text, so we decode at the indexer layer. The printable-UTF-8
// guard means genuinely-binary values (hashes, SCIDs stored as raw bytes)
// pass through untouched.
func decodeHexIfPrintable(s string) string {
	if len(s) < 2 || len(s)%2 != 0 {
		return s
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9') && !(c >= 'a' && c <= 'f') && !(c >= 'A' && c <= 'F') {
			return s
		}
	}
	decoded := make([]byte, len(s)/2)
	for i := 0; i < len(decoded); i++ {
		decoded[i] = hexNibbleByte(s[i*2])<<4 | hexNibbleByte(s[i*2+1])
	}
	// Printability check: ASCII printable (space..tilde) or common whitespace,
	// OR any byte >= 0x80 that's valid UTF-8 when combined with neighbours.
	// Conservative: require all bytes to be printable ASCII or valid UTF-8
	// multibyte sequence. If any byte is a control char below 0x20 (except
	// tab/newline), reject the hex interpretation.
	if !looksLikePrintableUTF8(decoded) {
		return s
	}
	return string(decoded)
}

func hexNibbleByte(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	}
	return 0
}

// looksLikePrintableUTF8 reports whether b is mostly printable text —
// either ASCII in [0x20, 0x7e] plus tab/newline/carriage-return, or valid
// UTF-8 multibyte sequences. Used to gate the hex-decode heuristic so we
// don't mangle genuinely-binary values.
func looksLikePrintableUTF8(b []byte) bool {
	if len(b) == 0 {
		return false
	}
	for i := 0; i < len(b); {
		c := b[i]
		switch {
		case c == '\t' || c == '\n' || c == '\r':
			i++
		case c >= 0x20 && c <= 0x7e:
			i++
		case c >= 0xc2 && c <= 0xf4:
			// UTF-8 multibyte lead: advance past the expected continuation bytes.
			n := utf8RuneLen(c)
			if i+n > len(b) {
				return false
			}
			for j := 1; j < n; j++ {
				if b[i+j] < 0x80 || b[i+j] > 0xbf {
					return false
				}
			}
			i += n
		default:
			return false
		}
	}
	return true
}

func utf8RuneLen(lead byte) int {
	switch {
	case lead < 0x80:
		return 1
	case lead < 0xe0:
		return 2
	case lead < 0xf0:
		return 3
	case lead < 0xf8:
		return 4
	}
	return 1
}

// extractG45Metadata parses the JSON "metadata" variable common to G45 assets.
// If individual header fields were not already populated, this fills them in
// from the metadata blob (keys: "name", "description", "icon").
func extractG45Metadata(sc *SCClass, vars map[string]interface{}) {
	raw, ok := vars["metadata"]
	if !ok {
		return
	}
	str, ok := raw.(string)
	if !ok {
		return
	}
	var meta map[string]interface{}
	if err := json.Unmarshal([]byte(str), &meta); err != nil {
		return
	}
	if sc.Name == "" {
		if v, ok := meta["name"]; ok {
			sc.Name = fmt.Sprintf("%v", v)
		}
	}
	if sc.Desc == "" {
		if v, ok := meta["description"]; ok {
			sc.Desc = fmt.Sprintf("%v", v)
		}
	}
	if sc.IconURL == "" {
		if v, ok := meta["icon"]; ok {
			sc.IconURL = fmt.Sprintf("%v", v)
		}
	}
}
