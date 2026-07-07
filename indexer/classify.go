package indexer

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/hypergnomon/hypergnomon/structures"
)

// SCClass represents a classified smart contract type.
//
// Tags aliases a shared, package-global slice (len == cap): callers MUST NOT
// mutate its elements in place or reorder it — doing so corrupts the tag set
// for every subsequent classification process-wide. Appending is safe (len ==
// cap forces a reallocation); to modify, copy first (slices.Clone).
type SCClass struct {
	Class   string   // e.g. "G45-NFT", "TELA-DOC-1", "NFA", "NAMESERVICE", "UNKNOWN"
	Tags    []string // shared read-only slice, e.g. ["all", "g45"] — see struct doc
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
	// tags is the precomputed, shared ClassMeta.Tags slice for this rule's
	// class — ["all", tag] with len==cap. Populated once in init() so the
	// classify hot paths alias it instead of allocating per call. len==cap
	// makes any caller append reallocate (copy-on-append safety); see the
	// anti-alias gate in classify_tags_alias_test.go.
	tags []string
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
	// Swap / DEX family — entrypoint-name heuristic. A StartSwap entrypoint
	// marks an atomic-swap / DEX contract (parity with siteraiser/simple-gnomon's
	// `swaps` default filter). This sits in the rules table so it matches in
	// ClassifySC step 2, ahead of the step-3 DERO-ASSET fallback: a swap
	// contract that also disburses assets (InitializePrivate +
	// SEND_ASSET_TO_ADDRESS) classifies as SWAP, not DERO-ASSET. No substring
	// collision with the branded tokens above or the TELA tokens below.
	{pattern: "StartSwap", class: "SWAP", tag: "swap"},
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
	// EPOCH fair-mining family (civilware/epoch) — LAST so it never overrides a
	// primary standard (TELA/G45/NFA/swap): epoch is an add-on a dApp can enable,
	// so only a contract with no other branded token classifies as EPOCH. Two
	// distinctive markers (parity with HOLOGRAM gnomon_tags.go's epoch filter);
	// bare "EPOCH" is avoided as it risks comment false-positives.
	{pattern: "epochEnabled", class: "EPOCH", tag: "epoch"},
	{pattern: "crowd_mining", class: "EPOCH", tag: "epoch"},
}

// Precomputed, shared Tags slices returned by classification. Each is a slice
// literal (guaranteed len==cap), so any caller append reallocates rather than
// writing into the shared backing array. The specials cover the well-known
// SCIDs and the DERO-ASSET fallback; tagsAll is the bare universal-filter tag
// returned for UNKNOWN. Per-rule class tags live on classRule.tags (built in
// init). These arrays are never mutated in place — see the audit note on
// classRule.tags and the anti-alias gate in classify_tags_alias_test.go.
var (
	tagsAll         = []string{"all"}
	tagsNameservice = []string{"all", "nameservice"}
	tagsGnomon      = []string{"all", "gnomon"}
	tagsAsset       = []string{"all", "asset"}
)

// init builds the per-rule shared Tags slice once, mirroring the historic
// []string{"all", r.tag}. Package-var initialization (including rules) is
// guaranteed complete before init runs.
func init() {
	for i := range rules {
		rules[i].tags = []string{"all", rules[i].tag}
	}
}

// tagsForClass returns the shared ClassMeta.Tags slice for a given class name.
// Mirrors the rules table so the fastsync probe can label SCIDs without
// re-running the full pattern match when it already knows the class.
// Always includes "all" as the first tag for the universal-filter convention.
// The returned slice is shared and immutable (len==cap); callers must not
// mutate elements in place — append is safe (it reallocates).
func tagsForClass(class string) []string {
	for i := range rules {
		if rules[i].class == class {
			return rules[i].tags
		}
	}
	switch class {
	case "NAMESERVICE":
		return tagsNameservice
	case "GNOMONSC":
		return tagsGnomon
	case "DERO-ASSET":
		return tagsAsset
	}
	return tagsAll
}

// classifyDEROAsset is a last-resort fallback that recognizes pre-G45 token
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
//
// The returned Tags slice is shared and read-only — never mutate it in place;
// see the SCClass doc.
func ClassifySC(scid string, code string, vars map[string]interface{}) SCClass {
	// Tags alias precomputed, shared, len==cap slices instead of being built
	// per call: UNKNOWN keeps the bare ["all"]; each matched / well-known-SCID
	// path assigns its 2-tag slice (value/len/order identical to the old
	// make+append). Zero Tags allocation per classification; append-safe
	// because len==cap.
	sc := SCClass{Class: "UNKNOWN", Tags: tagsAll}

	// 1. Well-known SCIDs take priority over code inspection.
	switch scid {
	case structures.NameServiceSCID:
		sc.Class = "NAMESERVICE"
		sc.Tags = tagsNameservice
		extractHeaders(&sc, vars)
		return sc
	case structures.GnomonSCID_Mainnet, structures.GnomonSCID_Testnet:
		sc.Class = "GNOMONSC"
		sc.Tags = tagsGnomon
		extractHeaders(&sc, vars)
		return sc
	}

	// 2. Pattern-match against SC code (first match wins).
	for i := range rules {
		if strings.Contains(code, rules[i].pattern) {
			sc.Class = rules[i].class
			sc.Tags = rules[i].tags
			break
		}
	}

	// 3. Last-resort: pre-G45 asset fallback. Only triggers when no branded
	// rule matched above.
	if sc.Class == "UNKNOWN" && code != "" && classifyDEROAsset(code) {
		sc.Class = "DERO-ASSET"
		sc.Tags = tagsAsset
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

// ClassifySCVars is the allocation-light variant for indexer hot paths that
// already hold parsed SC variables as a slice. It mirrors ClassifySC without
// first materializing a map.
//
// The returned Tags slice is shared and read-only — never mutate it in place;
// see the SCClass doc.
func ClassifySCVars(scid string, code string, vars []*structures.SCIDVariable) SCClass {
	// Tags alias precomputed, shared, len==cap slices instead of being built
	// per call — identical value/len/order to the old make+append, zero Tags
	// allocation, append-safe because len==cap. See ClassifySC.
	sc := SCClass{Class: "UNKNOWN", Tags: tagsAll}

	switch scid {
	case structures.NameServiceSCID:
		sc.Class = "NAMESERVICE"
		sc.Tags = tagsNameservice
		extractClassVars(&sc, vars)
		return sc
	case structures.GnomonSCID_Mainnet, structures.GnomonSCID_Testnet:
		sc.Class = "GNOMONSC"
		sc.Tags = tagsGnomon
		extractClassVars(&sc, vars)
		return sc
	}

	for i := range rules {
		if strings.Contains(code, rules[i].pattern) {
			sc.Class = rules[i].class
			sc.Tags = rules[i].tags
			break
		}
	}
	if sc.Class == "UNKNOWN" && code != "" && classifyDEROAsset(code) {
		sc.Class = "DERO-ASSET"
		sc.Tags = tagsAsset
	}
	extractClassVars(&sc, vars)
	return sc
}

// ClassifySCVarsWithClass classifies the variables of a SCID whose class was
// already proven by a code probe (fastsync phase 1) or the class bucket
// (RefreshClassVars). Seeding sc.Class/Tags up front lets extractClassVars
// apply the class-gated fields — Version, Mods, DocType, DURL, DocShard, and
// the G45 metadata blob — in its single pass, replacing the pre-fix pattern
// of ClassifySCVars + telaFieldsForClassVars that walked the variable slice
// three times per SCID (audit #8).
//
// An empty class falls back to ClassifySCVars' code-less path (UNKNOWN).
func ClassifySCVarsWithClass(scid, class string, vars []*structures.SCIDVariable) SCClass {
	if class == "" {
		return ClassifySCVars(scid, "", vars)
	}
	sc := SCClass{
		Class: class,
		Tags:  tagsForClass(class),
	}
	extractClassVars(&sc, vars)
	return sc
}

func extractClassVars(sc *SCClass, vars []*structures.SCIDVariable) {
	if vars == nil {
		return
	}
	var headerName, legacyName, freeName string
	var headerDesc, legacyDesc, freeDesc string
	var headerIcon, legacyIcon string
	var durl, telaVersion, docVersion, docType, mods, metadata string

	// Stringify ONLY inside matched cases: a live TELA INDEX carries dozens
	// of non-matching vars (rating-address keys with uint64 values, DOC
	// pointers, counters), and paying varString's formatting cost for every
	// one of them dominated this loop pre-fix (audit #7).
	for _, v := range vars {
		k, ok := v.Key.(string)
		if !ok {
			continue
		}
		switch k {
		case "var_header_name":
			headerName = decodeHexIfPrintable(varString(v.Value))
		case "var_header_description":
			headerDesc = decodeHexIfPrintable(varString(v.Value))
		case "var_header_icon":
			headerIcon = decodeHexIfPrintable(varString(v.Value))
		case "nameHdr":
			legacyName = decodeHexIfPrintable(varString(v.Value))
		case "descrHdr":
			legacyDesc = decodeHexIfPrintable(varString(v.Value))
		case "iconURLHdr":
			legacyIcon = decodeHexIfPrintable(varString(v.Value))
		case "name":
			freeName = decodeHexIfPrintable(varString(v.Value))
		case "description":
			freeDesc = decodeHexIfPrintable(varString(v.Value))
		case "dURL":
			durl = decodeHexIfPrintable(varString(v.Value))
		case "telaVersion":
			telaVersion = decodeHexIfPrintable(varString(v.Value))
		case "docVersion":
			docVersion = decodeHexIfPrintable(varString(v.Value))
		case "docType":
			docType = decodeHexIfPrintable(varString(v.Value))
		case "mods":
			mods = decodeHexIfPrintable(varString(v.Value))
		case "metadata":
			metadata = varString(v.Value)
		}
	}

	switch {
	case headerName != "":
		sc.Name = headerName
	case legacyName != "":
		sc.Name = legacyName
	case freeName != "":
		sc.Name = freeName
	}
	switch {
	case headerDesc != "":
		sc.Desc = headerDesc
	case legacyDesc != "":
		sc.Desc = legacyDesc
	case freeDesc != "":
		sc.Desc = freeDesc
	}
	switch {
	case headerIcon != "":
		sc.IconURL = headerIcon
	case legacyIcon != "":
		sc.IconURL = legacyIcon
	}

	if len(sc.Tags) > 1 && sc.Tags[1] == "g45" && metadata != "" {
		extractG45MetadataString(sc, metadata)
	}

	if durl != "" {
		sc.DURL = durl
	}
	switch sc.Class {
	case "TELA-INDEX-1":
		if telaVersion != "" {
			sc.Version = telaVersion
		}
		if mods != "" {
			for _, m := range strings.Split(mods, ",") {
				m = strings.TrimSpace(m)
				if m != "" {
					sc.Mods = append(sc.Mods, m)
				}
			}
		}
	case "TELA-DOC-1":
		if docVersion != "" {
			sc.Version = docVersion
		}
		if docType != "" {
			sc.DocType = docType
		}
	}
	if sc.DURL != "" {
		if strings.HasSuffix(sc.DURL, ".shard") || strings.HasSuffix(sc.DURL, ".shards") {
			sc.DocShard = true
		}
	}
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

func telaFieldsForClassVars(class string, vars []*structures.SCIDVariable) (string, string) {
	if vars == nil {
		return "", ""
	}
	var durl, version string
	if v, ok := lookupVar(vars, "dURL"); ok {
		durl = decodeHexIfPrintable(varString(v))
	}
	switch class {
	case "TELA-INDEX-1":
		if v, ok := lookupVar(vars, "telaVersion"); ok {
			version = decodeHexIfPrintable(varString(v))
		}
	case "TELA-DOC-1":
		if v, ok := lookupVar(vars, "docVersion"); ok {
			version = decodeHexIfPrintable(varString(v))
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
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return s
		}
	}
	decoded := make([]byte, len(s)/2)
	for i := 0; i < len(decoded); i++ {
		decoded[i] = hexNibbleByte(s[i*2])<<4 | hexNibbleByte(s[i*2+1])
	}
	// Printability check: ASCII printable (space..tilde) or common whitespace,
	// OR any byte >= 0x80 that's valid UTF-8 when combined with neighbors.
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
	extractG45MetadataString(sc, str)
}

func extractG45MetadataString(sc *SCClass, str string) {
	// Scanner tier (H9): a hand-rolled, zero-alloc top-level scan that extracts
	// simple-string "name"/"description"/"icon" values without any encoding/json
	// machinery. It fires only when it can prove byte-equivalence with the
	// decoders below (see classify_g45_scan.go); on ANY deviation it hands
	// back a non-OK verdict and the untouched SCClass either falls through to
	// the original map decode (g45vFallback) or skips it outright when that
	// decode provably sets nothing (g45vNoFields — bad JSON, non-object, or a
	// number the map decode would reject; see the verdict constants). The
	// empty-guards here mirror the map path exactly (fill only when the field
	// is still ""). The scanner returns zero-copy substrings of str; for
	// blobs past g45CloneThreshold the extracted fields are cloned so a tiny
	// Name cannot pin a huge (potentially hostile) blob in ClassMeta held by
	// eventbus queues or the seed cache. Real corpus blobs max out under 2KB,
	// so the guard never fires on the benchmark path.
	switch name, desc, icon, verdict := g45ScanMetaVerdict(str); verdict {
	case g45vOK:
		if len(str) > g45CloneThreshold {
			name = strings.Clone(name)
			desc = strings.Clone(desc)
			icon = strings.Clone(icon)
		}
		if sc.Name == "" {
			sc.Name = name
		}
		if sc.Desc == "" {
			sc.Desc = desc
		}
		if sc.IconURL == "" {
			sc.IconURL = icon
		}
		return
	case g45vNoFields:
		return
	}
	extractG45MetadataFallback(sc, str)
}

// extractG45MetadataFallback is the ORIGINAL map[string]interface{} decode,
// kept byte-for-byte equivalent to pre-optimization Gnomon behavior: exact
// (case-sensitive) key lookups, and whole-blob strictness — if ANY value in
// the blob fails decoding (e.g. a number outside float64 range), nothing is
// set. It is the correctness oracle behind the scanner tier and the
// differential-test reference; every scanner decline lands here so unusual
// shapes always get original semantics. A previous json.RawMessage struct
// fast path was removed: encoding/json struct decoding matches keys
// case-insensitively and skips unconvertible unknown fields, both of which
// silently diverged from this original algorithm.
//
// varString renders every JSON-decoded type identically to
// fmt.Sprintf("%v", …): string and float64 via its typed branches (float uses
// 'g'/-1/64, exactly what %v produces), every other type through the same fmt
// default.
func extractG45MetadataFallback(sc *SCClass, str string) {
	// Read-only view over str's backing array: json.Unmarshal only reads its
	// input, and map decoding copies out any retained bytes, so nothing
	// aliases this view. See readOnlyBytes.
	var meta map[string]interface{}
	if err := json.Unmarshal(readOnlyBytes(str), &meta); err != nil {
		return
	}
	if sc.Name == "" {
		if v, ok := meta["name"]; ok {
			sc.Name = varString(v)
		}
	}
	if sc.Desc == "" {
		if v, ok := meta["description"]; ok {
			sc.Desc = varString(v)
		}
	}
	if sc.IconURL == "" {
		if v, ok := meta["icon"]; ok {
			sc.IconURL = varString(v)
		}
	}
}

func lookupVar(vars []*structures.SCIDVariable, key string) (interface{}, bool) {
	for _, v := range vars {
		if k, ok := v.Key.(string); ok && k == key {
			return v.Value, true
		}
	}
	return nil, false
}

func varString(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case uint64:
		return strconv.FormatUint(x, 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case int:
		return strconv.Itoa(x)
	case float64:
		// 'g'/-1/64 is exactly what fmt's %v produces for float64.
		return strconv.FormatFloat(x, 'g', -1, 64)
	default:
		return fmt.Sprintf("%v", v)
	}
}
