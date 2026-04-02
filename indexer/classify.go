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
}

// classRule maps a code-level pattern to its class and tag.
type classRule struct {
	pattern string // substring to look for in SC code
	class   string
	tag     string
}

// rules are evaluated in order; first match wins.
// More specific patterns come before less specific ones (e.g. G45-FAT before G45-C).
var rules = []classRule{
	{pattern: "ART-NFA-MS1", class: "NFA", tag: "nfa"},
	{pattern: "G45-NFT", class: "G45-NFT", tag: "g45"},
	{pattern: "G45-AT", class: "G45-AT", tag: "g45"},
	{pattern: "G45-FAT", class: "G45-FAT", tag: "g45"},
	{pattern: "G45-NAME", class: "G45-NAME", tag: "g45"},
	{pattern: "G45-C", class: "G45-C", tag: "g45"},
	{pattern: "T345", class: "T345", tag: "g45"},
	{pattern: "telaVersion", class: "TELA-INDEX-1", tag: "tela"},
	{pattern: "docVersion", class: "TELA-DOC-1", tag: "tela"},
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

	// 3. Extract human-readable headers from variables.
	extractHeaders(&sc, vars)

	// 4. For G45 family, try to parse the JSON metadata blob.
	if len(sc.Tags) > 1 && sc.Tags[1] == "g45" {
		extractG45Metadata(&sc, vars)
	}

	return sc
}

// extractHeaders pulls name, description, and iconURL from SC string variables.
func extractHeaders(sc *SCClass, vars map[string]interface{}) {
	if vars == nil {
		return
	}
	if v, ok := vars["name"]; ok {
		sc.Name = fmt.Sprintf("%v", v)
	}
	if v, ok := vars["nameHdr"]; ok && sc.Name == "" {
		sc.Name = fmt.Sprintf("%v", v)
	}
	if v, ok := vars["description"]; ok {
		sc.Desc = fmt.Sprintf("%v", v)
	}
	if v, ok := vars["descrHdr"]; ok && sc.Desc == "" {
		sc.Desc = fmt.Sprintf("%v", v)
	}
	if v, ok := vars["iconURLHdr"]; ok {
		sc.IconURL = fmt.Sprintf("%v", v)
	}
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
