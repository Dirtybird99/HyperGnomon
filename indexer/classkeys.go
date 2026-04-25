package indexer

import (
	"sync"
	"sync/atomic"

	"github.com/deroproject/derohe/rpc"

	"github.com/hypergnomon/hypergnomon/structures"
)

// pickRefreshKeys decides whether this invocation of RefreshClassVars
// should use the fast-path (non-empty returned keys) or the full manifest
// (nil return). Per-class counter rolls over every fullRefreshEvery calls
// to catch new keys.
func pickRefreshKeys(class string) []string {
	keys := classKeys[class]
	if len(keys) == 0 {
		return nil
	}
	v, _ := refreshCounters.LoadOrStore(class, new(atomic.Int64))
	n := v.(*atomic.Int64).Add(1)
	if n%fullRefreshEvery == 0 {
		return nil
	}
	return keys
}

// scVarsFromKeyValues zips the input keys with the daemon's ordered
// ValuesString response (see derohe GetSC handler — when p.KeysString is
// set, result.ValuesString[i] is the value for input[i]). Output matches
// parseSCVariables shape so downstream code is unchanged.
//
// Skips "NOT AVAILABLE" entries (daemon's signal for missing keys) so
// ClassMeta.Name etc. don't get filled with error strings.
func scVarsFromKeyValues(keys []string, values []string) []*structures.SCIDVariable {
	n := len(keys)
	if len(values) < n {
		n = len(values)
	}
	out := make([]*structures.SCIDVariable, 0, n)
	for i := 0; i < n; i++ {
		v := values[i]
		if len(v) >= 13 && v[:13] == "NOT AVAILABLE" {
			continue
		}
		out = append(out, &structures.SCIDVariable{Key: keys[i], Value: v})
	}
	return out
}

// buildGetSCParams returns the jrpc2 params for one SCID, selecting
// fast-path (Variables=false + KeysString) vs full (Variables=true) based
// on whether keys is nil.
func buildGetSCParams(scid string, keys []string) rpc.GetSC_Params {
	if keys == nil {
		return rpc.GetSC_Params{SCID: scid, Variables: true}
	}
	return rpc.GetSC_Params{SCID: scid, Variables: false, KeysString: keys}
}

// classKeys maps a class name to the string-keyed SC variables we actually
// read for its metadata. When set, RefreshClassVars can ask the daemon for
// only these keys (GetSC Variables=false KeysString=[...]) instead of the
// full manifest — daemon skips its cursor scan and response is O(|keys|)
// rather than O(|all SC vars|). Derived from extractHeaders in classify.go
// plus class-specific fields.
//
// Leave empty → fall back to full Variables=true fetch.
//
// TELA-INDEX-1 is intentionally absent from this map so every refresh
// fetches the full variable set. Rationale: the INDEX's route table is a
// variable-length list of `DOC1..DOCn` entries (n unknown without a full
// fetch), and we also need to tolerate both v1.0.0 legacy keys
// (nameHdr/descrHdr/iconURLHdr/telaVersion) and v1.1.0 canonical keys
// (var_header_*). There are only ~100 TELA-INDEX-1 contracts on mainnet,
// so the per-refresh cost of 100 batched Variables=true calls is
// negligible (under a second, end to end).
var classKeys = map[string][]string{
	// TELA-DOC-1: bounded, known-shape — fast-path is a clear win. Covers
	// both the v1.0.0 (nameHdr/descrHdr/iconURLHdr/docVersion) and v1.1.0
	// (var_header_*) key families plus docType, subDir, and the two
	// Schnorr signature components.
	"TELA-DOC-1": {
		"var_header_name", "var_header_description", "var_header_icon",
		"nameHdr", "descrHdr", "iconURLHdr",
		"dURL", "docType", "subDir",
		"docVersion", "fileCheckC", "fileCheckS",
	},
}

// fullRefreshEvery N: every N'th call to RefreshClassVars falls back to the
// full Variables=true fetch so new keys added to an SC (e.g. ratings, votes)
// still get picked up. 10 * 60s = 10 minutes staleness ceiling for unknown
// keys, in exchange for 9/10 fast-path calls.
const fullRefreshEvery = 10

// refreshCounters counts RefreshClassVars invocations per class; bumped every
// call, mod'd against fullRefreshEvery to pick full vs fast path. Package
// level (not per-Indexer) because the class key manifest is also package
// level — the state is a cache of static knowledge, not indexer-specific.
var refreshCounters sync.Map // map[string]*atomic.Int64
