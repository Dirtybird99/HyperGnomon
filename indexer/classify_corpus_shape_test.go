package indexer

import (
	"encoding/json"
	"os"
	"testing"
)

// Corpus shape gate.
//
// The classify corpus is only useful if it holds what derod actually sends.
// The previous fixture did not: derod hex-encodes DVM STORE string values, but
// the committed `metadata` (and, in nfts.json.gz, `type`) were already decoded
// by some unrecorded step in whatever produced the files.
//
// The consequence was not cosmetic. Every gate in this package — the golden,
// the differential fuzz, the media-count oracle — exercised the extractors
// against a shape production never sees, so a live bug where the G45 extractors
// were handed hex, parsed nothing, and left Name/Desc/IconURL empty for every
// asset on mainnet passed all of them. It was caught only by querying
// /api/assets against a real daemon.
//
// This test is the tripwire for that. It asserts the property whose absence
// caused the bug, so the fixture cannot silently drift back to a convenient
// shape — including via a well-meaning "cleanup" of the capture tool.
//
// If this fails: do not decode the corpus to make it pass. Re-capture with
// cmd/corpusdump, which writes variable values verbatim.

// hexStringVars are keys whose values derod returns hex-encoded and which the
// corpus must therefore hold as hex. Deliberately not "all string vars": some
// values are genuinely binary or empty, and the point is to pin the ones the
// classifier parses.
var hexStringVars = []string{"metadata", "type", "metadataFormat"}

func TestCorpusHoldsRawDaemonShape(t *testing.T) {
	cols, nfts := mustCorpus(t)

	checked := map[string]int{}
	for _, half := range [][]corpusEntry{nfts, cols} {
		for i := range half {
			e := &half[i]
			for _, v := range e.Vars {
				key, ok := v.Key.(string)
				if !ok {
					continue
				}
				if !contains(hexStringVars, key) {
					continue
				}
				s, ok := v.Value.(string)
				if !ok || s == "" {
					// Empty values are legitimate — a contract simply has not
					// set that variable. Nothing to assert about their shape.
					continue
				}
				checked[key]++
				if len(s)%2 != 0 || !isHexString(s) {
					t.Errorf("%s: var %q is not hex-encoded — the corpus has been decoded, "+
						"which is exactly the drift that hid a live extraction bug.\n"+
						"  value: %.80q\n"+
						"  fix: re-capture with cmd/corpusdump, do not decode to satisfy this test",
						e.SCID, key, s)
					return // one report is enough; the whole file is suspect
				}
			}
		}
	}

	for _, key := range hexStringVars {
		if checked[key] == 0 {
			t.Errorf("no non-empty %q values found in the corpus — this gate is vacuous", key)
		}
	}
	t.Logf("verified hex shape: %v", checked)
}

// TestCorpusManifestMatches keeps the manifest honest about what sits beside
// it, so "which chain state is this fixture?" has an answer that cannot rot
// independently of the data.
func TestCorpusManifestMatches(t *testing.T) {
	raw, err := os.ReadFile("testdata/corpus_manifest.json")
	if err != nil {
		t.Fatalf("read manifest: %v (regenerate with cmd/corpusdump)", err)
	}
	var m struct {
		TopoHeight int64 `json:"topoheight"`
		Files      map[string]struct {
			Entries int `json:"entries"`
		} `json:"files"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("parse manifest: %v", err)
	}
	if m.TopoHeight <= 0 {
		t.Errorf("manifest topoheight = %d, want a real captured height", m.TopoHeight)
	}
	cols, nfts := mustCorpus(t)
	for name, gotLen := range map[string]int{"nfts.json.gz": len(nfts), "collections.json.gz": len(cols)} {
		want := m.Files[name].Entries
		if want != gotLen {
			t.Errorf("%s holds %d entries, manifest claims %d", name, gotLen, want)
		}
	}
}

func contains(hay []string, needle string) bool {
	for _, s := range hay {
		if s == needle {
			return true
		}
	}
	return false
}
