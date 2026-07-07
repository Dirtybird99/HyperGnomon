package indexer

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// This file is part of the classify-corpus measurement apparatus (loader +
// correctness gates). It is IMMUTABLE to optimization experiments: a loop
// that can edit its own ruler always wins. The paired benchmark lives in
// classify_corpus_bench_test.go; both feed off the real-mainnet G45 corpus
// committed under testdata/ (45,514 G45-NFT + 75 G45-C snapshots).

// updateClassifyGolden rewrites testdata/classify_golden.json.gz from the
// current classifier output:
//
//	go test ./indexer/ -run '^TestClassifyCorpusGolden$' -update
//
// Regeneration is a deliberate human act after an intended behavior change —
// never part of an optimization experiment.
var updateClassifyGolden = flag.Bool("update", false, "rewrite testdata/classify_golden.json.gz from current classifier output")

// corpusEntry is one real mainnet SC: hex-decoded code plus its variable
// snapshot shaped exactly like parseSCVariables output (string keys; string
// or uint64 values). Vars are key-sorted and entries SCID-sorted at load so
// every run sees identical input order.
type corpusEntry struct {
	SCID string
	Code string
	Vars []*structures.SCIDVariable
}

var (
	corpusOnce        sync.Once
	corpusCollections []corpusEntry
	corpusNFTs        []corpusEntry
	corpusErr         error
)

// mustCorpus loads (once per test binary) and returns the two corpus halves.
// Failures are fatal, never skips: the golden gate must not be passable by
// breaking the corpus.
func mustCorpus(tb testing.TB) (collections, nfts []corpusEntry) {
	tb.Helper()
	corpusOnce.Do(func() {
		corpusCollections, corpusErr = loadCorpusFile("testdata/collections.json.gz")
		if corpusErr == nil {
			corpusNFTs, corpusErr = loadCorpusFile("testdata/nfts.json.gz")
		}
	})
	if corpusErr != nil {
		tb.Fatalf("corpus load: %v", corpusErr)
	}
	return corpusCollections, corpusNFTs
}

// corpusAll returns collections+nfts merged and SCID-sorted — the canonical
// iteration order shared by the golden gate and the benchmarks.
func corpusAll(tb testing.TB) []corpusEntry {
	cols, nfts := mustCorpus(tb)
	all := make([]corpusEntry, 0, len(cols)+len(nfts))
	all = append(all, cols...)
	all = append(all, nfts...)
	sort.Slice(all, func(i, j int) bool { return all[i].SCID < all[j].SCID })
	return all
}

// loadCorpusFile stream-decodes one gzipped SCID->snapshot JSON map. The raw
// JSON (108MB for nfts) never exists in memory as one blob: entries decode
// one at a time. The "C" hex code decodes once per DISTINCT body (the corpus
// has 8 across 45,589 entries) so template NFTs share one backing string.
func loadCorpusFile(path string) ([]corpusEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	zr, err := gzip.NewReader(bufio.NewReaderSize(f, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	defer zr.Close()

	dec := json.NewDecoder(zr)
	// UseNumber is load-bearing: float64 would lose uint64 precision AND
	// route timestamp/frozenAssets through varString's float branch instead
	// of the uint64 branch derod actually produces.
	dec.UseNumber()

	if tok, err := dec.Token(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	} else if d, ok := tok.(json.Delim); !ok || d != '{' {
		return nil, fmt.Errorf("%s: expected top-level object, got %v", path, tok)
	}

	codeBodies := make(map[string]string) // hex -> decoded, deduped
	keyIntern := make(map[string]string)  // field names repeat 45k times
	var entries []corpusEntry

	for dec.More() {
		tok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		scid, ok := tok.(string)
		if !ok {
			return nil, fmt.Errorf("%s: non-string key %v", path, tok)
		}
		var m map[string]interface{}
		if err := dec.Decode(&m); err != nil {
			return nil, fmt.Errorf("%s: entry %s: %w", path, scid, err)
		}
		// collections.json.gz carries one "": {} placeholder entry — not an SC.
		if scid == "" && len(m) == 0 {
			continue
		}
		e := corpusEntry{SCID: scid}
		for k, v := range m {
			if k == "C" {
				hexCode, ok := v.(string)
				if !ok {
					return nil, fmt.Errorf("%s: entry %s: non-string C", path, scid)
				}
				code, ok := codeBodies[hexCode]
				if !ok {
					raw, err := hex.DecodeString(hexCode)
					if err != nil {
						return nil, fmt.Errorf("%s: entry %s: C hex: %w", path, scid, err)
					}
					code = string(raw)
					codeBodies[hexCode] = code
				}
				e.Code = code
				continue
			}
			ik, ok := keyIntern[k]
			if !ok {
				ik = k
				keyIntern[k] = ik
			}
			var val interface{}
			switch x := v.(type) {
			case string:
				// Deliberately NOT pre-hex-decoded: decodeHexIfPrintable
				// must run inside the timed region, as in production.
				val = x
			case json.Number:
				u, err := strconv.ParseUint(x.String(), 10, 64)
				if err != nil {
					return nil, fmt.Errorf("%s: entry %s: key %s: non-uint64 number %q", path, scid, k, x.String())
				}
				val = u
			default:
				return nil, fmt.Errorf("%s: entry %s: key %s: unsupported value type %T", path, scid, k, v)
			}
			e.Vars = append(e.Vars, &structures.SCIDVariable{Key: ik, Value: val})
		}
		sort.Slice(e.Vars, func(i, j int) bool {
			return e.Vars[i].Key.(string) < e.Vars[j].Key.(string)
		})
		entries = append(entries, e)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].SCID < entries[j].SCID })
	return entries, nil
}

// classifyCorpusGoldenBytes serializes the full classifier output for every
// corpus entry as sorted JSONL — one {"scid":...,"r":<SCClass>} object per
// line. Marshaling the whole SCClass means a drift in Name/Desc/IconURL/
// Mods/DocShard fails the gate, not just Class.
func classifyCorpusGoldenBytes(all []corpusEntry) ([]byte, error) {
	type goldenLine struct {
		SCID string  `json:"scid"`
		R    SCClass `json:"r"`
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for i := range all {
		e := &all[i]
		if err := enc.Encode(goldenLine{SCID: e.SCID, R: ClassifySCVars(e.SCID, e.Code, e.Vars)}); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func gunzipFile(tb testing.TB, path string) []byte {
	tb.Helper()
	f, err := os.Open(path)
	if err != nil {
		tb.Fatalf("open %s: %v (regenerate with: go test ./indexer/ -run '^TestClassifyCorpusGolden$' -update)", path, err)
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		tb.Fatalf("gunzip %s: %v", path, err)
	}
	defer zr.Close()
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(zr); err != nil {
		tb.Fatalf("read %s: %v", path, err)
	}
	return buf.Bytes()
}

// TestClassifyCorpusGolden is the primary correctness gate for classify
// optimizations: the full SCClass output over all 45,589 real SCs must stay
// byte-identical to the committed snapshot. Comparison is on DECOMPRESSED
// bytes only — gzip output is not stable across Go releases.
func TestClassifyCorpusGolden(t *testing.T) {
	all := corpusAll(t)
	got, err := classifyCorpusGoldenBytes(all)
	if err != nil {
		t.Fatalf("serialize: %v", err)
	}

	const goldenPath = "testdata/classify_golden.json.gz"
	if *updateClassifyGolden {
		var gz bytes.Buffer
		zw := gzip.NewWriter(&gz)
		if _, err := zw.Write(got); err != nil {
			t.Fatalf("gzip golden: %v", err)
		}
		if err := zw.Close(); err != nil {
			t.Fatalf("gzip golden: %v", err)
		}
		if err := os.WriteFile(goldenPath, gz.Bytes(), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		t.Logf("golden rewritten: %d entries, %d bytes raw, %d bytes gz", len(all), len(got), gz.Len())
		return
	}

	want := gunzipFile(t, goldenPath)
	if bytes.Equal(got, want) {
		return
	}
	gotLines := bytes.Split(got, []byte("\n"))
	wantLines := bytes.Split(want, []byte("\n"))
	n := len(gotLines)
	if len(wantLines) > n {
		n = len(wantLines)
	}
	diffs := 0
	for i := 0; i < n && diffs < 10; i++ {
		var g, w []byte
		if i < len(gotLines) {
			g = gotLines[i]
		}
		if i < len(wantLines) {
			w = wantLines[i]
		}
		if !bytes.Equal(g, w) {
			diffs++
			t.Errorf("golden mismatch line %d:\n  want: %s\n  got:  %s", i+1, w, g)
		}
	}
	t.Fatalf("classifier output diverged from golden (%d lines want vs %d got; first %d diffs above)",
		len(wantLines), len(gotLines), diffs)
}

// TestClassifyCorpusMapSliceEquivalence pins ClassifySC (map path) and
// ClassifySCVars (slice path) to identical results over the whole corpus, so
// an optimization cannot speed up one production path while diverging from
// the other. Both paths are live: slice on installs (indexer.go:1132), map on
// turbo installs and the validatesc diagnostic (indexer.go:982, ws.go:1296).
func TestClassifyCorpusMapSliceEquivalence(t *testing.T) {
	all := corpusAll(t)
	for i := range all {
		e := &all[i]
		fromSlice := ClassifySCVars(e.SCID, e.Code, e.Vars)
		fromMap := ClassifySC(e.SCID, e.Code, varsToMap(e.Vars))
		if !reflect.DeepEqual(fromSlice, fromMap) {
			t.Fatalf("map/slice divergence for %s:\n  slice: %+v\n  map:   %+v", e.SCID, fromSlice, fromMap)
		}
	}
}
