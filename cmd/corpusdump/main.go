// corpusdump captures a raw GetSC snapshot of the mainnet G45 contract set
// into the classify corpus fixture under indexer/testdata.
//
// WHY THIS EXISTS: the previous fixture was produced by an unrecorded process
// that decoded some values and not others. derod hex-encodes DVM STORE string
// values, but the committed corpus held `metadata` (and, in nfts.json.gz,
// `type`) already decoded. The G45 extractors were therefore exercised against
// a shape the daemon never sends — which hid a live bug for the fixture's whole
// life: on a real chain the extractors were handed hex, parsed nothing, and
// left Name/Desc/IconURL empty for every G45 asset while every gate passed.
//
// THE RULE THIS TOOL EXISTS TO ENFORCE: variable values are written VERBATIM.
// No hex decoding, no normalization, no cleanup. Whatever derod returns is what
// lands in the fixture. If a value looks unreadable, that is the point — the
// indexer's job is to cope with it, and the fixture's job is to make it do so.
//
// The one deliberate exception is the "C" (code) field, which is hex-ENCODED on
// write. That is a JSON-safety encoding of the fixture file, NOT the wire shape:
// derod returns SC code as plaintext (fastsync.go matches rule patterns against
// it with strings.Contains, which only works on plaintext), and loadCorpusFile
// hex-decodes it back on load. Conflating "hex because JSON safety" with "hex
// because that is what the daemon sends" is exactly how the original bug
// survived, so the two are kept visibly distinct here.
//
// Regenerating the corpus is a deliberate operator act, like regenerating the
// golden — it needs a synced DB and a live daemon, so it cannot run in CI.
//
// Usage:
//
//	go run ./cmd/corpusdump \
//	    --db-dir=/path/to/gnomondb \
//	    --daemon-rpc-address=192.168.2.251:10102 \
//	    --out=indexer/testdata
package main

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	hgrpc "github.com/hypergnomon/hypergnomon/rpc"
	"github.com/hypergnomon/hypergnomon/storage"
)

// corpusEntry is one SC's snapshot: hex-encoded code under "C", plus every
// string-keyed variable verbatim.
type corpusEntry map[string]interface{}

// manifest records what was captured so a future reader can tell whether the
// fixture is current, and so a re-capture is reproducible.
type manifest struct {
	TopoHeight    int64          `json:"topoheight"`
	CapturedAt    string         `json:"captured_at"`
	DaemonVersion string         `json:"daemon_version"`
	Endpoint      string         `json:"endpoint"`
	Classes       map[string]int `json:"scids_by_class"`
	Files         map[string]struct {
		Entries int    `json:"entries"`
		SHA256  string `json:"sha256"`
		Bytes   int    `json:"bytes"`
	} `json:"files"`
	// Uint64KeyVars counts variables returned under uint64 keys. The fixture
	// schema is string-keyed only, so a non-zero value here means the capture
	// is lossy and the schema needs extending — surfaced rather than silently
	// dropped.
	Uint64KeyVars int `json:"uint64_key_vars_skipped"`
}

func main() {
	dbDir := flag.String("db-dir", "", "synced HyperGnomon DB directory (SCID enumeration source)")
	endpoint := flag.String("daemon-rpc-address", "127.0.0.1:10102", "DERO daemon RPC address")
	outDir := flag.String("out", "indexer/testdata", "output directory")
	topoFlag := flag.Int64("topoheight", 0, "topoheight to capture at (0 = current tip)")
	nftClasses := flag.String("nft-classes", "G45-NFT,G45-FAT,G45-AT,G45-NAME,T345", "classes written to nfts.json.gz")
	colClasses := flag.String("collection-classes", "G45-C", "classes written to collections.json.gz")
	poolSize := flag.Int("rpc-pool-size", 8, "RPC connection pool size")
	flag.Parse()

	if *dbDir == "" {
		fmt.Fprintln(os.Stderr, "--db-dir is required (a HyperGnomon DB synced with --fastsync supplies the SCID list)")
		os.Exit(2)
	}

	store, err := storage.Open("bbolt", *dbDir, "")
	if err != nil {
		fatal("open store %s: %v", *dbDir, err)
	}
	defer store.Close()

	pool, err := hgrpc.NewPool(*endpoint, *poolSize)
	if err != nil {
		fatal("rpc pool: %v", err)
	}
	defer pool.Close()

	// Pin the height up front so every SCID in the capture is read at the same
	// chain state; without this a long capture straddles blocks and the fixture
	// is internally inconsistent.
	topo := *topoFlag
	var daemonVersion string
	if err := pool.WithConn(func(c *hgrpc.Client) error {
		info, err := c.GetInfo()
		if err != nil {
			return err
		}
		daemonVersion = info.Version
		if topo == 0 {
			topo = info.TopoHeight
		}
		return nil
	}); err != nil {
		fatal("GetInfo: %v", err)
	}
	fmt.Printf("capturing at topoheight %d (daemon %s)\n", topo, daemonVersion)

	classCounts := map[string]int{}
	nfts, err := collectSCIDs(store, splitCSV(*nftClasses), classCounts)
	if err != nil {
		fatal("enumerate NFT classes: %v", err)
	}
	cols, err := collectSCIDs(store, splitCSV(*colClasses), classCounts)
	if err != nil {
		fatal("enumerate collection classes: %v", err)
	}
	fmt.Printf("enumerated %d NFT-class and %d collection-class SCIDs\n", len(nfts), len(cols))

	var uint64Keys int
	nftEntries := fetchAll(pool, nfts, topo, &uint64Keys)
	colEntries := fetchAll(pool, cols, topo, &uint64Keys)

	m := manifest{
		TopoHeight:    topo,
		CapturedAt:    time.Now().UTC().Format(time.RFC3339),
		DaemonVersion: daemonVersion,
		Endpoint:      *endpoint,
		Classes:       classCounts,
		Uint64KeyVars: uint64Keys,
	}
	m.Files = map[string]struct {
		Entries int    `json:"entries"`
		SHA256  string `json:"sha256"`
		Bytes   int    `json:"bytes"`
	}{}

	for name, entries := range map[string]map[string]corpusEntry{
		"nfts.json.gz":        nftEntries,
		"collections.json.gz": colEntries,
	} {
		path := filepath.Join(*outDir, name)
		sum, n, err := writeGz(path, entries)
		if err != nil {
			fatal("write %s: %v", path, err)
		}
		m.Files[name] = struct {
			Entries int    `json:"entries"`
			SHA256  string `json:"sha256"`
			Bytes   int    `json:"bytes"`
		}{Entries: len(entries), SHA256: sum, Bytes: n}
		fmt.Printf("wrote %s: %d entries, %d bytes gz\n", path, len(entries), n)
	}

	if uint64Keys > 0 {
		fmt.Fprintf(os.Stderr, "WARNING: skipped %d uint64-keyed variables; the fixture schema is string-keyed only\n", uint64Keys)
	}

	mPath := filepath.Join(*outDir, "corpus_manifest.json")
	buf, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		fatal("marshal manifest: %v", err)
	}
	if err := os.WriteFile(mPath, append(buf, '\n'), 0o644); err != nil {
		fatal("write %s: %v", mPath, err)
	}
	fmt.Printf("wrote %s\n", mPath)
}

// collectSCIDs gathers the SCIDs of every named class, deduped and sorted.
func collectSCIDs(store storage.Storage, classes []string, counts map[string]int) ([]string, error) {
	seen := map[string]struct{}{}
	for _, class := range classes {
		installs, err := store.GetClassInstalls(class, 0)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", class, err)
		}
		counts[class] = len(installs)
		for _, inst := range installs {
			seen[inst.SCID] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for scid := range seen {
		out = append(out, scid)
	}
	sort.Strings(out)
	return out, nil
}

// fetchAll pulls every SCID's full state at topo, in parallel across the pool.
func fetchAll(pool *hgrpc.Pool, scids []string, topo int64, uint64Keys *int) map[string]corpusEntry {
	out := make(map[string]corpusEntry, len(scids))
	work := make(chan string, len(scids))
	for _, s := range scids {
		work <- s
	}
	close(work)

	var mu sync.Mutex
	var wg sync.WaitGroup
	var done, failed int
	workers := pool.Size()
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for scid := range work {
				var entry corpusEntry
				var skipped int
				err := pool.WithConn(func(c *hgrpc.Client) error {
					// code=true: the fixture needs the SC source, and it is the
					// only thing classification actually keys on.
					res, err := c.GetSC(scid, topo, nil, nil, true)
					if err != nil {
						return err
					}
					entry = corpusEntry{}
					// "C" is hex-ENCODED here for JSON safety — see the package
					// comment. derod returned this as plaintext.
					entry["C"] = hex.EncodeToString([]byte(res.Code))
					for k, v := range res.VariableStringKeys {
						// VERBATIM. Do not decode, trim, or normalize.
						entry[k] = normalizeNumber(v)
					}
					skipped = len(res.VariableUint64Keys)
					return nil
				})
				mu.Lock()
				if err != nil || entry == nil {
					failed++
				} else {
					out[scid] = entry
					*uint64Keys += skipped
				}
				done++
				if done%5000 == 0 {
					fmt.Printf("  fetched %d/%d\n", done, len(scids))
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if failed > 0 {
		fmt.Fprintf(os.Stderr, "WARNING: %d/%d GetSC calls failed and were omitted\n", failed, len(scids))
	}
	return out
}

// normalizeNumber undoes an artifact of the typed RPC client rather than
// touching the value itself: encoding/json decodes every JSON number into
// float64, and re-encoding a large float64 emits scientific notation
// ("1.671011379e+09") where derod sent an integer. Converting integral floats
// back to exact integers reproduces the bytes derod actually sent. Strings —
// including every hex-encoded value, which is the whole point — pass through
// completely untouched.
func normalizeNumber(v interface{}) interface{} {
	f, ok := v.(float64)
	if !ok {
		return v
	}
	if f != math.Trunc(f) || f < 0 || f > math.MaxUint64 {
		return v
	}
	return json.Number(strconv.FormatUint(uint64(f), 10))
}

// writeGz writes entries as gzipped JSON and returns the SHA256 and size of the
// compressed file.
func writeGz(path string, entries map[string]corpusEntry) (string, int, error) {
	f, err := os.Create(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()

	h := sha256.New()
	counter := &countingWriter{}
	zw := gzip.NewWriter(io.MultiWriter(f, h, counter))

	// Marshal with sorted keys (encoding/json sorts map keys) and indentation
	// matching the previous fixture, so a diff between captures is readable.
	buf, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return "", 0, err
	}
	if _, err := zw.Write(buf); err != nil {
		return "", 0, err
	}
	if err := zw.Close(); err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), counter.n, nil
}

type countingWriter struct{ n int }

func (c *countingWriter) Write(p []byte) (int, error) { c.n += len(p); return len(p), nil }

func splitCSV(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
