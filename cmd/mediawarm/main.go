// mediawarm bulk-fetches every asset media URL into the same on-disk cache
// /api/media serves from, and writes a census of what is still retrievable.
//
// Why a census: measured 2026-07-27, most corpus root CIDs have NO remaining
// public-gateway copy and NO DHT provider — the bytes referenced by those
// NFTs may be gone. This tool turns that from a suspicion into a per-root
// number, and archives everything that IS still reachable while the last
// providers are up. Resume is free: a file already in the cache is skipped
// (media.Fetcher no-ops on existing files), so the tool can be re-run
// against the same directory forever.
//
// Retrieval strategy per ipfs root (grouped by root CID, most-referenced
// first, so the big collections land earliest):
//
//  1. Probe one file through the normal hedged race (local kubo gateway
//     first, then the public set).
//  2. On probe failure, ask the local kubo API for DHT providers
//     (routing/findprovs, bounded). Providers found -> retry through the
//     local gateway with a long timeout (a first DHT retrieval from a rare
//     provider legitimately takes minutes). None -> the root is recorded
//     MISS and its files are skipped: no point burning per-file timeouts on
//     a root with zero sources.
//
// Note on kubo block retention: blocks fetched through the local gateway
// stay in kubo's blockstore (GC is off by default), so warmed ipfs content
// is retained twice — kubo's store and this cache. Content that only a
// public gateway still serves never enters kubo (kubo cannot fetch what has
// no providers); for those roots the disk cache is the only archive.
//
// Like cmd/corpusdump, this needs a synced DB (media URLs come from
// ClassMeta, populated by --postscan-vars=all or RefreshClassVars) and a
// network — it is an operator tool, not CI material.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hypergnomon/hypergnomon/media"
	"github.com/hypergnomon/hypergnomon/storage"
)

type rootReport struct {
	CID     string `json:"cid"`
	Files   int    `json:"files"`
	Status  string `json:"status"` // ok | partial | miss
	Via     string `json:"via,omitempty"`
	ProbeMs int64  `json:"probe_ms,omitempty"`
	Cached  int    `json:"cached"`
	Fetched int    `json:"fetched"`
	Failed  int    `json:"failed"`
	Bytes   int64  `json:"bytes"`
}

type census struct {
	StartedAt  string       `json:"started_at"`
	FinishedAt string       `json:"finished_at"`
	MediaDir   string       `json:"media_dir"`
	Totals     totals       `json:"totals"`
	Roots      []rootReport `json:"roots"`
}

type totals struct {
	Roots        int   `json:"roots"`
	Files        int   `json:"files"`
	Cached       int   `json:"cached"`
	Fetched      int   `json:"fetched"`
	Failed       int   `json:"failed"`
	MissingRoots int   `json:"missing_roots"`
	BytesFetched int64 `json:"bytes_fetched"`
}

func main() {
	dbDir := flag.String("db-dir", "", "synced HyperGnomon DB (media URLs come from its ClassMeta records)")
	mediaDir := flag.String("media-dir", "", "cache directory (the same one hypergnomon --media-dir serves)")
	ipfsGateway := flag.String("ipfs-gateway", "http://127.0.0.1:18080", "local kubo gateway base URL (empty = public gateways only)")
	ipfsAPI := flag.String("ipfs-api", "http://127.0.0.1:5001", "local kubo API base URL for DHT provider lookups (empty = skip findprovs)")
	censusOut := flag.String("census-out", "", "census JSON path (empty = <media-dir>/media-census.json)")
	maxFileMB := flag.Int64("max-file-mb", 50, "per-file byte cap in MB")
	includeAV := flag.Bool("include-av", false, "also fetch audio/video URLs (can be large)")
	rootWorkers := flag.Int("root-workers", 4, "roots processed concurrently")
	fileWorkers := flag.Int("file-workers", 8, "files fetched concurrently within a root")
	classesFlag := flag.String("classes", "G45-NFT,G45-FAT,G45-AT,G45-C", "asset classes to warm")
	flag.Parse()
	if *dbDir == "" || *mediaDir == "" {
		fmt.Fprintln(os.Stderr, "--db-dir and --media-dir are required")
		os.Exit(2)
	}

	if err := os.MkdirAll(*mediaDir, 0o755); err != nil {
		fatal("create media dir: %v", err)
	}
	store, err := storage.Open("bbolt", *dbDir, "")
	if err != nil {
		fatal("open store: %v", err)
	}
	defer store.Close()

	// Collect URLs. ipfs URLs group under their root CID; https URLs go in a
	// single synthetic group (each is its own only source anyway).
	byRoot := map[string][]string{}
	var httpsURLs []string
	total := 0
	for _, class := range strings.Split(*classesFlag, ",") {
		class = strings.TrimSpace(class)
		installs, err := store.GetClassInstalls(class, 0)
		if err != nil {
			fatal("GetClassInstalls(%s): %v", class, err)
		}
		for _, inst := range installs {
			if inst.Meta == nil {
				continue
			}
			urls := []string{inst.Meta.Image, inst.Meta.AltImage}
			if *includeAV {
				urls = append(urls, inst.Meta.Audio, inst.Meta.Video)
			}
			for _, u := range urls {
				if u == "" {
					continue
				}
				if p := media.IPFSPath(u); p != "" {
					root := strings.SplitN(p, "/", 2)[0]
					byRoot[root] = append(byRoot[root], u)
					total++
				} else if strings.HasPrefix(u, "https://") {
					httpsURLs = append(httpsURLs, u)
					total++
				}
			}
		}
	}
	roots := make([]string, 0, len(byRoot))
	for r := range byRoot {
		roots = append(roots, r)
	}
	// Most-referenced roots first: the big collections are both the most
	// valuable to archive and the cheapest per image (one provider
	// connection amortizes across thousands of files).
	sort.Slice(roots, func(i, j int) bool { return len(byRoot[roots[i]]) > len(byRoot[roots[j]]) })
	fmt.Printf("warming %d URLs: %d ipfs roots + %d https, into %s\n", total, len(roots), len(httpsURLs), *mediaDir)

	fetcher := &media.Fetcher{
		LocalGateway: *ipfsGateway,
		MaxBytes:     *maxFileMB << 20,
	}

	c := census{StartedAt: time.Now().UTC().Format(time.RFC3339), MediaDir: *mediaDir}
	var mu sync.Mutex
	var wg sync.WaitGroup
	rootCh := make(chan string, len(roots))
	for _, r := range roots {
		rootCh <- r
	}
	close(rootCh)
	done := 0
	for w := 0; w < *rootWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for root := range rootCh {
				rep := warmRoot(fetcher, *ipfsAPI, *mediaDir, root, byRoot[root], *fileWorkers)
				mu.Lock()
				c.Roots = append(c.Roots, rep)
				done++
				fmt.Printf("[%d/%d] %s: %s files=%d cached=%d fetched=%d failed=%d via=%s\n",
					done, len(roots), root, rep.Status, rep.Files, rep.Cached, rep.Fetched, rep.Failed, rep.Via)
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	// https stragglers: direct fetches, one synthetic report.
	if len(httpsURLs) > 0 {
		rep := warmFiles(fetcher, *mediaDir, "https-direct", httpsURLs, *fileWorkers)
		rep.Status = "ok"
		if rep.Fetched == 0 && rep.Cached == 0 {
			rep.Status = "miss"
		} else if rep.Failed > 0 {
			rep.Status = "partial"
		}
		c.Roots = append(c.Roots, rep)
	}

	sort.Slice(c.Roots, func(i, j int) bool { return c.Roots[i].Files > c.Roots[j].Files })
	for _, r := range c.Roots {
		c.Totals.Roots++
		c.Totals.Files += r.Files
		c.Totals.Cached += r.Cached
		c.Totals.Fetched += r.Fetched
		c.Totals.Failed += r.Failed
		c.Totals.BytesFetched += r.Bytes
		if r.Status == "miss" {
			c.Totals.MissingRoots++
		}
	}
	c.FinishedAt = time.Now().UTC().Format(time.RFC3339)

	out := *censusOut
	if out == "" {
		out = filepath.Join(*mediaDir, "media-census.json")
	}
	buf, _ := json.MarshalIndent(c, "", "  ")
	if err := os.WriteFile(out, append(buf, '\n'), 0o644); err != nil {
		fatal("write census: %v", err)
	}
	fmt.Printf("\ncensus: %d roots, %d files | cached %d + fetched %d, failed %d, %d roots MISS | %.1f MB fetched | %s\n",
		c.Totals.Roots, c.Totals.Files, c.Totals.Cached, c.Totals.Fetched, c.Totals.Failed,
		c.Totals.MissingRoots, float64(c.Totals.BytesFetched)/1e6, out)
}

// warmRoot probes a root's availability, then fetches its files.
func warmRoot(f *media.Fetcher, ipfsAPI, mediaDir, root string, urls []string, fileWorkers int) rootReport {
	rep := rootReport{CID: root, Files: len(urls)}

	// Fully-cached roots need no probe — resume must not spend network on
	// roots a previous run already archived.
	allCached := true
	for _, u := range urls {
		p, err := media.CachePath(mediaDir, u)
		if err != nil || !fileExists(p) {
			allCached = false
			break
		}
	}
	if allCached {
		rep.Status, rep.Via, rep.Cached = "ok", "cache", len(urls)
		return rep
	}

	probeStart := time.Now()
	probeURL := urls[0]
	probePath, err := media.CachePath(mediaDir, probeURL)
	if err != nil {
		rep.Status = "miss"
		return rep
	}
	// The probe runs gateways-only. With the local node in the race, every
	// DEAD root would cost kubo's long timeout before the census could say
	// MISS — and dead roots are the majority. Gateways answer (or fail) in
	// seconds; whether kubo gets involved is findprovs' call below. Files of
	// a reachable root still use the full fetcher, where hedging means the
	// local source costs nothing when a gateway is faster.
	probeFetcher := &media.Fetcher{Gateways: f.Gateways, MaxBytes: f.MaxBytes}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	res, probeErr := probeFetcher.Fetch(ctx, probeURL, probePath)
	cancel()
	if probeErr != nil {
		// No gateway (nor already-running kubo lookup) produced it. One
		// bounded DHT provider check decides between "slow but alive" and
		// "no sources exist".
		if !hasDHTProviders(ipfsAPI, root) {
			rep.Status = "miss"
			return rep
		}
		// Providers exist: give the local node one long retrieval window.
		lctx, lcancel := context.WithTimeout(context.Background(), 10*time.Minute)
		res, probeErr = (&media.Fetcher{LocalGateway: f.LocalGateway, Gateways: []string{}, MaxBytes: f.MaxBytes, LocalTimeout: 10 * time.Minute}).Fetch(lctx, probeURL, probePath)
		lcancel()
		if probeErr != nil {
			rep.Status = "miss"
			return rep
		}
	}
	rep.Via = res.Via
	rep.ProbeMs = time.Since(probeStart).Milliseconds()

	fileRep := warmFiles(f, mediaDir, root, urls, fileWorkers)
	rep.Cached, rep.Fetched, rep.Failed, rep.Bytes = fileRep.Cached, fileRep.Fetched, fileRep.Failed, fileRep.Bytes
	switch {
	case rep.Failed == 0:
		rep.Status = "ok"
	case rep.Fetched+rep.Cached > 0:
		rep.Status = "partial"
	default:
		rep.Status = "miss"
	}
	return rep
}

// warmFiles fetches a URL list with bounded concurrency.
func warmFiles(f *media.Fetcher, mediaDir, label string, urls []string, workers int) rootReport {
	rep := rootReport{CID: label, Files: len(urls)}
	var mu sync.Mutex
	var wg sync.WaitGroup
	ch := make(chan string, len(urls))
	for _, u := range urls {
		ch <- u
	}
	close(ch)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for u := range ch {
				p, err := media.CachePath(mediaDir, u)
				if err != nil {
					mu.Lock()
					rep.Failed++
					mu.Unlock()
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
				res, err := f.Fetch(ctx, u, p)
				cancel()
				mu.Lock()
				switch {
				case err != nil:
					rep.Failed++
				case res.Via == "cache":
					rep.Cached++
				default:
					rep.Fetched++
					rep.Bytes += res.Size
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return rep
}

// hasDHTProviders asks the local kubo API whether any provider announces the
// CID. Bounded: a routing walk that finds nothing in 45s is a "no" for
// census purposes.
func hasDHTProviders(apiBase, cid string) bool {
	if apiBase == "" {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	u := strings.TrimRight(apiBase, "/") + "/api/v0/routing/findprovs?arg=" + url.QueryEscape(cid) + "&num-providers=1"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, nil)
	if err != nil {
		return false
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	// The endpoint streams ndjson routing events; Type 4 is a provider.
	dec := json.NewDecoder(resp.Body)
	for {
		var ev struct {
			Type      int `json:"Type"`
			Responses []struct {
				ID string `json:"ID"`
			} `json:"Responses"`
		}
		if err := dec.Decode(&ev); err != nil {
			if err != io.EOF {
				return false
			}
			return false
		}
		if ev.Type == 4 && len(ev.Responses) > 0 {
			return true
		}
	}
}

func fileExists(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && fi.Size() > 0
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
