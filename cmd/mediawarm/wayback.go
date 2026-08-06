package main

// Wayback Machine recovery for roots no live source serves.
//
// The G45 minting platform ran its own gateway, ipfs.deronfts.com, whose DNS
// is now dead — and the Internet Archive crawled it while it lived. Verified
// 2026-07-27: 497 unique image captures across 95 root CIDs, and the
// `id_`-suffixed snapshot URL returns the ORIGINAL bytes verbatim (fetched a
// 1,104,044-byte Dero Apes PNG with an intact header). For content whose
// every provider has vanished, the archive is the last remaining source.
//
// Etiquette: the Archive is a shared nonprofit resource and its CDX endpoint
// rate-limits aggressively (observed mid-probe). Everything here is SERIAL
// with fixed pacing and one retry — a few hundred fetches total, never a
// hammering. This phase runs only for roots the live pass could not serve,
// so its cost scales with what is lost, not with the corpus.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/hypergnomon/hypergnomon/media"
)

const (
	cdxPace     = 1500 * time.Millisecond // between CDX queries
	fetchPace   = 700 * time.Millisecond  // between snapshot downloads
	cdxTimeout  = 45 * time.Second
	snapTimeout = 120 * time.Second // full-res captures run to ~1 MB+
)

// capture is one archived URL of interest.
type capture struct {
	Original  string
	Timestamp string
}

// cdxQuery lists unique captures under host/ipfs/<cid>. One retry after a
// backoff — the endpoint intermittently returns non-JSON when throttling.
func cdxQuery(client *http.Client, host, cid string) ([]capture, error) {
	q := "http://web.archive.org/cdx/search/cdx?url=" +
		url.QueryEscape(host+"/ipfs/"+cid) + "*" +
		"&output=json&collapse=urlkey&fl=original,timestamp&filter=statuscode:200&limit=5000"
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			time.Sleep(15 * time.Second)
		}
		ctx, cancel := context.WithTimeout(context.Background(), cdxTimeout)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, q, nil)
		resp, err := client.Do(req)
		if err != nil {
			cancel()
			lastErr = err
			continue
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		resp.Body.Close()
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		var rows [][]string
		if err := json.Unmarshal(body, &rows); err != nil {
			lastErr = fmt.Errorf("CDX non-JSON (throttled?): %.60q", body)
			continue
		}
		out := make([]capture, 0, len(rows))
		for i, r := range rows {
			if i == 0 || len(r) < 2 { // row 0 is the field-name header
				continue
			}
			out = append(out, capture{Original: r[0], Timestamp: r[1]})
		}
		return out, nil
	}
	return nil, lastErr
}

// captureIPFSPath extracts the "cid/sub/path" tail of an archived gateway URL,
// or "" when the capture is not a direct /ipfs/ object (e.g. the marketplace's
// /img/250/ resize endpoints — those are derivatives, not the content).
func captureIPFSPath(original string) string {
	i := strings.Index(original, "/ipfs/")
	if i < 0 {
		return ""
	}
	return strings.Trim(original[i+len("/ipfs/"):], "/")
}

// normalizeIPFSPath makes metadata URLs and crawler URLs comparable: both
// sides percent-decoded, since the archive stores "Dero%20Apes%20%23494.png"
// while a minter may have written either form on chain.
func normalizeIPFSPath(p string) string {
	if u, err := url.PathUnescape(p); err == nil {
		return u
	}
	return p
}

// waybackRecoverRoot tries to recover a lost root's wanted files from the
// archive. wanted maps the normalized ipfs path -> the on-chain URL (whose
// CachePath is where the bytes must land so /api/media serves them).
// Returns files recovered and bytes written.
func waybackRecoverRoot(client *http.Client, mediaDir string, hosts []string, cid string, wanted map[string]string, maxBytes int64) (int, int64) {
	recovered, bytes := 0, int64(0)
	for _, host := range hosts {
		time.Sleep(cdxPace)
		caps, err := cdxQuery(client, host, cid)
		if err != nil || len(caps) == 0 {
			continue
		}
		for _, cp := range caps {
			p := captureIPFSPath(cp.Original)
			if p == "" {
				continue
			}
			chainURL, ok := wanted[normalizeIPFSPath(p)]
			if !ok {
				continue // archived, but not a file any asset references
			}
			dest, err := media.CachePath(mediaDir, chainURL)
			if err != nil {
				continue
			}
			if fi, err := os.Stat(dest); err == nil && fi.Size() > 0 {
				continue // already recovered (possibly by a live source)
			}
			time.Sleep(fetchPace)
			n, err := fetchSnapshot(client, cp, dest, maxBytes)
			if err != nil {
				continue
			}
			recovered++
			bytes += n
		}
	}
	return recovered, bytes
}

// fetchSnapshot downloads one archived object's original bytes. The `id_`
// flag after the timestamp asks the archive for the capture verbatim —
// no replay-banner rewriting.
func fetchSnapshot(client *http.Client, cp capture, dest string, maxBytes int64) (int64, error) {
	snapURL := "https://web.archive.org/web/" + cp.Timestamp + "id_/" + cp.Original
	ctx, cancel := context.WithTimeout(context.Background(), snapTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, snapURL, nil)
	if err != nil {
		return 0, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %d", resp.StatusCode)
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return 0, err
	}
	tmp := dest + ".tmp-wb"
	out, err := os.Create(tmp)
	if err != nil {
		return 0, err
	}
	n, err := io.Copy(out, io.LimitReader(resp.Body, maxBytes+1))
	closeErr := out.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil && (n == 0 || n > maxBytes) {
		err = fmt.Errorf("size %d out of bounds", n)
	}
	if err != nil {
		os.Remove(tmp)
		return 0, err
	}
	if err := os.Rename(tmp, dest); err != nil {
		os.Remove(tmp)
		return 0, err
	}
	return n, nil
}
