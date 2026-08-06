// Package media fetches and caches the media bytes referenced by asset
// metadata (G45 `image`/`backdropImage`/`alt-image`/`audio`/`video` URLs).
//
// Design constraints, in order of importance — all measured against the real
// corpus on 2026-07-27 (see DOCS/BENCHMARKS.md and the media section of the
// README):
//
//  1. AVAILABILITY, not bandwidth, is the bottleneck. 7 of the top 10 root
//     CIDs (~19k images) are served by NO live public gateway; they are
//     reachable only via patient DHT retrieval through a local kubo node, if
//     at all. So the local node, when configured, is always the first source.
//  2. Content is immutable (`ipfs://` URLs are content-addressed), so a byte
//     fetched is a byte never fetched again: fetch once, store on disk
//     forever, serve locally. The cache layout IS the index — no database
//     record backs it, so cache presence is simply file existence and two
//     writers (the API and cmd/mediawarm) can share a directory safely.
//  3. Public gateways die (cloudflare-ipfs and fleek are gone) and
//     rate-limit (pinata 429s), so multi-gateway fetch is hedged, not
//     serial: each subsequent source starts HedgeDelay after the previous
//     rather than waiting for its timeout, first 200 wins, the rest are
//     canceled, and a 429 puts that host on a cooldown.
package media

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// DefaultGateways is the public-gateway race order: measured speed order for
// the corpus CIDs (w3s.link 177–293 ms when it has content), with pinata last
// — slow (4.5–6.6 s) and aggressively rate-limited, but load-bearing: it is
// the only public gateway serving some corpus roots (the Dero Ducks
// directory, 3,378 images).
var DefaultGateways = []string{
	"https://w3s.link",
	"https://dweb.link",
	"https://ipfs.io",
	"https://gateway.pinata.cloud",
}

const (
	// DefaultMaxBytes caps one fetched file. Corpus images run 14–27 KB, but
	// the audio (295) and video (148) URLs can be large; the cap keeps a
	// hostile metadata URL from filling the disk.
	DefaultMaxBytes int64 = 50 << 20

	// DefaultHedgeDelay is how long a source gets to itself before the next
	// one starts. 800 ms lets a healthy w3s.link win outright (its measured
	// hit latency is ~200 ms) while a dead gateway costs under a second, not
	// its full timeout.
	DefaultHedgeDelay = 800 * time.Millisecond

	// DefaultPerTryTimeout bounds one source attempt. Public gateways that
	// will answer at all do so well inside this; the local kubo source gets
	// LocalTimeout instead, because a DHT retrieval legitimately takes tens
	// of seconds on first contact with a rare provider.
	DefaultPerTryTimeout = 25 * time.Second

	// DefaultLocalTimeout is the per-try budget for the local kubo gateway.
	DefaultLocalTimeout = 120 * time.Second

	// cooldownAfter429 keeps a rate-limiting host out of the race long
	// enough for its window to reset.
	cooldownAfter429 = 60 * time.Second
)

// Fetcher retrieves one URL into the on-disk cache. Safe for concurrent use;
// zero-value fields fall back to the defaults above.
type Fetcher struct {
	// LocalGateway is the base URL of a local kubo gateway (e.g.
	// "http://127.0.0.1:18080"). Empty disables the local source.
	LocalGateway string
	// Gateways overrides DefaultGateways when non-nil.
	Gateways      []string
	MaxBytes      int64
	HedgeDelay    time.Duration
	PerTryTimeout time.Duration
	LocalTimeout  time.Duration
	// Client is the HTTP client for every source. Nil uses a dedicated
	// client with sane transport limits.
	Client *http.Client

	// cooldown maps host -> time.Time before which the host is skipped
	// (set on HTTP 429).
	cooldown sync.Map

	clientOnce sync.Once
}

func (f *Fetcher) client() *http.Client {
	f.clientOnce.Do(func() {
		if f.Client == nil {
			f.Client = &http.Client{
				Transport: &http.Transport{
					MaxIdleConnsPerHost: 8,
					IdleConnTimeout:     90 * time.Second,
					// A gateway that redirects (dweb.link 301s path-style to
					// subdomain-style) must be followed; the default client
					// does. Compression: images are already compressed.
					DisableCompression: true,
				},
			}
		}
	})
	return f.Client
}

func (f *Fetcher) maxBytes() int64 {
	if f.MaxBytes > 0 {
		return f.MaxBytes
	}
	return DefaultMaxBytes
}

// IPFSPath returns the "cid/sub/path" tail of an ipfs:// URL, or "" if the
// URL is not ipfs.
func IPFSPath(rawURL string) string {
	if !strings.HasPrefix(rawURL, "ipfs://") {
		return ""
	}
	p := strings.TrimPrefix(rawURL, "ipfs://")
	// Some minters wrote ipfs://ipfs/<cid>/... — normalize the double prefix.
	p = strings.TrimPrefix(p, "ipfs/")
	return strings.Trim(p, "/")
}

// CachePath maps a media URL to its deterministic location under dir, or an
// error for schemes we refuse to touch. The mapping is the whole index:
// existence of the returned path == cached.
//
//	ipfs://<cid>/<sub>  -> dir/ipfs/<cid>/<sanitized sub>   (content-addressed)
//	https://...         -> dir/https/<sha256(url)><ext>     (opaque key)
//
// Anything else — http://, data:, javascript:, a bare word — is rejected:
// metadata URLs are attacker-controlled strings and the fetcher must never be
// talked into arbitrary schemes or into writing outside dir.
func CachePath(dir, rawURL string) (string, error) {
	if p := IPFSPath(rawURL); p != "" {
		segs := strings.Split(p, "/")
		clean := make([]string, 0, len(segs))
		for _, s := range segs {
			s = sanitizeSegment(s)
			if s == "" {
				return "", fmt.Errorf("unusable ipfs path segment in %q", rawURL)
			}
			clean = append(clean, s)
		}
		if len(clean) == 1 {
			// Bare-CID image (the CID is the file itself, no directory).
			clean = append(clean, "_file")
		}
		return filepath.Join(append([]string{dir, "ipfs"}, clean...)...), nil
	}
	if strings.HasPrefix(rawURL, "https://") {
		sum := sha256.Sum256([]byte(rawURL))
		ext := path.Ext(strings.SplitN(strings.TrimPrefix(rawURL, "https://"), "?", 2)[0])
		if len(ext) > 6 || strings.ContainsAny(ext, `\/:*?"<>|%`) {
			ext = ""
		}
		return filepath.Join(dir, "https", hex.EncodeToString(sum[:])+ext), nil
	}
	return "", fmt.Errorf("unsupported media URL scheme: %q", rawURL)
}

// sanitizeSegment makes one path segment safe for the local filesystem while
// staying deterministic. "." / ".." are rejected outright (returned empty) —
// they are traversal, not filenames. Characters Windows forbids are replaced.
func sanitizeSegment(s string) string {
	if s == "" || s == "." || s == ".." {
		return ""
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r < 0x20, strings.ContainsRune(`<>:"\|?*`, r):
			return '_'
		}
		return r
	}, s)
}

// source is one place a URL can be fetched from.
type source struct {
	name    string
	httpURL string
	timeout time.Duration
}

// sources builds the race list for a URL. ipfs URLs fan out to the local
// gateway (if configured) plus the public set; https URLs have exactly one
// source — themselves.
func (f *Fetcher) sources(rawURL string) []source {
	if p := IPFSPath(rawURL); p != "" {
		gws := f.Gateways
		if gws == nil {
			gws = DefaultGateways
		}
		out := make([]source, 0, len(gws)+1)
		perTry := f.PerTryTimeout
		if perTry <= 0 {
			perTry = DefaultPerTryTimeout
		}
		if f.LocalGateway != "" {
			lt := f.LocalTimeout
			if lt <= 0 {
				lt = DefaultLocalTimeout
			}
			out = append(out, source{
				name:    "local",
				httpURL: strings.TrimRight(f.LocalGateway, "/") + "/ipfs/" + p,
				timeout: lt,
			})
		}
		for _, gw := range gws {
			out = append(out, source{
				name:    gw,
				httpURL: strings.TrimRight(gw, "/") + "/ipfs/" + p,
				timeout: perTry,
			})
		}
		return out
	}
	perTry := f.PerTryTimeout
	if perTry <= 0 {
		perTry = DefaultPerTryTimeout
	}
	return []source{{name: "direct", httpURL: rewriteGitHubBlob(rawURL), timeout: perTry}}
}

// rewriteGitHubBlob converts a github.com HTML page URL into its raw-content
// form. Several NFA minters stored `github.com/<o>/<r>/blob/<branch>/<path>`
// as their fileURL — that URL serves the repository web PAGE, not the image.
// The rewrite is the documented, stable mapping to raw.githubusercontent.com;
// non-matching URLs pass through untouched. The cache path stays keyed on the
// ORIGINAL on-chain URL, so the rewrite is invisible to /api/media lookups.
func rewriteGitHubBlob(rawURL string) string {
	const prefix = "https://github.com/"
	rest, ok := strings.CutPrefix(rawURL, prefix)
	if !ok {
		return rawURL
	}
	parts := strings.SplitN(rest, "/", 4) // owner / repo / "blob" / branch+path
	if len(parts) != 4 || parts[2] != "blob" {
		return rawURL
	}
	return "https://raw.githubusercontent.com/" + parts[0] + "/" + parts[1] + "/" + parts[3]
}

// Result reports a completed fetch.
type Result struct {
	Via     string        // source name that won
	Size    int64         // bytes written
	Elapsed time.Duration // time to first success
}

// Fetch retrieves rawURL into destPath (atomically: temp file + rename).
// destPath must come from CachePath. If destPath already exists the fetch is
// a no-op success with Via "cache".
func (f *Fetcher) Fetch(ctx context.Context, rawURL, destPath string) (Result, error) {
	start := time.Now()
	if fi, err := os.Stat(destPath); err == nil && fi.Size() > 0 {
		return Result{Via: "cache", Size: fi.Size()}, nil
	}
	srcs := f.sources(rawURL)
	if len(srcs) == 0 {
		return Result{}, fmt.Errorf("no sources for %q", rawURL)
	}
	hedge := f.HedgeDelay
	if hedge <= 0 {
		hedge = DefaultHedgeDelay
	}

	raceCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	type attempt struct {
		res Result
		err error
	}
	results := make(chan attempt, len(srcs))
	launched := 0
	for i, s := range srcs {
		if until, ok := f.cooldown.Load(hostOf(s.httpURL)); ok {
			if time.Now().Before(until.(time.Time)) {
				continue // rate-limited host sits this race out
			}
			f.cooldown.Delete(hostOf(s.httpURL))
		}
		launched++
		go func(i int, s source) {
			// Hedge stagger: source i waits i*hedge before spending network.
			select {
			case <-time.After(time.Duration(i) * hedge):
			case <-raceCtx.Done():
				results <- attempt{err: raceCtx.Err()}
				return
			}
			size, err := f.tryOne(raceCtx, s, destPath)
			if err != nil {
				results <- attempt{err: fmt.Errorf("%s: %w", s.name, err)}
				return
			}
			results <- attempt{res: Result{Via: s.name, Size: size, Elapsed: time.Since(start)}}
		}(i, s)
	}
	if launched == 0 {
		return Result{}, errors.New("all sources on rate-limit cooldown")
	}

	var errs []error
	for i := 0; i < launched; i++ {
		a := <-results
		if a.err == nil {
			cancel() // first success wins; stop the stragglers
			return a.res, nil
		}
		errs = append(errs, a.err)
	}
	return Result{}, fmt.Errorf("all %d sources failed: %w", launched, errors.Join(errs...))
}

// tryOne performs a single source attempt: GET, cap, temp-write, rename.
func (f *Fetcher) tryOne(ctx context.Context, s source, destPath string) (int64, error) {
	tryCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(tryCtx, http.MethodGet, s.httpURL, nil)
	if err != nil {
		return 0, err
	}
	resp, err := f.client().Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		f.cooldown.Store(hostOf(s.httpURL), time.Now().Add(cooldownAfter429))
		return 0, fmt.Errorf("429 (host on %s cooldown)", cooldownAfter429)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %d", resp.StatusCode)
	}

	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return 0, err
	}
	// Temp-then-rename so a concurrent reader never sees a torn file and a
	// killed process never leaves a plausible-looking partial at destPath.
	var rnd [8]byte
	_, _ = rand.Read(rnd[:])
	tmp := destPath + ".tmp-" + hex.EncodeToString(rnd[:])
	out, err := os.Create(tmp)
	if err != nil {
		return 0, err
	}
	limit := f.maxBytes()
	n, err := io.Copy(out, io.LimitReader(resp.Body, limit+1))
	closeErr := out.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil && n > limit {
		err = fmt.Errorf("exceeds %d-byte cap", limit)
	}
	if err == nil && n == 0 {
		err = errors.New("empty body")
	}
	if err != nil {
		os.Remove(tmp)
		return 0, err
	}
	if err := os.Rename(tmp, destPath); err != nil {
		os.Remove(tmp)
		// A concurrent fetcher (the API and mediawarm share the cache dir)
		// may have won the rename; content is identical either way.
		if fi, statErr := os.Stat(destPath); statErr == nil && fi.Size() > 0 {
			return fi.Size(), nil
		}
		return 0, err
	}
	return n, nil
}

func hostOf(httpURL string) string {
	if u, err := url.Parse(httpURL); err == nil {
		return u.Host
	}
	return httpURL
}
