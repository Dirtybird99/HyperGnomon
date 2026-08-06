package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gorilla/mux"

	"github.com/hypergnomon/hypergnomon/media"
	"github.com/hypergnomon/hypergnomon/structures"
)

// /api/media/{scid} — serve the bytes behind an asset's media URLs from the
// local fetch-once cache.
//
// Why this endpoint exists: /api/assets surfaces the URLs, but 45k of them
// are ipfs:// on a network where most public gateways are dead, the rest
// rate-limit, and (measured 2026-07-27) the majority of corpus roots have NO
// remaining gateway or DHT provider. Loading images fast — and at all —
// means serving from a local, permanent cache. See package media.
//
// Contract:
//
//	GET /api/media/{scid}?kind=image|alt|audio|video   (default image)
//	  200  cached bytes, immutable cache headers, ranges supported
//	  404  {"error": ..., "url": <on-chain URL if any>}  — the URL is
//	       returned so a client can fall back to its own gateway
//
// Cache misses trigger a live fetch only when the operator opted in with
// --media-fetch; otherwise the endpoint serves what cmd/mediawarm has
// already cached and never touches the network. Off by default so a public
// deployment cannot be used as an open fetch proxy.

// mediaFetchTimeout bounds one on-demand fetch from the request path. Kept
// under typical proxy/browser timeouts; the bulk path (cmd/mediawarm) uses
// its own, much longer budgets.
const mediaFetchTimeout = 90 * time.Second

// SetMediaOptions wires the media cache. dir is the cache root (empty
// disables the endpoint with 503s), fetch enables on-demand retrieval,
// ipfsGateway is a local kubo gateway base URL ("" = public gateways only).
// Call before Start, like SetTELAVerifySigs.
func (s *Server) SetMediaOptions(dir string, fetch bool, ipfsGateway string) {
	s.mediaDir = dir
	s.mediaFetch = fetch
	s.mediaFetcher = &media.Fetcher{LocalGateway: ipfsGateway}
}

// mediaURLForKind picks the on-chain URL the kind refers to.
func mediaURLForKind(meta *structures.ClassMeta, kind string) (string, bool) {
	switch kind {
	case "", "image":
		return meta.Image, true
	case "alt":
		return meta.AltImage, true
	case "audio":
		return meta.Audio, true
	case "video":
		return meta.Video, true
	}
	return "", false
}

func (s *Server) handleGetMedia(w http.ResponseWriter, r *http.Request) {
	if s.mediaDir == "" {
		writeError(w, http.StatusServiceUnavailable, "media cache not configured (--media-dir)")
		return
	}
	scid := mux.Vars(r)["scid"]
	if !isValidSCID(scid) {
		writeError(w, http.StatusBadRequest, "invalid scid")
		return
	}
	kind := r.URL.Query().Get("kind")
	meta, err := s.store.GetSCIDClass(scid)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get asset metadata: "+err.Error())
		return
	}
	// Same asset gate as /api/assets: this endpoint serves asset artwork,
	// not arbitrary SC content.
	if !isAssetMeta(meta) {
		writeError(w, http.StatusNotFound, "asset not found")
		return
	}
	rawURL, ok := mediaURLForKind(meta, kind)
	if !ok {
		writeError(w, http.StatusBadRequest, "kind must be image|alt|audio|video")
		return
	}
	if rawURL == "" {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "asset has no " + orDefault(kind, "image") + " URL"})
		return
	}
	cachePath, err := media.CachePath(s.mediaDir, rawURL)
	if err != nil {
		// Unsupported scheme (http://, data:, …): metadata is an
		// attacker-controlled string; we refuse rather than fetch. The client
		// gets the raw URL and can make its own judgment.
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "unsupported media URL", "url": rawURL})
		return
	}

	if !fileExists(cachePath) {
		if !s.mediaFetch {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "media not cached", "url": rawURL})
			return
		}
		// Singleflight per cache path: a gallery page requesting the same
		// uncached image 50 times must cost one upstream fetch, not 50.
		_, err, _ := s.mediaSF.Do(cachePath, func() (interface{}, error) {
			// Deliberately NOT derived from r.Context(): the singleflight
			// result is shared, so the first requester hitting Stop must not
			// cancel the fetch out from under everyone coalesced behind it.
			ctx, cancel := context.WithTimeout(context.Background(), mediaFetchTimeout)
			defer cancel()
			_, ferr := s.mediaFetcher.Fetch(ctx, rawURL, cachePath)
			return nil, ferr
		})
		if err != nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "fetch failed: " + err.Error(), "url": rawURL})
			return
		}
	}
	s.serveMediaFile(w, r, cachePath)
}

// serveMediaFile serves one cached file with immutable-cache semantics.
func (s *Server) serveMediaFile(w http.ResponseWriter, r *http.Request, cachePath string) {
	f, err := os.Open(cachePath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "open cached media: "+err.Error())
		return
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "stat cached media: "+err.Error())
		return
	}

	// The cache key is derived from the (content-addressed) URL, so the key
	// IS the version: a strong ETag that never changes. A year of immutable
	// is the standard "never revalidate" idiom.
	sum := sha256.Sum256([]byte(strings.TrimPrefix(cachePath, s.mediaDir)))
	w.Header().Set("ETag", `"`+hex.EncodeToString(sum[:16])+`"`)
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	// The bytes came from an attacker-controlled URL. Serving them from this
	// origin must never execute anything: no sniffing past the declared
	// type, and a CSP sandbox in case a browser renders the response
	// directly (e.g. an SVG or HTML file minted as an "image").
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Security-Policy", "default-src 'none'; sandbox")

	// ServeContent gets Content-Type from the file extension (falling back
	// to sniffing the first block), and handles ranges, HEAD, If-None-Match
	// and If-Modified-Since for us.
	http.ServeContent(w, r, fi.Name(), fi.ModTime(), f)
}

func fileExists(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && fi.Size() > 0
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
