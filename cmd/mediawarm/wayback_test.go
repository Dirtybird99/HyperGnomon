package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/hypergnomon/hypergnomon/media"
)

func TestCaptureIPFSPath(t *testing.T) {
	tests := []struct{ in, want string }{
		{"https://ipfs.deronfts.com/ipfs/bafyRoot/Dero%20Apes%20%23494.png", "bafyRoot/Dero%20Apes%20%23494.png"},
		{"http://ipfs.deronfts.com/ipfs/QmRoot/1.jpg", "QmRoot/1.jpg"},
		// Marketplace resize endpoints are derivatives, not the content.
		{"https://ipfs.deronfts.com/img/250/bafyRoot/870.jpg", ""},
		{"https://ipfs.deronfts.com/", ""},
	}
	for _, tt := range tests {
		if got := captureIPFSPath(tt.in); got != tt.want {
			t.Errorf("captureIPFSPath(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestNormalizeIPFSPathMatchesBothEncodings(t *testing.T) {
	// The crawler archived the percent-encoded form; a minter may have written
	// either form on chain. Both must normalize to the same key.
	a := normalizeIPFSPath("bafyRoot/Dero%20Apes%20%23494.png")
	b := normalizeIPFSPath("bafyRoot/Dero Apes #494.png")
	if a != b {
		t.Errorf("encodings do not converge: %q vs %q", a, b)
	}
	// Invalid escapes must degrade to the raw string, not error out.
	if got := normalizeIPFSPath("bafyRoot/100%.png"); got == "" {
		t.Error("invalid escape should fall back to the raw path")
	}
}

// TestWaybackRecoverRoot drives the full recovery path against a fake CDX +
// snapshot server: enumerate, map to the wanted on-chain URL, fetch id_
// bytes, land them at the exact CachePath /api/media serves.
func TestWaybackRecoverRoot(t *testing.T) {
	var cdxHits, snapHits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/cdx/search/cdx":
			cdxHits++
			w.Write([]byte(`[["original","timestamp"],
["https://ipfs.deronfts.com/ipfs/bafyRoot/Dero%20Apes%20%23494.png","20230503155830"]]`))
		case strings.HasPrefix(r.URL.Path, "/web/") && strings.Contains(r.URL.Path, "id_/"):
			// r.URL.Path arrives percent-DECODED; match on the id_ marker
			// rather than the encoded filename. Content is arbitrary bytes —
			// the fetcher checks size, not magic.
			snapHits++
			w.Write([]byte("recovered-image-bytes"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	// Redirect web.archive.org to the fake server, preserving path + query.
	client := &http.Client{Transport: rewriteHost(srv)}

	dir := t.TempDir()
	chainURL := "ipfs://bafyRoot/Dero%20Apes%20%23494.png"
	wanted := map[string]string{normalizeIPFSPath("bafyRoot/Dero%20Apes%20%23494.png"): chainURL}

	n, bytes := waybackRecoverRoot(client, dir, []string{"ipfs.deronfts.com"}, "bafyRoot", wanted, 1<<20)
	if n != 1 || bytes == 0 {
		t.Fatalf("recovered %d files / %d bytes, want 1 file", n, bytes)
	}
	dest, _ := media.CachePath(dir, chainURL)
	if fi, err := os.Stat(dest); err != nil || fi.Size() == 0 {
		t.Errorf("bytes did not land at the servable CachePath %q: %v", dest, err)
	}
	if cdxHits != 1 || snapHits != 1 {
		t.Errorf("cdx=%d snap=%d, want 1 each", cdxHits, snapHits)
	}
	// Resume property: with the file present, the wanted-filter (as main.go
	// builds it) must exclude it, so a re-run costs nothing.
	for _, v := range wanted {
		p, _ := media.CachePath(dir, v)
		if fi, err := os.Stat(p); err != nil || fi.Size() == 0 {
			t.Error("resume filter should have excluded the recovered file")
		}
	}
}

// rewriteHost redirects every request to the test server, preserving path and
// query — the shape of an HTTP client with a stubbed origin.
func rewriteHost(srv *httptest.Server) http.RoundTripper {
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		u := *r.URL
		u.Scheme = "http"
		u.Host = srv.Listener.Addr().String()
		req2 := r.Clone(r.Context())
		req2.URL = &u
		req2.Host = u.Host
		return http.DefaultTransport.RoundTrip(req2)
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
