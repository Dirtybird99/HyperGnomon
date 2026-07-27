package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/gorilla/mux"

	"github.com/hypergnomon/hypergnomon/media"
	"github.com/hypergnomon/hypergnomon/structures"
)

// /api/media handler behavior. The fetcher's race mechanics are covered in
// package media; these tests cover the HTTP contract: gating, headers,
// fallback URLs, and the asset guard.

const mediaTestSCID = "80aa000000000000000000000000000000000000000000000000000000000abc"

func mediaHarness(t *testing.T, fetch bool) (*Server, string) {
	t.Helper()
	srv, store := newAssetHTTPHarness(t)
	dir := t.TempDir()
	srv.mediaDir = dir
	srv.mediaFetch = fetch
	srv.mediaFetcher = &media.Fetcher{}
	seedAssetTestSC(t, store, mediaTestSCID, "ownerA", assetTestHeight, &structures.ClassMeta{
		Class: "G45-NFT", Tags: []string{"all", "g45"},
		Name:  "Duck",
		Image: "ipfs://QmMediaTestRoot/1801.png",
	}, nil)
	return srv, dir
}

func getMedia(srv *Server, scid, query string) *httptest.ResponseRecorder {
	r := mux.NewRouter()
	r.HandleFunc("/api/media/{scid}", srv.handleGetMedia).Methods(http.MethodGet, http.MethodHead)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/media/"+scid+query, nil))
	return w
}

func TestMediaServesCachedFileWithImmutableHeaders(t *testing.T) {
	srv, dir := mediaHarness(t, false)
	// Pre-place the file exactly where CachePath maps the seeded URL —
	// the filesystem-as-index contract.
	p, err := media.CachePath(dir, "ipfs://QmMediaTestRoot/1801.png")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	// A real PNG header so extension and sniffing agree.
	if err := os.WriteFile(p, append([]byte("\x89PNG\r\n\x1a\n"), make([]byte, 64)...), 0o644); err != nil {
		t.Fatal(err)
	}

	w := getMedia(srv, mediaTestSCID, "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", w.Code, w.Body.String())
	}
	h := w.Header()
	if h.Get("Cache-Control") != "public, max-age=31536000, immutable" {
		t.Errorf("Cache-Control = %q", h.Get("Cache-Control"))
	}
	if h.Get("ETag") == "" {
		t.Error("ETag missing")
	}
	if h.Get("X-Content-Type-Options") != "nosniff" {
		t.Error("nosniff missing — cached bytes are attacker-supplied")
	}
	if h.Get("Content-Security-Policy") == "" {
		t.Error("CSP sandbox missing — cached bytes are attacker-supplied")
	}
	if ct := h.Get("Content-Type"); ct != "image/png" {
		t.Errorf("Content-Type = %q, want image/png", ct)
	}

	// Conditional revalidation: same ETag back => 304.
	r := mux.NewRouter()
	r.HandleFunc("/api/media/{scid}", srv.handleGetMedia).Methods(http.MethodGet)
	req := httptest.NewRequest(http.MethodGet, "/api/media/"+mediaTestSCID, nil)
	req.Header.Set("If-None-Match", h.Get("ETag"))
	w2 := httptest.NewRecorder()
	r.ServeHTTP(w2, req)
	if w2.Code != http.StatusNotModified {
		t.Errorf("revalidation status = %d, want 304", w2.Code)
	}
}

func TestMediaMissWithoutFetchReturnsURL(t *testing.T) {
	srv, _ := mediaHarness(t, false)
	w := getMedia(srv, mediaTestSCID, "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d", w.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("body not JSON: %v", err)
	}
	// The fallback contract: a client that gets 404 must receive the
	// on-chain URL so it can try its own gateway.
	if body["url"] != "ipfs://QmMediaTestRoot/1801.png" {
		t.Errorf("fallback url = %q", body["url"])
	}
}

func TestMediaKindSelectsURL(t *testing.T) {
	srv, _ := mediaHarness(t, false)
	// Asset has no audio URL -> distinct 404 without a url field.
	w := getMedia(srv, mediaTestSCID, "?kind=audio")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d", w.Code)
	}
	var body map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if body["url"] != "" {
		t.Errorf("no-URL kind should not offer a fallback url, got %q", body["url"])
	}

	if w := getMedia(srv, mediaTestSCID, "?kind=bogus"); w.Code != http.StatusBadRequest {
		t.Errorf("bogus kind status = %d, want 400", w.Code)
	}
}

func TestMediaRejectsNonAssets(t *testing.T) {
	srv, store := newAssetHTTPHarness(t)
	srv.mediaDir = t.TempDir()
	srv.mediaFetcher = &media.Fetcher{}
	const telaSCID = "80bb000000000000000000000000000000000000000000000000000000000abc"
	seedAssetTestSC(t, store, telaSCID, "ownerB", assetTestHeight, &structures.ClassMeta{
		Class: "TELA-INDEX-1", Tags: []string{"all", "tela"},
		Image: "ipfs://QmShouldNotServe/x.png",
	}, nil)
	if w := getMedia(srv, telaSCID, ""); w.Code != http.StatusNotFound {
		t.Errorf("non-asset class served media: status %d", w.Code)
	}
}

func TestMediaUnsupportedSchemeRefused(t *testing.T) {
	srv, store := newAssetHTTPHarness(t)
	srv.mediaDir = t.TempDir()
	srv.mediaFetch = true // even with fetching on, bad schemes never fetch
	srv.mediaFetcher = &media.Fetcher{}
	const evilSCID = "80cc000000000000000000000000000000000000000000000000000000000abc"
	seedAssetTestSC(t, store, evilSCID, "ownerC", assetTestHeight, &structures.ClassMeta{
		Class: "G45-NFT", Tags: []string{"all", "g45"},
		Image: "javascript:alert(1)",
	}, nil)
	w := getMedia(srv, evilSCID, "")
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d", w.Code)
	}
	var body map[string]string
	_ = json.Unmarshal(w.Body.Bytes(), &body)
	if body["error"] == "" {
		t.Error("expected an explanatory error for the refused scheme")
	}
}

func TestMediaUnconfiguredReturns503(t *testing.T) {
	srv, _ := newAssetHTTPHarness(t)
	if w := getMedia(srv, mediaTestSCID, ""); w.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 when --media-dir is unset", w.Code)
	}
}
