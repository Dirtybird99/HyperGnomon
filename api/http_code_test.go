package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/mux"

	"github.com/hypergnomon/hypergnomon/indexer"
	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

// newTestHTTPServer stands up a Server bound to a tempdir store with the
// sccode route mounted on a mux. Returns (httptest server, underlying
// Indexer, cleanup).
func newTestHTTPServer(t *testing.T) (*httptest.Server, *indexer.Indexer, func()) {
	t.Helper()
	store, err := storage.NewBboltStore(t.TempDir(), "")
	if err != nil {
		t.Fatalf("NewBboltStore: %v", err)
	}
	idx := &indexer.Indexer{Store: store}
	idx.ChainHeight.Store(9999)
	s := &Server{store: store, tela: idx, telaCache: newTELAContentCache(1024)}

	r := mux.NewRouter()
	r.HandleFunc("/api/initialscidcode", s.handleGetInitialSCIDCode).Methods(http.MethodGet)
	srv := httptest.NewServer(r)
	return srv, idx, func() {
		srv.Close()
		store.Close()
	}
}

func TestHTTP_GetInitialSCIDCode_Hit(t *testing.T) {
	srv, idx, cleanup := newTestHTTPServer(t)
	defer cleanup()

	scid := strings.Repeat("a", 64)
	if err := idx.Store.PutSCCode(scid, &structures.SCCodeEntry{
		Code: "FUNCTION A() END FUNCTION", InstallHeight: 123,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	resp, err := http.Get(srv.URL + "/api/initialscidcode?scid=" + scid)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}

	var got map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["scid"] != scid {
		t.Errorf("scid mismatch: %v", got["scid"])
	}
	if got["code"] != "FUNCTION A() END FUNCTION" {
		t.Errorf("code mismatch: %v", got["code"])
	}
	if got["install_height"].(float64) != 123 {
		t.Errorf("height mismatch: %v", got["install_height"])
	}
}

func TestHTTP_GetInitialSCIDCode_MissingScid(t *testing.T) {
	srv, _, cleanup := newTestHTTPServer(t)
	defer cleanup()

	resp, err := http.Get(srv.URL + "/api/initialscidcode")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHTTP_GetInitialSCIDCode_InvalidLength(t *testing.T) {
	srv, _, cleanup := newTestHTTPServer(t)
	defer cleanup()

	resp, err := http.Get(srv.URL + "/api/initialscidcode?scid=tooshort")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHTTP_GetInitialSCIDCode_NotFound(t *testing.T) {
	srv, _, cleanup := newTestHTTPServer(t)
	defer cleanup()

	// Stub fetcher so unknown scid returns (nil, nil) cleanly instead of
	// attempting a real daemon call.
	prev := indexer.SetSCCodeFetcherForTest(func(_ *indexer.Indexer, _ string) (string, int64, error) {
		return "", 0, nil
	})
	defer indexer.SetSCCodeFetcherForTest(prev)

	scid := strings.Repeat("b", 64)
	resp, err := http.Get(srv.URL + "/api/initialscidcode?scid=" + scid)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, body = %s, want 404", resp.StatusCode, body)
	}
}
