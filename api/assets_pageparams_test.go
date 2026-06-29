package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hypergnomon/hypergnomon/structures"
)

func TestHTTPAssetsPageParamsAndOrder(t *testing.T) {
	s, store := newAssetHTTPHarness(t)
	s.assetCatalogTTL = time.Hour
	owner := "dero1qpageparams000000000000000000000000000000000000000000000000000"
	scidA := strings.Repeat("1", 64)
	scidB := strings.Repeat("2", 64)
	seedAssetTestSC(t, store, scidA, owner, assetTestHeight, &structures.ClassMeta{
		Class: "NFA", Tags: []string{"all", "nfa"}, Name: "First NFA",
	}, nil)
	seedAssetTestSC(t, store, scidB, owner, assetTestHeight+1, &structures.ClassMeta{
		Class: "NFA", Tags: []string{"all", "nfa"}, Name: "Second NFA",
	}, nil)

	// offset/limit query params are threaded through and applied to the window.
	req := httptest.NewRequest(http.MethodGet, "/api/assets?class=NFA&offset=1&limit=1", nil)
	w := httptest.NewRecorder()
	s.handleGetAssets(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("paged /api/assets status=%d body=%s", w.Code, w.Body.String())
	}
	resp := decodeAssetResponse(t, w)
	if resp.Count != 2 || resp.Offset != 1 || resp.Limit != 1 {
		t.Fatalf("paged response meta = %+v, want count=2 offset=1 limit=1", resp)
	}
	if len(resp.Assets) != 1 || resp.Assets[0].SCID != scidB {
		t.Fatalf("paged window = %+v, want only %s", resp.Assets, scidB)
	}

	// Page-param validation must run BEFORE class validation: a bad offset
	// combined with a bad class must surface the offset error.
	req = httptest.NewRequest(http.MethodGet, "/api/assets?offset=-1&class=not-a-class", nil)
	w = httptest.NewRecorder()
	s.handleGetAssets(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad offset status=%d, want 400", w.Code)
	}
	// json.Marshal escapes '>' as >, so match the unambiguous prefix that
	// still distinguishes the offset error from the class error.
	if !strings.Contains(w.Body.String(), "offset must be") {
		t.Fatalf("bad offset body=%s, want offset error (page params before class)", w.Body.String())
	}
	if strings.Contains(w.Body.String(), "class is not an asset class") {
		t.Fatalf("bad offset surfaced class error instead: %s", w.Body.String())
	}

	// A bad class with valid page params surfaces the class error.
	req = httptest.NewRequest(http.MethodGet, "/api/assets?class=not-a-class", nil)
	w = httptest.NewRecorder()
	s.handleGetAssets(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad class status=%d, want 400", w.Code)
	}
	if !strings.Contains(w.Body.String(), "class is not an asset class") {
		t.Fatalf("bad class body=%s, want class error", w.Body.String())
	}
}
