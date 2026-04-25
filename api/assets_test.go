package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"

	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

const assetTestHeight int64 = 8000000

func newAssetHTTPHarness(t *testing.T) (*Server, *storage.BboltStore) {
	t.Helper()
	store, err := storage.NewBboltStore(t.TempDir(), "")
	if err != nil {
		t.Fatalf("NewBboltStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return &Server{store: store, telaCache: newTELAContentCache(1024)}, store
}

func seedAssetTestSC(t *testing.T, store storage.Storage, scid, owner string, height int64, meta *structures.ClassMeta, touches map[string][]int64) {
	t.Helper()
	if meta.InstallHeight == 0 {
		meta.InstallHeight = height
	}
	if meta.LastHeight == 0 {
		meta.LastHeight = height
	}

	batch := storage.NewWriteBatch()
	defer storage.PutWriteBatch(batch)
	batch.AddOwner(scid, owner)
	batch.AddInstall(scid, height, &structures.InstallRecord{Owner: owner, Entrypoint: "InitializePrivate"})
	batch.AddClass(scid, meta)
	for addr, heights := range touches {
		for _, h := range heights {
			batch.AddAddrSCID(addr, scid, h)
		}
	}
	batch.LastHeight = height
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch(%s): %v", scid, err)
	}
}

func decodeAssetResponse(t *testing.T, w *httptest.ResponseRecorder) struct {
	Address string       `json:"address"`
	Assets  []assetEntry `json:"assets"`
	Count   int          `json:"count"`
	Offset  int          `json:"offset"`
	Limit   int          `json:"limit"`
} {
	t.Helper()
	var resp struct {
		Address string       `json:"address"`
		Assets  []assetEntry `json:"assets"`
		Count   int          `json:"count"`
		Offset  int          `json:"offset"`
		Limit   int          `json:"limit"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v body=%s", err, w.Body.String())
	}
	return resp
}

func TestHTTPAssetsCatalogAndClassFilter(t *testing.T) {
	s, store := newAssetHTTPHarness(t)
	ownerA := "dero1qassetowner000000000000000000000000000000000000000000000000000"
	ownerB := "dero1qassetowner111111111111111111111111111111111111111111111111111"
	nfaSCID := strings.Repeat("1", 64)
	g45SCID := strings.Repeat("2", 64)
	telaSCID := strings.Repeat("3", 64)
	legacySCID := strings.Repeat("4", 64)

	seedAssetTestSC(t, store, nfaSCID, ownerA, assetTestHeight, &structures.ClassMeta{
		Class:   "NFA",
		Tags:    []string{"all", "nfa"},
		Name:    "Legacy Art",
		Desc:    "NFA description",
		IconURL: "https://example.com/nfa.png",
	}, nil)
	seedAssetTestSC(t, store, g45SCID, ownerB, assetTestHeight+1, &structures.ClassMeta{
		Class: "G45-NFT",
		Tags:  []string{"all", "g45"},
		Name:  "G45 Collectible",
	}, nil)
	seedAssetTestSC(t, store, telaSCID, ownerA, assetTestHeight+2, &structures.ClassMeta{
		Class: "TELA-INDEX-1",
		Tags:  []string{"all", "tela"},
		Name:  "Not An Asset",
	}, nil)
	seedAssetTestSC(t, store, legacySCID, ownerA, assetTestHeight+3, &structures.ClassMeta{
		Class: "DERO-ASSET",
		Tags:  []string{"all", "asset"},
		Name:  "Legacy Token",
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/assets", nil)
	w := httptest.NewRecorder()
	s.handleGetAssets(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("/api/assets status=%d body=%s", w.Code, w.Body.String())
	}
	resp := decodeAssetResponse(t, w)
	if resp.Count != 3 || len(resp.Assets) != 3 {
		t.Fatalf("asset count=%d len=%d, want 3", resp.Count, len(resp.Assets))
	}
	if resp.Assets[0].SCID != nfaSCID || resp.Assets[0].Owner != ownerA || resp.Assets[0].Description != "NFA description" {
		t.Fatalf("first asset mismatch: %+v", resp.Assets[0])
	}
	for _, a := range resp.Assets {
		if a.Class == "TELA-INDEX-1" {
			t.Fatalf("catalog included non-asset: %+v", a)
		}
	}

	req = httptest.NewRequest(http.MethodGet, "/api/assets?class=g45-nft", nil)
	w = httptest.NewRecorder()
	s.handleGetAssets(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("/api/assets class status=%d body=%s", w.Code, w.Body.String())
	}
	resp = decodeAssetResponse(t, w)
	if resp.Count != 1 || len(resp.Assets) != 1 || resp.Assets[0].SCID != g45SCID {
		t.Fatalf("class filter response = %+v, want only %s", resp, g45SCID)
	}
}

func TestHTTPAssetsCatalogCacheHit(t *testing.T) {
	s, store := newAssetHTTPHarness(t)
	s.assetCatalogTTL = time.Hour
	owner := "dero1qassetcache0000000000000000000000000000000000000000000000000000"
	firstSCID := strings.Repeat("b", 64)
	secondSCID := strings.Repeat("c", 64)

	seedAssetTestSC(t, store, firstSCID, owner, assetTestHeight, &structures.ClassMeta{
		Class: "NFA",
		Tags:  []string{"all", "nfa"},
		Name:  "Cached First",
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/assets?class=NFA", nil)
	w := httptest.NewRecorder()
	s.handleGetAssets(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("first /api/assets status=%d body=%s", w.Code, w.Body.String())
	}
	first := decodeAssetResponse(t, w)
	if first.Count != 1 || len(first.Assets) != 1 || first.Assets[0].SCID != firstSCID {
		t.Fatalf("first response = %+v, want only %s", first, firstSCID)
	}

	seedAssetTestSC(t, store, secondSCID, owner, assetTestHeight+1, &structures.ClassMeta{
		Class: "NFA",
		Tags:  []string{"all", "nfa"},
		Name:  "Hidden Until TTL",
	}, nil)

	req = httptest.NewRequest(http.MethodGet, "/api/assets?class=NFA", nil)
	w = httptest.NewRecorder()
	s.handleGetAssets(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("cached /api/assets status=%d body=%s", w.Code, w.Body.String())
	}
	cached := decodeAssetResponse(t, w)
	if cached.Count != 1 || len(cached.Assets) != 1 || cached.Assets[0].SCID != firstSCID {
		t.Fatalf("cached response = %+v, want cached first asset only", cached)
	}
}

func TestHTTPAssetsEmptyCatalogCacheExpiresQuickly(t *testing.T) {
	s, store := newAssetHTTPHarness(t)
	s.assetCatalogEmptyTTL = 20 * time.Millisecond
	owner := "dero1qemptycache000000000000000000000000000000000000000000000000000"
	scid := strings.Repeat("d", 64)

	req := httptest.NewRequest(http.MethodGet, "/api/assets?class=NFA", nil)
	w := httptest.NewRecorder()
	s.handleGetAssets(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("empty /api/assets status=%d body=%s", w.Code, w.Body.String())
	}
	empty := decodeAssetResponse(t, w)
	if empty.Count != 0 || len(empty.Assets) != 0 {
		t.Fatalf("empty response = %+v, want no assets", empty)
	}

	seedAssetTestSC(t, store, scid, owner, assetTestHeight, &structures.ClassMeta{
		Class: "NFA",
		Tags:  []string{"all", "nfa"},
		Name:  "Appears After Empty TTL",
	}, nil)
	time.Sleep(40 * time.Millisecond)

	req = httptest.NewRequest(http.MethodGet, "/api/assets?class=NFA", nil)
	w = httptest.NewRecorder()
	s.handleGetAssets(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("refreshed /api/assets status=%d body=%s", w.Code, w.Body.String())
	}
	refreshed := decodeAssetResponse(t, w)
	if refreshed.Count != 1 || len(refreshed.Assets) != 1 || refreshed.Assets[0].SCID != scid {
		t.Fatalf("refreshed response = %+v, want %s", refreshed, scid)
	}
}

func TestHTTPAssetDetailValidation(t *testing.T) {
	s, store := newAssetHTTPHarness(t)
	owner := "dero1qassetowner222222222222222222222222222222222222222222222222222"
	assetSCID := strings.Repeat("5", 64)
	nonAssetSCID := strings.Repeat("6", 64)

	seedAssetTestSC(t, store, assetSCID, owner, assetTestHeight, &structures.ClassMeta{
		Class: "G45-FAT",
		Tags:  []string{"all", "g45"},
		Name:  "Fungible Token",
	}, nil)
	seedAssetTestSC(t, store, nonAssetSCID, owner, assetTestHeight+1, &structures.ClassMeta{
		Class: "TELA-DOC-1",
		Tags:  []string{"all", "tela"},
		Name:  "Document",
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/assets/"+assetSCID, nil)
	req = mux.SetURLVars(req, map[string]string{"scid": assetSCID})
	w := httptest.NewRecorder()
	s.handleGetAsset(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("asset detail status=%d body=%s", w.Code, w.Body.String())
	}
	var detail assetEntry
	if err := json.Unmarshal(w.Body.Bytes(), &detail); err != nil {
		t.Fatalf("decode detail: %v", err)
	}
	if detail.SCID != assetSCID || detail.Owner != owner || detail.Class != "G45-FAT" {
		t.Fatalf("asset detail mismatch: %+v", detail)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/assets/"+nonAssetSCID, nil)
	req = mux.SetURLVars(req, map[string]string{"scid": nonAssetSCID})
	w = httptest.NewRecorder()
	s.handleGetAsset(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("non-asset status=%d body=%s, want 404", w.Code, w.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/assets/not-a-scid", nil)
	req = mux.SetURLVars(req, map[string]string{"scid": "not-a-scid"})
	w = httptest.NewRecorder()
	s.handleGetAsset(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid scid status=%d body=%s, want 400", w.Code, w.Body.String())
	}
}

func TestHTTPAddressAssetViews(t *testing.T) {
	s, store := newAssetHTTPHarness(t)
	owner := "dero1qcreatedasset00000000000000000000000000000000000000000000000000"
	otherOwner := "dero1qcreatedasset11111111111111111111111111111111111111111111111111"
	wallet := "dero1qwalletasset000000000000000000000000000000000000000000000000000"
	nfaSCID := strings.Repeat("7", 64)
	g45SCID := strings.Repeat("8", 64)
	telaSCID := strings.Repeat("9", 64)
	legacySCID := strings.Repeat("a", 64)

	seedAssetTestSC(t, store, nfaSCID, owner, assetTestHeight, &structures.ClassMeta{
		Class: "NFA",
		Tags:  []string{"all", "nfa"},
		Name:  "Owned NFA",
	}, map[string][]int64{wallet: {500}})
	seedAssetTestSC(t, store, g45SCID, otherOwner, assetTestHeight+1, &structures.ClassMeta{
		Class: "G45-AT",
		Tags:  []string{"all", "g45"},
		Name:  "Touched Asset",
	}, map[string][]int64{wallet: {600, 650}})
	seedAssetTestSC(t, store, telaSCID, owner, assetTestHeight+2, &structures.ClassMeta{
		Class: "TELA-INDEX-1",
		Tags:  []string{"all", "tela"},
		Name:  "Touched TELA",
	}, map[string][]int64{wallet: {700}})
	seedAssetTestSC(t, store, legacySCID, owner, assetTestHeight+3, &structures.ClassMeta{
		Class: "DERO-ASSET",
		Tags:  []string{"all", "asset"},
		Name:  "Created Token",
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/address/"+owner+"/created-assets", nil)
	req = mux.SetURLVars(req, map[string]string{"address": owner})
	w := httptest.NewRecorder()
	s.handleGetAddressCreatedAssets(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("created-assets status=%d body=%s", w.Code, w.Body.String())
	}
	created := decodeAssetResponse(t, w)
	if created.Count != 2 || len(created.Assets) != 2 {
		t.Fatalf("created count=%d len=%d, want 2", created.Count, len(created.Assets))
	}
	if created.Assets[0].SCID != nfaSCID || created.Assets[1].SCID != legacySCID {
		t.Fatalf("created assets mismatch: %+v", created.Assets)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/address/"+wallet+"/touched-assets", nil)
	req = mux.SetURLVars(req, map[string]string{"address": wallet})
	w = httptest.NewRecorder()
	s.handleGetAddressTouchedAssets(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("touched-assets status=%d body=%s", w.Code, w.Body.String())
	}
	touched := decodeAssetResponse(t, w)
	if touched.Count != 2 || len(touched.Assets) != 2 {
		t.Fatalf("touched count=%d len=%d, want 2", touched.Count, len(touched.Assets))
	}
	if touched.Assets[0].SCID != g45SCID || touched.Assets[0].TouchCount != 2 || touched.Assets[0].LastTouchHeight != 650 {
		t.Fatalf("first touched asset mismatch: %+v", touched.Assets[0])
	}
	if touched.Assets[1].SCID != nfaSCID || touched.Assets[1].TouchCount != 1 {
		t.Fatalf("second touched asset mismatch: %+v", touched.Assets[1])
	}
}
