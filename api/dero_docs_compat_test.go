package api

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gorilla/mux"

	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

const deroDocsCompatHeight int64 = 7000000

func newDeroDocsCompatHarness(t *testing.T) (*Server, *WSServer, *storage.BboltStore) {
	t.Helper()
	store, err := storage.NewBboltStore(t.TempDir(), "")
	if err != nil {
		t.Fatalf("NewBboltStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	var safe atomic.Int64
	safe.Store(deroDocsCompatHeight - 10)

	s := &Server{store: store, telaCache: newTELAContentCache(1024)}
	ws := NewWSServer("", store, &safe, nil, nil)
	return s, ws, store
}

func seedDeroDocsCompatSC(t *testing.T, store storage.Storage, scid, owner, class, name, durl string, vars []*structures.SCIDVariable) {
	t.Helper()
	batch := storage.NewWriteBatch()
	defer storage.PutWriteBatch(batch)

	batch.AddOwner(scid, owner)
	batch.AddInstall(scid, deroDocsCompatHeight, &structures.InstallRecord{Owner: owner, Entrypoint: "InitializePrivate"})
	batch.AddClass(scid, &structures.ClassMeta{
		Class:         class,
		Tags:          []string{"all", "tela"},
		Name:          name,
		DURL:          durl,
		InstallHeight: deroDocsCompatHeight,
		LastHeight:    deroDocsCompatHeight,
	})
	batch.AddVariables(scid, deroDocsCompatHeight, vars)
	batch.AddInteractionHeight(scid, deroDocsCompatHeight)
	batch.LastHeight = deroDocsCompatHeight

	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch(%s): %v", scid, err)
	}
}

func telaHex(s string) string {
	return hex.EncodeToString([]byte(s))
}

func mustJSONRaw(t *testing.T, v interface{}) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return b
}

func seedDeroDocsCompatGraph(t *testing.T, store storage.Storage) (indexSCID, htmlDocSCID, cssDocSCID, owner string) {
	t.Helper()
	indexSCID = strings.Repeat("a", 64)
	htmlDocSCID = strings.Repeat("b", 64)
	cssDocSCID = strings.Repeat("c", 64)
	legacyDocSCID := strings.Repeat("d", 64)
	owner = "dero1qcompatowner0000000000000000000000000000000000000000000000000000"
	raterA := "dero1qcompatuser00000000000000000000000000000000000000000000000000000"
	raterB := "deto1qcompatuser00000000000000000000000000000000000000000000000000000"

	seedDeroDocsCompatSC(t, store, htmlDocSCID, owner, "TELA-DOC-1", "index.html", "compat-index-doc.tela", []*structures.SCIDVariable{
		{Key: "var_header_name", Value: telaHex("index.html")},
		{Key: "subDir", Value: telaHex("app")},
		{Key: "docType", Value: "TELA-HTML-1"},
		{Key: "dURL", Value: telaHex("compat-index-doc.tela")},
		{Key: "fileCheckC", Value: strings.Repeat("1", 64)},
		{Key: "fileCheckS", Value: strings.Repeat("2", 64)},
	})
	seedDeroDocsCompatSC(t, store, cssDocSCID, owner, "TELA-DOC-1", "style.css", "compat-style-doc.tela", []*structures.SCIDVariable{
		{Key: "var_header_name", Value: telaHex("style.css")},
		{Key: "docType", Value: "TELA-CSS-1"},
		{Key: "dURL", Value: telaHex("compat-style-doc.tela")},
	})
	seedDeroDocsCompatSC(t, store, legacyDocSCID, owner, "TELA-DOC-1", "legacy.js", "compat-legacy-doc.tela", []*structures.SCIDVariable{
		{Key: "nameHdr", Value: telaHex("legacy.js")},
		{Key: "docType", Value: "TELA-JS-1"},
		{Key: "dURL", Value: telaHex("compat-legacy-doc.tela")},
	})
	seedDeroDocsCompatSC(t, store, indexSCID, owner, "TELA-INDEX-1", "Compat App", "compat.tela", []*structures.SCIDVariable{
		{Key: "DOC1", Value: telaHex(htmlDocSCID)},
		{Key: "DOC2", Value: telaHex(cssDocSCID)},
		{Key: "DOC3", Value: telaHex(legacyDocSCID)},
		{Key: "var_header_name", Value: telaHex("Compat App")},
		{Key: "dURL", Value: telaHex("compat.tela")},
		{Key: "likes", Value: uint64(1)},
		{Key: "dislikes", Value: uint64(1)},
		{Key: raterA, Value: hex.EncodeToString([]byte("90_7000000"))},
		{Key: raterB, Value: hex.EncodeToString([]byte("25_7000000"))},
	})
	return indexSCID, htmlDocSCID, cssDocSCID, owner
}

func TestDeroDocsCompat_RouteResolution(t *testing.T) {
	s, _, store := newDeroDocsCompatHarness(t)
	indexSCID, htmlDocSCID, cssDocSCID, _ := seedDeroDocsCompatGraph(t, store)

	tests := []struct {
		name     string
		reqPath  string
		wantSCID string
	}{
		{name: "canonical subdir", reqPath: "app/index.html", wantSCID: htmlDocSCID},
		{name: "basename fallback", reqPath: "index.html", wantSCID: htmlDocSCID},
		{name: "canonical flat file", reqPath: "style.css", wantSCID: cssDocSCID},
		{name: "legacy nameHdr fallback", reqPath: "legacy.js", wantSCID: strings.Repeat("d", 64)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotSCID, gotPath, err := s.resolveTELARoute(indexSCID, tc.reqPath)
			if err != nil {
				t.Fatalf("resolveTELARoute: %v", err)
			}
			if gotSCID != tc.wantSCID {
				t.Fatalf("doc scid = %s, want %s", gotSCID, tc.wantSCID)
			}
			if gotPath != tc.reqPath {
				t.Fatalf("doc path = %q, want %q", gotPath, tc.reqPath)
			}
		})
	}

	if _, _, err := s.resolveTELARoute(indexSCID, "missing.html"); err == nil {
		t.Fatal("missing route returned nil error")
	}

	modSCID := strings.Repeat("e", 64)
	seedDeroDocsCompatSC(t, store, modSCID, "dero1qmodowner000000000000000000000000000000000000000000000000000000", "TELA-MOD-1", "Policy", "policy.tela", nil)
	if _, _, err := s.resolveTELARoute(modSCID, "index.html"); err == nil || !strings.Contains(err.Error(), "not servable") {
		t.Fatalf("TELA-MOD route error = %v, want not servable", err)
	}
}

func TestDeroDocsCompat_HTTPDiscoveryAndRatings(t *testing.T) {
	s, _, store := newDeroDocsCompatHarness(t)
	indexSCID, _, _, _ := seedDeroDocsCompatGraph(t, store)

	oldCount := structures.TELACount.Load()
	structures.TELACount.Store(3)
	t.Cleanup(func() { structures.TELACount.Store(oldCount) })

	req := httptest.NewRequest(http.MethodGet, "/api/tela", nil)
	w := httptest.NewRecorder()
	s.handleGetTELA(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("/api/tela status = %d body=%s", w.Code, w.Body.String())
	}
	var appsResp struct {
		Count    int `json:"count"`
		TELAApps []struct {
			SCID  string `json:"scid"`
			Name  string `json:"name"`
			DURL  string `json:"durl"`
			Owner string `json:"owner"`
		} `json:"tela_apps"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &appsResp); err != nil {
		t.Fatalf("decode /api/tela: %v", err)
	}
	if appsResp.Count != 1 || len(appsResp.TELAApps) != 1 {
		t.Fatalf("/api/tela count=%d len=%d, want 1", appsResp.Count, len(appsResp.TELAApps))
	}
	if appsResp.TELAApps[0].SCID != indexSCID || appsResp.TELAApps[0].DURL != "compat.tela" {
		t.Fatalf("/api/tela app mismatch: %+v", appsResp.TELAApps[0])
	}

	req = httptest.NewRequest(http.MethodGet, "/api/tela/count", nil)
	w = httptest.NewRecorder()
	s.handleGetTELACount(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("/api/tela/count status = %d body=%s", w.Code, w.Body.String())
	}
	var countResp struct {
		Count int64 `json:"tela_count"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &countResp); err != nil {
		t.Fatalf("decode /api/tela/count: %v", err)
	}
	if countResp.Count != 3 {
		t.Fatalf("tela_count = %d, want 3", countResp.Count)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/tela/"+indexSCID+"/ratings", nil)
	req = mux.SetURLVars(req, map[string]string{"scid": indexSCID})
	w = httptest.NewRecorder()
	s.handleGetTELARatings(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("/api/tela/{scid}/ratings status = %d body=%s", w.Code, w.Body.String())
	}
	var ratingResp struct {
		Count   int     `json:"count"`
		Avg     float64 `json:"avg"`
		Summary struct {
			Likes    uint64 `json:"likes"`
			Dislikes uint64 `json:"dislikes"`
		} `json:"summary"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &ratingResp); err != nil {
		t.Fatalf("decode ratings: %v", err)
	}
	if ratingResp.Count != 2 || ratingResp.Avg != 57.5 {
		t.Fatalf("ratings count/avg = %d/%v, want 2/57.5", ratingResp.Count, ratingResp.Avg)
	}
	if ratingResp.Summary.Likes != 1 || ratingResp.Summary.Dislikes != 1 {
		t.Fatalf("summary = %+v, want 1 like and 1 dislike", ratingResp.Summary)
	}
}

func TestDeroDocsCompat_WSListSCFamily(t *testing.T) {
	_, ws, store := newDeroDocsCompatHarness(t)
	indexSCID, _, _, owner := seedDeroDocsCompatGraph(t, store)

	result, rpcErr := ws.handleListSC(nil, mustJSONRaw(t, map[string]interface{}{
		"class": "TELA-INDEX-1",
		"limit": 10,
	}))
	if rpcErr != nil {
		t.Fatalf("listsc: %+v", rpcErr)
	}
	listResp := result.(listSCResponse)
	if listResp.Count != 1 || len(listResp.Results) != 1 {
		t.Fatalf("listsc count=%d len=%d, want 1", listResp.Count, len(listResp.Results))
	}
	if listResp.Results[0].SCID != indexSCID || listResp.Results[0].Class != "TELA-INDEX-1" {
		t.Fatalf("listsc row mismatch: %+v", listResp.Results[0])
	}

	result, rpcErr = ws.handleListSCVariables(nil, mustJSONRaw(t, map[string]interface{}{"scid": indexSCID}))
	if rpcErr != nil {
		t.Fatalf("listsc_variables: %+v", rpcErr)
	}
	varsResp := result.(listSCVariablesResponse)
	if varsResp.Height != deroDocsCompatHeight {
		t.Fatalf("variables height=%d, want %d", varsResp.Height, deroDocsCompatHeight)
	}
	if len(varsResp.Variables) == 0 {
		t.Fatal("listsc_variables returned no variables")
	}

	result, rpcErr = ws.handleListSCRatings(nil, mustJSONRaw(t, map[string]interface{}{"scid": indexSCID}))
	if rpcErr != nil {
		t.Fatalf("listsc_ratings: %+v", rpcErr)
	}
	ratingsResp := result.(listSCRatingsResponse)
	if ratingsResp.Count != 2 || ratingsResp.Avg != 57.5 {
		t.Fatalf("listsc_ratings count/avg = %d/%v, want 2/57.5", ratingsResp.Count, ratingsResp.Avg)
	}

	result, rpcErr = ws.handleListSCByOwner(nil, mustJSONRaw(t, map[string]interface{}{"owner": owner}))
	if rpcErr != nil {
		t.Fatalf("listsc_byowner: %+v", rpcErr)
	}
	ownerResp := result.(listSCByOwnerResponse)
	if len(ownerResp.SCIDs) != 4 {
		t.Fatalf("owner scids len=%d, want 4", len(ownerResp.SCIDs))
	}

	result, rpcErr = ws.handleListSCByClass(nil, mustJSONRaw(t, map[string]interface{}{"class": "TELA-DOC-1"}))
	if rpcErr != nil {
		t.Fatalf("listsc_byclass: %+v", rpcErr)
	}
	classResp := result.(listSCResponse)
	if classResp.Count != 3 || len(classResp.Results) != 3 {
		t.Fatalf("listsc_byclass count=%d len=%d, want 3", classResp.Count, len(classResp.Results))
	}
}

func TestDeroDocsCompat_InvalidGzipFails(t *testing.T) {
	if _, err := decompressTELAGzip([]byte("not base64 and not gzip")); err == nil {
		t.Fatal("decompressTELAGzip returned nil error for invalid payload")
	}
}
