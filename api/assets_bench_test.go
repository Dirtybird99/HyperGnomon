package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

// BenchmarkHTTP_GetAssets measures per-request allocations of the assets
// catalog handler on a WARM cache (TTL = 1h). Because the catalog is already
// cached, each iteration exercises only query-param parsing, the cache hit,
// the slice window, and JSON encoding — isolating the query-parse cost that
// the single-parse change targets. The httptest recorder adds a constant
// per-iteration baseline; the before/after allocs/op DELTA is the win.
func BenchmarkHTTP_GetAssets(b *testing.B) {
	store, err := storage.NewBboltStore(b.TempDir(), "")
	if err != nil {
		b.Fatalf("NewBboltStore: %v", err)
	}
	b.Cleanup(func() { _ = store.Close() })
	s := &Server{store: store, telaCache: newTELAContentCache(1024)}
	s.assetCatalogTTL = time.Hour

	scid := strings.Repeat("1", 64)
	owner := "dero1qassetbench00000000000000000000000000000000000000000000000000000"
	batch := storage.NewWriteBatch()
	batch.AddOwner(scid, owner)
	batch.AddInstall(scid, assetTestHeight, &structures.InstallRecord{Owner: owner, Entrypoint: "InitializePrivate"})
	batch.AddClass(scid, &structures.ClassMeta{
		Class:         "NFA",
		Tags:          []string{"all", "nfa"},
		Name:          "Bench Asset",
		InstallHeight: assetTestHeight,
		LastHeight:    assetTestHeight,
	})
	batch.LastHeight = assetTestHeight
	if err := store.FlushBatch(batch); err != nil {
		b.Fatalf("FlushBatch: %v", err)
	}
	storage.PutWriteBatch(batch)

	req := httptest.NewRequest(http.MethodGet, "/api/assets?class=NFA&offset=0&limit=50", nil)

	// Warm the catalog cache once so the measured loop is a pure cache hit.
	s.handleGetAssets(httptest.NewRecorder(), req)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		s.handleGetAssets(rec, req)
		if rec.Code != http.StatusOK {
			b.Fatalf("status %d body=%s", rec.Code, rec.Body.String())
		}
	}
}
