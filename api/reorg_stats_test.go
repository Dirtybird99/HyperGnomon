package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/hypergnomon/hypergnomon/storage"
)

// getStatsReorg drives handleGetStats and returns the reorg_detected field.
func getStatsReorg(t *testing.T, s *Server) (float64, bool) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/getstats", nil)
	w := httptest.NewRecorder()
	s.handleGetStats(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("handleGetStats status = %d, body=%s", w.Code, w.Body.String())
	}
	var body map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode getstats: %v", err)
	}
	v, ok := body["reorg_detected"]
	if !ok {
		return 0, false
	}
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("reorg_detected is %T, want number", v)
	}
	return f, true
}

// TestHandleGetStatsReorgDetected verifies /getstats surfaces the wired
// reorg-detection counter, mirroring how safe_height is surfaced.
func TestHandleGetStatsReorgDetected(t *testing.T) {
	store, err := storage.NewBboltStore(t.TempDir(), "")
	if err != nil {
		t.Fatalf("NewBboltStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	var ra atomic.Int64
	ra.Store(7)
	s := &Server{store: store, reorgDetected: &ra, telaCache: newTELAContentCache(1024)}

	got, ok := getStatsReorg(t, s)
	if !ok {
		t.Fatal("getstats response missing reorg_detected field")
	}
	if got != 7 {
		t.Fatalf("reorg_detected = %v, want 7", got)
	}
}

// TestHandleGetStatsReorgDetectedNil verifies a nil counter pointer is
// tolerated and reported as zero (mirrors safeHeight's nil handling).
func TestHandleGetStatsReorgDetectedNil(t *testing.T) {
	store, err := storage.NewBboltStore(t.TempDir(), "")
	if err != nil {
		t.Fatalf("NewBboltStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })

	s := &Server{store: store, telaCache: newTELAContentCache(1024)}
	got, ok := getStatsReorg(t, s)
	if !ok {
		t.Fatal("getstats response missing reorg_detected field")
	}
	if got != 0 {
		t.Fatalf("reorg_detected with nil pointer = %v, want 0", got)
	}
}
