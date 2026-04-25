package api

import (
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/hypergnomon/hypergnomon/eventbus"
	"github.com/hypergnomon/hypergnomon/indexer"
	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

func newTestWSWithIndexer(t *testing.T) (*WSServer, *indexer.Indexer, func()) {
	t.Helper()
	store, err := storage.NewBboltStore(t.TempDir(), "")
	if err != nil {
		t.Fatalf("NewBboltStore: %v", err)
	}
	idx := &indexer.Indexer{Store: store}
	idx.ChainHeight.Store(9999)

	bus := eventbus.New(16)
	go bus.Run()

	var safe atomic.Int64
	ws := NewWSServer("", store, &safe, bus, idx)
	return ws, idx, func() {
		bus.Close()
		store.Close()
	}
}

func TestWS_GetInitialSCIDCode_Hit(t *testing.T) {
	ws, idx, cleanup := newTestWSWithIndexer(t)
	defer cleanup()

	scid := strings.Repeat("a", 64)
	if err := idx.Store.PutSCCode(scid, &structures.SCCodeEntry{
		Code: "FUNCTION A() END FUNCTION", InstallHeight: 42,
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	conn, closer := dialTestWS(t, ws)
	defer closer()

	req := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "GetInitialSCIDCode",
		"params":  map[string]interface{}{"scid": scid},
	}
	if err := conn.WriteJSON(req); err != nil {
		t.Fatalf("write: %v", err)
	}

	var resp struct {
		ID     int             `json:"id"`
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err := conn.ReadJSON(&resp); err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(resp.Error) > 0 && string(resp.Error) != "null" {
		t.Fatalf("unexpected error: %s", resp.Error)
	}
	var out struct {
		SCID          string `json:"scid"`
		Code          string `json:"code"`
		InstallHeight int64  `json:"install_height"`
	}
	if err := json.Unmarshal(resp.Result, &out); err != nil {
		t.Fatalf("decode result: %v (raw=%s)", err, resp.Result)
	}
	if out.SCID != scid || out.Code != "FUNCTION A() END FUNCTION" || out.InstallHeight != 42 {
		t.Fatalf("result mismatch: %+v", out)
	}
}

func TestWS_GetInitialSCIDCode_InvalidSCID(t *testing.T) {
	ws, _, cleanup := newTestWSWithIndexer(t)
	defer cleanup()

	conn, closer := dialTestWS(t, ws)
	defer closer()

	req := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "GetInitialSCIDCode",
		"params":  map[string]interface{}{"scid": "tooshort"},
	}
	if err := conn.WriteJSON(req); err != nil {
		t.Fatalf("write: %v", err)
	}

	var resp struct {
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := conn.ReadJSON(&resp); err != nil {
		t.Fatalf("read: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error, got none")
	}
	if resp.Error.Code != -32602 {
		t.Fatalf("error code = %d, want -32602 (invalid params)", resp.Error.Code)
	}
}

func TestWS_GetInitialSCIDCode_NotFound(t *testing.T) {
	ws, _, cleanup := newTestWSWithIndexer(t)
	defer cleanup()

	// Stub fetcher to return (empty, nil) so the lazy-backfill path
	// produces a not-found without contacting a real daemon.
	prev := indexer.SetSCCodeFetcherForTest(func(_ *indexer.Indexer, _ string) (string, int64, error) {
		return "", 0, nil
	})
	defer indexer.SetSCCodeFetcherForTest(prev)

	conn, closer := dialTestWS(t, ws)
	defer closer()

	scid := strings.Repeat("b", 64)
	req := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "GetInitialSCIDCode",
		"params":  map[string]interface{}{"scid": scid},
	}
	if err := conn.WriteJSON(req); err != nil {
		t.Fatalf("write: %v", err)
	}

	var resp struct {
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := conn.ReadJSON(&resp); err != nil {
		t.Fatalf("read: %v", err)
	}
	if resp.Error == nil {
		t.Fatal("expected error for unknown scid, got none")
	}
	if resp.Error.Code != -32004 {
		t.Fatalf("error code = %d, want -32004 (not found)", resp.Error.Code)
	}
}
