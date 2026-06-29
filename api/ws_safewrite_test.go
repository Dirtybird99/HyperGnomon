package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// dialServerConn establishes a real client<->server websocket pair over a
// loopback httptest server and returns the SERVER-side *websocket.Conn (the
// side safeWrite writes to, exercising gorilla's server WriteMessage fast
// path) plus the client conn used to read back the exact frame bytes. No
// per-message compression is negotiated, so the client receives the raw text
// payload safeWrite produced.
func dialServerConn(t *testing.T) (server, client *websocket.Conn, cleanup func()) {
	t.Helper()
	connCh := make(chan *websocket.Conn, 1)
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		connCh <- c
	}))
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	client, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		srv.Close()
		t.Fatalf("dial: %v", err)
	}
	if resp != nil {
		_ = resp.Body.Close()
	}
	select {
	case server = <-connCh:
	case <-time.After(5 * time.Second):
		srv.Close()
		t.Fatal("server upgrade timed out")
	}
	cleanup = func() {
		client.Close()
		server.Close()
		srv.Close()
	}
	return server, client, cleanup
}

// TestSafeWrite_ByteEqualityNoStale is the mandatory anti-stale / byte-equality
// gate for the reused-buffer encoder in safeWrite. It writes payload A (small),
// then B (larger, differently shaped, carrying HTML-escapable runes), then A
// again, on ONE connContext, and asserts each delivered frame equals
// append(json.Marshal(v), '\n') — the exact bytes the old WriteJSON path
// produced. Catches a missing encBuf.Reset(), a retained WriteMessage slice, a
// dropped trailing newline, and any accidental SetEscapeHTML(false).
func TestSafeWrite_ByteEqualityNoStale(t *testing.T) {
	server, client, cleanup := dialServerConn(t)
	defer cleanup()

	cctx := newConnContext(server)

	a := jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      json.RawMessage(`1`),
		Result:  map[string]int64{"safe_height": 1234},
	}
	b := eventNotification{
		JSONRPC: "2.0",
		Method:  "event",
		Params: eventNotificationParam{
			SubscriptionID: "sub-xyz-larger-than-a",
			Type:           "invoke",
			Height:         9001,
			SafeHeight:     8999,
			SCID:           "a<b&c>d",
			Class:          "TELA-INDEX-1",
			Entrypoint:     "Rate",
		},
	}
	payloads := []interface{}{a, b, a}

	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	for i, v := range payloads {
		if err := cctx.safeWrite(v); err != nil {
			t.Fatalf("payload %d safeWrite: %v", i, err)
		}
		mt, frame, err := client.ReadMessage()
		if err != nil {
			t.Fatalf("payload %d read frame: %v", i, err)
		}
		if mt != websocket.TextMessage {
			t.Fatalf("payload %d message type = %d, want TextMessage(%d)", i, mt, websocket.TextMessage)
		}
		marshaled, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("payload %d marshal: %v", i, err)
		}
		want := append(marshaled, '\n')
		if string(frame) != string(want) {
			t.Fatalf("payload %d frame mismatch:\n got: %q\nwant: %q", i, frame, want)
		}
	}
}

// TestWSServer_NoPerMessageCompression pins the production Upgrader: enabling
// per-message-deflate would both void the WriteMessage server fast-path alloc
// win and break the byte-equality gate above (compressed frames != raw JSON+'\n').
func TestWSServer_NoPerMessageCompression(t *testing.T) {
	ws := NewWSServer("", nil, nil, nil, nil)
	if ws.upgrader.EnableCompression {
		t.Fatal("WSServer.upgrader.EnableCompression must stay false")
	}
}
