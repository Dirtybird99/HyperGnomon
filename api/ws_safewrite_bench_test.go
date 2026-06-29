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

// dialServerConnBench is the benchmark twin of dialServerConn: it returns the
// real server-side *websocket.Conn (no per-message compression negotiated) plus
// the client used to drain frames so WriteMessage never blocks on a full socket
// buffer.
func dialServerConnBench(b *testing.B) (server, client *websocket.Conn, cleanup func()) {
	b.Helper()
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
		b.Fatalf("dial: %v", err)
	}
	if resp != nil {
		_ = resp.Body.Close()
	}
	select {
	case server = <-connCh:
	case <-time.After(5 * time.Second):
		srv.Close()
		b.Fatal("server upgrade timed out")
	}
	cleanup = func() {
		client.Close()
		server.Close()
		srv.Close()
	}
	return server, client, cleanup
}

// BenchmarkWS_SafeWrite measures allocs/op of the per-connection write path over
// a real server-side *websocket.Conn (gorilla's alloc-free server WriteMessage
// fast path; no per-message-deflate). One write per op via sub-benchmarks so
// allocs/op reads directly as per-write framing+encode cost.
func BenchmarkWS_SafeWrite(b *testing.B) {
	server, client, cleanup := dialServerConnBench(b)
	defer cleanup()

	go func() {
		for {
			if _, _, err := client.ReadMessage(); err != nil {
				return
			}
		}
	}()

	cctx := newConnContext(server)
	resp := jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      json.RawMessage(`1`),
		Result:  map[string]int64{"safe_height": 1234},
	}
	notif := eventNotification{
		JSONRPC: "2.0",
		Method:  "event",
		Params: eventNotificationParam{
			SubscriptionID: "sub-xyz",
			Type:           "invoke",
			Height:         9001,
			SafeHeight:     8999,
			SCID:           "deadbeef",
			Class:          "TELA-INDEX-1",
			Entrypoint:     "Rate",
		},
	}

	cases := []struct {
		name string
		v    interface{}
	}{
		{"response", resp},
		{"notification", notif},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < 4; i++ { // prewarm buffer + encodeState pool
				if err := cctx.safeWrite(tc.v); err != nil {
					b.Fatalf("prewarm: %v", err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := cctx.safeWrite(tc.v); err != nil {
					b.Fatalf("safeWrite: %v", err)
				}
			}
			b.StopTimer()
		})
	}
}
