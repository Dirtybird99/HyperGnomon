package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/hypergnomon/hypergnomon/eventbus"
)

// BenchmarkWSForwardEvents pushes events through a single subscription forwarder
// one-at-a-time (publish, then read the notification) and reports allocs/op.
// Go's AllocsPerOp counts heap allocations across ALL goroutines during the
// window, so the forwarder goroutine's per-event notification boxing is
// included. Hoisting the notification struct and passing a pointer removes the
// ~190B value->interface{} box, dropping allocs/op by ~1 (measured 13 -> 12).
//
// The no-stale-field property of the hoisted/reused struct (all 11 Params event
// fields reassigned every iteration so no prior event leaks through an omitempty
// field) is verified by inspection of forwardEvents plus the existing
// TestWSSubscribe_FanOutToMultipleConns / TestWSUnsubscribe_StopsEventDelivery
// end-to-end delivery tests (byte-identical wire output: json of *T == T).
func BenchmarkWSForwardEvents(b *testing.B) {
	bus := eventbus.New(4096)
	go bus.Run()
	defer bus.Close()

	var safe atomic.Int64
	safe.Store(1234)
	ws := NewWSServer("", nil, &safe, bus, nil)

	srv := httptest.NewServer(http.HandlerFunc(ws.ServeWS))
	defer srv.Close()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	c, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	if resp != nil {
		_ = resp.Body.Close()
	}
	defer c.Close()

	if err := c.WriteJSON(map[string]interface{}{
		"jsonrpc": "2.0", "id": 1, "method": "subscribe",
		"params": map[string]interface{}{},
	}); err != nil {
		b.Fatalf("subscribe write: %v", err)
	}
	var subResp struct {
		Result subscribeResult `json:"result"`
		Error  *jsonRPCError   `json:"error"`
	}
	if err := c.ReadJSON(&subResp); err != nil {
		b.Fatalf("subscribe resp: %v", err)
	}
	if subResp.Error != nil {
		b.Fatalf("subscribe error: %+v", subResp.Error)
	}

	time.Sleep(50 * time.Millisecond)

	ev := eventbus.Event{
		Type:        eventbus.EventInvoke,
		Height:      42,
		SafeHeight:  40,
		SCID:        "deadbeefdeadbeefdeadbeefdeadbeef",
		Class:       "TELA-DOC-1",
		Tags:        []string{"alpha", "beta"},
		Owner:       "owneraddr",
		Sender:      "senderaddr",
		Entrypoint:  "Update",
		Speculative: false,
		Payload:     map[string]interface{}{"k": "v"},
	}

	c.SetReadDeadline(time.Now().Add(60 * time.Second))
	var raw json.RawMessage

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bus.Publish(ev)
		if err := c.ReadJSON(&raw); err != nil {
			b.Fatalf("read event %d: %v", i, err)
		}
	}
	b.StopTimer()
}
