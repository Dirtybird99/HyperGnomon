package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/hypergnomon/hypergnomon/eventbus"
)

// dialTestWS is a small test helper: spins up a real http.Server on a
// loopback port with ws.ServeWS wired to /ws, opens a client connection,
// and returns conn + cleanup. The server and client are closed when the
// test ends. Using httptest.Server keeps the port discovery portable
// (Windows, Linux) and avoids the production WSServer.Start Serve loop.
func dialTestWS(t *testing.T, ws *WSServer) (*websocket.Conn, func()) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(ws.ServeWS))
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws"
	c, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		srv.Close()
		t.Fatalf("dial: %v", err)
	}
	// The Dial response body carries handshake details; bodyclose wants it
	// explicitly closed even on WS upgrade.
	if resp != nil {
		_ = resp.Body.Close()
	}
	return c, func() {
		c.Close()
		srv.Close()
	}
}

// TestWSSubscribe_FanOutToMultipleConns is the race-check fan-out test
// described in the subscribe milestone: three concurrent connections
// subscribe to all events, the test publishes N events to the bus, each
// connection must receive all N. Run with -race -count=1.
func TestWSSubscribe_FanOutToMultipleConns(t *testing.T) {
	const (
		nConns = 3
		nMsgs  = 100
	)

	bus := eventbus.New(4096)
	go bus.Run()
	defer bus.Close()

	var safe atomic.Int64
	safe.Store(1234)
	ws := NewWSServer("", nil, &safe, bus, nil)

	// Connect nConns clients and subscribe each. Keep connections + sub
	// ids parallel-indexed with per-conn counters.
	conns := make([]*websocket.Conn, nConns)
	closers := make([]func(), nConns)
	counts := make([]*atomic.Int64, nConns)
	subIDs := make([]string, nConns)

	for i := 0; i < nConns; i++ {
		c, closer := dialTestWS(t, ws)
		conns[i] = c
		closers[i] = closer
		counts[i] = new(atomic.Int64)

		subReq := map[string]interface{}{
			"jsonrpc": "2.0",
			"id":      1,
			"method":  "subscribe",
			"params":  map[string]interface{}{},
		}
		if err := c.WriteJSON(subReq); err != nil {
			t.Fatalf("conn %d write subscribe: %v", i, err)
		}

		// Read the subscribe response (first JSON frame).
		var resp struct {
			Result subscribeResult `json:"result"`
			Error  *jsonRPCError   `json:"error"`
		}
		if err := c.ReadJSON(&resp); err != nil {
			t.Fatalf("conn %d read subscribe resp: %v", i, err)
		}
		if resp.Error != nil {
			t.Fatalf("conn %d subscribe error: %+v", i, resp.Error)
		}
		if resp.Result.SubscriptionID == "" {
			t.Fatalf("conn %d empty subscription_id", i)
		}
		if resp.Result.SafeHeight != 1234 {
			t.Fatalf("conn %d safe_height=%d, want 1234", i, resp.Result.SafeHeight)
		}
		subIDs[i] = resp.Result.SubscriptionID
	}
	defer func() {
		for _, closer := range closers {
			closer()
		}
	}()

	// Start one reader per connection that counts matching notifications.
	var wg sync.WaitGroup
	for i := 0; i < nConns; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c := conns[i]
			cnt := counts[i]
			c.SetReadDeadline(time.Now().Add(10 * time.Second))
			for cnt.Load() < int64(nMsgs) {
				var raw json.RawMessage
				if err := c.ReadJSON(&raw); err != nil {
					return
				}
				var env struct {
					Method string                 `json:"method"`
					Params eventNotificationParam `json:"params"`
				}
				if err := json.Unmarshal(raw, &env); err != nil {
					t.Errorf("conn %d unmarshal: %v", i, err)
					return
				}
				if env.Method != "event" {
					continue
				}
				if env.Params.SubscriptionID != subIDs[i] {
					t.Errorf("conn %d wrong sub id: got %s want %s",
						i, env.Params.SubscriptionID, subIDs[i])
					return
				}
				cnt.Add(1)
			}
		}(i)
	}

	// Tiny delay so the subscribe goroutines are all waiting on the bus
	// channel before we start publishing (otherwise the central ring can
	// outpace per-sub delivery registration and drop events we expected).
	time.Sleep(50 * time.Millisecond)

	for i := 0; i < nMsgs; i++ {
		bus.Publish(eventbus.Event{
			Type:   eventbus.EventInvoke,
			Height: int64(i),
			SCID:   "testscid",
		})
	}

	// Wait for readers to finish or time out.
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		// Let the assertions below surface per-conn counts.
	}

	for i, c := range counts {
		got := c.Load()
		if got != int64(nMsgs) {
			t.Errorf("conn %d got %d events, want %d", i, got, nMsgs)
		}
	}
}

// TestWSUnsubscribe_StopsEventDelivery verifies that after an unsubscribe
// response, further publishes produce no notifications for that sub.
func TestWSUnsubscribe_StopsEventDelivery(t *testing.T) {
	bus := eventbus.New(256)
	go bus.Run()
	defer bus.Close()

	var safe atomic.Int64
	ws := NewWSServer("", nil, &safe, bus, nil)
	c, closer := dialTestWS(t, ws)
	defer closer()

	// Subscribe.
	if err := c.WriteJSON(map[string]interface{}{
		"jsonrpc": "2.0", "id": 1, "method": "subscribe",
	}); err != nil {
		t.Fatal(err)
	}
	var subResp struct {
		Result subscribeResult `json:"result"`
	}
	if err := c.ReadJSON(&subResp); err != nil {
		t.Fatal(err)
	}
	subID := subResp.Result.SubscriptionID

	// Publish one event and drain it.
	bus.Publish(eventbus.Event{Type: eventbus.EventInvoke, Height: 1})
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	var first json.RawMessage
	if err := c.ReadJSON(&first); err != nil {
		t.Fatalf("read first event: %v", err)
	}

	// Unsubscribe.
	if err := c.WriteJSON(map[string]interface{}{
		"jsonrpc": "2.0", "id": 2, "method": "unsubscribe",
		"params": map[string]string{"subscription_id": subID},
	}); err != nil {
		t.Fatal(err)
	}
	var unsubResp struct {
		Result map[string]bool `json:"result"`
	}
	if err := c.ReadJSON(&unsubResp); err != nil {
		t.Fatal(err)
	}
	if !unsubResp.Result["ok"] {
		t.Fatalf("unsubscribe not ok: %+v", unsubResp)
	}

	// Give the bus a moment to propagate the cancel to its fan-out goroutine.
	time.Sleep(50 * time.Millisecond)

	// Publish more events; we should NOT receive them.
	for i := 0; i < 5; i++ {
		bus.Publish(eventbus.Event{Type: eventbus.EventInvoke, Height: int64(100 + i)})
	}
	bus.WaitForDrain(time.Second)

	c.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	var extra json.RawMessage
	if err := c.ReadJSON(&extra); err == nil {
		t.Fatalf("got unexpected frame after unsubscribe: %s", string(extra))
	}
}

// TestWSSubscribe_MaxSubsRejected validates the -32099 cap response when a
// connection asks for more than maxSubsPerConn subscriptions. Each request
// goes on a fresh JSON-RPC id so responses can be correlated positionally.
func TestWSSubscribe_MaxSubsRejected(t *testing.T) {
	bus := eventbus.New(256)
	go bus.Run()
	defer bus.Close()

	var safe atomic.Int64
	ws := NewWSServer("", nil, &safe, bus, nil)
	c, closer := dialTestWS(t, ws)
	defer closer()

	// Open exactly maxSubsPerConn subscriptions — all should succeed.
	for i := 0; i < maxSubsPerConn; i++ {
		if err := c.WriteJSON(map[string]interface{}{
			"jsonrpc": "2.0", "id": i + 1, "method": "subscribe",
		}); err != nil {
			t.Fatal(err)
		}
		var resp struct {
			Result *subscribeResult `json:"result"`
			Error  *jsonRPCError    `json:"error"`
		}
		if err := c.ReadJSON(&resp); err != nil {
			t.Fatal(err)
		}
		if resp.Error != nil {
			t.Fatalf("subscribe %d unexpectedly errored: %+v", i, resp.Error)
		}
	}

	// The (maxSubsPerConn+1)-th should fail with codeTooManySubs.
	if err := c.WriteJSON(map[string]interface{}{
		"jsonrpc": "2.0", "id": 9999, "method": "subscribe",
	}); err != nil {
		t.Fatal(err)
	}
	var resp struct {
		Error *jsonRPCError `json:"error"`
	}
	if err := c.ReadJSON(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error == nil || resp.Error.Code != codeTooManySubs {
		t.Fatalf("expected code %d, got %+v", codeTooManySubs, resp.Error)
	}
}
