// Command wstest is a minimal smoke-test client for the HyperGnomon
// WebSocket JSON-RPC server's subscribe method.
//
// Usage:
//
//	go run ./cmd/wstest                        # dial default ws://127.0.0.1:9190/ws, print 5 events
//	go run ./cmd/wstest -addr 10.0.0.5:9190    # custom server
//	go run ./cmd/wstest -events invoke,install # filter to specific event types
//	go run ./cmd/wstest -max 10                # stop after N events instead of 5
//
// Designed as a human-driven smoke test to verify the server's subscribe
// fan-out works against a live indexer. Not a test in the Go sense.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9190", "HyperGnomon WS server host:port")
	path := flag.String("path", "/ws", "WebSocket path")
	eventsCSV := flag.String("events", "", "Comma-separated event types to subscribe to (empty = all)")
	scidFilter := flag.String("scid", "", "Optional SCID filter")
	max := flag.Int("max", 5, "Number of events to print before exiting")
	timeoutSec := flag.Int("timeout", 120, "Total timeout in seconds before giving up")
	flag.Parse()

	u := url.URL{Scheme: "ws", Host: *addr, Path: *path}
	fmt.Fprintf(os.Stderr, "dialing %s ...\n", u.String())

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial failed: %v\n", err)
		os.Exit(1)
	}
	defer c.Close()

	// Build subscribe params.
	params := map[string]interface{}{}
	if *eventsCSV != "" {
		list := strings.Split(*eventsCSV, ",")
		for i := range list {
			list[i] = strings.TrimSpace(list[i])
		}
		params["events"] = list
	}
	if *scidFilter != "" {
		params["filters"] = map[string]interface{}{"scid": *scidFilter}
	}

	subReq := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "subscribe",
		"params":  params,
	}
	if err := c.WriteJSON(subReq); err != nil {
		fmt.Fprintf(os.Stderr, "send subscribe: %v\n", err)
		os.Exit(1)
	}

	// First frame is the subscribe response.
	var subResp struct {
		JSONRPC string `json:"jsonrpc"`
		ID      int    `json:"id"`
		Result  struct {
			SubscriptionID string `json:"subscription_id"`
			SafeHeight     int64  `json:"safe_height"`
		} `json:"result"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := c.ReadJSON(&subResp); err != nil {
		fmt.Fprintf(os.Stderr, "read subscribe resp: %v\n", err)
		os.Exit(1)
	}
	if subResp.Error != nil {
		fmt.Fprintf(os.Stderr, "subscribe error: %d %s\n",
			subResp.Error.Code, subResp.Error.Message)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "subscribed: id=%s safe_height=%d; waiting for %d events ...\n",
		subResp.Result.SubscriptionID, subResp.Result.SafeHeight, *max)

	c.SetReadDeadline(time.Now().Add(time.Duration(*timeoutSec) * time.Second))

	for n := 0; n < *max; {
		var raw json.RawMessage
		if err := c.ReadJSON(&raw); err != nil {
			fmt.Fprintf(os.Stderr, "read: %v\n", err)
			os.Exit(1)
		}
		var env struct {
			Method string `json:"method"`
		}
		if err := json.Unmarshal(raw, &env); err != nil {
			continue
		}
		if env.Method != "event" {
			// Stray response or future notification — ignore.
			continue
		}
		// Pretty-print the notification.
		var pretty map[string]interface{}
		if err := json.Unmarshal(raw, &pretty); err == nil {
			b, _ := json.MarshalIndent(pretty, "", "  ")
			fmt.Printf("--- event %d ---\n%s\n", n+1, b)
		} else {
			fmt.Printf("--- event %d ---\n%s\n", n+1, raw)
		}
		n++
	}

	fmt.Fprintln(os.Stderr, "done.")
}
