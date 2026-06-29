package api

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"

	"github.com/hypergnomon/hypergnomon/eventbus"
	"github.com/hypergnomon/hypergnomon/indexer"
	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

// _ references keep optional imports alive while other concurrent agents'
// handlers (listsc/listsc_byheight, subscribe/unsubscribe) are still landing.
var _ = sort.Strings

var logger = logrus.WithField("pkg", "api")

// JSON-RPC 2.0 wire types.
type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  interface{}     `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Standard JSON-RPC error codes.
const (
	codeParseError     = -32700
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
	codeInternalError  = -32603

	// codeTooManySubs is an application-level error returned when a
	// single WS connection exceeds maxSubsPerConn active subscriptions.
	codeTooManySubs = -32099

	// codeNotFound is an application-level error returned when a requested
	// entity (e.g. a SCID with no class meta) does not exist.
	codeNotFound = -32004

	// maxSubsPerConn caps the number of active subscriptions per
	// connection to bound memory and fan-out cost.
	maxSubsPerConn = 32

	// listSCDefaultLimit is the default page size for listsc when the
	// caller omits limit. 100 matches civilware/Gnomon behavior.
	listSCDefaultLimit = 100
	// listSCMaxLimit caps listsc / listsc_byheight page size to bound
	// response size and DB scan cost.
	listSCMaxLimit = 1000
)

// WSServer serves a JSON-RPC 2.0 API over WebSocket connections,
// providing read access to the indexed blockchain data.
type WSServer struct {
	store    storage.Storage
	upgrader websocket.Upgrader
	addr     string

	// safeHeight mirrors api.Server.safeHeight: a pointer to the indexer's
	// atomic.Int64 so the ws GetSafeHeight method returns a live value
	// without this package importing indexer. nil is tolerated (returns 0).
	safeHeight *atomic.Int64

	// bus is the in-process event fan-out used by subscribe/unsubscribe.
	// nil is tolerated (subscribe returns codeInternalError).
	bus *eventbus.Bus

	// idx is the running Indexer. Needed by methods that mutate index state
	// (addscid_toindex) or that must reach through the RPC pool. nil is
	// tolerated — those methods return codeInternalError if missing.
	idx *indexer.Indexer

	// dispatch table: method name -> handler func
	methods map[string]methodHandler
}

// methodHandler processes a JSON-RPC request and returns a result or error.
// The connCtx argument is non-nil for stateful methods (subscribe/unsubscribe)
// and nil-tolerant for stateless ones — handlers that don't need it ignore it.
type methodHandler func(connCtx *connContext, params json.RawMessage) (interface{}, *jsonRPCError)

// NewWSServer creates a WSServer bound to the given address.
// The storage backend must already be open.
//
// safeHeight may be nil; GetSafeHeight returns 0 in that case. Passing a
// pointer to indexer.Indexer.SafeHeight gives live finality reads without
// pulling an indexer import into this package.
//
// bus may be nil; subscribe/unsubscribe return an internal error in that
// case. Wiring the indexer's bus enables live event fan-out to clients.
//
// idx may be nil; methods that need it (addscid_toindex) return an internal
// error when absent. Passing the live Indexer enables on-demand SCID import.
func NewWSServer(addr string, store storage.Storage, safeHeight *atomic.Int64, bus *eventbus.Bus, idx *indexer.Indexer) *WSServer {
	ws := &WSServer{
		store:      store,
		addr:       addr,
		safeHeight: safeHeight,
		bus:        bus,
		idx:        idx,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
	}
	ws.methods = map[string]methodHandler{
		"GetAllOwnersAndSCIDs":               ws.handleGetAllOwnersAndSCIDs,
		"GetAllSCIDs":                        ws.handleGetAllSCIDs,
		"GetSCIDVariableDetailsAtTopoheight": ws.handleGetSCIDVariableDetailsAtTopoheight,
		"GetSCIDInteractionHeight":           ws.handleGetSCIDInteractionHeight,
		"GetAllSCIDInvokeDetails":            ws.handleGetAllSCIDInvokeDetails,
		"GetSafeHeight":                      ws.handleGetSafeHeight,
		"subscribe":                          ws.handleSubscribe,
		"unsubscribe":                        ws.handleUnsubscribe,

		// listsc_* family: breadth helpers clients expect when replacing
		// civilware/Gnomon with HyperGnomon. See DESIGN.md route B.
		"listsc":           ws.handleListSC,
		"listsc_byheight":  ws.handleListSCByHeight,
		"listsc_byclass":   ws.handleListSCByClass,
		"listsc_variables": ws.handleListSCVariables,
		"listsc_hardcoded": ws.handleListSCHardcoded,
		"listsc_ratings":   ws.handleListSCRatings,

		// addscid_toindex: on-demand import of a single SCID. Useful for
		// contracts not registered in GnomonSC. Port of civilware/Gnomon.
		"addscid_toindex": ws.handleAddSCIDToIndex,

		// GetInitialSCIDCode: install-time code for scid. Drop-in compat
		// with simple-gnomon's method of the same name. Reads from the
		// sccode bucket with lazy-backfill via the RPC pool for SCIDs
		// indexed before the bucket existed.
		"GetInitialSCIDCode": ws.handleGetInitialSCIDCode,

		// validatesc: diagnostic — run ClassifySC + header/TELA-field
		// extraction on a single scid and return the full class shape.
		// Useful for operators debugging why a contract is/isn't
		// classified as expected.
		"validatesc": ws.handleValidateSC,

		// listsc_byowner: SCIDs installed by a given wallet address.
		// Reads the reverse owner→scid index populated at install time.
		"listsc_byowner": ws.handleListSCByOwner,

		// getscidlist_byaddr: alias of listsc_byowner for civilware naming
		// compatibility (both spellings appear in upstream client code).
		"getscidlist_byaddr": ws.handleListSCByOwner,
	}
	return ws
}

// loadSafeHeight returns the live safe-height, or 0 if not wired.
func (ws *WSServer) loadSafeHeight() int64 {
	if ws.safeHeight == nil {
		return 0
	}
	return ws.safeHeight.Load()
}

// Start begins listening for WebSocket connections. Blocks until the
// listener is closed or an unrecoverable error occurs.
func (ws *WSServer) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", ws.ServeWS)

	ln, err := net.Listen("tcp", ws.addr)
	if err != nil {
		return err
	}
	logger.Infof("WS JSON-RPC listening on %s/ws", ws.addr)
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return srv.Serve(ln)
}

// ServeWS is the http.HandlerFunc for the /ws endpoint.
// Attach it to any external mux: mux.HandleFunc("/ws", server.ServeWS)
func (ws *WSServer) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := ws.upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Errorf("websocket upgrade: %v", err)
		return
	}
	go ws.handleConn(conn)
}

// connContext is the per-connection state shared between the read loop,
// the dispatch layer, and each subscription forwarder goroutine.
//
// Write serialization: writeMu ensures a subscription forwarder's
// notification bytes never interleave with a response or another
// forwarder's notification. All conn.WriteJSON calls for a single
// connection go through this mutex.
//
// Subscription tracking: subsMu guards subs (id -> cancel func). Both the
// dispatch goroutine (subscribe/unsubscribe) and the close path
// (cancelAllSubs) touch subs, so mutation must be under the lock.
type connContext struct {
	conn *websocket.Conn

	writeMu sync.Mutex

	subsMu sync.Mutex
	subs   map[string]func()

	// closeOnce ensures cancelAllSubs runs exactly once, whether triggered
	// by the read loop's defer or a forwarder that saw a write error.
	closeOnce sync.Once

	// done is closed when the connection is torn down. Forwarder goroutines
	// select on it so they exit promptly during shutdown.
	done chan struct{}

	// encBuf + enc are a per-connection reused JSON encoder. safeWrite resets
	// encBuf, encodes v into it (preserving json.Encoder's trailing newline and
	// default HTML escaping), then ships the bytes via WriteMessage — which hits
	// gorilla's alloc-free server fast path when no per-message compression is
	// negotiated (the Upgrader sets none). Both are touched only under writeMu.
	// Because the encoder writes into a bytes.Buffer (whose Write never errors),
	// json.Encoder's sticky error never latches, so lifetime reuse is safe.
	encBuf *bytes.Buffer
	enc    *json.Encoder
}

// safeWrite serializes writes through writeMu. Returns the write error,
// if any; callers typically treat an error as fatal for the connection.
func (c *connContext) safeWrite(v interface{}) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.encBuf.Reset()
	if err := c.enc.Encode(v); err != nil {
		return err
	}
	return c.conn.WriteMessage(websocket.TextMessage, c.encBuf.Bytes())
}

// addSub registers a cancel func under id. Returns false if the cap is hit.
// The caller must not hold subsMu.
func (c *connContext) addSub(id string, cancel func()) bool {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	if len(c.subs) >= maxSubsPerConn {
		return false
	}
	c.subs[id] = cancel
	return true
}

// removeSub looks up a cancel func by id and removes it from the map.
// Returns the cancel func (or nil if unknown). The caller is responsible
// for invoking it; this keeps the lock hold brief.
func (c *connContext) removeSub(id string) func() {
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	cancel, ok := c.subs[id]
	if !ok {
		return nil
	}
	delete(c.subs, id)
	return cancel
}

// cancelAllSubs cancels every outstanding subscription for this connection.
// Idempotent via closeOnce. Safe to call from any goroutine.
func (c *connContext) cancelAllSubs() {
	c.closeOnce.Do(func() {
		c.subsMu.Lock()
		cancels := make([]func(), 0, len(c.subs))
		for _, fn := range c.subs {
			cancels = append(cancels, fn)
		}
		c.subs = nil
		c.subsMu.Unlock()
		close(c.done)
		for _, fn := range cancels {
			fn()
		}
	})
}

// newConnContext builds a per-connection context with an initialized reused
// JSON encoder. handleConn and the write-path tests both use it so the encoder
// is constructed identically in production and under test.
func newConnContext(conn *websocket.Conn) *connContext {
	buf := new(bytes.Buffer)
	return &connContext{
		conn:   conn,
		subs:   make(map[string]func()),
		done:   make(chan struct{}),
		encBuf: buf,
		enc:    json.NewEncoder(buf),
	}
}

// handleConn reads JSON-RPC requests from a single WebSocket connection
// until it closes or errors. Each request is dispatched and answered
// sequentially per connection — no head-of-line blocking across connections.
func (ws *WSServer) handleConn(conn *websocket.Conn) {
	cctx := newConnContext(conn)
	defer func() {
		cctx.cancelAllSubs()
		_ = conn.Close() // best-effort teardown; read loop already exited
	}()

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				logger.Debugf("ws read: %v", err)
			}
			return
		}

		resp := ws.dispatch(cctx, msg)

		if err := cctx.safeWrite(resp); err != nil {
			logger.Debugf("ws write: %v", err)
			return
		}
	}
}

// dispatch parses a raw JSON-RPC message and routes it to the correct handler.
func (ws *WSServer) dispatch(cctx *connContext, msg []byte) jsonRPCResponse {
	var req jsonRPCRequest
	if err := json.Unmarshal(msg, &req); err != nil {
		return jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      nil,
			Error:   &jsonRPCError{Code: codeParseError, Message: "parse error"},
		}
	}

	if req.Method == "" {
		return errorResponse(req.ID, codeInvalidRequest, "missing method")
	}

	handler, ok := ws.methods[req.Method]
	if !ok {
		return errorResponse(req.ID, codeMethodNotFound, "method not found: "+req.Method)
	}

	result, rpcErr := handler(cctx, req.Params)
	if rpcErr != nil {
		return errorResponse(req.ID, rpcErr.Code, rpcErr.Message)
	}

	return jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result:  result,
	}
}

// ---------------------------------------------------------------------------
// Method handlers
// ---------------------------------------------------------------------------

func (ws *WSServer) handleGetAllOwnersAndSCIDs(_ *connContext, _ json.RawMessage) (interface{}, *jsonRPCError) {
	owners, err := ws.store.GetAllOwnersAndSCIDs()
	if err != nil {
		return nil, internalErr(err)
	}
	return owners, nil
}

func (ws *WSServer) handleGetAllSCIDs(_ *connContext, _ json.RawMessage) (interface{}, *jsonRPCError) {
	scids, err := ws.store.GetAllSCIDs()
	if err != nil {
		return nil, internalErr(err)
	}
	return scids, nil
}

// scidHeightParams is shared by methods that take {scid, height}.
type scidHeightParams struct {
	SCID   string `json:"scid"`
	Height int64  `json:"height"`
}

// scidParams is shared by methods that take {scid}.
type scidParams struct {
	SCID string `json:"scid"`
}

func (ws *WSServer) handleGetSCIDVariableDetailsAtTopoheight(_ *connContext, raw json.RawMessage) (interface{}, *jsonRPCError) {
	var p scidHeightParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "invalid params: " + err.Error()}
	}
	if p.SCID == "" {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "missing scid"}
	}

	vars, err := ws.store.GetSCIDVariableDetailsAtHeight(p.SCID, p.Height)
	if err != nil {
		return nil, internalErr(err)
	}
	return vars, nil
}

func (ws *WSServer) handleGetSCIDInteractionHeight(_ *connContext, raw json.RawMessage) (interface{}, *jsonRPCError) {
	var p scidParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "invalid params: " + err.Error()}
	}
	if p.SCID == "" {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "missing scid"}
	}

	heights, err := ws.store.GetSCIDInteractionHeights(p.SCID)
	if err != nil {
		return nil, internalErr(err)
	}
	return heights, nil
}

// handleGetSafeHeight returns the current finality-lag "safe" height as
// {"safe_height": N}. Zero-param method; clients poll it to know the
// greatest height they can trust past reorg risk.
func (ws *WSServer) handleGetSafeHeight(_ *connContext, _ json.RawMessage) (interface{}, *jsonRPCError) {
	return map[string]int64{"safe_height": ws.loadSafeHeight()}, nil
}

func (ws *WSServer) handleGetAllSCIDInvokeDetails(_ *connContext, raw json.RawMessage) (interface{}, *jsonRPCError) {
	var p scidParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "invalid params: " + err.Error()}
	}
	if p.SCID == "" {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "missing scid"}
	}

	details, err := ws.store.GetInvokeDetailsBySCID(p.SCID)
	if err != nil {
		return nil, internalErr(err)
	}
	return details, nil
}

// ---------------------------------------------------------------------------
// Subscription methods (live-only for M1 — backfill from_height is M2+).
// ---------------------------------------------------------------------------

// subscribeParams describes the subscribe request payload. Events is a list
// of wire-level event-type names; unknown names are skipped silently. Filters
// holds the scalar AND-filters (scid/owner/sender/class). IncludeSpeculative
// opts into mempool-derived events once they start publishing (M6); the bus
// filter drops speculative events for subscribers who didn't opt in.
type subscribeParams struct {
	Events             []string               `json:"events"`
	Filters            map[string]interface{} `json:"filters"`
	IncludeSpeculative bool                   `json:"include_speculative,omitempty"`
}

// subscribeResult is returned to the subscribe caller. safe_height gives
// the client a synchronization point it can use to decide whether to
// backfill from historical APIs before trusting the live stream.
type subscribeResult struct {
	SubscriptionID string `json:"subscription_id"`
	SafeHeight     int64  `json:"safe_height"`
}

// eventNotification is the JSON-RPC 2.0 notification (no id) sent for each
// matched event. Method is always "event"; subscription_id in Params lets
// clients demux multiple subs on one connection.
type eventNotification struct {
	JSONRPC string                 `json:"jsonrpc"`
	Method  string                 `json:"method"`
	Params  eventNotificationParam `json:"params"`
}

type eventNotificationParam struct {
	SubscriptionID string      `json:"subscription_id"`
	Type           string      `json:"type"`
	Height         int64       `json:"height"`
	SafeHeight     int64       `json:"safe_height"`
	SCID           string      `json:"scid,omitempty"`
	Class          string      `json:"class,omitempty"`
	Tags           []string    `json:"tags,omitempty"`
	Owner          string      `json:"owner,omitempty"`
	Sender         string      `json:"sender,omitempty"`
	Entrypoint     string      `json:"entrypoint,omitempty"`
	Speculative    bool        `json:"speculative,omitempty"`
	Payload        interface{} `json:"payload,omitempty"`
}

// handleSubscribe registers a new bus subscription on the connection and
// launches a forwarder goroutine that serializes matched events as JSON-RPC
// notifications over the same socket. All writes go through
// connContext.safeWrite so notifications never interleave with responses.
func (ws *WSServer) handleSubscribe(cctx *connContext, raw json.RawMessage) (interface{}, *jsonRPCError) {
	if ws.bus == nil {
		return nil, &jsonRPCError{Code: codeInternalError, Message: "event bus not configured"}
	}
	if cctx == nil {
		// Defensive: dispatch always passes cctx for real connections.
		return nil, &jsonRPCError{Code: codeInternalError, Message: "no connection context"}
	}

	var p subscribeParams
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, &jsonRPCError{Code: codeInvalidParams, Message: "invalid params: " + err.Error()}
		}
	}

	filter := buildFilter(p)
	id, ch, cancel := ws.bus.Subscribe(filter)

	if !cctx.addSub(id, cancel) {
		// Cap hit — undo the bus-side registration so we don't leak.
		cancel()
		return nil, &jsonRPCError{Code: codeTooManySubs, Message: "too many subscriptions"}
	}

	go ws.forwardEvents(cctx, id, ch)

	return subscribeResult{SubscriptionID: id, SafeHeight: ws.loadSafeHeight()}, nil
}

// buildFilter converts wire-level subscribe params into an eventbus.Filter.
// Unknown event names are silently dropped (ParseEventType returns 0).
// Filter fields come from a generic map so the subscribe wire-schema can
// accept missing or explicit-empty keys without imposing a rigid struct on
// clients.
func buildFilter(p subscribeParams) eventbus.Filter {
	f := eventbus.Filter{IncludeSpeculative: p.IncludeSpeculative}
	if len(p.Events) > 0 {
		f.Events = make(map[eventbus.EventType]struct{}, len(p.Events))
		for _, name := range p.Events {
			t := eventbus.ParseEventType(name)
			if t == 0 {
				continue
			}
			f.Events[t] = struct{}{}
		}
	}
	if s, ok := stringField(p.Filters, "scid"); ok {
		f.SCID = s
	}
	if s, ok := stringField(p.Filters, "owner"); ok {
		f.Owner = s
	}
	if s, ok := stringField(p.Filters, "sender"); ok {
		f.Sender = s
	}
	if s, ok := stringField(p.Filters, "class"); ok {
		f.Class = s
	}
	return f
}

// stringField extracts a string value from a generic filter map, tolerating
// non-string or absent entries by returning ("", false).
func stringField(m map[string]interface{}, key string) (string, bool) {
	if m == nil {
		return "", false
	}
	v, ok := m[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	if !ok {
		return "", false
	}
	return s, true
}

// forwardEvents drains a subscription channel and writes each event as a
// JSON-RPC notification on the connection. Exits when:
//   - the events channel is closed (bus canceled the sub), or
//   - the connection's done channel closes (shutdown), or
//   - safeWrite errors (peer is gone — tear down the whole connection).
func (ws *WSServer) forwardEvents(cctx *connContext, id string, ch <-chan eventbus.Event) {
	// Hoist the notification struct above the loop: safeWrite serializes it
	// synchronously (encodes fully before returning) and this forwarder
	// goroutine is the only writer of notif, so a single per-sub instance is
	// reused. Passing the POINTER avoids boxing the ~190B value into interface{}
	// every event (json of *T == T). ALL Params event fields are reassigned each
	// iteration so no value from a prior event leaks through an omitempty field.
	notif := &eventNotification{JSONRPC: "2.0", Method: "event"}
	notif.Params.SubscriptionID = id
	for {
		select {
		case <-cctx.done:
			return
		case e, ok := <-ch:
			if !ok {
				return
			}
			notif.Params.Type = e.Type.String()
			notif.Params.Height = e.Height
			notif.Params.SafeHeight = e.SafeHeight
			notif.Params.SCID = e.SCID
			notif.Params.Class = e.Class
			notif.Params.Tags = e.Tags
			notif.Params.Owner = e.Owner
			notif.Params.Sender = e.Sender
			notif.Params.Entrypoint = e.Entrypoint
			notif.Params.Speculative = e.Speculative
			notif.Params.Payload = e.Payload
			if err := cctx.safeWrite(notif); err != nil {
				logger.Debugf("ws event write (sub=%s): %v", id, err)
				// Peer is gone — cancel all subs on this connection so the
				// read loop's next ReadMessage returns and handleConn's
				// defer runs. cancelAllSubs is idempotent.
				cctx.cancelAllSubs()
				return
			}
		}
	}
}

// unsubscribeParams is the unsubscribe request payload.
type unsubscribeParams struct {
	SubscriptionID string `json:"subscription_id"`
}

// handleUnsubscribe cancels a single subscription. Returns ok=true whether
// or not the subscription existed — clients may retry unsubscribe during a
// reconnect and shouldn't see a spurious error. The subscription's cancel
// func closes its bus channel, which makes the forwarder goroutine exit.
func (ws *WSServer) handleUnsubscribe(cctx *connContext, raw json.RawMessage) (interface{}, *jsonRPCError) {
	if cctx == nil {
		return nil, &jsonRPCError{Code: codeInternalError, Message: "no connection context"}
	}
	var p unsubscribeParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "invalid params: " + err.Error()}
	}
	if p.SubscriptionID == "" {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "missing subscription_id"}
	}
	if cancel := cctx.removeSub(p.SubscriptionID); cancel != nil {
		cancel()
	}
	return map[string]bool{"ok": true}, nil
}

// ---------------------------------------------------------------------------
// listsc_* method family
//
// Mirrors the civilware/Gnomon WS surface used by TELA, DeroPay, Commando, and
// other consumers. All five methods return a uniform paginated shape where
// applicable so clients can switch between them with one row renderer.
// ---------------------------------------------------------------------------

// listSCResult is the row shape returned by listsc, listsc_byheight, and
// listsc_byclass. Fields are fixed-order and non-omitempty so the JSON
// layout is deterministic for caching and snapshot tests.
type listSCResult struct {
	SCID          string `json:"scid"`
	Owner         string `json:"owner"`
	Class         string `json:"class"`
	InstallHeight int64  `json:"install_height"`
	Name          string `json:"name"`
}

// listSCResponse wraps a page of listSCResult with count + cursor metadata.
// count is the total number of results available before paging (so clients
// can render "showing 100 of 1234"); results is the current page.
type listSCResponse struct {
	Count   int            `json:"count"`
	Offset  int            `json:"offset"`
	Limit   int            `json:"limit"`
	Results []listSCResult `json:"results"`
}

// listSCParams covers listsc's three optional knobs. Class filter is honored
// via the class index (O(class_size)) when set; otherwise we scan all SCIDs.
type listSCParams struct {
	Offset int    `json:"offset"`
	Limit  int    `json:"limit"`
	Class  string `json:"class"`
}

// handleListSC returns a page of all SCIDs with owner + class enrichment.
// With class set, the class-index prefix scan gives height-ascending order
// for free. Without class, we iterate the owner map and sort by SCID so
// pages are stable across calls (Go map iteration is randomized).
func (ws *WSServer) handleListSC(_ *connContext, raw json.RawMessage) (interface{}, *jsonRPCError) {
	var p listSCParams
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &p); err != nil {
			return nil, &jsonRPCError{Code: codeInvalidParams, Message: "invalid params: " + err.Error()}
		}
	}
	if p.Offset < 0 {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "offset must be >= 0"}
	}
	limit := clampListLimit(p.Limit, listSCDefaultLimit)

	// Class-filtered path: one prefix scan, then slice.
	if p.Class != "" {
		installs, err := ws.store.GetClassInstalls(p.Class, 0)
		if err != nil {
			return nil, internalErr(err)
		}
		total := len(installs)

		// Owners lookup only needed for the window we return. Do it once
		// and index by SCID so we don't re-query per row.
		win := sliceWindow(total, p.Offset, limit)
		windowSCIDs := make([]string, 0, win.end-win.start)
		for i := win.start; i < win.end; i++ {
			windowSCIDs = append(windowSCIDs, installs[i].SCID)
		}
		owners, err := ws.store.GetOwnersForSCIDs(windowSCIDs)
		if err != nil {
			return nil, internalErr(err)
		}

		results := make([]listSCResult, 0, win.end-win.start)
		for i := win.start; i < win.end; i++ {
			inst := installs[i]
			row := listSCResult{
				SCID:          inst.SCID,
				Owner:         owners[inst.SCID],
				InstallHeight: inst.InstallHeight,
			}
			if inst.Meta != nil {
				row.Class = inst.Meta.Class
				row.Name = inst.Meta.Name
			}
			results = append(results, row)
		}
		return listSCResponse{
			Count:   total,
			Offset:  p.Offset,
			Limit:   limit,
			Results: results,
		}, nil
	}

	// Unfiltered path: iterate the owner map. Map ordering is non-deterministic
	// in Go, so we collect SCIDs into a sorted slice first for stable paging.
	owners, err := ws.store.GetAllOwnersAndSCIDs()
	if err != nil {
		return nil, internalErr(err)
	}
	scids := make([]string, 0, len(owners))
	for scid := range owners {
		scids = append(scids, scid)
	}
	sort.Strings(scids)

	total := len(scids)
	win := sliceWindow(total, p.Offset, limit)
	results := make([]listSCResult, 0, win.end-win.start)
	for i := win.start; i < win.end; i++ {
		scid := scids[i]
		row := listSCResult{
			SCID:  scid,
			Owner: owners[scid],
		}
		meta, err := ws.store.GetSCIDClass(scid)
		if err != nil {
			return nil, internalErr(err)
		}
		if meta != nil {
			row.Class = meta.Class
			row.Name = meta.Name
			row.InstallHeight = meta.InstallHeight
		}
		results = append(results, row)
	}
	return listSCResponse{
		Count:   total,
		Offset:  p.Offset,
		Limit:   limit,
		Results: results,
	}, nil
}

// listSCByHeightParams bounds the height window. We require to_height >
// from_height because the storage layer's [from, to) semantics would
// silently return zero rows otherwise — a quiet footgun we surface.
type listSCByHeightParams struct {
	FromHeight int64 `json:"from_height"`
	ToHeight   int64 `json:"to_height"`
	Limit      int   `json:"limit"`
}

// handleListSCByHeight returns installs in the half-open range [from, to).
// Uses the installs bucket's prefix scan so ordering is height-ascending.
func (ws *WSServer) handleListSCByHeight(_ *connContext, raw json.RawMessage) (interface{}, *jsonRPCError) {
	var p listSCByHeightParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "invalid params: " + err.Error()}
	}
	if p.FromHeight < 0 || p.ToHeight < 0 {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "heights must be >= 0"}
	}
	if p.ToHeight <= p.FromHeight {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "to_height must be > from_height"}
	}
	limit := clampListLimit(p.Limit, listSCDefaultLimit)

	installs, err := ws.store.GetInstallsInRange(p.FromHeight, p.ToHeight, limit)
	if err != nil {
		return nil, internalErr(err)
	}
	scids := make([]string, 0, len(installs))
	for _, inst := range installs {
		scids = append(scids, inst.SCID)
	}
	owners, err := ws.store.GetOwnersForSCIDs(scids)
	if err != nil {
		return nil, internalErr(err)
	}
	results := make([]listSCResult, 0, len(installs))
	for _, inst := range installs {
		row := listSCResult{
			SCID:          inst.SCID,
			Owner:         owners[inst.SCID],
			InstallHeight: inst.InstallHeight,
		}
		if inst.Meta != nil {
			row.Class = inst.Meta.Class
			row.Name = inst.Meta.Name
		}
		results = append(results, row)
	}
	return listSCResponse{
		Count:   len(results),
		Offset:  0,
		Limit:   limit,
		Results: results,
	}, nil
}

// listSCByClassParams takes a mandatory class and an optional limit. Limit 0
// passes through to GetClassInstalls as "no limit" — callers that genuinely
// want everything (e.g., TELA enumerating all INDEX-1 docs) rely on this.
type listSCByClassParams struct {
	Class string `json:"class"`
	Limit int    `json:"limit"`
}

// handleListSCByClass returns every SCID of a given class, height-ascending.
// Limit 0 means unbounded; any positive value is clamped to listSCMaxLimit.
func (ws *WSServer) handleListSCByClass(_ *connContext, raw json.RawMessage) (interface{}, *jsonRPCError) {
	var p listSCByClassParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "invalid params: " + err.Error()}
	}
	if p.Class == "" {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "missing class"}
	}
	if p.Limit < 0 {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "limit must be >= 0"}
	}

	var limitForStore int
	switch {
	case p.Limit == 0:
		limitForStore = 0 // unbounded per spec
	case p.Limit > listSCMaxLimit:
		limitForStore = listSCMaxLimit
	default:
		limitForStore = p.Limit
	}

	installs, err := ws.store.GetClassInstalls(p.Class, limitForStore)
	if err != nil {
		return nil, internalErr(err)
	}
	scids := make([]string, 0, len(installs))
	for _, inst := range installs {
		scids = append(scids, inst.SCID)
	}
	owners, err := ws.store.GetOwnersForSCIDs(scids)
	if err != nil {
		return nil, internalErr(err)
	}

	results := make([]listSCResult, 0, len(installs))
	for _, inst := range installs {
		row := listSCResult{
			SCID:          inst.SCID,
			Owner:         owners[inst.SCID],
			InstallHeight: inst.InstallHeight,
		}
		if inst.Meta != nil {
			row.Class = inst.Meta.Class
			row.Name = inst.Meta.Name
		}
		results = append(results, row)
	}
	return listSCResponse{
		Count:   len(results),
		Offset:  0,
		Limit:   limitForStore,
		Results: results,
	}, nil
}

// listSCVariablesParams selects a point-in-time view of a SCID's state.
// Height 0 means "latest known" and is resolved via GetSCIDClass.LastHeight.
type listSCVariablesParams struct {
	SCID   string `json:"scid"`
	Height int64  `json:"height"`
}

// listSCVariablesRow normalizes one state var. Key/Value are interface{} in
// storage to tolerate DVM's mixed U64/String shapes; we pass them through
// unchanged and let the JSON encoder render them.
type listSCVariablesRow struct {
	Key   interface{} `json:"key"`
	Value interface{} `json:"value"`
}

// listSCVariablesResponse echoes the requested scid + resolved height so
// callers that passed height=0 can tell what "latest" meant at query time.
type listSCVariablesResponse struct {
	SCID      string               `json:"scid"`
	Height    int64                `json:"height"`
	Variables []listSCVariablesRow `json:"variables"`
}

// handleListSCVariables dumps a SCID's variables at the given height. If the
// caller passes height<=0, we resolve it to ClassMeta.LastHeight so clients
// don't need a second round-trip to learn the top height.
func (ws *WSServer) handleListSCVariables(_ *connContext, raw json.RawMessage) (interface{}, *jsonRPCError) {
	var p listSCVariablesParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "invalid params: " + err.Error()}
	}
	if !isValidSCID(p.SCID) {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "invalid scid"}
	}

	height := p.Height
	if height <= 0 {
		meta, err := ws.store.GetSCIDClass(p.SCID)
		if err != nil {
			return nil, internalErr(err)
		}
		if meta == nil {
			return nil, &jsonRPCError{Code: codeNotFound, Message: "scid not found"}
		}
		height = meta.LastHeight
	}

	vars, err := ws.store.GetSCIDVariableDetailsAtHeight(p.SCID, height)
	if err != nil {
		return nil, internalErr(err)
	}

	rows := make([]listSCVariablesRow, 0, len(vars))
	for _, v := range vars {
		if v == nil {
			continue
		}
		rows = append(rows, listSCVariablesRow{Key: v.Key, Value: v.Value})
	}
	return listSCVariablesResponse{
		SCID:      p.SCID,
		Height:    height,
		Variables: rows,
	}, nil
}

// listSCRatingsParams selects the SCID and (optional) snapshot height. height
// <= 0 resolves to the latest snapshot — same semantics as listsc_variables.
type listSCRatingsParams struct {
	SCID   string `json:"scid"`
	Height int64  `json:"height,omitempty"`
}

// listSCRatingsResponse is the wire shape for listsc_ratings. Mirrors the
// REST /api/tela/{scid}/ratings envelope so clients can share code.
type listSCRatingsResponse struct {
	SCID    string              `json:"scid"`
	Height  int64               `json:"height"`
	Ratings []structures.Rating `json:"ratings"`
	Count   int                 `json:"count"`
	Avg     float64             `json:"avg"`
}

// handleListSCRatings returns TELA ratings for the given SCID as stored in
// scvars. Intended for TELA app UIs that want inline rating display without
// issuing a second REST call.
func (ws *WSServer) handleListSCRatings(_ *connContext, raw json.RawMessage) (interface{}, *jsonRPCError) {
	var p listSCRatingsParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "invalid params: " + err.Error()}
	}
	if !isValidSCID(p.SCID) {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "invalid scid"}
	}

	ratings, err := ws.store.GetRatingsForSCID(p.SCID, p.Height)
	if err != nil {
		return nil, internalErr(err)
	}
	if ratings == nil {
		ratings = []structures.Rating{}
	}
	height := p.Height
	if height <= 0 && len(ratings) > 0 {
		height = ratings[0].Height
	}
	var sum float64
	for _, r := range ratings {
		sum += r.Score
	}
	var avg float64
	if len(ratings) > 0 {
		avg = sum / float64(len(ratings))
	}
	return listSCRatingsResponse{
		SCID:    p.SCID,
		Height:  height,
		Ratings: ratings,
		Count:   len(ratings),
		Avg:     avg,
	}, nil
}

// hardcodedSCIDEntry is one well-known SCID with a human label.
type hardcodedSCIDEntry struct {
	SCID string `json:"scid"`
	Name string `json:"name"`
}

// hardcodedSCIDResponse is the envelope for listsc_hardcoded; wrapping in an
// object rather than a bare array leaves room to add fields (e.g., network
// tag) without a breaking change.
type hardcodedSCIDResponse struct {
	Hardcoded []hardcodedSCIDEntry `json:"hardcoded"`
}

// handleListSCHardcoded returns the protocol-reserved SCIDs: NameService and
// both Gnomon registry addresses. Stable output — wallets use this to detect
// which network a node is on by probing which Gnomon is populated.
func (ws *WSServer) handleListSCHardcoded(_ *connContext, _ json.RawMessage) (interface{}, *jsonRPCError) {
	return hardcodedSCIDResponse{
		Hardcoded: []hardcodedSCIDEntry{
			{SCID: structures.NameServiceSCID, Name: "NameService"},
			{SCID: structures.GnomonSCID_Mainnet, Name: "GnomonSC (mainnet)"},
			{SCID: structures.GnomonSCID_Testnet, Name: "GnomonSC (testnet)"},
		},
	}, nil
}

// clampListLimit normalizes a caller-supplied limit into [1, listSCMaxLimit].
// A zero or negative value yields def (the method's default). Any value over
// the cap is silently clamped — callers asking for 10k get 1000 rather than
// an error, matching civilware/Gnomon behavior.
func clampListLimit(limit, def int) int {
	if limit <= 0 {
		return def
	}
	if limit > listSCMaxLimit {
		return listSCMaxLimit
	}
	return limit
}

// window is a half-open [start, end) slice bound, already clamped to total.
// Used by handleListSC's class-filtered path to page through a fully
// materialized slice without risk of out-of-bounds.
type window struct{ start, end int }

// sliceWindow clamps offset/limit against total and returns a safe window.
// Offset past total collapses to empty; limit<=0 is treated as empty so
// callers should pass their own default before calling.
func sliceWindow(total, offset, limit int) window {
	if offset < 0 {
		offset = 0
	}
	if offset > total {
		offset = total
	}
	end := offset + limit
	if limit <= 0 {
		end = offset
	}
	if end > total {
		end = total
	}
	return window{start: offset, end: end}
}

// ---------------------------------------------------------------------------
// addscid_toindex
// ---------------------------------------------------------------------------

// addSCIDToIndexParams accepts the civilware-compatible shape.
// varsonly=true asks for variables without SC code (classifier generally
// returns UNKNOWN, since the rules need code). skipfsrecheck=true returns
// cached class metadata without hitting the daemon if the SCID is already
// known; otherwise we always refresh.
type addSCIDToIndexParams struct {
	SCID          string `json:"scid"`
	VarsOnly      bool   `json:"varsonly"`
	SkipFSRecheck bool   `json:"skipfsrecheck"`
}

// addSCIDToIndexResult is the JSON-RPC response shape. Mirrors ClassMeta
// plus the SCID and a vars_count tally so callers can sanity-check.
type addSCIDToIndexResult struct {
	SCID          string   `json:"scid"`
	Class         string   `json:"class"`
	Tags          []string `json:"tags"`
	Name          string   `json:"name,omitempty"`
	Description   string   `json:"description,omitempty"`
	IconURL       string   `json:"icon_url,omitempty"`
	InstallHeight int64    `json:"install_height"`
	LastHeight    int64    `json:"last_height"`
	VarsCount     int      `json:"vars_count"`
	FromCache     bool     `json:"from_cache,omitempty"`
}

// handleAddSCIDToIndex imports a specific SCID on demand. Useful for SCs
// that aren't in the GnomonSC registry (HyperGnomon's usual discovery).
// Returns -32602 for a malformed SCID and -32004 when GetSC says the SC
// doesn't exist on-chain.
func (ws *WSServer) handleAddSCIDToIndex(_ *connContext, raw json.RawMessage) (interface{}, *jsonRPCError) {
	var p addSCIDToIndexParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "invalid params: " + err.Error()}
	}
	if !isValidSCID(p.SCID) {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "invalid scid"}
	}

	if ws.idx == nil {
		logger.Error("addscid_toindex: indexer not wired")
		return nil, &jsonRPCError{Code: codeInternalError, Message: "internal error"}
	}

	res, err := ws.idx.IndexSingleSCID(p.SCID, p.VarsOnly, p.SkipFSRecheck)
	if err != nil {
		if errors.Is(err, indexer.ErrSCIDNotFound) {
			return nil, &jsonRPCError{Code: codeNotFound, Message: "scid not found"}
		}
		logger.Errorf("addscid_toindex(%s): %v", p.SCID, err)
		return nil, &jsonRPCError{Code: codeInternalError, Message: "internal error"}
	}

	meta := res.ClassMeta
	return addSCIDToIndexResult{
		SCID:          res.SCID,
		Class:         meta.Class,
		Tags:          meta.Tags,
		Name:          meta.Name,
		Description:   meta.Desc,
		IconURL:       meta.IconURL,
		InstallHeight: meta.InstallHeight,
		LastHeight:    meta.LastHeight,
		VarsCount:     res.VarsCount,
		FromCache:     res.FromCache,
	}, nil
}

// isValidSCID tests the DERO 32-byte (64-hex) SCID shape. Anything else is
// rejected up front with -32602 so we never hand a bad hex string to the
// daemon.
// getInitialSCIDCodeParams is the wire shape for GetInitialSCIDCode.
type getInitialSCIDCodeParams struct {
	SCID string `json:"scid"`
}

// getInitialSCIDCodeResult is the wire shape for GetInitialSCIDCode.
// Mirrors the REST /api/initialscidcode envelope so clients can share a
// decoder. code is the raw DVM source; install_height is the best-known
// height (exact on forward-populated entries, chain-tip-at-backfill for
// pre-feature SCIDs).
type getInitialSCIDCodeResult struct {
	SCID          string `json:"scid"`
	Code          string `json:"code"`
	InstallHeight int64  `json:"install_height"`
}

// handleGetInitialSCIDCode is the WS entry for simple-gnomon interop. Routes
// through idx.GetSCCode so both REST and WS share the lazy-backfill path
// and its single-flight guarantee.
func (ws *WSServer) handleGetInitialSCIDCode(_ *connContext, raw json.RawMessage) (interface{}, *jsonRPCError) {
	if ws.idx == nil {
		return nil, &jsonRPCError{Code: codeInternalError, Message: "indexer not configured"}
	}
	var p getInitialSCIDCodeParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "invalid params: " + err.Error()}
	}
	if !isValidSCID(p.SCID) {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "invalid scid"}
	}
	entry, err := ws.idx.GetSCCode(p.SCID)
	if err != nil {
		return nil, internalErr(err)
	}
	if entry == nil {
		return nil, &jsonRPCError{Code: codeNotFound, Message: "scid not found"}
	}
	return getInitialSCIDCodeResult{
		SCID:          p.SCID,
		Code:          entry.Code,
		InstallHeight: entry.InstallHeight,
	}, nil
}

// validateSCParams is the wire shape for validatesc. Accepts just the scid.
type validateSCParams struct {
	SCID string `json:"scid"`
}

// validateSCResult mirrors ClassMeta + the dynamic SCClass fields so
// operators can see everything the classifier decided. Populated fields
// only (JSON omitempty) so a report on an UNKNOWN scid doesn't carry
// noise like empty strings for 10 fields.
type validateSCResult struct {
	SCID     string   `json:"scid"`
	Found    bool     `json:"found"`
	Class    string   `json:"class,omitempty"`
	Tags     []string `json:"tags,omitempty"`
	Name     string   `json:"name,omitempty"`
	Desc     string   `json:"description,omitempty"`
	IconURL  string   `json:"icon,omitempty"`
	DURL     string   `json:"durl,omitempty"`
	Version  string   `json:"version,omitempty"`
	DocType  string   `json:"doc_type,omitempty"`
	Mods     []string `json:"mods,omitempty"`
	DocShard bool     `json:"doc_shard,omitempty"`
	VarCount int      `json:"var_count"`
	Height   int64    `json:"height"`
}

// handleValidateSC re-runs ClassifySC on a requested scid and returns the
// full classification diagnostic. Reads only — no writes. Useful to verify
// what the indexer thinks of a specific contract without grepping scvars
// manually. Falls back to current-vars-only classification when the code
// isn't in the sccode bucket (we don't touch the daemon here).
func (ws *WSServer) handleValidateSC(_ *connContext, raw json.RawMessage) (interface{}, *jsonRPCError) {
	var p validateSCParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "invalid params: " + err.Error()}
	}
	if !isValidSCID(p.SCID) {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "invalid scid"}
	}
	meta, err := ws.store.GetSCIDClass(p.SCID)
	if err != nil {
		return nil, internalErr(err)
	}
	if meta == nil {
		return validateSCResult{SCID: p.SCID, Found: false}, nil
	}
	vars, _ := ws.store.GetSCIDVariableDetailsAtHeight(p.SCID, meta.LastHeight)
	// Convert to map so we can run the dynamic-field extractors (docType,
	// mods, docShard) that ClassMeta doesn't currently store.
	varsMap := make(map[string]interface{}, len(vars))
	for _, v := range vars {
		if k, ok := v.Key.(string); ok {
			varsMap[k] = v.Value
		}
	}
	// ClassifySC with empty code produces the UNKNOWN class but still runs
	// the TELA-field extractors — so sc.DocType / sc.Mods / sc.DocShard
	// come from here, while class/name/desc/icon/durl/version come from
	// the stored ClassMeta (authoritative, populated at probe time with
	// fresh vars the fast-path ordering skipped).
	sc := indexer.ClassifySC(p.SCID, "", varsMap)
	return validateSCResult{
		SCID:     p.SCID,
		Found:    true,
		Class:    meta.Class,
		Tags:     meta.Tags,
		Name:     meta.Name,
		Desc:     meta.Desc,
		IconURL:  meta.IconURL,
		DURL:     meta.DURL,
		Version:  meta.Version,
		DocType:  sc.DocType,
		Mods:     sc.Mods,
		DocShard: sc.DocShard,
		VarCount: len(vars),
		Height:   meta.LastHeight,
	}, nil
}

// listSCByOwnerParams is the wire shape for listsc_byowner / getscidlist_byaddr.
type listSCByOwnerParams struct {
	Owner string `json:"owner"`
}

// listSCByOwnerEntry carries one scid + its ClassMeta Name + Class for
// quick client-side rendering.
type listSCByOwnerEntry struct {
	SCID  string `json:"scid"`
	Class string `json:"class,omitempty"`
	Name  string `json:"name,omitempty"`
}

// listSCByOwnerResponse is the envelope.
type listSCByOwnerResponse struct {
	Owner string               `json:"owner"`
	SCIDs []listSCByOwnerEntry `json:"scids"`
	Count int                  `json:"count"`
}

// handleListSCByOwner reads the reverse owner→scid index populated at
// install time. Returns each scid plus its current class + name.
// getscidlist_byaddr dispatches through this same handler.
func (ws *WSServer) handleListSCByOwner(_ *connContext, raw json.RawMessage) (interface{}, *jsonRPCError) {
	var p listSCByOwnerParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "invalid params: " + err.Error()}
	}
	if p.Owner == "" {
		return nil, &jsonRPCError{Code: codeInvalidParams, Message: "missing owner"}
	}
	scids, err := ws.store.GetSCIDsByOwner(p.Owner)
	if err != nil {
		return nil, internalErr(err)
	}
	out := listSCByOwnerResponse{
		Owner: p.Owner,
		SCIDs: make([]listSCByOwnerEntry, 0, len(scids)),
	}
	for _, scid := range scids {
		entry := listSCByOwnerEntry{SCID: scid}
		if meta, err := ws.store.GetSCIDClass(scid); err == nil && meta != nil {
			entry.Class = meta.Class
			entry.Name = meta.Name
		}
		out.SCIDs = append(out.SCIDs, entry)
	}
	out.Count = len(out.SCIDs)
	return out, nil
}

func isValidSCID(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func errorResponse(id json.RawMessage, code int, msg string) jsonRPCResponse {
	return jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error:   &jsonRPCError{Code: code, Message: msg},
	}
}

func internalErr(err error) *jsonRPCError {
	logger.Errorf("storage: %v", err)
	return &jsonRPCError{Code: codeInternalError, Message: "internal error"}
}

// Ensure compile-time compatibility: WSServer methods must stay in sync
// with the storage.Storage interface they depend on.
var _ = (*structures.SCTXParse)(nil)
var _ = (*structures.SCIDVariable)(nil)
