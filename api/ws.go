package api

import (
	"encoding/json"
	"net"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"

	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

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
)

// WSServer serves a JSON-RPC 2.0 API over WebSocket connections,
// providing read access to the indexed blockchain data.
type WSServer struct {
	store    storage.Storage
	upgrader websocket.Upgrader
	addr     string

	// dispatch table: method name -> handler func
	methods map[string]methodHandler
}

// methodHandler processes a JSON-RPC request and returns a result or error.
type methodHandler func(params json.RawMessage) (interface{}, *jsonRPCError)

// NewWSServer creates a WSServer bound to the given address.
// The storage backend must already be open.
func NewWSServer(addr string, store storage.Storage) *WSServer {
	ws := &WSServer{
		store: store,
		addr:  addr,
		upgrader: websocket.Upgrader{
			ReadBufferSize:  4096,
			WriteBufferSize: 4096,
			CheckOrigin:     func(r *http.Request) bool { return true },
		},
	}
	ws.methods = map[string]methodHandler{
		"GetAllOwnersAndSCIDs":              ws.handleGetAllOwnersAndSCIDs,
		"GetAllSCIDs":                       ws.handleGetAllSCIDs,
		"GetSCIDVariableDetailsAtTopoheight": ws.handleGetSCIDVariableDetailsAtTopoheight,
		"GetSCIDInteractionHeight":          ws.handleGetSCIDInteractionHeight,
		"GetAllSCIDInvokeDetails":           ws.handleGetAllSCIDInvokeDetails,
	}
	return ws
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
	return http.Serve(ln, mux)
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

// handleConn reads JSON-RPC requests from a single WebSocket connection
// until it closes or errors. Each request is dispatched and answered
// sequentially per connection — no head-of-line blocking across connections.
func (ws *WSServer) handleConn(conn *websocket.Conn) {
	defer conn.Close()

	// Reusable encoder avoids per-message allocation.
	var mu sync.Mutex // guards writes; reads are single-goroutine

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				logger.Debugf("ws read: %v", err)
			}
			return
		}

		resp := ws.dispatch(msg)

		mu.Lock()
		err = conn.WriteJSON(resp)
		mu.Unlock()
		if err != nil {
			logger.Debugf("ws write: %v", err)
			return
		}
	}
}

// dispatch parses a raw JSON-RPC message and routes it to the correct handler.
func (ws *WSServer) dispatch(msg []byte) jsonRPCResponse {
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

	result, rpcErr := handler(req.Params)
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

func (ws *WSServer) handleGetAllOwnersAndSCIDs(_ json.RawMessage) (interface{}, *jsonRPCError) {
	owners, err := ws.store.GetAllOwnersAndSCIDs()
	if err != nil {
		return nil, internalErr(err)
	}
	return owners, nil
}

func (ws *WSServer) handleGetAllSCIDs(_ json.RawMessage) (interface{}, *jsonRPCError) {
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

func (ws *WSServer) handleGetSCIDVariableDetailsAtTopoheight(raw json.RawMessage) (interface{}, *jsonRPCError) {
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

func (ws *WSServer) handleGetSCIDInteractionHeight(raw json.RawMessage) (interface{}, *jsonRPCError) {
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

func (ws *WSServer) handleGetAllSCIDInvokeDetails(raw json.RawMessage) (interface{}, *jsonRPCError) {
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
