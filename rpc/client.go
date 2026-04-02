package rpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/channel"
	"github.com/deroproject/derohe/rpc"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"

	hgrpc "github.com/hypergnomon/hypergnomon/rpc/rwc"
)

var logger = logrus.WithField("pkg", "rpc")

// Client wraps a WebSocket connection with JSON-RPC capabilities.
type Client struct {
	WS       *websocket.Conn
	RPC      *jrpc2.Client
	Endpoint string
	mu       sync.RWMutex
}

// NewClient creates a new RPC client connected to the given daemon endpoint.
func NewClient(endpoint string) (*Client, error) {
	c := &Client{Endpoint: endpoint}
	if err := c.Connect(); err != nil {
		return nil, err
	}
	return c, nil
}

// Connect establishes the WebSocket connection and JSON-RPC client.
func (c *Client) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.WS != nil {
		c.WS.Close()
	}

	dialer := websocket.Dialer{
		ReadBufferSize:   65536, // 64KB (up from 4KB default)
		WriteBufferSize:  65536,
		HandshakeTimeout: 3 * time.Second,
		EnableCompression: true,
	}
	ws, _, err := dialer.Dial("ws://"+c.Endpoint+"/ws", nil)
	if err != nil {
		return fmt.Errorf("dial %s: %w", c.Endpoint, err)
	}

	c.WS = ws
	inputOutput := hgrpc.New(ws)
	c.RPC = jrpc2.NewClient(channel.RawJSON(inputOutput, inputOutput), nil)
	return nil
}

// Close shuts down the RPC client and WebSocket.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.RPC != nil {
		c.RPC.Close()
	}
	if c.WS != nil {
		c.WS.Close()
	}
}

// GetInfo calls DERO.GetInfo to get current chain state.
func (c *Client) GetInfo() (*rpc.GetInfo_Result, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result rpc.GetInfo_Result
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()

	if err := c.RPC.CallResult(ctx, "DERO.GetInfo", nil, &result); err != nil {
		return nil, fmt.Errorf("GetInfo: %w", err)
	}
	return &result, nil
}

// GetBlock retrieves a block by its hash.
func (c *Client) GetBlock(hash string) (*rpc.GetBlock_Result, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result rpc.GetBlock_Result
	params := rpc.GetBlock_Params{Hash: hash}
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()

	if err := c.RPC.CallResult(ctx, "DERO.GetBlock", params, &result); err != nil {
		return nil, fmt.Errorf("GetBlock(%s): %w", hash, err)
	}
	return &result, nil
}

// GetBlockHeaderByTopoHeight retrieves a block header by topoheight.
func (c *Client) GetBlockHeaderByTopoHeight(height uint64) (*rpc.GetBlockHeaderByHeight_Result, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result rpc.GetBlockHeaderByHeight_Result
	params := rpc.GetBlockHeaderByTopoHeight_Params{TopoHeight: height}
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()

	if err := c.RPC.CallResult(ctx, "DERO.GetBlockHeaderByTopoHeight", params, &result); err != nil {
		return nil, fmt.Errorf("GetBlockHeaderByTopoHeight(%d): %w", height, err)
	}
	return &result, nil
}

// GetTransaction fetches transactions by hash. Supports batch (multiple hashes).
// This is the key optimization: one RPC call for all TXs in a block.
func (c *Client) GetTransaction(txHashes []string) (*rpc.GetTransaction_Result, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result rpc.GetTransaction_Result
	params := rpc.GetTransaction_Params{Tx_Hashes: txHashes}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := c.RPC.CallResult(ctx, "DERO.GetTransaction", params, &result); err != nil {
		return nil, fmt.Errorf("GetTransaction(%d hashes): %w", len(txHashes), err)
	}
	return &result, nil
}

// GetSC retrieves smart contract variables. Uses longer timeout for large contracts.
func (c *Client) GetSC(scid string, topoheight int64, keysstring []string, keysuint64 []uint64, code bool) (*rpc.GetSC_Result, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result rpc.GetSC_Result
	params := rpc.GetSC_Params{
		SCID:       scid,
		Code:       code,
		Variables:  true,
		TopoHeight: topoheight,
		KeysString: keysstring,
		KeysUint64: keysuint64,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	if err := c.RPC.CallResult(ctx, "DERO.GetSC", params, &result); err != nil {
		return nil, fmt.Errorf("GetSC(%s): %w", scid, err)
	}
	return &result, nil
}

// GetBlockHash returns the block hash at a given topoheight.
func (c *Client) GetBlockHash(height uint64) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result rpc.GetBlockHeaderByHeight_Result
	params := rpc.GetBlockHeaderByTopoHeight_Params{TopoHeight: height}
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()

	if err := c.RPC.CallResult(ctx, "DERO.GetBlockHeaderByTopoHeight", params, &result); err != nil {
		return "", fmt.Errorf("GetBlockHash(%d): %w", height, err)
	}
	return result.Block_Header.Hash, nil
}

// GetBlockByHeight retrieves a block directly by topoheight.
// This eliminates the GetBlockHash→GetBlock two-step, halving RPC calls.
func (c *Client) GetBlockByHeight(height uint64) (*rpc.GetBlock_Result, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result rpc.GetBlock_Result
	params := rpc.GetBlock_Params{Height: height}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := c.RPC.CallResult(ctx, "DERO.GetBlock", params, &result); err != nil {
		return nil, fmt.Errorf("GetBlockByHeight(%d): %w", height, err)
	}
	return &result, nil
}

// BatchGetBlocks fetches multiple blocks in a single JSON-RPC batch call.
// This is the nuclear optimization: 50 blocks per round trip instead of 1.
func (c *Client) BatchGetBlocks(heights []uint64) ([]*rpc.GetBlock_Result, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	specs := make([]jrpc2.Spec, len(heights))
	for i, h := range heights {
		specs[i] = jrpc2.Spec{
			Method: "DERO.GetBlock",
			Params: rpc.GetBlock_Params{Height: h},
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	responses, err := c.RPC.Batch(ctx, specs)
	if err != nil {
		return nil, fmt.Errorf("BatchGetBlocks(%d heights): %w", len(heights), err)
	}

	results := make([]*rpc.GetBlock_Result, len(responses))
	for i, resp := range responses {
		var result rpc.GetBlock_Result
		if err := resp.UnmarshalResult(&result); err != nil {
			return nil, fmt.Errorf("BatchGetBlocks unmarshal %d: %w", heights[i], err)
		}
		results[i] = &result
	}
	return results, nil
}
