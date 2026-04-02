package structures

import (
	"github.com/deroproject/derohe/rpc"
	"github.com/deroproject/derohe/transaction"
)

// Method constants -- uint8 instead of string to avoid allocations
const (
	MethodInstallSC uint8 = 1
	MethodInvokeSC  uint8 = 2
)

// SCTXParse represents a parsed smart contract transaction.
// Fixed-size arrays for SCID/TXID reduce GC scanner work.
type SCTXParse struct {
	Txid       string
	Scid       string
	Entrypoint string
	Method     uint8
	ScArgs     rpc.Arguments
	Sender     string
	Payloads   []transaction.AssetPayload
	Fees       uint64
	Height     int64
}

// Reset zeroes all fields for sync.Pool reuse (arena-style recycling).
func (s *SCTXParse) Reset() {
	s.Txid = ""
	s.Scid = ""
	s.Entrypoint = ""
	s.Method = 0
	s.ScArgs = nil
	s.Sender = ""
	s.Payloads = s.Payloads[:0]
	s.Fees = 0
	s.Height = 0
}

// BlockTxns holds the transaction hashes for a single block.
type BlockTxns struct {
	Topoheight int64
	TxHashes   []string
}

func (b *BlockTxns) Reset() {
	b.Topoheight = 0
	b.TxHashes = b.TxHashes[:0]
}

// SCIDVariable represents a single key-value pair from a smart contract.
type SCIDVariable struct {
	Key   interface{}
	Value interface{}
}

// NormalTXWithSCIDParse represents a normal transaction with a SCID in its payload.
type NormalTXWithSCIDParse struct {
	Txid   string
	Scid   string
	Fees   uint64
	Height int64
}

// InvokeRecord stores SC invocation details for batch writing.
type InvokeRecord struct {
	Scid       string
	Sender     string
	Entrypoint string
	Height     int64
	Details    *SCTXParse
}

// SCIDInfo stores indexed smart contract metadata.
type SCIDInfo struct {
	Owner  string
	Code   string
	Height int64
}

// GetInfoResult caches daemon getinfo response.
type GetInfoResult struct {
	Height    int64
	TopoHeight int64
	StableHeight int64
	Status    string
}

// WorkItem flows through the pipeline stages. Recycled via sync.Pool.
type WorkItem struct {
	Height     int64
	BlockTxns  *BlockTxns
	SCTxs      []SCTXParse
	RegCount   int64
	BurnCount  int64
	NormCount  int64
	NormalTxs  []NormalTXWithSCIDParse
	Err        error
}

func (w *WorkItem) Reset() {
	w.Height = 0
	w.BlockTxns = nil
	w.SCTxs = w.SCTxs[:0]
	w.RegCount = 0
	w.BurnCount = 0
	w.NormCount = 0
	w.NormalTxs = w.NormalTxs[:0]
	w.Err = nil
}
