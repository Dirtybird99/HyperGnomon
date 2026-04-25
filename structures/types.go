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
	Height       int64
	TopoHeight   int64
	StableHeight int64
	Status       string
}

// WorkItem flows through the pipeline stages. Recycled via sync.Pool.
type WorkItem struct {
	Height    int64
	BlockTxns *BlockTxns
	SCTxs     []SCTXParse
	RegCount  int64
	BurnCount int64
	NormCount int64
	NormalTxs []NormalTXWithSCIDParse
	Err       error
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

// ----- Route B (DESIGN.md §3) additions -----

// ClassMeta is stored in the class bucket under key
// "<class>:<BE8 install_height>:<scid>" and also on a per-SCID lookup bucket.
// Populated at index time by ClassifySC.
type ClassMeta struct {
	Class         string   `msgpack:"class"`
	Tags          []string `msgpack:"tags"`
	Name          string   `msgpack:"name,omitempty"`
	Desc          string   `msgpack:"desc,omitempty"`
	IconURL       string   `msgpack:"icon,omitempty"`
	DURL          string   `msgpack:"durl,omitempty"`
	Version       string   `msgpack:"version,omitempty"`
	InstallHeight int64    `msgpack:"install_h"`
	LastHeight    int64    `msgpack:"last_h"`
}

// InstallRecord is stored in the installs bucket under key
// "<BE8 height>:<scid>". Enables "SCs installed in [h1,h2)" in a prefix scan.
type InstallRecord struct {
	Owner      string `msgpack:"owner"`
	Entrypoint string `msgpack:"entrypoint,omitempty"`
	Fees       uint64 `msgpack:"fees"`
}

// AddrSCIDEntry is the value of a nested addr_scids/<addr>/<scid> record.
// Tracks first/last interaction and count; doubles as a per-address activity
// rollup for /api/address/{addr}/scs.
type AddrSCIDEntry struct {
	FirstHeight int64 `msgpack:"first"`
	LastHeight  int64 `msgpack:"last"`
	Count       int64 `msgpack:"count"`
}

// ClassInstall pairs a classified SCID with its install height. Returned by
// GetClassInstalls for the class index prefix scan.
type ClassInstall struct {
	SCID          string
	InstallHeight int64
	Meta          *ClassMeta
}

// SCCodeEntry is the install-time smart contract code snapshot, keyed by
// scid. DERO SC code is immutable once installed, so this is a write-once
// record. Populated on install (four sites in the indexer) and lazily
// backfilled on first read for SCIDs indexed before the sccode bucket
// existed.
type SCCodeEntry struct {
	Code          string `msgpack:"code"`
	InstallHeight int64  `msgpack:"h"`
}

// TELAContentEntry is the durable cache value for one (scid, path) pair. The
// ETag is the sha256 hex of Body — precomputed so every cache hit is a
// zero-alloc 304 comparison.
type TELAContentEntry struct {
	Body   []byte `msgpack:"body"`
	MIME   string `msgpack:"mime,omitempty"`
	ETag   string `msgpack:"etag,omitempty"`
	Height int64  `msgpack:"h"`
}

// Rating (spec reference above) — this older godoc block is kept purely
// so that grep finds the historical comment for future archeology. The
// actual type is declared immediately below.
// Rating is one rater's score on a TELA INDEX/DOC contract.
//
// Per the canonical TELA spec (github.com/civilware/tela), the STORE key
// is the rater's wallet address; the value is hex-encoded `"<score>_<height>"`.
// Score is 0-99. No comment field: ratings do not carry a message.
type Rating struct {
	Rater  string  `json:"rater"`
	Score  float64 `json:"score"`
	Height int64   `json:"height"`
}

// RatingSummary aggregates the `likes` / `dislikes` counters that the TELA
// Rate() entrypoint maintains as separate STORE keys. Height is the snapshot
// height the counters were read at.
type RatingSummary struct {
	Height   int64  `json:"height"`
	Likes    uint64 `json:"likes"`
	Dislikes uint64 `json:"dislikes"`
}
