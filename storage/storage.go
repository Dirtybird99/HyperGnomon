package storage

import (
	"github.com/hypergnomon/hypergnomon/structures"
)

// Storage defines the interface for all indexer database operations.
// Designed for batch writes: accumulate changes, flush atomically.
type Storage interface {
	// Lifecycle
	Close() error
	EnableSync()
	DisableSync()

	// Index height tracking
	GetLastIndexHeight() (int64, error)
	StoreLastIndexHeight(height int64) error

	// SC owner tracking
	StoreOwner(scid, owner string) error
	GetOwner(scid string) (string, error)

	// SC invocation details
	StoreInvokeDetails(scid, sender, entrypoint string, height int64, details *structures.SCTXParse) error
	GetInvokeDetailsBySCID(scid string) ([]*structures.SCTXParse, error)

	// SC variable snapshots
	StoreSCIDVariableDetails(scid string, vars []*structures.SCIDVariable, height int64) error
	GetSCIDVariableDetailsAtHeight(scid string, height int64) ([]*structures.SCIDVariable, error)

	// SC interaction heights
	StoreSCIDInteractionHeight(scid string, height int64) error
	GetSCIDInteractionHeights(scid string) ([]int64, error)

	// Normal TX with SCID payload
	StoreNormalTxWithSCIDByAddr(addr string, tx *structures.NormalTXWithSCIDParse) error

	// Invalid SC deploys
	StoreInvalidSCIDDeploys(scid string, fees uint64) error

	// TX counts
	StoreTxCounts(reg, burn, norm int64) error

	// Queries
	GetAllSCIDs() ([]string, error)
	GetAllOwnersAndSCIDs() (map[string]string, error)
	GetInvalidSCIDDeploys() (map[string]uint64, error)
	GetTxCounts() (reg, burn, norm int64, err error)
	GetNormalTxWithSCIDByAddr(addr string) ([]*structures.NormalTXWithSCIDParse, error)

	// Batch operations (arena-style bulk commit)
	FlushBatch(batch *WriteBatch) error
}

// WriteBatch accumulates writes across multiple blocks for atomic commit.
// This is the arena pattern applied to database writes:
// accumulate everything, flush once, instead of per-item writes.
type WriteBatch struct {
	Owners       map[string]string                               // scid -> owner
	Invocations  []structures.InvokeRecord                       // all invocations
	Variables    map[string]map[int64][]*structures.SCIDVariable // scid -> height -> vars
	Heights      map[string][]int64                              // scid -> interaction heights
	NormalTxs    map[string][]*structures.NormalTXWithSCIDParse  // addr -> txs
	InvalidSCIDs map[string]uint64                               // scid -> fees
	RegTxCount   int64
	BurnTxCount  int64
	NormTxCount  int64
	LastHeight   int64
}

// NewWriteBatch creates a fresh batch with pre-allocated maps.
func NewWriteBatch() *WriteBatch {
	return &WriteBatch{
		Owners:       make(map[string]string, 32),
		Invocations:  make([]structures.InvokeRecord, 0, 128),
		Variables:    make(map[string]map[int64][]*structures.SCIDVariable, 32),
		Heights:      make(map[string][]int64, 32),
		NormalTxs:    make(map[string][]*structures.NormalTXWithSCIDParse, 16),
		InvalidSCIDs: make(map[string]uint64, 4),
	}
}

// AddOwner adds an owner record to the batch.
func (b *WriteBatch) AddOwner(scid, owner string) {
	b.Owners[scid] = owner
}

// AddInvocation adds an invocation record to the batch.
func (b *WriteBatch) AddInvocation(rec structures.InvokeRecord) {
	b.Invocations = append(b.Invocations, rec)
}

// AddVariables adds SC variable snapshot to the batch.
func (b *WriteBatch) AddVariables(scid string, height int64, vars []*structures.SCIDVariable) {
	if b.Variables[scid] == nil {
		b.Variables[scid] = make(map[int64][]*structures.SCIDVariable, 4)
	}
	b.Variables[scid][height] = vars
}

// AddInteractionHeight adds a height record to the batch.
func (b *WriteBatch) AddInteractionHeight(scid string, height int64) {
	b.Heights[scid] = append(b.Heights[scid], height)
}

// Reset clears the batch for reuse (arena-style bulk free).
func (b *WriteBatch) Reset() {
	clear(b.Owners)
	b.Invocations = b.Invocations[:0]
	clear(b.Variables)
	clear(b.Heights)
	clear(b.NormalTxs)
	clear(b.InvalidSCIDs)
	b.RegTxCount = 0
	b.BurnTxCount = 0
	b.NormTxCount = 0
	b.LastHeight = 0
}
