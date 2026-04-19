package storage

import (
	"sync"

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

	// --- Route B: M0 foundation (DESIGN.md §3) ---

	// Block hash chain — one entry per flushed height, committed atomically
	// alongside batch data. Enables reorg detection.
	StoreBlockHash(height int64, hash string) error
	GetBlockHash(height int64) (string, error)

	// Class index — SCIDs by ClassifySC class, prefix-scannable by class name.
	GetClassInstalls(class string, limit int) ([]structures.ClassInstall, error)
	GetSCIDClass(scid string) (*structures.ClassMeta, error)

	// Install index — SCIDs installed in height range, prefix-scannable.
	GetInstallsInRange(fromHeight, toHeight int64, limit int) ([]structures.ClassInstall, error)

	// Address reverse index — SCIDs touched by an address.
	GetAddressSCIDs(addr string) (map[string]*structures.AddrSCIDEntry, error)
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

	// Route B (DESIGN.md §3) — accumulated alongside the rest so one bbolt
	// txn covers all state. Order inside FlushBatch does not matter for
	// correctness (all-or-nothing) but does for write locality (group by bucket).

	// BlockHashes: height -> 64-char hex block hash. Committed atomically.
	BlockHashes map[int64]string

	// Installs: scid -> install record. Lives under installs/<BE8:height>:<scid>.
	Installs map[string]*structures.InstallRecord

	// Classes: scid -> classified metadata. Under class/<class>:<BE8:h>:<scid>
	// and the lookup scvars class-lookup sibling bucket.
	Classes map[string]*structures.ClassMeta

	// AddrSCIDs: addr -> scid -> delta. Merged into the addr_scids nested
	// bucket. FirstHeight=oldest, LastHeight=newest in batch, Count=delta.
	// FlushBatch merges with existing entries (min/max/sum).
	AddrSCIDs map[string]map[string]*structures.AddrSCIDEntry
}

// batchPool recycles WriteBatch instances. At steady state, a batch is pulled,
// filled with a flush worth of data, passed to FlushBatch, then returned. This
// is the one "arena" gap in an otherwise arena-pure design: NewWriteBatch
// allocates 5 maps + 1 slice header per call, and the hot loop calls it ~14/s.
var batchPool = sync.Pool{
	New: func() interface{} {
		return newEmptyBatch()
	},
}

func newEmptyBatch() *WriteBatch {
	return &WriteBatch{
		Owners:       make(map[string]string, 32),
		Invocations:  make([]structures.InvokeRecord, 0, 128),
		Variables:    make(map[string]map[int64][]*structures.SCIDVariable, 32),
		Heights:      make(map[string][]int64, 32),
		NormalTxs:    make(map[string][]*structures.NormalTXWithSCIDParse, 16),
		InvalidSCIDs: make(map[string]uint64, 4),
		BlockHashes:  make(map[int64]string, 100),
		Installs:     make(map[string]*structures.InstallRecord, 4),
		Classes:      make(map[string]*structures.ClassMeta, 8),
		AddrSCIDs:    make(map[string]map[string]*structures.AddrSCIDEntry, 16),
	}
}

// NewWriteBatch returns a zeroed batch from the pool. Callers should invoke
// PutWriteBatch(batch) after FlushBatch to return it. NewWriteBatch always
// hands back a Reset() batch — safe even if a caller forgets to Put.
func NewWriteBatch() *WriteBatch {
	b := batchPool.Get().(*WriteBatch)
	b.Reset()
	return b
}

// PutWriteBatch returns a batch to the pool after Reset. Call after every
// successful (or unsuccessful) FlushBatch. Passing nil is a no-op.
func PutWriteBatch(b *WriteBatch) {
	if b == nil {
		return
	}
	b.Reset()
	batchPool.Put(b)
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

// AddBlockHash records the block hash at a given height for reorg detection.
// All block hashes in a batch flush together; reorg events are consistent
// with durable state.
func (b *WriteBatch) AddBlockHash(height int64, hash string) {
	b.BlockHashes[height] = hash
}

// AddInstall records a new SC deployment. Key becomes installs/<BE8:h>:<scid>.
func (b *WriteBatch) AddInstall(scid string, height int64, rec *structures.InstallRecord) {
	if b.Installs == nil {
		b.Installs = make(map[string]*structures.InstallRecord, 4)
	}
	// Encode (height, scid) in the map key so FlushBatch can build the
	// bucket key deterministically without re-threading height through.
	b.Installs[scidHeightKey(scid, height)] = rec
}

// AddClass records ClassifySC output for an SCID. Called on install and on
// invokes that update metadata (e.g. TELA version bump).
func (b *WriteBatch) AddClass(scid string, meta *structures.ClassMeta) {
	b.Classes[scid] = meta
}

// AddAddrSCID records a single interaction of addr touching scid at height.
// Multiple calls for the same (addr, scid) within a batch coalesce into
// min/max/sum. FlushBatch merges with any existing bucket entry too.
func (b *WriteBatch) AddAddrSCID(addr, scid string, height int64) {
	if addr == "" {
		return
	}
	inner, ok := b.AddrSCIDs[addr]
	if !ok {
		inner = make(map[string]*structures.AddrSCIDEntry, 2)
		b.AddrSCIDs[addr] = inner
	}
	e, ok := inner[scid]
	if !ok {
		inner[scid] = &structures.AddrSCIDEntry{
			FirstHeight: height,
			LastHeight:  height,
			Count:       1,
		}
		return
	}
	if height < e.FirstHeight {
		e.FirstHeight = height
	}
	if height > e.LastHeight {
		e.LastHeight = height
	}
	e.Count++
}

// scidHeightKey produces a stable map key for (scid, height). This is not a
// bucket key — FlushBatch reinterprets it with binary BE height encoding.
func scidHeightKey(scid string, height int64) string {
	// 64-char hex scid + decimal height. Collision-free for valid SCIDs.
	return scid + "@" + intToStr(height)
}

func intToStr(n int64) string {
	// Avoid strconv dep bloat at module level; the inliner handles this.
	var buf [20]byte
	neg := n < 0
	if neg {
		n = -n
	}
	i := len(buf)
	if n == 0 {
		i--
		buf[i] = '0'
	}
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// Reset clears the batch for reuse (arena-style bulk free).
func (b *WriteBatch) Reset() {
	clear(b.Owners)
	b.Invocations = b.Invocations[:0]
	clear(b.Variables)
	clear(b.Heights)
	clear(b.NormalTxs)
	clear(b.InvalidSCIDs)
	clear(b.BlockHashes)
	clear(b.Installs)
	clear(b.Classes)
	clear(b.AddrSCIDs)
	b.RegTxCount = 0
	b.BurnTxCount = 0
	b.NormTxCount = 0
	b.LastHeight = 0
}
