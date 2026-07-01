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
	GetSCIDCount() (int64, error)
	GetOwnersForSCIDs(scids []string) (map[string]string, error)
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

	// Ratings extracted from scvars. Canonical TELA format: STORE keys that
	// equal a DERO address, hex-encoded `"<score>_<height>"` values. height
	// <= 0 resolves to the latest snapshot for scid.
	GetRatingsForSCID(scid string, height int64) ([]structures.Rating, error)

	// Rating aggregates: the `likes` and `dislikes` STORE keys that the
	// TELA Rate() entrypoint maintains. Cheap read, independent of per-rater
	// enumeration.
	GetRatingSummary(scid string, height int64) (*structures.RatingSummary, error)
	GetRatingsAndSummaryForSCID(scid string, height int64) ([]structures.Rating, *structures.RatingSummary, error)

	// Owner → SCIDs reverse lookup for listsc_byowner / getscidlist_byaddr.
	// Populated from WriteBatch.OwnerSCIDs at install time.
	GetSCIDsByOwner(owner string) ([]string, error)

	// TELA content cache (2-tier with api/tela_content.go's in-memory LRU).
	GetTELAContent(scid, path string) (*structures.TELAContentEntry, error)
	PutTELAContent(scid, path string, entry *structures.TELAContentEntry) error
	DeleteTELAContentForSCID(scid string) error

	// SC code snapshots (install-time DVM code). Backfill path is
	// indexer.GetSCCode; direct Put is exposed for tests + lazy-fill.
	GetSCCode(scid string) (*structures.SCCodeEntry, error)
	PutSCCode(scid string, entry *structures.SCCodeEntry) error

	// ResetIndex drops every index bucket so the next indexer startup
	// rescans from height 0. Block-hash history is kept so reorg-detection
	// replay still works. Intended for the `hypergnomon resync` subcommand.
	ResetIndex() error
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

	// SCCodes: scid -> install-time code snapshot. Populated at install time
	// and on lazy backfill reads. Write-once per scid (DERO code is
	// immutable); duplicates in one batch coalesce on the map.
	SCCodes map[string]*structures.SCCodeEntry

	// OwnerSCIDs: owner address → set of SCIDs installed by that owner.
	// Populated at install time and read by listsc_byowner /
	// getscidlist_byaddr. The nested map is a set — value is struct{}.
	OwnerSCIDs map[string]map[string]struct{}

	// addrSCIDArena backs the *AddrSCIDEntry values stored in AddrSCIDs. New
	// entries are carved from this slice (append + take-address) instead of a
	// per-pair heap alloc. The canonical pointer always lives in the AddrSCIDs
	// map; the arena is never re-indexed after an entry is published, so a
	// mid-batch append-driven realloc cannot invalidate a live entry — the old
	// backing array stays alive via the map pointer, and all mutations go
	// through inner[scid]. Reset truncates to [:0] (keeping capacity).
	addrSCIDArena []structures.AddrSCIDEntry

	// normalTxArena backs the *NormalTXWithSCIDParse values stored in NormalTxs.
	// Each ring member's record is carved from this slice (append + take-address)
	// instead of a per-record heap alloc. The records are write-once — never
	// mutated after publish — so, as with addrSCIDArena, a mid-batch
	// append-driven realloc cannot invalidate a live record: earlier records stay
	// reachable (and their old backing array alive) via NormalTxs[addr], and no
	// arena slot is re-indexed after publish. Reset truncates to [:0].
	normalTxArena []structures.NormalTXWithSCIDParse
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
		Owners:        make(map[string]string, 32),
		Invocations:   make([]structures.InvokeRecord, 0, 128),
		Variables:     make(map[string]map[int64][]*structures.SCIDVariable, 32),
		Heights:       make(map[string][]int64, 32),
		NormalTxs:     make(map[string][]*structures.NormalTXWithSCIDParse, 16),
		InvalidSCIDs:  make(map[string]uint64, 4),
		BlockHashes:   make(map[int64]string, 100),
		Installs:      make(map[string]*structures.InstallRecord, 4),
		Classes:       make(map[string]*structures.ClassMeta, 8),
		AddrSCIDs:     make(map[string]map[string]*structures.AddrSCIDEntry, 16),
		SCCodes:       make(map[string]*structures.SCCodeEntry, 4),
		OwnerSCIDs:    make(map[string]map[string]struct{}, 16),
		addrSCIDArena: make([]structures.AddrSCIDEntry, 0, 1024),
		normalTxArena: make([]structures.NormalTXWithSCIDParse, 0, 512),
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
	// Keep the reverse index in lockstep — listsc_byowner reads this.
	if owner == "" || scid == "" {
		return
	}
	if b.OwnerSCIDs == nil {
		b.OwnerSCIDs = make(map[string]map[string]struct{}, 4)
	}
	set, ok := b.OwnerSCIDs[owner]
	if !ok {
		set = make(map[string]struct{}, 2)
		b.OwnerSCIDs[owner] = set
	}
	set[scid] = struct{}{}
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

// AddNormalTx records that addr participated (as a ring member) in a normal TX
// touching scid. The record is carved from the batch-owned arena instead of a
// per-call heap alloc; the pointer appended to NormalTxs[addr] is the canonical
// handle and the record is never mutated after publish, so an arena realloc
// cannot invalidate an already-published record (see normalTxArena doc).
func (b *WriteBatch) AddNormalTx(addr, txid, scid string, fees uint64, height int64) {
	b.normalTxArena = append(b.normalTxArena, structures.NormalTXWithSCIDParse{
		Txid:   txid,
		Scid:   scid,
		Fees:   fees,
		Height: height,
	})
	ntx := &b.normalTxArena[len(b.normalTxArena)-1]
	if b.NormalTxs[addr] == nil {
		b.NormalTxs[addr] = make([]*structures.NormalTXWithSCIDParse, 0, 4)
	}
	b.NormalTxs[addr] = append(b.NormalTxs[addr], ntx)
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

// AddSCCode records the install-time DVM code for scid. Called from the four
// indexer install sites and from the lazy-backfill path when an existing
// SCID gets its first read. Empty code strings are dropped — the populate
// guards are all upstream, but defense in depth keeps the bucket clean.
func (b *WriteBatch) AddSCCode(scid string, installHeight int64, code string) {
	if scid == "" || code == "" {
		return
	}
	if b.SCCodes == nil {
		b.SCCodes = make(map[string]*structures.SCCodeEntry, 4)
	}
	b.SCCodes[scid] = &structures.SCCodeEntry{
		Code:          code,
		InstallHeight: installHeight,
	}
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
		// Carve a new entry from the batch-owned arena instead of a per-pair
		// heap alloc. The pointer published into the map is the canonical
		// handle; the arena is never re-indexed after this point, so a later
		// append-driven realloc cannot invalidate it (the old backing array is
		// kept alive by this very pointer, and every mutation below goes through
		// inner[scid]).
		b.addrSCIDArena = append(b.addrSCIDArena, structures.AddrSCIDEntry{
			FirstHeight: height,
			LastHeight:  height,
			Count:       1,
		})
		inner[scid] = &b.addrSCIDArena[len(b.addrSCIDArena)-1]
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
	b.addrSCIDArena = b.addrSCIDArena[:0]
	b.normalTxArena = b.normalTxArena[:0]
	clear(b.SCCodes)
	clear(b.OwnerSCIDs)
	b.RegTxCount = 0
	b.BurnTxCount = 0
	b.NormTxCount = 0
	b.LastHeight = 0
}
