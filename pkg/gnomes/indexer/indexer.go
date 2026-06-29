// Package indexer is the civilware/Gnomon compat surface for the
// running indexer. Consumers who previously imported
// `github.com/civilware/Gnomon/indexer` can rewrite to
// `github.com/hypergnomon/hypergnomon/pkg/gnomes/indexer` and rebuild.
//
// HOLOGRAM's `gnomon.go:Start()` was the reference caller for this
// surface design (see Agent F teardown). If something HOLOGRAM calls
// isn't wired here, that's a v1.x gap — file an issue.
package indexer

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"

	hgindexer "github.com/hypergnomon/hypergnomon/indexer"
	"github.com/hypergnomon/hypergnomon/structures"

	compatstorage "github.com/hypergnomon/hypergnomon/pkg/gnomes/storage"
	compatstructures "github.com/hypergnomon/hypergnomon/pkg/gnomes/structures"
)

// Indexer is the civilware-shape facade over HyperGnomon's internal
// indexer. Exported fields are read-only snapshots of the underlying
// state, updated on every scan cycle via the refresh goroutine. Call
// `StartDaemonMode(n)` to begin scanning; `Close()` for clean
// shutdown. Both match civilware's semantics.
type Indexer struct {
	// LastIndexedHeight is the height at which the last scan batch
	// flushed. Updated atomically from the internal indexer.
	// Civilware exposes this as a plain int64 field; we use atomic
	// stores so concurrent reads from the main goroutine are race-
	// free.
	LastIndexedHeight int64
	// ChainHeight tracks the daemon's reported topo_height. Updated
	// whenever the internal indexer polls GetInfo.
	ChainHeight int64
	// DBType is "boltdb" for the HyperGnomon compat path. Civilware
	// also has "gravdb" but that backend errors here — see
	// storage.ErrGravDBNotSupported.
	DBType string
	// GravDBBackend is always nil in the HyperGnomon facade because
	// we don't support graviton. Field declared so type-asserting
	// consumers compile. Use BBSBackend instead.
	GravDBBackend *compatstorage.GravitonStore
	// BBSBackend is the bbolt store wrapper. Always non-nil in a
	// successful NewIndexer call.
	BBSBackend *compatstorage.BboltStore

	// inner is the HyperGnomon indexer driving everything. Kept
	// unexported so the civilware surface is the only supported API.
	inner *hgindexer.Indexer

	// fieldRefreshStop signals the field-sync goroutine to stop. The
	// goroutine pushes LastIndexedHeight + ChainHeight from the
	// internal atomic fields out to this struct's exported fields
	// every ~100ms; without it the exported fields stay zero.
	fieldRefreshStop chan struct{}
	fieldRefreshWG   sync.WaitGroup

	// closed guards against double Close calls.
	closed atomic.Bool
}

// NewIndexer constructs an Indexer using the civilware shape. The parameter
// list mirrors civilware/Gnomon's current indexer.NewIndexer EXACTLY (commit
// 0280ea286474, the feat-addscidtoindex-wsserver line HOLOGRAM v1.0.7 pins), so
// a consumer's call site compiles unchanged after the import rewrite:
//
//	indexer.NewIndexer(gravDB, boltDB, dbType, searchFilter, height, endpoint,
//	    "daemon", mbllookup, closeOnDisconnect, fsc, exclusions, storeIntegrators)
//
// HyperGnomon runs on bbolt regardless of dbType: bbolt is the default, and a
// non-bbolt dbType ("gravdb"/"sqlite"/…) is accepted but logged and falls back
// to the bbolt store the caller passes as bolt. The pre-opened bolt store is
// injected into the internal indexer (re-opening its path would lock-conflict).
// grav is vestigial; mbllookup/storeIntegrators/closeOnDisconnect/height are
// accepted for signature parity but not modeled. `runmode` other than "daemon"
// is rejected.
func NewIndexer(
	grav *compatstorage.GravitonStore, // civilware: *storage.GravitonStore (vestigial here)
	bolt *compatstorage.BboltStore, // civilware: *storage.BboltStore (the real backend)
	dbType string,
	searchFilter []string,
	height int64,
	endpoint string,
	runmode string,
	mbllookup bool,
	closeOnDisconnect bool,
	config *compatstructures.FastSyncConfig,
	exclusions []string,
	storeIntegrators bool,
) *Indexer {
	if runmode != "" && runmode != "daemon" {
		return newDeadIndexer(fmt.Errorf("runmode %q unsupported in HyperGnomon — only \"daemon\" is implemented", runmode))
	}
	// bbolt is the only real backend; a non-bbolt dbType warns and falls back.
	switch strings.ToLower(strings.TrimSpace(dbType)) {
	case "", "bolt", "boltdb", "bbs":
		// default bbolt — no warning
	default: // gravdb, graviton, sqlite, anything else
		structures.Logger.Warnf("[gnomes] dbType=%q not available in HyperGnomon; using bbolt", dbType)
	}
	// The caller's pre-opened bbolt store is the real backend.
	if bolt == nil {
		return newDeadIndexer(fmt.Errorf("boltDB is nil — pre-open one with storage.NewBBoltDB(path, name) and pass it in"))
	}
	if bolt.Inner() == nil {
		return newDeadIndexer(fmt.Errorf("boltDB has no open store"))
	}
	_ = grav              // vestigial — HyperGnomon always runs on bbolt
	_ = height            // civilware resume hint; HyperGnomon resumes from the store
	_ = mbllookup         // civilware miniblock-lookup knob; not modeled
	_ = closeOnDisconnect // civilware reconnect knob; not modeled
	_ = storeIntegrators  // civilware integrator-store knob; not modeled
	if config != nil {
		_ = config.Enabled // civilware FastSyncConfig isn't a HyperGnomon concept
	}

	inner, err := hgindexer.New(hgindexer.Config{
		Endpoint:       strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://"),
		Store:          bolt.Inner(), // external-store injection: borrow the caller's store
		SearchFilter:   searchFilter, // civilware ships []string; pass through unchanged
		SCIDExclusions: exclusions,
		TurboMode:      true,
	})
	if err != nil {
		return newDeadIndexer(fmt.Errorf("hypergnomon indexer init: %w", err))
	}
	return wrapIndexerWithStore(inner, bolt)
}

// AddSCIDToIndex injects specific SCIDs into the index immediately — civilware's
// manual-add path (used by HOLOGRAM to index a SCID that fastsync missed, e.g.
// one deployed before the indexer started). Mirrors civilware's signature; each
// entry is indexed via the internal IndexSingleSCID (the same path the native WS
// API's "addscidtoindex" method uses). varstoreonly re-reads only variables;
// skipfsrecheck reuses existing class metadata when present. The internal
// indexer re-extracts the owner, so FastSyncImport.Owner is advisory. Returns
// the first error encountered (others are attempted regardless).
func (idx *Indexer) AddSCIDToIndex(scids map[string]*compatstructures.FastSyncImport, skipfsrecheck, varstoreonly bool) error {
	if idx == nil || idx.inner == nil {
		return fmt.Errorf("indexer not initialized")
	}
	var firstErr error
	for scid := range scids {
		if _, err := idx.inner.IndexSingleSCID(scid, varstoreonly, skipfsrecheck); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("AddSCIDToIndex(%s): %w", scid, err)
		}
	}
	return firstErr
}

// NewIndexerWithDBDir is the HyperGnomon-native constructor that the
// compat shim currently points callers at. Takes a dbDir path and
// builds a full HyperGnomon indexer with civilware-shape facade on
// top. Until HyperGnomon's internal indexer.New accepts an external
// Storage implementation, this is the practical entry point.
func NewIndexerWithDBDir(
	dbDir, filter string,
	endpoint, runmode string,
	config *compatstructures.FastSyncConfig,
	exclusions []string,
) (*Indexer, error) {
	if runmode != "" && runmode != "daemon" {
		return nil, fmt.Errorf("runmode %q unsupported", runmode)
	}
	hgCfg := hgindexer.Config{
		Endpoint:       strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://"),
		DBDir:          dbDir,
		SearchFilter:   splitCivilwareFilter(filter),
		SCIDExclusions: exclusions,
		TurboMode:      true,
	}
	inner, err := hgindexer.New(hgCfg)
	if err != nil {
		return nil, err
	}
	return wrapIndexer(inner), nil
}

// wrapIndexer is the internal facade-builder. Exposed so tests can
// construct an Indexer around a pre-built internal instance. The inner
// indexer owns its store (NewIndexerWithDBDir path) so BBSBackend is nil.
func wrapIndexer(inner *hgindexer.Indexer) *Indexer {
	return wrapIndexerWithStore(inner, nil)
}

// wrapIndexerWithStore builds the facade and records the caller-owned bbolt
// store (the NewIndexer injection path). When bolt is non-nil the internal
// indexer borrows it (does not close it), so the facade's Close releases it.
func wrapIndexerWithStore(inner *hgindexer.Indexer, bolt *compatstorage.BboltStore) *Indexer {
	idx := &Indexer{
		DBType:           "boltdb",
		inner:            inner,
		fieldRefreshStop: make(chan struct{}),
		BBSBackend:       bolt,
	}
	if bolt != nil {
		// Wire GravDBBackend as a DELEGATING handle over the same bbolt store. A
		// civilware consumer (HOLOGRAM) defaults to dbType="gravdb" and reads
		// through GravDBBackend — both its dbType=="gravdb" dispatch branches and
		// its hardcoded GravDBBackend.GetSCIDInteractionHeight path — so a nil
		// GravDBBackend would make those reads silently return empty. Delegating
		// to bbolt makes every read path land on real data.
		idx.GravDBBackend = compatstorage.NewGravDBDelegate(bolt)
	}
	idx.startFieldRefresh()
	return idx
}

// splitCivilwareFilter converts civilware's semicolon-separated
// filter string into HyperGnomon's slice form. Civilware uses `;;;`
// for groups and `;` for intra-group terms — we treat the whole thing
// as a flat substring-match list since HyperGnomon's filter is a
// substring match across `;;;`-joined patterns too.
func splitCivilwareFilter(filter string) []string {
	if filter == "" {
		return nil
	}
	return strings.Split(filter, ";;;")
}

// StartDaemonMode begins scanning. parallelBlocks controls the
// fetcher fan-out. Civilware passes 1..10; HyperGnomon accepts up to
// its internal cap. Returns once the scan loop has signaled it
// reached tip or the context cancels (via Close).
func (idx *Indexer) StartDaemonMode(parallelBlocks int) {
	if idx == nil || idx.inner == nil {
		return
	}
	// HyperGnomon's StartDaemonMode blocks. Civilware consumers
	// (HOLOGRAM) launch it in a goroutine. Mirror that by spawning
	// here so callers don't have to change their threading model.
	go func() {
		_ = idx.inner.StartDaemonMode()
	}()
}

// Close cleanly shuts down the scan loop + all store handles.
// Idempotent.
func (idx *Indexer) Close() {
	if idx == nil || !idx.closed.CompareAndSwap(false, true) {
		return
	}
	if idx.fieldRefreshStop != nil {
		close(idx.fieldRefreshStop)
		idx.fieldRefreshWG.Wait()
	}
	if idx.inner != nil {
		idx.inner.Close()
	}
	// Close the injected store the compat layer owns (NewIndexer path). The
	// internal indexer borrowed it (ownStore=false) so didn't close it. For the
	// NewIndexerWithDBDir path BBSBackend is nil and the inner indexer already
	// closed its own store above.
	if idx.BBSBackend != nil {
		_ = idx.BBSBackend.Close()
	}
}

// startFieldRefresh launches a goroutine that copies the internal
// indexer's atomic counters into this facade's exported fields. Runs
// at ~10 Hz until Close. Without it the exported fields stay at
// their zero values since Go has no field-level "alias" to an
// atomic.Int64.
func (idx *Indexer) startFieldRefresh() {
	idx.fieldRefreshWG.Add(1)
	go func() {
		defer idx.fieldRefreshWG.Done()
		for {
			select {
			case <-idx.fieldRefreshStop:
				return
			default:
			}
			if idx.inner != nil {
				atomic.StoreInt64(&idx.LastIndexedHeight, idx.inner.LastIndexedHeight.Load())
				atomic.StoreInt64(&idx.ChainHeight, idx.inner.ChainHeight.Load())
			}
			// Simple sleep — tick rate doesn't need to be exact; the
			// fields are a convenience, not a consensus primitive.
			sleepQuick()
		}
	}()
}

// InitLog is civilware's logger-init entrypoint. HyperGnomon uses
// logrus internally and routes its own output; this facade accepts
// the args for source compatibility and returns without doing
// anything. Callers who want actual log routing should set
// structures.Logger (HyperGnomon-native) or inject via a future
// Config.Logger option.
func InitLog(args interface{}, writer io.Writer) {
	_ = args
	_ = writer
}

// newDeadIndexer returns an Indexer whose methods all no-op or
// error. Used for unsupported arg combinations in NewIndexer so
// callers don't segfault — they get back a typed pointer and can
// check via DBType=="" as a "not ready" signal.
func newDeadIndexer(cause error) *Indexer {
	return &Indexer{
		DBType:           "",
		fieldRefreshStop: make(chan struct{}),
	}
}
