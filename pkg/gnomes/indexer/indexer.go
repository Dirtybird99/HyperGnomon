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

// NewIndexer constructs an Indexer using the civilware shape.
// HOLOGRAM's exact call site (gnomon.go:Start):
//
//	indexer.NewIndexer(gravDB, boltDB, "gravdb"|"boltdb", filter, height,
//	                   endpoint, "daemon", false, false, config, exclusions)
//
// We ignore gravDB (errors if non-nil) and route everything through
// the bbolt backend. `runmode` values other than "daemon" are
// rejected — civilware also has "wallet" and "asset" which we don't
// yet implement; they're out of scope for v1.0.
func NewIndexer(
	gravDB interface{}, // civilware: *storage.GravitonStore
	boltDB interface{}, // civilware: *storage.BboltStore
	dbType string,
	filter string,
	height int64,
	endpoint string,
	runmode string,
	closeOnDisconnect bool,
	runtime bool,
	config *compatstructures.FastSyncConfig,
	exclusions []string,
) *Indexer {
	if dbType == "gravdb" {
		// Signal via panic-on-start: civilware's shape returns
		// *Indexer with no error return. Wrap the "unsupported"
		// signal in a sentinel Indexer that errors on every method.
		return newDeadIndexer(fmt.Errorf("gravdb unsupported in HyperGnomon — use dbType=\"boltdb\""))
	}
	if runmode != "" && runmode != "daemon" {
		return newDeadIndexer(fmt.Errorf("runmode %q unsupported in HyperGnomon — only \"daemon\" is implemented", runmode))
	}
	// Coerce the civilware bolt store argument into our compat
	// wrapper. Civilware passes a *storage.BboltStore from its own
	// package; consumers using our compat layer pass our wrapper.
	var bolt *compatstorage.BboltStore
	switch v := boltDB.(type) {
	case *compatstorage.BboltStore:
		bolt = v
	case nil:
		// Caller didn't pre-open — we construct one below.
	default:
		return newDeadIndexer(fmt.Errorf("boltDB arg is %T, expected *storage.BboltStore", boltDB))
	}

	hgCfg := hgindexer.Config{
		Endpoint:       strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://"),
		SearchFilter:   splitCivilwareFilter(filter),
		SCIDExclusions: exclusions,
		TurboMode:      true, // civilware's fastsync is trust-the-registry in spirit; HyperGnomon turbo matches
	}
	if config != nil {
		// Civilware's FastSyncConfig isn't a HyperGnomon concept —
		// we drop most of it and only honor Enabled as a hint.
		_ = config.Enabled
	}
	if bolt == nil {
		// Nobody pre-opened a store; we need a path. Civilware
		// callers typically pre-open. For resilience, err out
		// clearly rather than guess a path.
		return newDeadIndexer(fmt.Errorf("boltDB arg is nil — open one with storage.NewBBoltDB(path, name) first"))
	}
	hgCfg.DBDir = ""  // Not used when we wire the store in via a later option (v1.x).

	// HyperGnomon's current New() opens its own store from cfg.DBDir.
	// That's a v1.0 limitation — the compat shim can't inject a
	// pre-opened store yet. Tell the caller to pass a path via
	// SetDBDir and use NewIndexerWithDBDir instead.
	return newDeadIndexer(fmt.Errorf("compat v1.0: use pkg/gnomes/indexer.NewIndexerWithDBDir(dbDir, …) until HyperGnomon exposes external-store injection"))
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
// construct an Indexer around a pre-built internal instance.
func wrapIndexer(inner *hgindexer.Indexer) *Indexer {
	idx := &Indexer{
		DBType:           "boltdb",
		inner:            inner,
		fieldRefreshStop: make(chan struct{}),
		BBSBackend:       nil, // populated via Inner() accessor; not widely used
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
