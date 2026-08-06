package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	rpprof "runtime/pprof"
	"strings"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/hypergnomon/hypergnomon/api"
	"github.com/hypergnomon/hypergnomon/eventbus"
	"github.com/hypergnomon/hypergnomon/indexer"
	hgrpc "github.com/hypergnomon/hypergnomon/rpc"
	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

// apiDrainTimeout caps how long in-flight HTTP requests get to finish during
// shutdown. It does not cap the whole teardown: Stop still joins its
// background loops afterwards so the pool and store outlive their last reader.
const apiDrainTimeout = 10 * time.Second

func main() {
	// Operator subcommands run before flag parsing so `hypergnomon resync`
	// doesn't trip on a flag walker expecting only known flags.
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "resync":
			runResync(os.Args[2:])
			return
		case "clean":
			runClean(os.Args[2:])
			return
		case "compact":
			runCompact(os.Args[2:])
			return
		case "help", "-h", "--help":
			// Fall through to normal flag.Parse so `-h` prints the full flag list.
		}
	}

	startTime := time.Now()

	// CLI flags
	endpoint := flag.String("daemon-rpc-address", "127.0.0.1:10102", "DERO daemon RPC address")
	dbDir := flag.String("db-dir", "gnomondb", "Database directory")
	storageBackend := flag.String("storage-backend", "bbolt", "Storage backend: bbolt (default, shipped) | sqlite (planned) | graviton (unsupported)")
	searchFilter := flag.String("search-filter", "", "SC code search filter (;;; separated)")
	scidExclusions := flag.String("sf-scid-exclusions", "", "SCIDs to exclude (;;; separated)")
	parallelBlocks := flag.Int("num-parallel-blocks", structures.DefaultParallelBlocks, "Number of blocks to fetch in parallel")
	batchSize := flag.Int("batch-size", structures.DefaultBatchSize, "Blocks per DB flush")
	classifyProbeBatchSize := flag.Int("classify-probe-batch-size", structures.DefaultClassifyProbeBatchSize, "SCIDs per phase-1 classify GetSC(code=true) RPC batch")
	poolSize := flag.Int("rpc-pool-size", structures.DefaultPoolSize, "RPC connection pool size")
	rpcCompression := flag.Bool("rpc-compression", true, "Enable daemon WebSocket compression on new RPC connections")
	apiAddress := flag.String("api-address", "127.0.0.1:8082", "HTTP API listen address")
	wsAddress := flag.String("ws-address", "127.0.0.1:9190", "WebSocket server address")
	telaCacheMB := flag.Int64("tela-cache-mb", 128, "TELA content in-memory cache cap (MB)")
	mediaFetch := flag.Bool("media-fetch", false, "Enable on-demand fetching of asset media (images/audio/video) into the local cache; off = /api/media serves only what cmd/mediawarm pre-cached")
	mediaDir := flag.String("media-dir", "", "Asset media cache directory (empty = <db-dir>/media)")
	ipfsGateway := flag.String("ipfs-gateway", "", "Local IPFS (kubo) gateway base URL for media fetches, e.g. http://127.0.0.1:18080 (empty = public gateways only)")
	telaVerifySigs := flag.Bool("tela-verify-sigs", false, "Enable X-TELA-Verify response header on /tela/... endpoints (v1.0 reports signature presence only; cryptographic verification ships in v1.2)")
	// --persist-install-code: which SC classes get their install-time code
	// persisted to the sccode bucket. "tela" (default) persists only
	// TELA-INDEX-1 / TELA-DOC-1 / TELA-MOD-1 — the classes whose content
	// server + GetInitialSCIDCode consumers actually read. "none" disables
	// forward-populate entirely (lazy-fill on each read). "all" matches
	// the pre-class-aware behavior (grows mainnet DB by ~134 MB).
	//
	// The legacy --skip-tela-doc-code flag was removed in favor of this
	// policy flag; its old behavior maps to "none".
	codePolicy := flag.String("persist-install-code", "tela", "sccode persistence: none|tela|all (default tela — only TELA-{INDEX,DOC,MOD}-1 codes persisted)")
	fastsync := flag.Bool("fastsync", false, "Enable fastsync from GnomonSC")
	testnet := flag.Bool("testnet", false, "Use testnet GnomonSC SCID")
	memLimit := flag.Int64("mem-limit", 0, "GOMEMLIMIT in bytes (0 = unset)")
	pprofAddr := flag.String("pprof-address", "", "pprof HTTP address (e.g. 127.0.0.1:6060, empty=disabled)")
	// Named to match `go test -cpuprofile` / pprof muscle memory — the one
	// deliberate exception to this file's kebab-case flag convention.
	cpuProfile := flag.String("cpuprofile", "", "write CPU profile to file, flushed on shutdown (empty=disabled; source for refreshing cmd/hypergnomon/default.pgo)")
	debugMode := flag.Bool("debug", false, "Enable debug logging")
	// --turbo defaults to true. Non-turbo is now a diagnostic / replay mode:
	// it performs the slower per-SCID GetSC pass that predates the registry+probe
	// discovery flow. The non-turbo FastSync path now also launches probeTELA
	// so `--tela-only` + `--turbo=false` no longer hangs on TELACount==0.
	turboMode := flag.Bool("turbo", true, "Turbo scan: skip SC variable fetching during initial sync (default true; --turbo=false for slow diagnostic replay)")
	postScanVars := flag.String("postscan-vars", structures.DefaultPostScanVarsMode, "Turbo post-scan variable policy: lazy|all (lazy skips the all-SCID variable sweep)")
	segmentSync := flag.Bool("segment-sync", false, "Use parallel segment sync for initial chain scan")
	adaptBatch := flag.Bool("adapt-batch", true, "Auto-tune batch size based on block density")
	recentBlocks := flag.Int64("recent-blocks", 0, "Scan only last N blocks from chain tip (0 = scan all)")
	telaOnly := flag.Bool("tela-only", false, "Only discover TELA apps, then exit (no chain scanning)")
	timing := flag.Bool("timing", false, "Emit per-stage timing summaries (grouped fetcher/processor/flusher)")
	timingEvery := flag.Int("timing-every", 10, "How many batches between timing summaries")
	classifySeedCacheDir := flag.String("classify-seed-cache-dir", "", "Cross-DB classify seed cache directory (empty = OS user cache)")
	flag.Parse()

	// Validate the storage backend up front so an unsupported/unimplemented
	// choice fails fast with a clear message, rather than surfacing as a
	// generic failure inside the daemon-connection retry loop below.
	if err := storage.ValidateBackend(*storageBackend); err != nil {
		fmt.Fprintf(os.Stderr, "storage backend: %v\n", err)
		os.Exit(2)
	}

	// Start pprof server if requested. Handlers are registered on a
	// dedicated mux (not DefaultServeMux) so profiling is only ever exposed
	// on the operator-chosen --pprof address, and the server sets a header
	// timeout so a slow client can't hold connections open indefinitely.
	if *pprofAddr != "" {
		go func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/debug/pprof/", pprof.Index)
			mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
			mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
			mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
			mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
			srv := &http.Server{
				Addr:              *pprofAddr,
				Handler:           mux,
				ReadHeaderTimeout: 5 * time.Second,
			}
			structures.Logger.Infof("pprof listening on %s", *pprofAddr)
			if err := srv.ListenAndServe(); err != nil {
				structures.Logger.Errorf("pprof server exited: %v", err)
			}
		}()
	}

	// Configure logging
	structures.Logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	if *debugMode {
		structures.Logger.SetLevel(logrus.DebugLevel)
	} else {
		structures.Logger.SetLevel(logrus.InfoLevel)
	}

	// --tela-only exits when TELA discovery settles, and discovery only runs
	// from the FastSync registry probe — without --fastsync it would wait
	// forever. Fail fast at startup instead.
	if *telaOnly && !*fastsync {
		structures.Logger.Fatalf("--tela-only requires --fastsync: TELA discovery runs from the GnomonSC registry probe")
	}

	// A non-zero indexer exit has to travel past main's cleanup defers, so it
	// goes through a variable rather than a Fatalf that would skip them.
	// Registered first, hence run last: everything else has already flushed.
	exitCode := 0
	defer func() {
		if exitCode != 0 {
			os.Exit(exitCode)
		}
	}()

	// Whole-run CPU profile (the PGO refresh source for default.pgo). Flushed
	// by the deferred stop on every clean exit path: --tela-only's return and
	// the normal daemon shutdown (Ctrl+C flips Closing, scanLoop exits, main
	// returns). Fatalf/os.Exit paths skip defers and lose the profile: the
	// startup ones cost at most an almost-empty file, but abandoning the
	// drain with a second signal discards a whole run's profile.
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			structures.Logger.Fatalf("--cpuprofile: create %s: %v", *cpuProfile, err)
		}
		if err := rpprof.StartCPUProfile(f); err != nil {
			structures.Logger.Fatalf("--cpuprofile: start: %v", err)
		}
		structures.Logger.Infof("CPU profiling to %s", *cpuProfile)
		defer f.Close()
		defer rpprof.StopCPUProfile()
	}

	// Set GOMEMLIMIT for GC optimization
	if *memLimit > 0 {
		debug.SetMemoryLimit(*memLimit)
		structures.Logger.Infof("GOMEMLIMIT set to %d bytes", *memLimit)
	}
	hgrpc.SetWebSocketCompression(*rpcCompression)
	normalizedPostScanVars := strings.ToLower(strings.TrimSpace(*postScanVars))
	if normalizedPostScanVars == "" {
		normalizedPostScanVars = structures.DefaultPostScanVarsMode
	}
	if normalizedPostScanVars != indexer.PostScanVarsLazy && normalizedPostScanVars != indexer.PostScanVarsAll {
		structures.Logger.Fatalf("invalid --postscan-vars=%q (want lazy or all)", *postScanVars)
	}

	// Print banner
	fmt.Printf("\n  %s v%s\n", structures.AppName, structures.Version)
	fmt.Printf("  Arena-Accelerated DERO Blockchain Scanner | Turbo: %v | PostScanVars: %s | Adapt: %v\n", *turboMode, normalizedPostScanVars, *adaptBatch)
	fmt.Printf("  Endpoint: %s | Parallel: %d | Batch: %d | Pool: %d\n",
		*endpoint, *parallelBlocks, *batchSize, *poolSize)
	fmt.Printf("  API: %s | WS: %s | FastSync: %v | Testnet: %v\n\n",
		*apiAddress, *wsAddress, *fastsync, *testnet)

	// Parse search filters
	var filters []string
	if *searchFilter != "" {
		filters = strings.Split(*searchFilter, ";;;")
	}
	var exclusions []string
	if *scidExclusions != "" {
		exclusions = strings.Split(*scidExclusions, ";;;")
	}

	// Try to connect: configured endpoint → fallback nodes → interactive prompt
	fallbackNodes := []string{
		*endpoint,
		"203.0.113.10:10102",
		"node.derofoundation.org:11012",
		"example-pool.invalid:10102",
	}
	// Deduplicate (if user's endpoint matches a fallback)
	seen := map[string]bool{}
	unique := []string{}
	for _, n := range fallbackNodes {
		if !seen[n] {
			seen[n] = true
			unique = append(unique, n)
		}
	}

	// Event bus for subscription fan-out (DESIGN.md M1). One per process.
	// Started before Indexer so New() can take it via Config.
	bus := eventbus.New(1024)
	go bus.Run()
	defer bus.Close()

	var idx *indexer.Indexer
	var err error
	var connectedEndpoint string
	for _, node := range unique {
		fmt.Printf("  Trying %s ...", node)
		idx, err = indexer.New(indexer.Config{
			Endpoint:               node,
			DBDir:                  *dbDir,
			Backend:                *storageBackend,
			SearchFilter:           filters,
			SCIDExclusions:         exclusions,
			ParallelBlocks:         *parallelBlocks,
			BatchSize:              *batchSize,
			ClassifyProbeBatchSize: *classifyProbeBatchSize,
			PoolSize:               *poolSize,
			TurboMode:              *turboMode,
			PostScanVarsMode:       normalizedPostScanVars,
			AdaptBatchSize:         *adaptBatch,
			RecentBlocks:           *recentBlocks,
			Timing:                 *timing,
			TimingEvery:            *timingEvery,
			Bus:                    bus,
			CodePolicy:             *codePolicy,
			ClassifySeedCacheDir:   *classifySeedCacheDir,
		})
		if err == nil {
			connectedEndpoint = node
			fmt.Printf(" connected!\n")
			break
		}
		fmt.Printf(" failed\n")
	}

	// If all fallbacks failed, prompt the user
	for idx == nil {
		fmt.Println("\n  Could not connect to any DERO daemon.")
		fmt.Println("  Enter a daemon address (e.g. 127.0.0.1:10102) or 'quit' to exit:")
		fmt.Print("  > ")
		scanner := bufio.NewScanner(os.Stdin)
		if !scanner.Scan() {
			os.Exit(1)
		}
		input := strings.TrimSpace(scanner.Text())
		if input == "quit" || input == "exit" || input == "q" {
			fmt.Println("  Goodbye.")
			os.Exit(0)
		}
		if input == "" {
			continue
		}
		fmt.Printf("  Trying %s ...", input)
		idx, err = indexer.New(indexer.Config{
			Endpoint:               input,
			DBDir:                  *dbDir,
			Backend:                *storageBackend,
			SearchFilter:           filters,
			SCIDExclusions:         exclusions,
			ParallelBlocks:         *parallelBlocks,
			BatchSize:              *batchSize,
			ClassifyProbeBatchSize: *classifyProbeBatchSize,
			PoolSize:               *poolSize,
			TurboMode:              *turboMode,
			AdaptBatchSize:         *adaptBatch,
			RecentBlocks:           *recentBlocks,
			Timing:                 *timing,
			TimingEvery:            *timingEvery,
			Bus:                    bus,
			CodePolicy:             *codePolicy,
			ClassifySeedCacheDir:   *classifySeedCacheDir,
		})
		if err == nil {
			connectedEndpoint = input
			fmt.Printf(" connected!\n")
		} else {
			fmt.Printf(" failed: %v\n", err)
		}
	}
	_ = connectedEndpoint

	// FastSync: bulk-import validated SCIDs from GnomonSC before normal indexing
	if *fastsync {
		structures.Logger.Info("FastSync enabled, syncing from GnomonSC...")
		if err := idx.FastSync(*testnet); err != nil {
			if *telaOnly {
				// Discovery can never settle after a failed FastSync — exit
				// with the error instead of waiting forever below.
				structures.Logger.Fatalf("FastSync failed; --tela-only cannot discover apps: %v", err)
			}
			structures.Logger.Errorf("FastSync failed (continuing with normal sync): %v", err)
		} else {
			structures.Logger.Info("FastSync complete")
		}
	}

	// Segment parallel sync for initial chain scan
	if *segmentSync {
		structures.Logger.Info("Starting parallel segment sync...")
		ss := &indexer.SegmentSync{
			Endpoint:       *endpoint,
			MainStore:      idx.Store,
			SearchFilter:   filters,
			SCIDExclusions: idx.SCIDExclusions,
			DBDir:          *dbDir,
			SegmentSize:    10000,
		}
		// Get chain height for segment sync range
		var chainHeight int64
		if err := idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
			info, err := c.GetInfo()
			if err != nil {
				return err
			}
			chainHeight = info.TopoHeight
			return nil
		}); err != nil {
			structures.Logger.Errorf("segment sync GetInfo: %v", err)
		}
		if chainHeight > 0 {
			lastHeight, _ := idx.Store.GetLastIndexHeight()
			if lastHeight < chainHeight {
				if err := ss.Run(lastHeight+1, chainHeight); err != nil {
					structures.Logger.Errorf("Segment sync failed: %v", err)
				}
			}
		}
	}

	// Graceful shutdown. The signal only flips Closing, which drains the scan
	// pipeline; releasing the RPC pool and store is left to the deferred
	// teardown below, which runs once the API server can no longer touch
	// them. A second signal abandons the drain.
	sigChan := make(chan os.Signal, 2)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		structures.Logger.Info("Shutting down... (signal again to exit without draining)")
		idx.Closing.Store(true)
		<-sigChan
		structures.Logger.Warn("Second signal: exiting without draining")
		os.Exit(1)
	}()

	// Start API servers (deferred until after fastsync/segment-sync complete).
	// &idx.SafeHeight / &idx.ReorgDetected give the api package a live read of
	// the finality-lag height and the reorg-detection counter.
	apiServer := api.NewServer(idx.Store, idx.RPCPool, *apiAddress, &idx.SafeHeight, &idx.ReorgDetected, bus, idx, (*telaCacheMB)*1024*1024)
	apiServer.SetTELAVerifySigs(*telaVerifySigs)
	mDir := *mediaDir
	if mDir == "" {
		mDir = filepath.Join(*dbDir, "media")
	}
	apiServer.SetMediaOptions(mDir, *mediaFetch, *ipfsGateway)

	// Teardown runs in dependency order: drain HTTP and join its background
	// loops first, then release the pool and store those loops and handlers
	// read, then the bus. Registered after `defer bus.Close()`, so LIFO puts
	// it ahead of the bus.
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), apiDrainTimeout)
		defer cancel()
		if err := apiServer.Stop(ctx); err != nil {
			structures.Logger.Warnf("HTTP API shutdown: %v", err)
		}
		idx.Close()
	}()

	go func() {
		if err := apiServer.Start(); err != nil {
			structures.Logger.Errorf("HTTP API server error: %v", err)
		}
	}()

	wsServer := api.NewWSServer(*wsAddress, idx.Store, &idx.SafeHeight, bus, idx)
	go func() {
		if err := wsServer.Start(); err != nil {
			structures.Logger.Errorf("WebSocket server error: %v", err)
		}
	}()

	// TELA-only mode: discover TELA apps and exit without chain scanning.
	// TELAProbeSettled flips only once discovery results are durable — after
	// the probe's cache/seed saves, or after a cache hit's delta probe has
	// flushed any newly-deployed SCIDs — so no grace sleep is needed.
	if *telaOnly {
		for !structures.TELAProbeSettled.Load() {
			time.Sleep(100 * time.Millisecond)
		}
		structures.Logger.Infof("TELA-only mode: %d apps discovered. Exiting.", structures.TELACount.Load())
		return
	}

	// Start indexing
	structures.Logger.Infof("Startup complete in %s", time.Since(startTime).Round(time.Millisecond))
	if err := idx.StartDaemonMode(); err != nil {
		structures.Logger.Errorf("Indexer error: %v", err)
		exitCode = 1
	}
}

// runResync implements `hypergnomon resync [--db-dir=...]`: drops every data
// bucket so the next normal start rescans from height 0. Preserves block hash
// history for reorg-detection replay.
func runResync(args []string) {
	fs := flag.NewFlagSet("resync", flag.ExitOnError)
	dbDir := fs.String("db-dir", "gnomondb", "Database directory")
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}

	// NOTE: resync (like clean) is bbolt-only maintenance tooling — it bypasses
	// the storage.Open factory and hardwires the bbolt store. If a second backend
	// ever ships, route this through storage.Open(backend, *dbDir, "") so it
	// honors --storage-backend instead of silently resetting the wrong store.
	store, err := storage.NewBboltStore(*dbDir, "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "open db: %v\n", err)
		os.Exit(1)
	}
	defer store.Close()

	if err := store.ResetIndex(); err != nil {
		fmt.Fprintf(os.Stderr, "reset index: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("index reset: %s\n", *dbDir)
}

// runCompact implements `hypergnomon compact [--db-dir=...]`: rewrites the bbolt
// store dropping free/fragmented pages (bbolt.Compact), reclaiming disk and
// improving page locality. The indexer must be stopped — the store is locked
// while running, so this fails fast if it is in use. The original is kept as
// HYPERGNOMON.db.bak for rollback.
func runCompact(args []string) {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)
	dbDir := fs.String("db-dir", "gnomondb", "Database directory")
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}

	// NOTE: bbolt-only maintenance (like resync/clean) — bypasses the storage.Open
	// factory. Route through the factory if a second backend ever ships.
	const txMaxSize = 64 << 20 // bound the per-transaction copy during compaction
	res, err := storage.CompactBbolt(*dbDir, txMaxSize)
	if err != nil {
		fmt.Fprintf(os.Stderr, "compact: %v\n", err)
		os.Exit(1)
	}

	pct := 0.0
	if res.Before > 0 {
		pct = 100 * float64(res.Before-res.After) / float64(res.Before)
	}
	fmt.Printf("compacted %s: %.1f MiB -> %.1f MiB (reclaimed %.1f%%)\n",
		filepath.Join(*dbDir, "HYPERGNOMON.db"),
		float64(res.Before)/1048576, float64(res.After)/1048576, pct)
	fmt.Printf("original kept at %s — delete it once the compacted DB starts cleanly\n", res.BackupPath)
}

// runClean implements `hypergnomon clean <network> [--force]`. network names
// are used only for log clarity; the real action is `os.RemoveAll(--db-dir)`.
// mainnet is refused without --force so fat-fingered invocations can't wipe
// a long-running mainnet index.
func runClean(args []string) {
	fs := flag.NewFlagSet("clean", flag.ExitOnError)
	dbDir := fs.String("db-dir", "gnomondb", "Database directory")
	force := fs.Bool("force", false, "Required to clean a mainnet DB directory")
	if err := fs.Parse(args); err != nil {
		os.Exit(2)
	}
	network := "mainnet"
	if fs.NArg() > 0 {
		network = fs.Arg(0)
	}
	switch network {
	case "mainnet":
		if !*force {
			fmt.Fprintln(os.Stderr, "refusing to clean mainnet without --force")
			os.Exit(1)
		}
	case "testnet", "simulator":
		// no confirmation gate — these are transient networks
	default:
		fmt.Fprintf(os.Stderr, "unknown network %q (expected mainnet|testnet|simulator)\n", network)
		os.Exit(2)
	}

	abs, err := filepath.Abs(*dbDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve db-dir: %v\n", err)
		os.Exit(1)
	}
	if err := os.RemoveAll(abs); err != nil {
		fmt.Fprintf(os.Stderr, "remove %s: %v\n", abs, err)
		os.Exit(1)
	}
	fmt.Printf("cleaned %s (%s)\n", abs, network)
}
