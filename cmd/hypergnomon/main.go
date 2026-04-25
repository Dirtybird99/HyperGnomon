package main

import (
	"bufio"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
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
		case "help", "-h", "--help":
			// Fall through to normal flag.Parse so `-h` prints the full flag list.
		}
	}

	startTime := time.Now()

	// CLI flags
	endpoint := flag.String("daemon-rpc-address", "127.0.0.1:10102", "DERO daemon RPC address")
	dbDir := flag.String("db-dir", "gnomondb", "Database directory")
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
	telaVerifySigs := flag.Bool("tela-verify-sigs", false, "Enable X-TELA-Verify response header on /tela/... endpoints (v1.0 reports signature presence only; cryptographic verification ships in v1.1)")
	// --persist-install-code: which SC classes get their install-time code
	// persisted to the sccode bucket. "tela" (default) persists only
	// TELA-INDEX-1 / TELA-DOC-1 / TELA-MOD-1 — the classes whose content
	// server + GetInitialSCIDCode consumers actually read. "none" disables
	// forward-populate entirely (lazy-fill on each read). "all" matches
	// the pre-class-aware behavior (grows mainnet DB by ~134 MB).
	//
	// Legacy --skip-tela-doc-code still accepted and forces policy "none"
	// for TELA-DOC-1 — equivalent behavior is now "none" applied broadly.
	codePolicy := flag.String("persist-install-code", "tela", "sccode persistence: none|tela|all (default tela — only TELA-{INDEX,DOC,MOD}-1 codes persisted)")
	fastsync := flag.Bool("fastsync", false, "Enable fastsync from GnomonSC")
	testnet := flag.Bool("testnet", false, "Use testnet GnomonSC SCID")
	memLimit := flag.Int64("mem-limit", 0, "GOMEMLIMIT in bytes (0 = auto)")
	pprofAddr := flag.String("pprof-address", "", "pprof HTTP address (e.g. 127.0.0.1:6060, empty=disabled)")
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
	flag.Parse()

	// Start pprof server if requested
	if *pprofAddr != "" {
		go func() {
			structures.Logger.Infof("pprof listening on %s", *pprofAddr)
			if err := http.ListenAndServe(*pprofAddr, nil); err != nil {
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
		"192.168.2.251:10102",
		"node.derofoundation.org:11012",
		"community-pools.mysrv.cloud:10102",
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

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		structures.Logger.Info("Shutting down...")
		idx.Close()
	}()

	// Start API servers (deferred until after fastsync/segment-sync complete).
	// &idx.SafeHeight gives the api package a live read of the finality-lag
	// height without pulling in an indexer import.
	apiServer := api.NewServer(idx.Store, idx.RPCPool, *apiAddress, &idx.SafeHeight, bus, idx, (*telaCacheMB)*1024*1024)
	apiServer.SetTELAVerifySigs(*telaVerifySigs)
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

	// TELA-only mode: discover TELA apps and exit without chain scanning
	if *telaOnly {
		for structures.TELACount.Load() == 0 {
			time.Sleep(100 * time.Millisecond)
		}
		// Give probe a moment to finish writing cache
		time.Sleep(2 * time.Second)
		structures.Logger.Infof("TELA-only mode: %d apps discovered. Exiting.", structures.TELACount.Load())
		return
	}

	// Start indexing
	structures.Logger.Infof("Startup complete in %s", time.Since(startTime).Round(time.Millisecond))
	if err := idx.StartDaemonMode(); err != nil {
		structures.Logger.Fatalf("Indexer error: %v", err)
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
