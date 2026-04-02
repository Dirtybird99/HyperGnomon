package main

import (
	"bufio"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/hypergnomon/hypergnomon/api"
	"github.com/hypergnomon/hypergnomon/indexer"
	hgrpc "github.com/hypergnomon/hypergnomon/rpc"
	"github.com/hypergnomon/hypergnomon/structures"
	"github.com/sirupsen/logrus"
)

func main() {
	startTime := time.Now()

	// CLI flags
	endpoint := flag.String("daemon-rpc-address", "127.0.0.1:10102", "DERO daemon RPC address")
	dbDir := flag.String("db-dir", "gnomondb", "Database directory")
	searchFilter := flag.String("search-filter", "", "SC code search filter (;;; separated)")
	scidExclusions := flag.String("sf-scid-exclusions", "", "SCIDs to exclude (;;; separated)")
	parallelBlocks := flag.Int("num-parallel-blocks", structures.DefaultParallelBlocks, "Number of blocks to fetch in parallel")
	batchSize := flag.Int("batch-size", structures.DefaultBatchSize, "Blocks per DB flush")
	poolSize := flag.Int("rpc-pool-size", structures.DefaultPoolSize, "RPC connection pool size")
	apiAddress := flag.String("api-address", "127.0.0.1:8082", "HTTP API listen address")
	wsAddress := flag.String("ws-address", "127.0.0.1:9190", "WebSocket server address")
	fastsync := flag.Bool("fastsync", false, "Enable fastsync from GnomonSC")
	testnet := flag.Bool("testnet", false, "Use testnet GnomonSC SCID")
	memLimit := flag.Int64("mem-limit", 0, "GOMEMLIMIT in bytes (0 = auto)")
	pprofAddr := flag.String("pprof-address", "", "pprof HTTP address (e.g. 127.0.0.1:6060, empty=disabled)")
	debugMode := flag.Bool("debug", false, "Enable debug logging")
	turboMode := flag.Bool("turbo", false, "Turbo scan: skip SC variable fetching during initial sync")
	segmentSync := flag.Bool("segment-sync", false, "Use parallel segment sync for initial chain scan")
	adaptBatch := flag.Bool("adapt-batch", true, "Auto-tune batch size based on block density")
	recentBlocks := flag.Int64("recent-blocks", 0, "Scan only last N blocks from chain tip (0 = scan all)")
	telaOnly := flag.Bool("tela-only", false, "Only discover TELA apps, then exit (no chain scanning)")
	flag.Parse()

	// Start pprof server if requested
	if *pprofAddr != "" {
		go func() {
			structures.Logger.Infof("pprof listening on %s", *pprofAddr)
			http.ListenAndServe(*pprofAddr, nil)
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

	// Print banner
	fmt.Printf("\n  %s v%s\n", structures.AppName, structures.Version)
	fmt.Printf("  Arena-Accelerated DERO Blockchain Scanner | Turbo: %v | Adapt: %v\n", *turboMode, *adaptBatch)
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

	var idx *indexer.Indexer
	var err error
	var connectedEndpoint string
	for _, node := range unique {
		fmt.Printf("  Trying %s ...", node)
		idx, err = indexer.New(indexer.Config{
			Endpoint:       node,
			DBDir:          *dbDir,
			SearchFilter:   filters,
			SCIDExclusions: exclusions,
			ParallelBlocks: *parallelBlocks,
			BatchSize:      *batchSize,
			PoolSize:       *poolSize,
			TurboMode:      *turboMode,
			AdaptBatchSize: *adaptBatch,
			RecentBlocks:   *recentBlocks,
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
			Endpoint:       input,
			DBDir:          *dbDir,
			SearchFilter:   filters,
			SCIDExclusions: exclusions,
			ParallelBlocks: *parallelBlocks,
			BatchSize:      *batchSize,
			PoolSize:       *poolSize,
			TurboMode:      *turboMode,
			AdaptBatchSize: *adaptBatch,
			RecentBlocks:   *recentBlocks,
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
		idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
			info, err := c.GetInfo()
			if err != nil {
				return err
			}
			chainHeight = info.TopoHeight
			return nil
		})
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

	// Start API servers (deferred until after fastsync/segment-sync complete)
	apiServer := api.NewServer(idx.Store, idx.RPCPool, *apiAddress)
	go func() {
		if err := apiServer.Start(); err != nil {
			structures.Logger.Errorf("HTTP API server error: %v", err)
		}
	}()

	wsServer := api.NewWSServer(*wsAddress, idx.Store)
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
