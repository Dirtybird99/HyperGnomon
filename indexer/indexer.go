package indexer

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/deroproject/derohe/block"
	"github.com/deroproject/derohe/rpc"
	"github.com/deroproject/derohe/transaction"
	"github.com/sirupsen/logrus"

	hgpool "github.com/hypergnomon/hypergnomon/pool"
	hgrpc "github.com/hypergnomon/hypergnomon/rpc"
	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

var logger = logrus.WithField("pkg", "indexer")

// Indexer is the arena-optimized DERO blockchain scanner.
type Indexer struct {
	// Chain state
	LastIndexedHeight atomic.Int64
	ChainHeight       atomic.Int64

	// Configuration
	SearchFilter   []string
	SCIDExclusions map[string]struct{} // O(1) lookup, not linear scan
	ValidatedSCs   sync.Map            // concurrent map[string]struct{}
	Endpoint       string
	DBDir          string
	ParallelBlocks int
	BatchSize      int   // blocks per DB flush
	TurboMode      bool  // skip GetSC during scan, fetch variables post-scan
	AdaptBatchSize bool  // dynamically adjust batch size based on flush latency
	RecentBlocks   int64 // scan only last N blocks (0 = all)

	// Backends
	RPCPool *hgrpc.Pool
	Store   storage.Storage
	Closing atomic.Bool

	// syncedOnce ensures EnableSync + postScanVariableFetch run exactly once
	// after the initial fastsync catch-up, even if the caught-up condition
	// is detected multiple times.
	syncedOnce atomic.Bool
}

// Config holds indexer configuration.
type Config struct {
	Endpoint       string
	DBDir          string
	SearchFilter   []string
	SCIDExclusions []string
	ParallelBlocks int
	BatchSize      int
	PoolSize       int
	TurboMode      bool
	AdaptBatchSize bool
	RecentBlocks   int64 // scan only the last N blocks from chain tip (0 = scan all)
}

// New creates a new Indexer with the given configuration.
func New(cfg Config) (*Indexer, error) {
	if cfg.ParallelBlocks <= 0 {
		cfg.ParallelBlocks = structures.DefaultParallelBlocks
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = structures.DefaultBatchSize
	}
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = structures.DefaultPoolSize
	}

	// Build exclusion map (O(1) lookups vs O(n) linear scan)
	exclusions := make(map[string]struct{}, len(cfg.SCIDExclusions)+len(structures.DefaultExclusions))
	for _, scid := range cfg.SCIDExclusions {
		exclusions[scid] = struct{}{}
	}
	// Add default exclusions (large non-TELA contracts)
	for _, scid := range structures.DefaultExclusions {
		exclusions[scid] = struct{}{}
	}

	store, err := storage.NewBboltStore(cfg.DBDir, strings.Join(cfg.SearchFilter, ";;;"))
	if err != nil {
		return nil, fmt.Errorf("open store: %w", err)
	}

	// Create RPC connection pool early so API server can use it
	pool, err := hgrpc.NewPool(cfg.Endpoint, cfg.PoolSize)
	if err != nil {
		store.Close()
		return nil, fmt.Errorf("rpc pool: %w", err)
	}

	idx := &Indexer{
		SearchFilter:   cfg.SearchFilter,
		SCIDExclusions: exclusions,
		Endpoint:       cfg.Endpoint,
		DBDir:          cfg.DBDir,
		ParallelBlocks: cfg.ParallelBlocks,
		BatchSize:      cfg.BatchSize,
		TurboMode:      cfg.TurboMode,
		AdaptBatchSize: cfg.AdaptBatchSize,
		RecentBlocks:   cfg.RecentBlocks,
		Store:          store,
		RPCPool:        pool,
	}

	// Restore last indexed height
	lastHeight, err := store.GetLastIndexHeight()
	if err != nil {
		logger.Warnf("Could not read last index height: %v", err)
	}
	idx.LastIndexedHeight.Store(lastHeight)

	return idx, nil
}

// StartDaemonMode connects to the daemon and begins indexing.
func (idx *Indexer) StartDaemonMode() error {

	// Start background chain height monitor
	go idx.monitorChainHeight()

	// Wait for first height update
	for idx.ChainHeight.Load() == 0 {
		if idx.Closing.Load() {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Quick-scan: start from chain tip - N blocks instead of genesis
	if idx.RecentBlocks > 0 {
		skipTo := idx.ChainHeight.Load() - idx.RecentBlocks
		if skipTo > idx.LastIndexedHeight.Load() {
			idx.LastIndexedHeight.Store(skipTo)
			logger.Infof("Quick-scan: jumping to height %d (last %d blocks)", skipTo, idx.RecentBlocks)
		}
	}

	logger.Infof("Connected to %s | Chain height: %d | Last indexed: %d",
		idx.Endpoint, idx.ChainHeight.Load(), idx.LastIndexedHeight.Load())
	if idx.TurboMode {
		logger.Info("Turbo mode enabled: skipping GetSC during scan, variables fetched post-scan")
	}
	if idx.AdaptBatchSize {
		logger.Info("Adaptive batch sizing enabled")
	}

	// Main 3-stage pipeline (fetcher → processor → flusher)
	idx.scanLoop()
	return nil
}

// monitorChainHeight polls daemon for current chain height.
func (idx *Indexer) monitorChainHeight() {
	for !idx.Closing.Load() {
		err := idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
			info, err := c.GetInfo()
			if err != nil {
				return err
			}
			newHeight := info.TopoHeight
			oldHeight := idx.ChainHeight.Load()
			idx.ChainHeight.Store(newHeight)

			// Update TELA cache height whenever chain grows (keep cache fresh)
			if newHeight > oldHeight && oldHeight > 0 {
				if cached, err := loadTELACache(idx.DBDir); err == nil {
					cached.Height = newHeight
					saveTELACache(idx.DBDir, cached.IndexSCIDs, cached.DocSCIDs, newHeight)
				}
			}
			return nil
		})
		if err != nil {
			logger.Errorf("getInfo: %v", err)
		}
		// Adaptive polling: fast when caught up, slow during sync to reduce daemon load
		if idx.LastIndexedHeight.Load() >= idx.ChainHeight.Load()-10 {
			time.Sleep(500 * time.Millisecond) // caught up: fast polling
		} else {
			time.Sleep(5 * time.Second) // syncing: slow polling
		}
	}
}

// fetchedBatch carries raw block/TX data from the fetcher to the processor.
// Inspired by Kelvin (2504.06151) zero-copy pipeline stages: data flows
// through typed channels with no intermediate serialization.
type fetchedBatch struct {
	blockResults []*rpc.GetBlock_Result
	heights      []uint64
	fetchCount   int
	// Pre-collected TX data from the second RPC round trip
	txResult    *rpc.GetTransaction_Result
	allTxHashes []string
	// Per-block parsed info carried forward so processor skips re-parsing
	blocks []blockInfo
	// Registration TXs counted during block parsing (PoW prefix filter)
	regCount int64
}

// blockInfo holds parsed TX hashes for a single block height.
type blockInfo struct {
	height   int64
	txHashes []string
}

// processedBatch carries indexed data from the processor to the flusher.
type processedBatch struct {
	batch      *storage.WriteBatch
	newHeight  int64
	blockCount int
	// Timing for adaptive batch sizing
	batchStart time.Time
}

// scanLoop implements a 3-stage prefetch pipeline inspired by:
//   - Kelvin (arxiv 2504.06151): zero-copy pipeline stages with typed channels
//   - StreamDiffusion (arxiv 2312.12491): batch-streaming overlap
//
// Three concurrent goroutines connected by buffered channels:
//
//	[Fetcher] → fetchChan(cap:2) → [Processor] → flushChan(cap:2) → [Flusher]
//
// The fetcher issues RPC calls while the processor decodes the previous batch
// and the flusher writes the batch before that. At steady state all three
// stages run concurrently, tripling throughput vs the old sequential loop.
func (idx *Indexer) scanLoop() {
	fetchChan := make(chan *fetchedBatch, 2)
	flushChan := make(chan *processedBatch, 2)

	var wg sync.WaitGroup
	wg.Add(3)

	// --- Stage 1: Fetcher goroutine ---
	go func() {
		defer wg.Done()
		defer close(fetchChan)
		idx.fetcherLoop(fetchChan)
	}()

	// --- Stage 2: Processor goroutine ---
	go func() {
		defer wg.Done()
		defer close(flushChan)
		idx.processorLoop(fetchChan, flushChan)
	}()

	// --- Stage 3: Flusher goroutine ---
	go func() {
		defer wg.Done()
		idx.flusherLoop(flushChan)
	}()

	wg.Wait()
}

// fetcherLoop continuously fetches blocks and transactions, sending
// fetchedBatch values downstream. It owns all RPC I/O so the processor
// and flusher never block on the network.
func (idx *Indexer) fetcherLoop(out chan<- *fetchedBatch) {
	batchBlockCount := idx.ParallelBlocks
	caughtUp := false

	for !idx.Closing.Load() {
		lastHeight := idx.LastIndexedHeight.Load()
		chainHeight := idx.ChainHeight.Load()

		// --- Speculative prefetch when caught up ---
		if lastHeight >= chainHeight {
			if !caughtUp {
				caughtUp = true
				// Signal downstream that we reached tip (processor handles sync/turbo)
				out <- &fetchedBatch{fetchCount: 0}
			}
			// Try speculative fetch of the next block
			var result *rpc.GetBlock_Result
			err := idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
				var e error
				result, e = c.GetBlockByHeight(uint64(lastHeight + 1))
				return e
			})
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			// Got a new block! Build a single-block fetchedBatch.
			h := uint64(lastHeight + 1)
			fb := idx.fetchSingleBlock(result, h)
			if fb != nil {
				caughtUp = false
				out <- fb
			}
			continue
		}
		caughtUp = false

		remaining := int(chainHeight - lastHeight)
		fetchCount := batchBlockCount
		if fetchCount > remaining {
			fetchCount = remaining
		}

		// === ROUND TRIP 1: Batch fetch all blocks ===
		heights := make([]uint64, fetchCount)
		for i := 0; i < fetchCount; i++ {
			heights[i] = uint64(lastHeight) + uint64(i) + 1
		}

		var blockResults []*rpc.GetBlock_Result
		err := idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
			var e error
			blockResults, e = c.BatchGetBlocks(heights)
			return e
		})
		if err != nil {
			logger.Errorf("BatchGetBlocks at %d: %v", lastHeight+1, err)
			time.Sleep(1 * time.Second)
			continue
		}

		// Parse blocks and collect all TX hashes
		blocks := make([]blockInfo, 0, fetchCount)
		allTxHashes := make([]string, 0, fetchCount*10)
		var regCount int64

		for i, result := range blockResults {
			if result == nil {
				continue
			}
			bi := blockInfo{height: int64(heights[i])}

			var bl block.Block
			// Binary path first (faster than JSON for the hot loop)
			if blobBytes, err := hex.DecodeString(result.Blob); err == nil && len(blobBytes) > 0 {
				if err2 := bl.Deserialize(blobBytes); err2 != nil {
					if err3 := json.Unmarshal([]byte(result.Json), &bl); err3 != nil {
						logger.Errorf("parse block %d: blob=%v json=%v", heights[i], err2, err3)
						continue
					}
				}
			} else if err := json.Unmarshal([]byte(result.Json), &bl); err != nil {
				logger.Errorf("parse block %d: %v", heights[i], err)
				continue
			}

			for _, h := range bl.Tx_hashes {
				hashStr := h.String()
				hashBytes := h[:]
				if len(hashBytes) >= 3 && hashBytes[0] == 0 && hashBytes[1] == 0 && hashBytes[2] == 0 {
					regCount++
					continue
				}
				bi.txHashes = append(bi.txHashes, hashStr)
				allTxHashes = append(allTxHashes, hashStr)
			}
			blocks = append(blocks, bi)
		}

		// === ROUND TRIP 2: Mega-batch fetch ALL transactions ===
		var txResult *rpc.GetTransaction_Result
		if len(allTxHashes) > 0 {
			err = idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
				var e error
				txResult, e = c.GetTransaction(allTxHashes)
				return e
			})
			if err != nil {
				logger.Errorf("BatchGetTransaction (%d hashes): %v", len(allTxHashes), err)
			}
		}

		out <- &fetchedBatch{
			blockResults: blockResults,
			heights:      heights,
			fetchCount:   fetchCount,
			txResult:     txResult,
			allTxHashes:  allTxHashes,
			blocks:       blocks,
			regCount:     regCount,
		}
	}
}

// fetchSingleBlock builds a fetchedBatch from one speculative block result.
func (idx *Indexer) fetchSingleBlock(result *rpc.GetBlock_Result, height uint64) *fetchedBatch {
	bi := blockInfo{height: int64(height)}
	var regCount int64

	var bl block.Block
	// Binary path first (faster than JSON for speculative blocks)
	if blobBytes, err := hex.DecodeString(result.Blob); err == nil && len(blobBytes) > 0 {
		if err2 := bl.Deserialize(blobBytes); err2 != nil {
			if err3 := json.Unmarshal([]byte(result.Json), &bl); err3 != nil {
				logger.Errorf("parse speculative block %d: blob=%v json=%v", height, err2, err3)
				return nil
			}
		}
	} else if err := json.Unmarshal([]byte(result.Json), &bl); err != nil {
		logger.Errorf("parse speculative block %d: %v", height, err)
		return nil
	}

	allTxHashes := make([]string, 0, len(bl.Tx_hashes))
	for _, h := range bl.Tx_hashes {
		hashStr := h.String()
		hashBytes := h[:]
		if len(hashBytes) >= 3 && hashBytes[0] == 0 && hashBytes[1] == 0 && hashBytes[2] == 0 {
			regCount++
			continue
		}
		bi.txHashes = append(bi.txHashes, hashStr)
		allTxHashes = append(allTxHashes, hashStr)
	}

	var txResult *rpc.GetTransaction_Result
	if len(allTxHashes) > 0 {
		idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
			var e error
			txResult, e = c.GetTransaction(allTxHashes)
			return e
		})
	}

	return &fetchedBatch{
		blockResults: []*rpc.GetBlock_Result{result},
		heights:      []uint64{height},
		fetchCount:   1,
		txResult:     txResult,
		allTxHashes:  allTxHashes,
		blocks:       []blockInfo{bi},
		regCount:     regCount,
	}
}

// processorLoop receives fetched batches, decodes transactions, builds
// WriteBatch objects, and sends them to the flusher. All CPU-bound work
// (hex decode, protobuf deserialize, SC argument parsing) lives here,
// isolated from I/O stages.
func (idx *Indexer) processorLoop(in <-chan *fetchedBatch, out chan<- *processedBatch) {
	batchSize := idx.BatchSize
	batch := storage.NewWriteBatch()
	blocksInBatch := 0
	syncSignaled := false

	for fb := range in {
		if idx.Closing.Load() {
			break
		}

		// fetchCount == 0 is the "caught up" sentinel from the fetcher
		if fb.fetchCount == 0 {
			// Flush any accumulated partial batch before signaling caught-up
			if blocksInBatch > 0 {
				out <- &processedBatch{
					batch:      batch,
					newHeight:  batch.LastHeight,
					blockCount: blocksInBatch,
					batchStart: time.Now(),
				}
				batch = storage.NewWriteBatch()
				blocksInBatch = 0
			}
			// Send a zero-block sentinel so flusher enables sync + turbo
			if !syncSignaled {
				syncSignaled = true
				out <- &processedBatch{blockCount: 0}
			}
			continue
		}
		syncSignaled = false

		batchStart := time.Now()

		// Decode and index all transactions
		var burnCount, normCount int64

		if fb.txResult != nil && len(fb.txResult.Txs_as_hex) > 0 {
			txMap := make(map[string]int, len(fb.allTxHashes))
			for i, h := range fb.allTxHashes {
				txMap[h] = i
			}

			for _, bi := range fb.blocks {
				for _, hashStr := range bi.txHashes {
					txIdx, ok := txMap[hashStr]
					if !ok || txIdx >= len(fb.txResult.Txs_as_hex) {
						continue
					}

					txHex := fb.txResult.Txs_as_hex[txIdx]
					txBin, err := hex.DecodeString(txHex)
					if err != nil {
						continue
					}

					var tx transaction.Transaction
					if err := tx.Deserialize(txBin); err != nil {
						continue
					}

					switch tx.TransactionType {
					case transaction.SC_TX:
						idx.processSCTx(&tx, fb.txResult.Txs[txIdx], hashStr, bi.height, batch)
					case transaction.REGISTRATION:
						fb.regCount++
					case transaction.BURN_TX:
						burnCount++
					case transaction.NORMAL:
						normCount++
						idx.processNormalTx(&tx, fb.txResult.Txs[txIdx], hashStr, bi.height, batch)
					}
				}
			}
		}

		batch.RegTxCount += fb.regCount
		batch.BurnTxCount += burnCount
		batch.NormTxCount += normCount

		lastHeight := idx.LastIndexedHeight.Load()
		newHeight := lastHeight + int64(fb.fetchCount)
		idx.LastIndexedHeight.Store(newHeight)
		batch.LastHeight = newHeight
		blocksInBatch += fb.fetchCount

		// Flush when threshold reached
		if blocksInBatch >= batchSize {
			out <- &processedBatch{
				batch:      batch,
				newHeight:  newHeight,
				blockCount: blocksInBatch,
				batchStart: batchStart,
			}
			batch = storage.NewWriteBatch()
			blocksInBatch = 0
		}
	}

	// Final partial batch
	if blocksInBatch > 0 && batch.LastHeight > 0 {
		out <- &processedBatch{
			batch:      batch,
			newHeight:  batch.LastHeight,
			blockCount: blocksInBatch,
			batchStart: time.Now(),
		}
	}
}

// flusherLoop writes processedBatch values to storage. It owns all disk I/O
// so neither the fetcher nor the processor blocks on bbolt commits.
func (idx *Indexer) flusherLoop(in <-chan *processedBatch) {
	startTime := time.Now()
	batchSize := idx.BatchSize
	syncEnabled := false

	for pb := range in {
		if idx.Closing.Load() {
			// Drain: still flush remaining batches for data integrity
			if pb.batch != nil && pb.batch.LastHeight > 0 {
				if err := idx.Store.FlushBatch(pb.batch); err != nil {
					logger.Errorf("FlushBatch (drain): %v", err)
				}
			}
			continue
		}

		// blockCount == 0 is the "caught up" sentinel
		if pb.blockCount == 0 {
			if !syncEnabled {
				syncEnabled = true
				if idx.syncedOnce.CompareAndSwap(false, true) {
					idx.Store.EnableSync()
					if idx.TurboMode {
						// Skip post-scan variable fetch if TELA cache already has the data
						if _, err := loadTELACache(idx.DBDir); err != nil {
							// No cache — need to fetch variables
							idx.postScanVariableFetch()
						} else {
							logger.Info("Post-scan variable fetch skipped (TELA cache exists)")
						}
					}
				}
			}
			continue
		}
		syncEnabled = false

		if err := idx.Store.FlushBatch(pb.batch); err != nil {
			logger.Errorf("FlushBatch: %v", err)
		} else {
			chainHeight := idx.ChainHeight.Load()
			elapsed := time.Since(startTime)
			bps := float64(pb.newHeight) / elapsed.Seconds()
			eta := time.Duration(float64(chainHeight-pb.newHeight)/bps) * time.Second
			logger.Infof("Height: %d/%d | %.0f blk/s | ETA: %v | Batch: %d",
				pb.newHeight, chainHeight, bps, eta.Round(time.Second), batchSize)
		}

		// Adaptive batch sizing
		if idx.AdaptBatchSize {
			batchElapsed := time.Since(pb.batchStart)
			if batchElapsed < 1*time.Second && batchSize < 2000 {
				batchSize = min(batchSize*2, 2000)
			} else if batchElapsed > 5*time.Second && batchSize > 10 {
				batchSize = max(batchSize/2, 10)
			}
		}
	}
}

// postScanVariableFetch runs after turbo-mode scanning completes.
// It fetches SC variables for all discovered SCIDs using a parallel worker pool,
// making up for the GetSC calls that were skipped during the scan phase.
func (idx *Indexer) postScanVariableFetch() {
	scids, err := idx.Store.GetAllSCIDs()
	if err != nil {
		logger.Errorf("post-scan: GetAllSCIDs: %v", err)
		return
	}
	if len(scids) == 0 {
		logger.Info("Post-scan: no SCIDs to fetch variables for")
		return
	}
	logger.Infof("Post-scan: fetching variables for %d SCIDs", len(scids))

	// Parallel fetch with worker pool
	work := make(chan string, len(scids))
	for _, scid := range scids {
		work <- scid
	}
	close(work)

	var wg sync.WaitGroup
	batch := storage.NewWriteBatch()
	var mu sync.Mutex
	workers := runtime.GOMAXPROCS(0)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for scid := range work {
				idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
					result, err := c.GetSC(scid, -1, nil, nil, false)
					if err != nil {
						return err
					}
					vars := parseSCVariables(result)
					if len(vars) > 0 {
						mu.Lock()
						batch.AddVariables(scid, idx.ChainHeight.Load(), vars)
						mu.Unlock()
					}
					return nil
				})
			}
		}()
	}
	wg.Wait()

	if err := idx.Store.FlushBatch(batch); err != nil {
		logger.Errorf("post-scan flush: %v", err)
	}
	logger.Info("Post-scan variable fetch complete")
}

// processTxResults processes batch transaction results.
func (idx *Indexer) processTxResults(wi *structures.WorkItem, txResult *rpc.GetTransaction_Result, hashes []string, batch *storage.WriteBatch) {
	for i, txHex := range txResult.Txs_as_hex {
		txBin, err := hex.DecodeString(txHex)
		if err != nil {
			logger.Errorf("decode tx hex: %v", err)
			continue
		}

		var tx transaction.Transaction
		if err := tx.Deserialize(txBin); err != nil {
			logger.Errorf("deserialize tx: %v", err)
			continue
		}

		switch tx.TransactionType {
		case transaction.SC_TX:
			idx.processSCTx(&tx, txResult.Txs[i], hashes[i], wi.Height, batch)
		case transaction.REGISTRATION:
			wi.RegCount++
		case transaction.BURN_TX:
			wi.BurnCount++
		case transaction.NORMAL:
			wi.NormCount++
			idx.processNormalTx(&tx, txResult.Txs[i], hashes[i], wi.Height, batch)
		}
	}
}

// processSCTx handles a smart contract transaction.
// In turbo mode, GetSC calls are skipped entirely -- only TX metadata is recorded.
func (idx *Indexer) processSCTx(tx *transaction.Transaction, txInfo rpc.Tx_Related_Info, txid string, height int64, batch *storage.WriteBatch) {
	scArgs := tx.SCDATA
	scFees := tx.Fees()
	entrypoint := fmt.Sprintf("%v", scArgs.Value("entrypoint", "S"))
	scAction := fmt.Sprintf("%v", scArgs.Value("SC_ACTION", "U"))

	var method uint8
	var scid string

	if scAction == "1" {
		method = structures.MethodInstallSC
		scid = txid
	} else {
		method = structures.MethodInvokeSC
		scid = fmt.Sprintf("%v", scArgs.Value("SC_ID", "H"))
	}

	scid = hgpool.InternSCID(scid)

	// Check exclusions (O(1) map lookup)
	if _, excluded := idx.SCIDExclusions[scid]; excluded {
		return
	}

	var sender string
	if len(tx.Payloads) > 0 && tx.Payloads[0].Statement.RingSize == 2 {
		sender = hgpool.InternAddress(txInfo.Signer)
	}

	if method == structures.MethodInstallSC {
		if idx.TurboMode {
			// Turbo: record install from TX data, skip GetSC
			idx.ValidatedSCs.Store(scid, struct{}{})
			batch.AddOwner(scid, sender)
			batch.AddInvocation(structures.InvokeRecord{
				Scid: scid, Sender: sender, Entrypoint: entrypoint,
				Height: height, Details: &structures.SCTXParse{
					Txid: scid, Scid: scid, Entrypoint: entrypoint,
					Method: structures.MethodInstallSC, Sender: sender,
					Fees: scFees, Height: height,
				},
			})
			batch.AddInteractionHeight(scid, height)
		} else {
			idx.handleInstallSC(scid, sender, entrypoint, height, scArgs, scFees, tx, batch)
		}
	} else {
		if idx.TurboMode {
			// Turbo: record invoke from TX data, skip GetSC
			idx.ValidatedSCs.Store(scid, struct{}{})
			batch.AddInvocation(structures.InvokeRecord{
				Scid: scid, Sender: sender, Entrypoint: entrypoint,
				Height: height, Details: &structures.SCTXParse{
					Txid: txid, Scid: scid, Entrypoint: entrypoint,
					Method: structures.MethodInvokeSC, Sender: sender,
					Fees: scFees, Height: height,
				},
			})
			batch.AddInteractionHeight(scid, height)
		} else {
			idx.handleInvokeSC(scid, sender, entrypoint, height, scArgs, scFees, txid, batch)
		}
	}
}

// handleInstallSC processes a new SC deployment.
func (idx *Indexer) handleInstallSC(scid, sender, entrypoint string, height int64, scArgs rpc.Arguments, fees uint64, tx *transaction.Transaction, batch *storage.WriteBatch) {
	code := fmt.Sprintf("%v", scArgs.Value("SC_CODE", "S"))

	// Check search filter
	if !idx.matchesFilter(code) {
		return
	}

	// Fetch SC variables
	var scVars []*structures.SCIDVariable
	idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
		result, err := c.GetSC(scid, height, nil, nil, false)
		if err != nil {
			return err
		}
		scVars = parseSCVariables(result)
		return nil
	})

	if len(scVars) == 0 {
		batch.InvalidSCIDs[scid] = fees
		return
	}

	// Register validated SC
	idx.ValidatedSCs.Store(scid, struct{}{})

	// Add to batch
	batch.AddOwner(scid, sender)
	batch.AddInvocation(structures.InvokeRecord{
		Scid:       scid,
		Sender:     sender,
		Entrypoint: entrypoint,
		Height:     height,
		Details: &structures.SCTXParse{
			Txid:       scid,
			Scid:       scid,
			Entrypoint: entrypoint,
			Method:     structures.MethodInstallSC,
			ScArgs:     scArgs,
			Sender:     sender,
			Fees:       fees,
			Height:     height,
		},
	})
	batch.AddVariables(scid, height, scVars)
	batch.AddInteractionHeight(scid, height)
}

// handleInvokeSC processes an SC invocation.
func (idx *Indexer) handleInvokeSC(scid, sender, entrypoint string, height int64, scArgs rpc.Arguments, fees uint64, txid string, batch *storage.WriteBatch) {
	// Validate this SCID is known or discover it
	if _, ok := idx.ValidatedSCs.Load(scid); !ok {
		// Try to validate
		var scVars []*structures.SCIDVariable
		idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
			result, err := c.GetSC(scid, height, nil, nil, false)
			if err != nil {
				return err
			}
			scVars = parseSCVariables(result)
			return nil
		})
		if len(scVars) > 0 {
			idx.ValidatedSCs.Store(scid, struct{}{})
		} else {
			return // invalid SCID
		}
	}

	batch.AddInvocation(structures.InvokeRecord{
		Scid:       scid,
		Sender:     sender,
		Entrypoint: entrypoint,
		Height:     height,
		Details: &structures.SCTXParse{
			Txid:       txid,
			Scid:       scid,
			Entrypoint: entrypoint,
			Method:     structures.MethodInvokeSC,
			ScArgs:     scArgs,
			Sender:     sender,
			Fees:       fees,
			Height:     height,
		},
	})

	// Fetch updated variables
	var scVars []*structures.SCIDVariable
	idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
		result, err := c.GetSC(scid, height, nil, nil, false)
		if err != nil {
			return err
		}
		scVars = parseSCVariables(result)
		return nil
	})
	if len(scVars) > 0 {
		batch.AddVariables(scid, height, scVars)
	}
	batch.AddInteractionHeight(scid, height)
}

// processNormalTx handles a normal transaction with SCID payload.
func (idx *Indexer) processNormalTx(tx *transaction.Transaction, txInfo rpc.Tx_Related_Info, txid string, height int64, batch *storage.WriteBatch) {
	for j := 0; j < len(tx.Payloads); j++ {
		scidStr := tx.Payloads[j].SCID.String()
		// Zero hash means no SCID
		if scidStr == "0000000000000000000000000000000000000000000000000000000000000000" {
			continue
		}
		for _, addr := range txInfo.Ring[j] {
			addr = hgpool.InternAddress(addr)
			ntx := &structures.NormalTXWithSCIDParse{
				Txid:   txid,
				Scid:   scidStr,
				Fees:   tx.Fees(),
				Height: height,
			}
			if batch.NormalTxs[addr] == nil {
				batch.NormalTxs[addr] = make([]*structures.NormalTXWithSCIDParse, 0, 4)
			}
			batch.NormalTxs[addr] = append(batch.NormalTxs[addr], ntx)
		}
	}
}

// matchesFilter checks if SC code matches any search filter.
func (idx *Indexer) matchesFilter(code string) bool {
	if len(idx.SearchFilter) == 0 {
		return true // no filter = index all
	}
	for _, filter := range idx.SearchFilter {
		if strings.Contains(code, filter) {
			return true
		}
	}
	return false
}

// parseSCVariables extracts variables from a GetSC result.
func parseSCVariables(result *rpc.GetSC_Result) []*structures.SCIDVariable {
	if result == nil {
		return nil
	}
	vars := make([]*structures.SCIDVariable, 0, len(result.VariableStringKeys)+len(result.VariableUint64Keys))
	for k, v := range result.VariableStringKeys {
		vars = append(vars, &structures.SCIDVariable{Key: k, Value: v})
	}
	for k, v := range result.VariableUint64Keys {
		vars = append(vars, &structures.SCIDVariable{Key: k, Value: v})
	}
	return vars
}

// Close shuts down the indexer gracefully.
// Setting Closing causes the fetcher to exit, which closes fetchChan,
// which causes the processor to exit, which closes flushChan,
// which causes the flusher to drain and exit. The WaitGroup in scanLoop
// ensures all three goroutines have finished before StartDaemonMode returns.
func (idx *Indexer) Close() {
	idx.Closing.Store(true)
	if idx.RPCPool != nil {
		idx.RPCPool.Close()
	}
	if idx.Store != nil {
		idx.Store.Close()
	}
}
