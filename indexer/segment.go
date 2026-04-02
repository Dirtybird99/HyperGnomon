package indexer

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/deroproject/derohe/block"
	"github.com/deroproject/derohe/rpc"
	"github.com/deroproject/derohe/transaction"
	"github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"

	hgpool "github.com/hypergnomon/hypergnomon/pool"
	hgrpc "github.com/hypergnomon/hypergnomon/rpc"
	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

const DefaultSegmentSize = 10000

var segLog = logrus.WithField("pkg", "segment")

// segment represents a contiguous range of blocks to process.
type segment struct {
	ID    int
	Start int64 // inclusive
	End   int64 // inclusive
}

// SegmentSync performs parallel initial sync by dividing the chain into segments.
// Each segment gets its own temporary BoltDB and processes blocks independently.
// After all segments complete, results are merged into the main database.
//
// This is the MapReduce pattern for blockchain indexing:
//   - Map: each worker processes its segment into a temporary DB
//   - Reduce: merge all temporary DBs into the main store
type SegmentSync struct {
	Endpoint       string
	MainStore      storage.Storage
	SearchFilter   []string
	SCIDExclusions map[string]struct{}
	DBDir          string
	SegmentSize    int
	Workers        int // defaults to runtime.GOMAXPROCS(0)

	progress atomic.Int64
	total    atomic.Int64
}

// Run executes the parallel segment sync from startHeight to endHeight.
func (ss *SegmentSync) Run(startHeight, endHeight int64) error {
	if endHeight <= startHeight {
		return fmt.Errorf("segment sync: endHeight (%d) must be greater than startHeight (%d)", endHeight, startHeight)
	}

	if ss.SegmentSize <= 0 {
		ss.SegmentSize = DefaultSegmentSize
	}
	if ss.Workers <= 0 {
		ss.Workers = runtime.GOMAXPROCS(0)
	}
	if ss.SCIDExclusions == nil {
		ss.SCIDExclusions = make(map[string]struct{})
	}

	// Build segments
	segments := ss.buildSegments(startHeight, endHeight)
	ss.total.Store(endHeight - startHeight)
	ss.progress.Store(0)

	segLog.Infof("Segment sync: %d blocks [%d..%d] across %d segments with %d workers",
		endHeight-startHeight, startHeight, endHeight, len(segments), ss.Workers)

	// Create segment temp directory
	segDir := filepath.Join(ss.DBDir, "segments")
	if err := os.MkdirAll(segDir, 0700); err != nil {
		return fmt.Errorf("segment sync: mkdir %s: %w", segDir, err)
	}

	// Work channel: feed segments to workers
	workCh := make(chan segment, len(segments))
	for _, seg := range segments {
		workCh <- seg
	}
	close(workCh)

	// Launch workers
	var wg sync.WaitGroup
	errCh := make(chan error, ss.Workers)
	segDBPaths := make([]string, len(segments))
	var pathMu sync.Mutex

	wg.Add(ss.Workers)
	for w := 0; w < ss.Workers; w++ {
		go func(workerID int) {
			defer wg.Done()

			// Each worker gets its own RPC connection (not from pool -- long-lived)
			client, err := hgrpc.NewClient(ss.Endpoint)
			if err != nil {
				errCh <- fmt.Errorf("worker %d: rpc connect: %w", workerID, err)
				return
			}
			defer client.Close()

			for seg := range workCh {
				dbPath := filepath.Join(segDir, fmt.Sprintf("seg_%04d.db", seg.ID))

				segSubDir := fmt.Sprintf("%s_dir", strings.TrimSuffix(dbPath, ".db"))
				actualDBPath := filepath.Join(segSubDir, "HYPERGNOMON.db")

				if err := ss.processSegment(client, seg, dbPath); err != nil {
					errCh <- fmt.Errorf("worker %d seg %d [%d..%d]: %w",
						workerID, seg.ID, seg.Start, seg.End, err)
					return
				}

				pathMu.Lock()
				segDBPaths[seg.ID] = actualDBPath
				pathMu.Unlock()
			}
		}(w)
	}

	wg.Wait()
	close(errCh)

	// Check for worker errors
	var errs []string
	for err := range errCh {
		errs = append(errs, err.Error())
	}
	if len(errs) > 0 {
		return fmt.Errorf("segment sync: %d worker errors:\n  %s", len(errs), strings.Join(errs, "\n  "))
	}

	segLog.Info("All segments processed. Beginning merge into main database...")

	// Merge phase: fold each segment DB into the main store
	for i, dbPath := range segDBPaths {
		if dbPath == "" {
			continue // skip if segment was not processed (shouldn't happen without error)
		}
		if err := ss.mergeSegmentDB(dbPath); err != nil {
			return fmt.Errorf("merge segment %d (%s): %w", i, dbPath, err)
		}
		segLog.Infof("Merged segment %d/%d", i+1, len(segDBPaths))
	}

	// Update last indexed height
	if err := ss.MainStore.StoreLastIndexHeight(endHeight); err != nil {
		return fmt.Errorf("segment sync: store last height: %w", err)
	}

	// Cleanup temp directory
	if err := os.RemoveAll(segDir); err != nil {
		segLog.Warnf("Cleanup segment dir: %v", err)
	}

	segLog.Infof("Segment sync complete: indexed %d blocks [%d..%d]",
		endHeight-startHeight, startHeight, endHeight)
	return nil
}

// buildSegments divides [startHeight, endHeight) into chunks of SegmentSize.
func (ss *SegmentSync) buildSegments(startHeight, endHeight int64) []segment {
	var segments []segment
	id := 0
	for h := startHeight; h < endHeight; h += int64(ss.SegmentSize) {
		end := h + int64(ss.SegmentSize)
		if end > endHeight {
			end = endHeight
		}
		segments = append(segments, segment{
			ID:    id,
			Start: h,
			End:   end, // exclusive: process [Start, End)
		})
		id++
	}
	return segments
}

// processSegment indexes all blocks in the segment range into a temp BoltDB.
func (ss *SegmentSync) processSegment(client *hgrpc.Client, seg segment, dbPath string) error {
	// Open temporary BoltDB for this segment -- each segment gets its own
	// subdirectory so NewBboltStore's hardcoded "HYPERGNOMON.db" name works.
	segDir := fmt.Sprintf("%s_dir", strings.TrimSuffix(dbPath, ".db"))
	if err := os.MkdirAll(segDir, 0700); err != nil {
		return fmt.Errorf("mkdir segment dir: %w", err)
	}
	segStore, err := storage.NewBboltStore(segDir, strings.Join(ss.SearchFilter, ";;;"))
	if err != nil {
		return fmt.Errorf("open segment db: %w", err)
	}
	defer segStore.Close()

	batch := storage.NewWriteBatch()
	blocksInBatch := 0
	validatedSCs := make(map[string]struct{}) // local SC validation cache

	// Process blocks in small parallel batches within the segment
	parallelBlocks := structures.DefaultParallelBlocks
	for height := seg.Start; height < seg.End; {
		remaining := int(seg.End - height)
		fetchCount := parallelBlocks
		if fetchCount > remaining {
			fetchCount = remaining
		}

		// Parallel fetch for this mini-batch
		workItems := make([]*structures.WorkItem, 0, fetchCount)
		var wg sync.WaitGroup
		var mu sync.Mutex
		wg.Add(fetchCount)

		for i := 0; i < fetchCount; i++ {
			go func(h int64) {
				defer wg.Done()

				wi := hgpool.GetWorkItem()
				wi.Height = h

				hash, err := client.GetBlockHash(uint64(h))
				if err != nil {
					wi.Err = fmt.Errorf("block hash %d: %w", h, err)
					mu.Lock()
					workItems = append(workItems, wi)
					mu.Unlock()
					return
				}

				result, err := client.GetBlock(hash)
				if err != nil {
					wi.Err = fmt.Errorf("block %d: %w", h, err)
					mu.Lock()
					workItems = append(workItems, wi)
					mu.Unlock()
					return
				}

				var bl block.Block
				if err := json.Unmarshal([]byte(result.Json), &bl); err != nil {
					wi.Err = fmt.Errorf("unmarshal block %d: %w", h, err)
					mu.Lock()
					workItems = append(workItems, wi)
					mu.Unlock()
					return
				}

				bt := hgpool.GetBlockTxns()
				bt.Topoheight = int64(bl.Height)
				for _, txHash := range bl.Tx_hashes {
					bt.TxHashes = append(bt.TxHashes, txHash.String())
				}
				wi.BlockTxns = bt

				mu.Lock()
				workItems = append(workItems, wi)
				mu.Unlock()
			}(height + int64(i))
		}
		wg.Wait()

		// Sort by height for deterministic processing
		sort.Slice(workItems, func(i, j int) bool {
			return workItems[i].Height < workItems[j].Height
		})

		// Process each block's transactions
		for _, wi := range workItems {
			if wi.Err != nil {
				segLog.Warnf("Segment %d: %v", seg.ID, wi.Err)
				if wi.BlockTxns != nil {
					hgpool.PutBlockTxns(wi.BlockTxns)
				}
				hgpool.PutWorkItem(wi)
				continue
			}

			if wi.BlockTxns != nil && len(wi.BlockTxns.TxHashes) > 0 {
				ss.processBlockTxns(client, wi, batch, validatedSCs)
			}

			if wi.BlockTxns != nil {
				hgpool.PutBlockTxns(wi.BlockTxns)
			}
			hgpool.PutWorkItem(wi)
		}

		height += int64(fetchCount)
		blocksInBatch += fetchCount
		ss.progress.Add(int64(fetchCount))

		// Log progress periodically
		prog := ss.progress.Load()
		tot := ss.total.Load()
		if blocksInBatch%1000 == 0 && tot > 0 {
			pct := float64(prog) / float64(tot) * 100.0
			segLog.Infof("Progress: %.1f%% (%d/%d blocks)", pct, prog, tot)
		}

		// Flush batch when threshold reached
		if blocksInBatch >= structures.DefaultBatchSize {
			batch.LastHeight = height
			if err := segStore.FlushBatch(batch); err != nil {
				return fmt.Errorf("flush at height %d: %w", height, err)
			}
			batch.Reset()
			blocksInBatch = 0
		}
	}

	// Final flush for remaining blocks in this segment
	if blocksInBatch > 0 {
		batch.LastHeight = seg.End
		if err := segStore.FlushBatch(batch); err != nil {
			return fmt.Errorf("final flush segment %d: %w", seg.ID, err)
		}
	}

	return nil
}

// processBlockTxns handles all transactions in a single block -- mirrors the
// main indexer's processTxResults / processSCTx / handleInstallSC / handleInvokeSC logic.
func (ss *SegmentSync) processBlockTxns(client *hgrpc.Client, wi *structures.WorkItem, batch *storage.WriteBatch, validatedSCs map[string]struct{}) {
	// Separate registration txs from others
	nonRegHashes := make([]string, 0, len(wi.BlockTxns.TxHashes))
	for _, hashStr := range wi.BlockTxns.TxHashes {
		hashBytes, _ := hex.DecodeString(hashStr)
		if len(hashBytes) >= 3 && hashBytes[0] == 0 && hashBytes[1] == 0 && hashBytes[2] == 0 {
			wi.RegCount++
			continue
		}
		nonRegHashes = append(nonRegHashes, hashStr)
	}

	if len(nonRegHashes) == 0 {
		batch.RegTxCount += wi.RegCount
		return
	}

	txResult, err := client.GetTransaction(nonRegHashes)
	if err != nil {
		segLog.Errorf("GetTransaction block %d: %v", wi.Height, err)
		batch.RegTxCount += wi.RegCount
		return
	}

	for i, txHex := range txResult.Txs_as_hex {
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
			ss.processSCTx(client, &tx, txResult.Txs[i], nonRegHashes[i], wi.Height, batch, validatedSCs)
		case transaction.REGISTRATION:
			wi.RegCount++
		case transaction.BURN_TX:
			wi.BurnCount++
		case transaction.NORMAL:
			wi.NormCount++
			ss.processNormalTx(&tx, txResult.Txs[i], nonRegHashes[i], wi.Height, batch)
		}
	}

	batch.RegTxCount += wi.RegCount
	batch.BurnTxCount += wi.BurnCount
	batch.NormTxCount += wi.NormCount
}

// processSCTx handles a smart contract transaction within a segment worker.
func (ss *SegmentSync) processSCTx(client *hgrpc.Client, tx *transaction.Transaction, txInfo rpc.Tx_Related_Info, txid string, height int64, batch *storage.WriteBatch, validatedSCs map[string]struct{}) {
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

	// Check exclusions
	if _, excluded := ss.SCIDExclusions[scid]; excluded {
		return
	}

	var sender string
	if len(tx.Payloads) > 0 && tx.Payloads[0].Statement.RingSize == 2 {
		sender = hgpool.InternAddress(txInfo.Signer)
	}

	if method == structures.MethodInstallSC {
		ss.handleInstallSC(client, scid, sender, entrypoint, height, scArgs, scFees, batch, validatedSCs)
	} else {
		ss.handleInvokeSC(client, scid, sender, entrypoint, height, scArgs, scFees, txid, batch, validatedSCs)
	}
}

// handleInstallSC processes a new SC deployment within a segment.
func (ss *SegmentSync) handleInstallSC(client *hgrpc.Client, scid, sender, entrypoint string, height int64, scArgs rpc.Arguments, fees uint64, batch *storage.WriteBatch, validatedSCs map[string]struct{}) {
	code := fmt.Sprintf("%v", scArgs.Value("SC_CODE", "S"))

	// Check search filter
	if !ss.matchesFilter(code) {
		return
	}

	// Fetch SC variables
	result, err := client.GetSC(scid, height, nil, nil, false)
	if err != nil {
		return
	}
	scVars := parseSCVariables(result)

	if len(scVars) == 0 {
		batch.InvalidSCIDs[scid] = fees
		return
	}

	validatedSCs[scid] = struct{}{}

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

// handleInvokeSC processes an SC invocation within a segment.
func (ss *SegmentSync) handleInvokeSC(client *hgrpc.Client, scid, sender, entrypoint string, height int64, scArgs rpc.Arguments, fees uint64, txid string, batch *storage.WriteBatch, validatedSCs map[string]struct{}) {
	// Validate SCID
	if _, ok := validatedSCs[scid]; !ok {
		result, err := client.GetSC(scid, height, nil, nil, false)
		if err != nil {
			return
		}
		scVars := parseSCVariables(result)
		if len(scVars) > 0 {
			validatedSCs[scid] = struct{}{}
		} else {
			return
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
	result, err := client.GetSC(scid, height, nil, nil, false)
	if err == nil {
		scVars := parseSCVariables(result)
		if len(scVars) > 0 {
			batch.AddVariables(scid, height, scVars)
		}
	}
	batch.AddInteractionHeight(scid, height)
}

// processNormalTx handles a normal transaction with SCID payload within a segment.
func (ss *SegmentSync) processNormalTx(tx *transaction.Transaction, txInfo rpc.Tx_Related_Info, txid string, height int64, batch *storage.WriteBatch) {
	for j := 0; j < len(tx.Payloads); j++ {
		scidStr := tx.Payloads[j].SCID.String()
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
func (ss *SegmentSync) matchesFilter(code string) bool {
	if len(ss.SearchFilter) == 0 {
		return true
	}
	for _, filter := range ss.SearchFilter {
		if strings.Contains(code, filter) {
			return true
		}
	}
	return false
}

// mergeSegmentDB opens a temporary segment BoltDB and copies all key-value
// pairs into the main database via WriteBatch. This is the "reduce" phase.
func (ss *SegmentSync) mergeSegmentDB(dbPath string) error {
	segDB, err := bolt.Open(dbPath, 0600, &bolt.Options{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("open segment db for merge: %w", err)
	}
	defer segDB.Close()

	// Read segment data into a WriteBatch, then flush to main store.
	// This avoids holding two BoltDB write transactions simultaneously.
	batch := storage.NewWriteBatch()

	err = segDB.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			bucketName := string(name)
			return ss.mergeBucket(bucketName, b, batch)
		})
	})
	if err != nil {
		return fmt.Errorf("read segment db: %w", err)
	}

	// Flush accumulated data into main store
	if err := ss.MainStore.FlushBatch(batch); err != nil {
		return fmt.Errorf("flush merge batch: %w", err)
	}

	return nil
}

// mergeBucket reads all entries from a segment bucket and populates the WriteBatch.
// Each bucket type is handled according to its data format.
func (ss *SegmentSync) mergeBucket(bucketName string, b *bolt.Bucket, batch *storage.WriteBatch) error {
	switch bucketName {
	case "owners":
		return b.ForEach(func(k, v []byte) error {
			batch.AddOwner(string(k), string(v))
			return nil
		})

	case "scvars":
		return b.ForEach(func(k, v []byte) error {
			// Key format: "scid:height"
			parts := strings.SplitN(string(k), ":", 2)
			if len(parts) != 2 {
				return nil
			}
			scid := parts[0]
			var height int64
			fmt.Sscanf(parts[1], "%d", &height)
			var vars []*structures.SCIDVariable
			if err := json.Unmarshal(v, &vars); err != nil {
				return nil // skip corrupt entries
			}
			batch.AddVariables(scid, height, vars)
			return nil
		})

	case "height":
		return b.ForEach(func(k, v []byte) error {
			scid := string(k)
			var heights []int64
			if err := json.Unmarshal(v, &heights); err != nil {
				return nil
			}
			for _, h := range heights {
				batch.AddInteractionHeight(scid, h)
			}
			return nil
		})

	case "normaltxwithscid":
		return b.ForEach(func(k, v []byte) error {
			addr := string(k)
			var txs []*structures.NormalTXWithSCIDParse
			if err := json.Unmarshal(v, &txs); err != nil {
				return nil
			}
			if batch.NormalTxs[addr] == nil {
				batch.NormalTxs[addr] = make([]*structures.NormalTXWithSCIDParse, 0, len(txs))
			}
			batch.NormalTxs[addr] = append(batch.NormalTxs[addr], txs...)
			return nil
		})

	case "invalidscidinvokes":
		return b.ForEach(func(k, v []byte) error {
			var fees uint64
			fmt.Sscanf(string(v), "%d", &fees)
			batch.InvalidSCIDs[string(k)] = fees
			return nil
		})

	case "stats":
		// TX counts and heights are handled specially -- accumulate deltas
		return b.ForEach(func(k, v []byte) error {
			key := string(k)
			switch key {
			case "regtxcount":
				var n int64
				fmt.Sscanf(string(v), "%d", &n)
				batch.RegTxCount += n
			case "burntxcount":
				var n int64
				fmt.Sscanf(string(v), "%d", &n)
				batch.BurnTxCount += n
			case "normtxcount":
				var n int64
				fmt.Sscanf(string(v), "%d", &n)
				batch.NormTxCount += n
			// lastindexedheight is set by the caller after merge, skip here
			}
			return nil
		})

	default:
		// Per-SCID invocation buckets: bucket name is the SCID itself
		return b.ForEach(func(k, v []byte) error {
			var detail structures.SCTXParse
			if err := json.Unmarshal(v, &detail); err != nil {
				return nil
			}
			batch.AddInvocation(structures.InvokeRecord{
				Scid:       bucketName,
				Sender:     detail.Sender,
				Entrypoint: detail.Entrypoint,
				Height:     detail.Height,
				Details:    &detail,
			})
			return nil
		})
	}
}

// Progress returns the number of blocks processed so far and the total.
func (ss *SegmentSync) Progress() (done, total int64) {
	return ss.progress.Load(), ss.total.Load()
}
