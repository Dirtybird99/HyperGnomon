package indexer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/creachadair/jrpc2"
	"github.com/deroproject/derohe/rpc"

	hgpool "github.com/hypergnomon/hypergnomon/pool"
	hgrpc "github.com/hypergnomon/hypergnomon/rpc"
	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

// registryEntry represents a SCID from the GnomonSC registry.
type registryEntry struct {
	scid   string
	owner  string
	height int64
}

// FastSync bootstraps the indexer from the on-chain GnomonSC registry.
// Instead of scanning from block 1, it:
//  1. Queries the Gnomon SC for all registered SCIDs
//  2. Extracts owner and deployment height for each
//  3. Fetches current variables for each matching SCID
//  4. Sets LastIndexedHeight to current chain height
//  5. Normal scanning resumes from chain tip
func (idx *Indexer) FastSync(testnet bool) error {
	start := time.Now()

	// Select the correct GnomonSC SCID for the network
	gnomonSCID := structures.GnomonSCID_Mainnet
	if testnet {
		gnomonSCID = structures.GnomonSCID_Testnet
	}

	// JOFITO: Skip registry fetch if cache is fresh (within 1000 blocks)
	var chainHeight int64
	if cached, err := loadTELACache(idx.DBDir); err == nil && cached.Height > 0 {
		if err := idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
			info, err := c.GetInfo()
			if err != nil {
				return err
			}
			chainHeight = info.TopoHeight
			idx.ChainHeight.Store(chainHeight)
			return nil
		}); err == nil {
			if chainHeight-cached.Height < 1000 {
				// Cache is fresh! Skip entire registry fetch.
				idx.LastIndexedHeight.Store(chainHeight)
				structures.TELACount.Store(int64(len(cached.IndexSCIDs) + len(cached.DocSCIDs)))
				elapsed := time.Since(start)
				logger.Infof("FastSync: cache fresh (height %d, %d blocks behind) — %d INDEX + %d DOC loaded in %s",
					cached.Height, chainHeight-cached.Height,
					len(cached.IndexSCIDs), len(cached.DocSCIDs), elapsed.Round(time.Millisecond))
				return nil
			}
		}
	}

	// Get current chain height so we know where to resume scanning
	if chainHeight == 0 {
		if err := idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
			info, err := c.GetInfo()
			if err != nil {
				return err
			}
			chainHeight = info.TopoHeight
			return nil
		}); err != nil {
			return fmt.Errorf("fastsync: get chain height: %w", err)
		}
		idx.ChainHeight.Store(chainHeight)
	}

	logger.Infof("FastSync: querying GnomonSC %s at height %d", gnomonSCID[:16]+"...", chainHeight)

	// Step 1: Fetch the GnomonSC with all variables
	var gnomonVarStrings map[string]interface{}
	if err := idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
		result, err := c.GetSC(gnomonSCID, -1, nil, nil, false)
		if err != nil {
			return err
		}
		if result == nil || len(result.VariableStringKeys) == 0 {
			return fmt.Errorf("GnomonSC returned no variables")
		}
		gnomonVarStrings = result.VariableStringKeys
		return nil
	}); err != nil {
		return fmt.Errorf("fastsync: query GnomonSC: %w", err)
	}

	// Step 2: Parse registry entries from VariableStringKeys
	// Key patterns: 64 chars = SCID, 64+"owner" = owner, 64+"height" = deploy height

	// Single pass: categorize by key length
	scidSet := make(map[string]*registryEntry, len(gnomonVarStrings)/3)
	for key, val := range gnomonVarStrings {
		klen := len(key)
		switch {
		case klen == 64 && isHexString(key):
			if _, ok := scidSet[key]; !ok {
				scidSet[key] = &registryEntry{scid: key}
			}
		case klen == 69 && key[64:] == "owner":
			scid := key[:64]
			if entry, ok := scidSet[scid]; ok {
				if s, ok := val.(string); ok {
					entry.owner = s
				}
			} else {
				// SCID entry not seen yet, create it
				e := &registryEntry{scid: scid}
				if s, ok := val.(string); ok {
					e.owner = s
				}
				scidSet[scid] = e
			}
		case klen == 70 && key[64:] == "height":
			scid := key[:64]
			entry, ok := scidSet[scid]
			if !ok {
				entry = &registryEntry{scid: scid}
				scidSet[scid] = entry
			}
			switch v := val.(type) {
			case float64:
				entry.height = int64(v)
			case int64:
				entry.height = v
			case uint64:
				entry.height = int64(v)
			}
		}
	}

	// Step 3: Filter SCIDs -- apply search filter, skip NameServiceSCID and exclusions
	candidates := make([]*registryEntry, 0, len(scidSet))
	for _, entry := range scidSet {
		// Skip the DERO Nameservice SCID (always present, not user-deployed)
		if entry.scid == structures.NameServiceSCID {
			continue
		}

		// Check exclusions (O(1) map lookup)
		if _, excluded := idx.SCIDExclusions[entry.scid]; excluded {
			continue
		}

		candidates = append(candidates, entry)
	}

	if len(candidates) == 0 {
		logger.Warn("FastSync: no SCIDs found in GnomonSC registry")
		return idx.Store.StoreLastIndexHeight(chainHeight)
	}

	logger.Infof("FastSync: %d SCIDs in registry, %d candidates after filtering", len(scidSet), len(candidates))

	// Step 4: Import SCIDs into database
	batch := storage.NewWriteBatch()

	if idx.TurboMode {
		// TURBO FASTSYNC: Skip ALL GetSC calls. Just store SCID + owner + height
		// from the registry. DB flush + TELA probe run in background.
		for _, entry := range candidates {
			scid := hgpool.InternSCID(entry.scid)
			idx.ValidatedSCs.Store(scid, struct{}{})
			batch.AddOwner(scid, entry.owner)
			batch.AddInteractionHeight(scid, entry.height)
		}

		idx.LastIndexedHeight.Store(chainHeight)
		elapsed := time.Since(start)
		logger.Infof("FastSync complete: %d SCIDs ready in %s", len(candidates), elapsed.Round(time.Millisecond))

		// Background: flush to disk (non-blocking)
		batch.LastHeight = chainHeight
		go func() {
			if err := idx.Store.FlushBatch(batch); err != nil {
				logger.Errorf("FastSync background flush: %v", err)
			} else {
				logger.Infof("FastSync DB flush complete (%d SCIDs persisted)", len(batch.Owners))
			}
		}()

		// JOFITO CACHE: Check if we have a cached TELA list from a previous run.
		// If so, load it instantly and only probe NEW SCIDs (delta since cached height).
		if cached, err := loadTELACache(idx.DBDir); err == nil && cached.Height > 0 {
			// Cache hit! Load known TELA SCIDs instantly.
			structures.TELACount.Store(int64(len(cached.IndexSCIDs) + len(cached.DocSCIDs)))
			logger.Infof("TELA cache hit: %d INDEX + %d DOC from height %d (instant load)",
				len(cached.IndexSCIDs), len(cached.DocSCIDs), cached.Height)

			// Only probe SCIDs deployed AFTER the cached height (delta scan)
			var deltaCandidates []*registryEntry
			for _, c := range candidates {
				if c.height > cached.Height {
					deltaCandidates = append(deltaCandidates, c)
				}
			}

			if len(deltaCandidates) > 0 {
				logger.Infof("TELA delta probe: %d new SCIDs since height %d", len(deltaCandidates), cached.Height)
				go idx.probeTELA(deltaCandidates, chainHeight)
			} else {
				logger.Info("TELA delta probe: no new SCIDs since last run")
			}
		} else {
			// No cache -- full probe (first ever cold start)
			logger.Info("TELA cache miss: running full probe")
			go idx.probeTELA(candidates, chainHeight)
		}

		return nil
	} else {
		// NORMAL FASTSYNC: Fetch SC code + variables for each candidate (parallel)
		workers := runtime.GOMAXPROCS(0)
		if workers > len(candidates) {
			workers = len(candidates)
		}

		var batchMu sync.Mutex
		var processed atomic.Int64
		total := int64(len(candidates))

		work := make(chan *registryEntry, workers*2)
		var wg sync.WaitGroup

		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for entry := range work {
					if idx.Closing.Load() {
						return
					}

					scid := hgpool.InternSCID(entry.scid)
					var scVars []*structures.SCIDVariable
					var matchesFilter bool

					err := idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
						result, err := c.GetSC(scid, chainHeight, nil, nil, len(idx.SearchFilter) > 0)
						if err != nil {
							return err
						}
						if len(idx.SearchFilter) > 0 && result.Code != "" {
							matchesFilter = idx.matchesFilter(result.Code)
						} else if len(idx.SearchFilter) == 0 {
							matchesFilter = true
						}
						if matchesFilter {
							scVars = parseSCVariables(result)
						}
						return nil
					})

					if err != nil || !matchesFilter || len(scVars) == 0 {
						count := processed.Add(1)
						if count%100 == 0 || count == total {
							logger.Infof("FastSync: %d/%d (%.1f%%)", count, total, float64(count)/float64(total)*100)
						}
						continue
					}

					idx.ValidatedSCs.Store(scid, struct{}{})
					batchMu.Lock()
					batch.AddOwner(scid, entry.owner)
					batch.AddVariables(scid, chainHeight, scVars)
					batch.AddInteractionHeight(scid, entry.height)
					batchMu.Unlock()

					count := processed.Add(1)
					if count%100 == 0 || count == total {
						logger.Infof("FastSync: %d/%d (%.1f%%)", count, total, float64(count)/float64(total)*100)
					}
				}
			}()
		}

		for _, entry := range candidates {
			if idx.Closing.Load() {
				break
			}
			work <- entry
		}
		close(work)
		wg.Wait()
	}

	if idx.Closing.Load() {
		return fmt.Errorf("fastsync: interrupted by shutdown")
	}

	// Step 5: Flush batch and set last indexed height to current chain height
	batch.LastHeight = chainHeight
	if err := idx.Store.FlushBatch(batch); err != nil {
		return fmt.Errorf("fastsync: flush batch: %w", err)
	}

	idx.LastIndexedHeight.Store(chainHeight)

	scidCount := len(batch.Owners)
	elapsed := time.Since(start)
	logger.Infof("FastSync complete: %d SCIDs indexed to height %d in %s", scidCount, chainHeight, elapsed.Round(time.Millisecond))
	return nil
}

// probeTELA discovers TELA apps using batch code probing, then fetches their variables.
// Phase 1: Batch GetSC(code=true) across 8 connections to find SCIDs with telaVersion/docVersion
// Phase 2: Batch GetSC(variables=true) for only the matched TELA SCIDs to get metadata
func (idx *Indexer) probeTELA(candidates []*registryEntry, chainHeight int64) {
	start := time.Now()
	probeBatchSize := 100
	var probed atomic.Int64
	total := int64(len(candidates))

	// Sort candidates by height descending -- newer SCIDs first
	// TELA apps are more likely among recent deployments
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].height > candidates[j].height
	})

	scids := make([]string, len(candidates))
	for i, c := range candidates {
		scids[i] = c.scid
	}

	// Phase 1: Batch code probe to find TELA SCIDs (split INDEX vs DOC)
	telaIndexSCIDs := make([]string, 0, 256)
	telaDocSCIDs := make([]string, 0, 768)
	var telaMu sync.Mutex
	work := make(chan []string, 16)

	var sinceLastFind atomic.Int64
	var probeComplete atomic.Bool

	var wg sync.WaitGroup
	poolSize := idx.RPCPool.Size()
	for i := 0; i < poolSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range work {
				if idx.Closing.Load() || probeComplete.Load() {
					return
				}
				idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
					specs := make([]jrpc2.Spec, len(batch))
					for i, scid := range batch {
						specs[i] = jrpc2.Spec{
							Method: "DERO.GetSC",
							Params: rpc.GetSC_Params{SCID: scid, Code: true},
						}
					}
					ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
					defer cancel()

					results, err := c.RPC.Batch(ctx, specs)
					if err != nil {
						return err
					}

					for i, resp := range results {
						var r rpc.GetSC_Result
						if err := resp.UnmarshalResult(&r); err != nil {
							continue
						}
						if r.Code == "" {
							continue
						}
						if strings.Contains(r.Code, `STORE("telaVersion"`) {
							sinceLastFind.Store(0)
							telaMu.Lock()
							telaIndexSCIDs = append(telaIndexSCIDs, batch[i])
							found := len(telaIndexSCIDs) + len(telaDocSCIDs)
							telaMu.Unlock()
							if found%200 == 0 {
								logger.Infof("TELA milestone: %d TELA apps discovered so far", found)
							}
						} else if strings.Contains(r.Code, `STORE("docVersion"`) {
							sinceLastFind.Store(0)
							telaMu.Lock()
							telaDocSCIDs = append(telaDocSCIDs, batch[i])
							found := len(telaIndexSCIDs) + len(telaDocSCIDs)
							telaMu.Unlock()
							if found%200 == 0 {
								logger.Infof("TELA milestone: %d TELA apps discovered so far", found)
							}
						}
					}

					count := probed.Add(int64(len(batch)))
					sinceLastFind.Add(int64(len(batch)))
					if count%5000 == 0 || count >= total {
						telaMu.Lock()
						found := len(telaIndexSCIDs) + len(telaDocSCIDs)
						telaMu.Unlock()
						logger.Infof("TELA probe: %d/%d checked, %d TELA found (%.0f%%)",
							count, total, found, float64(count)/float64(total)*100)
					}

					// Early exit: if we've checked 3k SCIDs since last TELA find, stop all workers
					if sinceLastFind.Load() > 3000 {
						telaMu.Lock()
						found := len(telaIndexSCIDs) + len(telaDocSCIDs)
						telaMu.Unlock()
						if found > 0 {
							probeComplete.Store(true)
							return nil
						}
					}
					return nil
				})
			}
		}()
	}

	for i := 0; i < len(scids); i += probeBatchSize {
		if probeComplete.Load() {
			break
		}
		end := min(i+probeBatchSize, len(scids))
		work <- scids[i:end]
	}
	close(work)
	wg.Wait()

	if probeComplete.Load() {
		telaMu.Lock()
		found := len(telaIndexSCIDs) + len(telaDocSCIDs)
		telaMu.Unlock()
		checked := probed.Load()
		logger.Infof("TELA probe: early exit at %d/%d SCIDs — found %d total", checked, total, found)
	}

	phase1Time := time.Since(start)
	telaCount := len(telaIndexSCIDs) + len(telaDocSCIDs)
	logger.Infof("TELA probe phase 1: %d INDEX + %d DOC = %d total found in %s",
		len(telaIndexSCIDs), len(telaDocSCIDs), telaCount, phase1Time.Round(time.Millisecond))

	if telaCount == 0 {
		return
	}

	// Phase 2: Batch fetch full variables for ONLY the INDEX SCIDs (DOCs don't need variable fetch)
	phase2Start := time.Now()
	varBatch := storage.NewWriteBatch()
	var varMu sync.Mutex
	varWork := make(chan []string, 16)

	var wg2 sync.WaitGroup
	for i := 0; i < poolSize; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			for batch := range varWork {
				if idx.Closing.Load() {
					return
				}
				idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
					specs := make([]jrpc2.Spec, len(batch))
					for i, scid := range batch {
						specs[i] = jrpc2.Spec{
							Method: "DERO.GetSC",
							Params: rpc.GetSC_Params{SCID: scid, Variables: true},
						}
					}
					ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
					defer cancel()

					results, err := c.RPC.Batch(ctx, specs)
					if err != nil {
						return err
					}

					for i, resp := range results {
						var r rpc.GetSC_Result
						if err := resp.UnmarshalResult(&r); err != nil {
							continue
						}
						vars := parseSCVariables(&r)
						if len(vars) > 0 {
							varMu.Lock()
							varBatch.AddVariables(batch[i], chainHeight, vars)
							varMu.Unlock()
						}
					}
					return nil
				})
			}
		}()
	}

	// Feed INDEX SCIDs in batches of 25 (variables are larger than code)
	varBatchSize := 25
	for i := 0; i < len(telaIndexSCIDs); i += varBatchSize {
		end := min(i+varBatchSize, len(telaIndexSCIDs))
		varWork <- telaIndexSCIDs[i:end]
	}
	close(varWork)
	wg2.Wait()

	// Flush TELA variables to DB
	if err := idx.Store.FlushBatch(varBatch); err != nil {
		logger.Errorf("TELA variable flush: %v", err)
	}

	phase2Time := time.Since(phase2Start)
	totalTime := time.Since(start)
	logger.Infof("TELA probe complete: %d INDEX + %d DOC apps | Phase1: %s Phase2: %s Total: %s",
		len(telaIndexSCIDs), len(telaDocSCIDs), phase1Time.Round(time.Millisecond),
		phase2Time.Round(time.Millisecond), totalTime.Round(time.Millisecond))

	structures.TELACount.Store(int64(telaCount))

	// Save TELA cache for instant subsequent startups (Jofito: never repeat work)
	if err := saveTELACache(idx.DBDir, telaIndexSCIDs, telaDocSCIDs, chainHeight); err != nil {
		logger.Errorf("TELA cache save: %v", err)
	} else {
		logger.Infof("TELA cache saved: %d INDEX + %d DOC at height %d", len(telaIndexSCIDs), len(telaDocSCIDs), chainHeight)
	}
}

// telaCache stores discovered TELA SCIDs for instant subsequent startups.
// The Jofito endgame: never probe what you already know.
type telaCache struct {
	IndexSCIDs []string `msgpack:"index"`
	DocSCIDs   []string `msgpack:"doc"`
	Height     int64    `msgpack:"height"`
	Timestamp  int64    `msgpack:"ts"`
}

func telaCachePath(dbDir string) string {
	return filepath.Join(dbDir, "tela_cache.bin")
}

func loadTELACache(dbDir string) (*telaCache, error) {
	data, err := os.ReadFile(telaCachePath(dbDir))
	if err != nil {
		return nil, err
	}
	var cache telaCache
	if err := msgpack.Unmarshal(data, &cache); err != nil {
		return nil, err
	}
	return &cache, nil
}

func saveTELACache(dbDir string, indexSCIDs, docSCIDs []string, height int64) error {
	cache := telaCache{
		IndexSCIDs: indexSCIDs,
		DocSCIDs:   docSCIDs,
		Height:     height,
		Timestamp:  time.Now().Unix(),
	}
	data, err := msgpack.Marshal(&cache)
	if err != nil {
		return err
	}
	return os.WriteFile(telaCachePath(dbDir), data, 0644)
}

// hexTable is a lookup table for hex character validation.
var hexTable = func() [256]bool {
	var t [256]bool
	for c := byte('0'); c <= '9'; c++ {
		t[c] = true
	}
	for c := byte('a'); c <= 'f'; c++ {
		t[c] = true
	}
	for c := byte('A'); c <= 'F'; c++ {
		t[c] = true
	}
	return t
}()

// isHexString returns true if s contains only hexadecimal characters.
func isHexString(s string) bool {
	if len(s) == 0 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !hexTable[s[i]] {
			return false
		}
	}
	return true
}
