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
	// AND written by the current schema version AND the on-disk scvars
	// bucket still yields non-empty variable reads for sampled TELA INDEX
	// SCIDs. Strict version equality invalidates any cache older than
	// telaCacheVersion — that forces a one-time full re-probe after an
	// HG upgrade so schema changes (like the 60s refresher's fresh-rating
	// contract) take effect without manual reindex.
	var chainHeight int64
	if cached, err := loadTELACache(idx.DBDir); err == nil && cached.Height > 0 && cached.Version == telaCacheVersion {
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
				// Sanity-probe the scvars bucket. Sample up to 5 TELA INDEX
				// SCIDs from the cached list; read their variables at the
				// cache's height. If every sampled read comes back empty or
				// error, the persistence layer is broken — invalidate the
				// cache and fall through to a fresh probe. If at least one
				// succeeds, honor the cache.
				ok := cachedScvarsLookReadable(idx, cached)
				if ok {
					idx.LastIndexedHeight.Store(chainHeight)
					otherCount := 0
					for _, v := range cached.Classes {
						otherCount += len(v)
					}
					structures.TELACount.Store(int64(len(cached.IndexSCIDs) + len(cached.DocSCIDs)))
					elapsed := time.Since(start)
					logger.Infof("FastSync: cache fresh v%d (height %d, %d blocks behind) — %d INDEX + %d DOC + %d other classes loaded in %s",
						cached.Version, cached.Height, chainHeight-cached.Height,
						len(cached.IndexSCIDs), len(cached.DocSCIDs), otherCount, elapsed.Round(time.Millisecond))
					return nil
				}
				logger.Warnf("FastSync: cache height-fresh but scvars reads failed — forcing re-probe to heal corrupted variable blobs (pre-fix HyperGnomon artifact)")
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
		// TURBO FASTSYNC: Skip ALL GetSC calls. Store SCID + owner + height
		// from the registry, then claim progress only after the DB commit.
		for _, entry := range candidates {
			scid := hgpool.InternSCID(entry.scid)
			idx.ValidatedSCs.Store(scid, struct{}{})
			batch.AddOwner(scid, entry.owner)
			batch.AddInteractionHeight(scid, entry.height)
			// Route B: install record + owner→scid edge.
			// Class is filled in later by probeTELA's phase-1 code probe.
			batch.AddInstall(scid, entry.height, &structures.InstallRecord{
				Owner: entry.owner,
			})
			if entry.owner != "" {
				batch.AddAddrSCID(entry.owner, scid, entry.height)
			}
		}

		batch.LastHeight = chainHeight
		flushStart := time.Now()
		if err := idx.Store.FlushBatch(batch); err != nil {
			storage.PutWriteBatch(batch)
			return fmt.Errorf("fastsync: turbo flush batch: %w", err)
		}
		persisted := len(batch.Owners)
		storage.PutWriteBatch(batch)
		idx.LastIndexedHeight.Store(chainHeight)
		elapsed := time.Since(start)
		logger.Infof("FastSync complete: %d SCIDs persisted to height %d in %s (flush=%s)",
			persisted, chainHeight, elapsed.Round(time.Millisecond), time.Since(flushStart).Round(time.Millisecond))

		// JOFITO CACHE: Check if we have a cached TELA list from a previous run.
		// If so, load it instantly and only probe NEW SCIDs (delta since cached height).
		// Apply the same gates the up-front cache-hit path uses — version match
		// and a scvars sanity sample — so we don't trust a corrupt cache twice
		// in the same FastSync call.
		cached, cacheErr := loadTELACache(idx.DBDir)
		cacheUsable := cacheErr == nil && cached != nil && cached.Height > 0 &&
			cached.Version == telaCacheVersion && cachedScvarsLookReadable(idx, cached)
		if cacheUsable {
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
				go idx.probeTELA(deltaCandidates, chainHeight, true)
			} else {
				logger.Info("TELA delta probe: no new SCIDs since last run")
			}
		} else {
			// Cache missing, old-version, or corrupt — full probe. This is the
			// self-heal path that overwrites historical malformed scvars blobs
			// with cleanly-encoded bytes from the current marshaler.
			if cacheErr != nil {
				logger.Info("TELA cache miss: running full probe")
			} else {
				logger.Infof("TELA cache reject: v%d != v%d or scvars sanity failed — running full probe to heal",
					cached.Version, telaCacheVersion)
			}
			go idx.probeTELA(candidates, chainHeight, false)
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
		storage.PutWriteBatch(batch)
		return fmt.Errorf("fastsync: flush batch: %w", err)
	}

	idx.LastIndexedHeight.Store(chainHeight)

	scidCount := len(batch.Owners)
	storage.PutWriteBatch(batch)
	elapsed := time.Since(start)
	logger.Infof("FastSync complete: %d SCIDs indexed to height %d in %s", scidCount, chainHeight, elapsed.Round(time.Millisecond))

	// Non-turbo FastSync used to skip probeTELA entirely, leaving TELACount
	// at 0 and --tela-only hanging forever. Run the probe here so every
	// FastSync path populates the TELA bucket regardless of scan strategy.
	// The turbo branch above has its own cache-aware probe launch; this
	// only fires on the non-turbo codepath.
	go idx.probeTELA(candidates, chainHeight, false)

	return nil
}

// probeTELA discovers TELA apps using batch code probing, then fetches their variables.
// Phase 1: Batch GetSC(code=true) across 8 connections to find SCIDs with telaVersion/docVersion
// Phase 2: Batch GetSC(variables=true) for only the matched TELA SCIDs to get metadata
func (idx *Indexer) probeTELA(candidates []*registryEntry, chainHeight int64, allowEarlyExit bool) {
	start := time.Now()
	probeBatchSize := normalizeClassifyProbeBatchSize(idx.ClassifyProbeBatchSize)
	var probed atomic.Int64
	total := int64(len(candidates))
	var phase1SpecNanos atomic.Int64
	var phase1RPCNanos atomic.Int64
	var phase1DecodeNanos atomic.Int64
	var phase1ScanNanos atomic.Int64
	var phase1Batches atomic.Int64
	var phase1RPCErrors atomic.Int64
	var phase1DecodeErrors atomic.Int64
	var phase1CodeBytes atomic.Int64
	var phase2SpecNanos atomic.Int64
	var phase2RPCNanos atomic.Int64
	var phase2DecodeNanos atomic.Int64
	var phase2ParseNanos atomic.Int64
	var phase2ClassifyNanos atomic.Int64
	var phase2Batches atomic.Int64
	var phase2RPCErrors atomic.Int64
	var phase2DecodeErrors atomic.Int64
	var phase2Vars atomic.Int64
	var codeFlushTime time.Duration
	var varFlushTime time.Duration
	var cacheSaveTime time.Duration

	// Sort candidates by height descending -- newer SCIDs first
	// TELA apps are more likely among recent deployments
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].height > candidates[j].height
	})

	scids := make([]string, len(candidates))
	heightByScid := make(map[string]int64, len(candidates))
	for i, c := range candidates {
		scids[i] = c.scid
		heightByScid[c.scid] = c.height
	}

	// codeBatch accumulates install-time code for every SCID whose phase-1
	// probe matched a known class. Flushed between phase 1 and phase 2 so
	// phase-2's varBatch stays focused on variables + class metadata.
	codeBatch := storage.NewWriteBatch()
	var codeMu sync.Mutex

	// Phase 1: Batch code probe to find TELA SCIDs (split INDEX vs DOC) plus
	// any other classes classify.go knows about (NFA, G45-*, T345). We reuse
	// the same rules slice the scanner uses so classification stays
	// consistent across the fastsync path and the live-scan path.
	telaIndexSCIDs := make([]string, 0, 256)
	telaDocSCIDs := make([]string, 0, 768)
	otherClassSCIDs := make(map[string][]string)
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
				var lastErr error
				for attempt := 1; attempt <= 2; attempt++ {
					lastErr = idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
						tSpec := time.Now()
						specs := make([]jrpc2.Spec, len(batch))
						for i, scid := range batch {
							specs[i] = jrpc2.Spec{
								Method: "DERO.GetSC",
								Params: rpc.GetSC_Params{SCID: scid, Code: true},
							}
						}
						phase1SpecNanos.Add(time.Since(tSpec).Nanoseconds())
						ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
						defer cancel()

						tRPC := time.Now()
						results, err := c.RPC.Batch(ctx, specs)
						phase1RPCNanos.Add(time.Since(tRPC).Nanoseconds())
						phase1Batches.Add(1)
						if err != nil {
							return err
						}

						for i, resp := range results {
							var r rpc.GetSC_Result
							tDecode := time.Now()
							if err := resp.UnmarshalResult(&r); err != nil {
								phase1DecodeNanos.Add(time.Since(tDecode).Nanoseconds())
								phase1DecodeErrors.Add(1)
								continue
							}
							phase1DecodeNanos.Add(time.Since(tDecode).Nanoseconds())
							if r.Code == "" {
								continue
							}
							phase1CodeBytes.Add(int64(len(r.Code)))
							// Walk classify.go's rule list in order — first match wins.
							// More specific patterns come first (G45-FAT before G45-C, etc.).
							tScan := time.Now()
							var matched string
							for _, rule := range rules {
								if strings.Contains(r.Code, rule.pattern) {
									matched = rule.class
									break
								}
							}
							phase1ScanNanos.Add(time.Since(tScan).Nanoseconds())
							if matched == "" {
								continue
							}
							sinceLastFind.Store(0)
							scidStr := batch[i]
							telaMu.Lock()
							switch matched {
							case "TELA-INDEX-1":
								telaIndexSCIDs = append(telaIndexSCIDs, scidStr)
							case "TELA-DOC-1":
								telaDocSCIDs = append(telaDocSCIDs, scidStr)
							default:
								otherClassSCIDs[matched] = append(otherClassSCIDs[matched], scidStr)
							}
							// Persist the install-time code. Skip TELA-DOC-1 when
							// the operator opted out (flag --skip-tela-doc-code).
							persistCode := idx.shouldPersistCode(matched)
							telaMu.Unlock()
							if persistCode {
								codeMu.Lock()
								codeBatch.AddSCCode(scidStr, heightByScid[scidStr], r.Code)
								codeMu.Unlock()
							}
							telaMu.Lock()
							found := len(telaIndexSCIDs) + len(telaDocSCIDs)
							for _, v := range otherClassSCIDs {
								found += len(v)
							}
							telaMu.Unlock()
							if found%200 == 0 {
								logger.Infof("Classify milestone: %d classified SCIDs discovered so far", found)
							}
						}

						count := probed.Add(int64(len(batch)))
						sinceLastFind.Add(int64(len(batch)))
						if count%5000 == 0 || count >= total {
							telaMu.Lock()
							found := len(telaIndexSCIDs) + len(telaDocSCIDs)
							for _, v := range otherClassSCIDs {
								found += len(v)
							}
							telaMu.Unlock()
							logger.Infof("Classify probe: %d/%d checked, %d classified (%.0f%%)",
								count, total, found, float64(count)/float64(total)*100)
						}

						// Early exit is only safe for cache-backed delta probes.
						if allowEarlyExit && sinceLastFind.Load() > 3000 {
							telaMu.Lock()
							found := len(telaIndexSCIDs) + len(telaDocSCIDs)
							for _, v := range otherClassSCIDs {
								found += len(v)
							}
							telaMu.Unlock()
							if found > 0 {
								probeComplete.Store(true)
								return nil
							}
						}
						return nil
					})
					if lastErr == nil || idx.Closing.Load() || probeComplete.Load() {
						break
					}
					time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
				}
				if lastErr != nil {
					phase1RPCErrors.Add(1)
					logger.Warnf("Classify phase 1 batch failed after retry (%d SCIDs): %v", len(batch), lastErr)
				}
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

	// Roll up class counts for logging
	totalClassified := len(telaIndexSCIDs) + len(telaDocSCIDs)
	for _, v := range otherClassSCIDs {
		totalClassified += len(v)
	}

	if probeComplete.Load() {
		checked := probed.Load()
		logger.Infof("Classify probe: early exit at %d/%d SCIDs — found %d total", checked, total, totalClassified)
	}

	phase1Time := time.Since(start)
	logger.Infof("Classify probe phase 1: %d INDEX + %d DOC + %d other classes = %d total in %s",
		len(telaIndexSCIDs), len(telaDocSCIDs), totalClassified-len(telaIndexSCIDs)-len(telaDocSCIDs),
		totalClassified, phase1Time.Round(time.Millisecond))
	telaCount := len(telaIndexSCIDs) + len(telaDocSCIDs) // kept for cache compatibility

	if totalClassified == 0 {
		logger.Infof("Classify probe timings: phase1_spec=%s phase1_rpc=%s phase1_decode=%s phase1_scan=%s phase1_batches=%d phase1_batch_size=%d phase1_rpc_errors=%d phase1_decode_errors=%d code_bytes=%d early_exit_allowed=%v",
			time.Duration(phase1SpecNanos.Load()).Round(time.Millisecond),
			time.Duration(phase1RPCNanos.Load()).Round(time.Millisecond),
			time.Duration(phase1DecodeNanos.Load()).Round(time.Millisecond),
			time.Duration(phase1ScanNanos.Load()).Round(time.Millisecond),
			phase1Batches.Load(), probeBatchSize, phase1RPCErrors.Load(), phase1DecodeErrors.Load(),
			phase1CodeBytes.Load(), allowEarlyExit)
		storage.PutWriteBatch(codeBatch)
		return
	}

	// Flush phase-1 code snapshots before phase-2 starts so a crash between
	// phases still leaves the DB with authoritative install code for any
	// SCID we successfully classified.
	if len(codeBatch.SCCodes) > 0 {
		codeFlushStart := time.Now()
		if err := idx.Store.FlushBatch(codeBatch); err != nil {
			logger.Errorf("FastSync phase-1 code flush: %v", err)
		}
		codeFlushTime = time.Since(codeFlushStart)
	}
	storage.PutWriteBatch(codeBatch)

	// Phase 2: Batch fetch full variables for BOTH INDEX and DOC SCIDs.
	// HOLOGRAM's GetMyDOCs / SearchByKey("docVersion") / GetAllDOCTypes / GetMyINDEXes
	// all read scvars for TELA-DOC-1 SCIDs, so we must persist their variables here.
	phase2Start := time.Now()
	varBatch := storage.NewWriteBatch()
	var varMu sync.Mutex
	type varBatchItem struct {
		scids []string
		class string
	}
	varWork := make(chan varBatchItem, 16)

	var wg2 sync.WaitGroup
	for i := 0; i < poolSize; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			for item := range varWork {
				if idx.Closing.Load() {
					return
				}
				var lastErr error
				for attempt := 1; attempt <= 2; attempt++ {
					lastErr = idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
						tSpec := time.Now()
						specs := make([]jrpc2.Spec, len(item.scids))
						for i, scid := range item.scids {
							specs[i] = jrpc2.Spec{
								Method: "DERO.GetSC",
								Params: rpc.GetSC_Params{SCID: scid, Variables: true},
							}
						}
						phase2SpecNanos.Add(time.Since(tSpec).Nanoseconds())
						ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
						defer cancel()

						tRPC := time.Now()
						results, err := c.RPC.Batch(ctx, specs)
						phase2RPCNanos.Add(time.Since(tRPC).Nanoseconds())
						phase2Batches.Add(1)
						if err != nil {
							return err
						}

						for i, resp := range results {
							var r rpc.GetSC_Result
							tDecode := time.Now()
							if err := resp.UnmarshalResult(&r); err != nil {
								phase2DecodeNanos.Add(time.Since(tDecode).Nanoseconds())
								phase2DecodeErrors.Add(1)
								continue
							}
							phase2DecodeNanos.Add(time.Since(tDecode).Nanoseconds())
							tParse := time.Now()
							vars := parseSCVariables(&r)
							phase2ParseNanos.Add(time.Since(tParse).Nanoseconds())
							phase2Vars.Add(int64(len(vars)))
							tClassify := time.Now()
							varMap := varsToMap(vars)
							sc := ClassifySC(item.scids[i], "", varMap)
							durl, version := telaFieldsForClass(item.class, varMap)
							meta := &structures.ClassMeta{
								Class:         item.class,
								Tags:          tagsForClass(item.class),
								Name:          sc.Name,
								Desc:          sc.Desc,
								IconURL:       sc.IconURL,
								DURL:          durl,
								Version:       version,
								InstallHeight: chainHeight,
								LastHeight:    chainHeight,
							}
							phase2ClassifyNanos.Add(time.Since(tClassify).Nanoseconds())

							varMu.Lock()
							if len(vars) > 0 {
								varBatch.AddVariables(item.scids[i], chainHeight, vars)
							}
							varBatch.AddClass(item.scids[i], meta)
							varMu.Unlock()
						}
						return nil
					})
					if lastErr == nil || idx.Closing.Load() {
						break
					}
					time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
				}
				if lastErr != nil {
					phase2RPCErrors.Add(1)
					logger.Warnf("Classify phase 2 batch failed after retry (%s, %d SCIDs): %v", item.class, len(item.scids), lastErr)
				}
			}
		}()
	}

	// Feed INDEX SCIDs in batches of 25 (variables are larger than code)
	varBatchSize := 25
	for i := 0; i < len(telaIndexSCIDs); i += varBatchSize {
		end := min(i+varBatchSize, len(telaIndexSCIDs))
		varWork <- varBatchItem{scids: telaIndexSCIDs[i:end], class: "TELA-INDEX-1"}
	}
	for i := 0; i < len(telaDocSCIDs); i += varBatchSize {
		end := min(i+varBatchSize, len(telaDocSCIDs))
		varWork <- varBatchItem{scids: telaDocSCIDs[i:end], class: "TELA-DOC-1"}
	}
	// Non-TELA classes: skip the variable fetch. HOLOGRAM's Browser renders
	// them from ClassMeta alone (class name + optional Name/Desc/Icon from
	// later on-invoke ClassifySC calls). Writing class meta directly here
	// keeps the class bucket populated without burning RPC on ~50k GetSC
	// variables-true calls that nobody currently reads.
	for class, scids := range otherClassSCIDs {
		meta := &structures.ClassMeta{
			Class:         class,
			Tags:          tagsForClass(class),
			InstallHeight: chainHeight,
			LastHeight:    chainHeight,
		}
		for _, scid := range scids {
			varMu.Lock()
			varBatch.AddClass(scid, meta)
			varMu.Unlock()
		}
	}
	close(varWork)
	wg2.Wait()

	// Flush classified variables + class metadata to DB
	varFlushStart := time.Now()
	if err := idx.Store.FlushBatch(varBatch); err != nil {
		logger.Errorf("Classify flush: %v", err)
	}
	varFlushTime = time.Since(varFlushStart)
	storage.PutWriteBatch(varBatch)

	phase2Time := time.Since(phase2Start)
	totalTime := time.Since(start)
	otherCount := 0
	for _, v := range otherClassSCIDs {
		otherCount += len(v)
	}
	logger.Infof("Classify probe complete: %d INDEX + %d DOC + %d other | Phase1: %s Phase2: %s Total: %s",
		len(telaIndexSCIDs), len(telaDocSCIDs), otherCount, phase1Time.Round(time.Millisecond),
		phase2Time.Round(time.Millisecond), totalTime.Round(time.Millisecond))
	logger.Infof("Classify probe timings: phase1_spec=%s phase1_rpc=%s phase1_decode=%s phase1_scan=%s code_flush=%s phase2_spec=%s phase2_rpc=%s phase2_decode=%s phase2_parse=%s phase2_classify=%s var_flush=%s phase1_batches=%d phase1_batch_size=%d phase2_batches=%d rpc_errors=%d/%d decode_errors=%d/%d code_bytes=%d vars=%d early_exit_allowed=%v",
		time.Duration(phase1SpecNanos.Load()).Round(time.Millisecond),
		time.Duration(phase1RPCNanos.Load()).Round(time.Millisecond),
		time.Duration(phase1DecodeNanos.Load()).Round(time.Millisecond),
		time.Duration(phase1ScanNanos.Load()).Round(time.Millisecond),
		codeFlushTime.Round(time.Millisecond),
		time.Duration(phase2SpecNanos.Load()).Round(time.Millisecond),
		time.Duration(phase2RPCNanos.Load()).Round(time.Millisecond),
		time.Duration(phase2DecodeNanos.Load()).Round(time.Millisecond),
		time.Duration(phase2ParseNanos.Load()).Round(time.Millisecond),
		time.Duration(phase2ClassifyNanos.Load()).Round(time.Millisecond),
		varFlushTime.Round(time.Millisecond),
		phase1Batches.Load(), probeBatchSize, phase2Batches.Load(),
		phase1RPCErrors.Load(), phase2RPCErrors.Load(),
		phase1DecodeErrors.Load(), phase2DecodeErrors.Load(),
		phase1CodeBytes.Load(), phase2Vars.Load(), allowEarlyExit)

	structures.TELACount.Store(int64(telaCount))

	// Save cache for instant subsequent startups (Jofito: never repeat work)
	cacheSaveStart := time.Now()
	if err := saveTELACache(idx.DBDir, telaIndexSCIDs, telaDocSCIDs, otherClassSCIDs, chainHeight); err != nil {
		logger.Errorf("Classify cache save: %v", err)
	} else {
		cacheSaveTime = time.Since(cacheSaveStart)
		logger.Infof("Classify cache v%d saved: %d INDEX + %d DOC + %d other classes at height %d",
			telaCacheVersion, len(telaIndexSCIDs), len(telaDocSCIDs), otherCount, chainHeight)
	}
	if cacheSaveTime > 0 {
		logger.Infof("Classify cache save timing: cache_save=%s", cacheSaveTime.Round(time.Millisecond))
	}
}

// telaCache stores discovered SCIDs for instant subsequent startups.
// The Jofito endgame: never probe what you already know.
//
// v1 shape had only IndexSCIDs/DocSCIDs. v2 adds Classes for the
// extended classify probe (NFA, G45-*). When loadTELACache decodes a v1
// file, Classes comes back nil — callers treat nil as "no cache" so a
// one-shot re-probe picks up the new classes and the on-disk cache
// upgrades itself naturally.
type telaCache struct {
	Version    int                 `msgpack:"v,omitempty"`
	IndexSCIDs []string            `msgpack:"index"`
	DocSCIDs   []string            `msgpack:"doc"`
	Classes    map[string][]string `msgpack:"classes,omitempty"`
	Height     int64               `msgpack:"height"`
	Timestamp  int64               `msgpack:"ts"`
}

// telaCacheVersion is the current on-disk cache version. Bump when the
// scvars/classify pipeline changes in a way that older snapshots can't
// serve — a mismatch forces a one-time full re-probe on next launch,
// which self-heals any stale or corrupted state from prior builds.
//
// v3 (2026-04-20): paired with the 60s TELA refresher — old v2 caches
// were written before the refresher existed, so their rating data is
// frozen at probe time. Strict equality (not >=) invalidates them.
const telaCacheVersion = 3

func telaCachePath(dbDir string) string {
	return filepath.Join(dbDir, "tela_cache.bin")
}

// telaCacheMu serializes concurrent reads/writes of tela_cache.bin.
// Writers: probeTELA (full refresh), monitorChainHeight (height bump),
// fastsync-cache-hit (no-op). A plain sync.Mutex is enough — the file is tiny
// and contention is low.
var telaCacheMu sync.Mutex

func loadTELACache(dbDir string) (*telaCache, error) {
	telaCacheMu.Lock()
	defer telaCacheMu.Unlock()
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

func saveTELACache(dbDir string, indexSCIDs, docSCIDs []string, otherClasses map[string][]string, height int64) error {
	telaCacheMu.Lock()
	defer telaCacheMu.Unlock()
	cache := telaCache{
		Version:    telaCacheVersion,
		IndexSCIDs: indexSCIDs,
		DocSCIDs:   docSCIDs,
		Classes:    otherClasses,
		Height:     height,
		Timestamp:  time.Now().Unix(),
	}
	data, err := msgpack.Marshal(&cache)
	if err != nil {
		return err
	}
	path := telaCachePath(dbDir)
	tmp := path + ".tmp"
	// 0600 matches BoltDB permissions — cache contains indexer-derived data only,
	// but tighter is fine. Write a temp file first so a crash does not leave
	// a truncated cache at the canonical path.
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		// Windows does not replace existing files with Rename. Remove+rename is
		// not fully atomic, but still avoids serving partially-written bytes.
		_ = os.Remove(path)
		if err2 := os.Rename(tmp, path); err2 != nil {
			_ = os.Remove(tmp)
			return err
		}
	}
	return nil
}

// cachedScvarsLookReadable samples cached TELA INDEX SCIDs and checks
// whether their variable snapshots decode AND contain the shape we
// expect for a TELA INDEX (at least one "dero1q…" rating-address key).
// Returns true only if the majority of the sample looks healthy.
//
// A TELA INDEX always initializes with STORE("likes",0)+STORE("dislikes",0)
// plus STORE(addr, ...) entries as users rate — so a healthy scvars for
// this SCID must contain `likes`/`dislikes` plus typically one or more
// rating-address keys. Pre-fix HyperGnomon wrote garbage payload bytes
// that decode as "valid" structurally but lose address keys; this check
// catches that drift in a bounded O(5) probe.
func cachedScvarsLookReadable(idx *Indexer, cached *telaCache) bool {
	if idx == nil || cached == nil {
		return false
	}
	sample := cached.IndexSCIDs
	if len(sample) == 0 {
		// No TELA INDEXes cached at all — nothing to validate; trust cache.
		return true
	}
	if len(sample) > 5 {
		sample = sample[:5]
	}
	healthy := 0
	for _, scid := range sample {
		vars, err := idx.Store.GetSCIDVariableDetailsAtHeight(scid, cached.Height)
		if err != nil {
			logger.Debugf("cache sanity: scid %s decode err: %v", scid[:16], err)
			continue
		}
		if len(vars) == 0 {
			continue
		}
		// A healthy TELA INDEX snapshot always carries the likes/dislikes
		// counter keys — their presence means the blob decoded into the
		// real variable set, not a truncated/scrambled payload.
		seenLikes := false
		seenDislikes := false
		for _, v := range vars {
			switch k := v.Key.(type) {
			case string:
				if k == "likes" {
					seenLikes = true
				}
				if k == "dislikes" {
					seenDislikes = true
				}
			}
			if seenLikes && seenDislikes {
				break
			}
		}
		if seenLikes && seenDislikes {
			healthy++
		}
	}
	// ≥ 60% of the sample must look healthy to trust the cache.
	return healthy*10 >= len(sample)*6
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
