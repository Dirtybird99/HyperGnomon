package indexer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/deroproject/derohe/rpc"

	hgrpc "github.com/hypergnomon/hypergnomon/rpc"
	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

// telaRefreshInterval is how often the refresher re-fetches variable
// snapshots for every TELA INDEX in the class bucket. 60s is a compromise
// between rating freshness in the Browser (users expect sub-minute
// feedback after they rate or update an app) and daemon load (~90 GetSC
// calls per minute is ~2/s, well inside a healthy node's headroom).
const telaRefreshInterval = 60 * time.Second

// telaRefreshBatch is the chunk size for the RPC batch call. Mirrors
// the phase-2 probe's batch size so we reuse the same per-connection
// characteristics the probe already tuned.
const telaRefreshBatch = 25

// startTELARefresher launches a goroutine that periodically re-fetches
// variable snapshots for every SCID in the TELA-INDEX-1 class bucket.
// Rationale: HOLOGRAM defaults HG to TurboMode, which skips GetSC on
// scan invokes — so without a second pass the scvars bucket freezes
// at fastsync time and new ratings / state changes never land. This
// goroutine closes that staleness window without changing the scan's
// hot path.
//
// Cheap to call — no-op if the class bucket is empty. Exits on
// idx.Closing.
func (idx *Indexer) startTELARefresher() {
	go func() {
		// Stagger the first tick so the probe that ran moments before
		// us doesn't contend with our refresh. The probe already wrote
		// fresh data — our first meaningful refresh can wait a minute.
		ticker := time.NewTicker(telaRefreshInterval)
		defer ticker.Stop()
		for range ticker.C {
			if idx.Closing.Load() {
				return
			}
			n, err := idx.RefreshTELAVars()
			if err != nil {
				logger.Debugf("TELA refresh: %v", err)
			} else if n > 0 {
				logger.Debugf("TELA refresh: refreshed %d apps", n)
			}
		}
	}()
}

// RefreshTELAVars is a convenience shim that refreshes TELA-INDEX-1
// apps. Kept for the existing 60s background loop and HOLOGRAM's
// RefreshBrowserApps Wails method.
func (idx *Indexer) RefreshTELAVars() (int, error) {
	return idx.RefreshClassVars("TELA-INDEX-1")
}

// RefreshClassVars fetches current variables for every SCID in the named
// class bucket and writes them to scvars at current ChainHeight. Returns
// the number of SCIDs whose vars were persisted.
//
// Rationale: only TELA INDEX/DOC get their vars fetched during fastsync
// phase 2 (by design — saves time on a cold start). Other classes like
// NFA and G45 stay in the class bucket with empty metadata until a
// consumer asks for them. This method is the on-demand var-hydration
// path for those classes, called once per (class, user-session-window)
// when the Browser's NFA / G45 chip is selected.
//
// Batches GetSC(Variables=true) across the RPC pool. For ~2,600 NFA
// contracts on mainnet this is 5-15s on a fresh daemon, then subsequent
// calls are cache hits until the chain advances.
//
// Safe to call concurrently with the scan loop — bolt's MVCC handles
// reader/writer isolation. Touches scvars + classIdx + the class bucket
// (updates ClassMeta.Name/Desc/Icon from the freshly-fetched vars).
func (idx *Indexer) RefreshClassVars(class string) (int, error) {
	if idx == nil || idx.Store == nil || idx.RPCPool == nil {
		return 0, nil
	}
	if idx.Closing.Load() {
		return 0, nil
	}
	if class == "" {
		return 0, nil
	}

	installs, err := idx.Store.GetClassInstalls(class, 0)
	if err != nil {
		return 0, err
	}
	if len(installs) == 0 {
		return 0, nil
	}

	chainHeight := idx.ChainHeight.Load()
	if chainHeight == 0 {
		return 0, nil
	}

	scids := make([]string, 0, len(installs))
	for _, inst := range installs {
		scids = append(scids, inst.SCID)
	}

	start := time.Now()
	batch := storage.NewWriteBatch()
	var batchMu sync.Mutex
	persisted := 0
	var rpcErrors atomic.Int64
	var decodeErrors atomic.Int64

	workChan := make(chan []string, 8)
	var wg sync.WaitGroup
	poolSize := idx.RPCPool.Size()
	if poolSize < 1 {
		poolSize = 1
	}
	if poolSize > 8 {
		poolSize = 8
	}

	classTags := tagsForClass(class)

	// Fast-path keys: when the class has a known manifest and we're not due
	// for a full refresh, ask the daemon for only those keys (Variables=false
	// + KeysString). Daemon skips its cursor scan; response is O(|keys|).
	// Every fullRefreshEvery'th call falls back to full Variables=true so
	// new keys still get picked up.
	fastKeys := pickRefreshKeys(class)

	for i := 0; i < poolSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range workChan {
				if idx.Closing.Load() {
					return
				}
				var lastErr error
				for attempt := 1; attempt <= 2; attempt++ {
					lastErr = idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
						specs := make([]jrpc2.Spec, len(chunk))
						for i, scid := range chunk {
							specs[i] = jrpc2.Spec{
								Method: "DERO.GetSC",
								Params: buildGetSCParams(scid, fastKeys),
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
								decodeErrors.Add(1)
								continue
							}
							var vars []*structures.SCIDVariable
							if fastKeys != nil {
								vars = scVarsFromKeyValues(fastKeys, r.ValuesString)
							} else {
								vars = parseSCVariables(&r)
							}
							if len(vars) == 0 {
								continue
							}
							// Single-pass: class comes from the class bucket we're
							// refreshing, so seed it and let extractClassVars pull
							// DURL/Version in the same walk (audit #8).
							sc := ClassifySCVarsWithClass(chunk[i], class, vars)
							meta := &structures.ClassMeta{
								Class:         class,
								Tags:          classTags,
								Name:          sc.Name,
								Desc:          sc.Desc,
								IconURL:       sc.IconURL,
								DURL:          sc.DURL,
								Version:       sc.Version,
								InstallHeight: chainHeight,
								LastHeight:    chainHeight,
							}
							batchMu.Lock()
							batch.AddVariables(chunk[i], chainHeight, vars)
							batch.AddClass(chunk[i], meta)
							persisted++
							batchMu.Unlock()
						}
						return nil
					})
					if lastErr == nil || idx.Closing.Load() {
						break
					}
					time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
				}
				if lastErr != nil {
					rpcErrors.Add(1)
				}
			}
		}()
	}

	for i := 0; i < len(scids); i += telaRefreshBatch {
		if idx.Closing.Load() {
			break
		}
		end := min(i+telaRefreshBatch, len(scids))
		workChan <- scids[i:end]
	}
	close(workChan)
	wg.Wait()

	if persisted > 0 {
		if err := idx.Store.FlushBatch(batch); err != nil {
			storage.PutWriteBatch(batch)
			return 0, err
		}
	}
	storage.PutWriteBatch(batch)

	mode := "full"
	if fastKeys != nil {
		mode = "fast"
	}
	logger.Infof("Class refresh %s (%s): %d apps persisted at height %d in %s",
		class, mode, persisted, chainHeight, time.Since(start).Round(time.Millisecond))
	if rpcErrors.Load() > 0 || decodeErrors.Load() > 0 {
		logger.Warnf("Class refresh %s (%s): rpc_errors=%d decode_errors=%d", class, mode, rpcErrors.Load(), decodeErrors.Load())
	}
	return persisted, nil
}
