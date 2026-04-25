package indexer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/deroproject/derohe/rpc"

	hgrpc "github.com/hypergnomon/hypergnomon/rpc"
	"github.com/hypergnomon/hypergnomon/structures"
)

// sccodeBackfillTimeout bounds the GetSC call a lazy backfill issues. Mirrors
// the fastsync phase-1 probe's 60 s ceiling — install code is usually small
// (<10 KB), but pathological TELA-DOC-1 contracts can reach 500 KB and need
// headroom on slow daemons.
const sccodeBackfillTimeout = 60 * time.Second

// sccodeSingleFlight de-dupes concurrent backfill requests for the same
// SCID. A thundering herd of WS clients asking for GetInitialSCIDCode on the
// same uncached SCID should trigger exactly one daemon round-trip; once the
// persist completes the entry is deleted so the map never grows unbounded.
//
// Package-level — only one Indexer per process in practice, and making this
// per-Indexer would require threading state through the Config struct for a
// feature that doesn't benefit from instance isolation.
var sccodeSingleFlight sync.Map // map[string]*sccodeInflight

type sccodeInflight struct {
	once  sync.Once
	entry *structures.SCCodeEntry
	err   error
}

// sccodeFetcher is the pluggable daemon-side fetch used by GetSCCode on a
// cache miss. Defaults to the real DERO.GetSC RPC call; tests override via
// setSCCodeFetcher to inject deterministic responses + counters without
// spinning up a fake daemon.
//
// Signature: (idx, scid) -> (code, installHeight, err). Empty code + nil err
// means "SCID doesn't exist on-chain." installHeight is the best known value
// (chain tip for backfill; the fastsync probe's exact install height
// populates forward-writes through WriteBatch.AddSCCode, not this function).
type sccodeFetcher func(idx *Indexer, scid string) (string, int64, error)

var currentSCCodeFetcher sccodeFetcher = defaultSCCodeFetcher

// setSCCodeFetcher swaps the backfill fetcher. Returns the previous fetcher
// so tests can restore it in t.Cleanup. Not safe for concurrent test
// execution against the same process — go test's default t.Parallel groups
// should avoid toggling this simultaneously.
func setSCCodeFetcher(f sccodeFetcher) sccodeFetcher {
	prev := currentSCCodeFetcher
	currentSCCodeFetcher = f
	return prev
}

// SCCodeFetcherForTest matches sccodeFetcher's shape but is exported so the
// api package's tests can inject fakes without standing up a live daemon.
// Not intended for production use — nothing in the call graph references it
// outside test files. Kept exported rather than package-private because the
// alternative is an api→internal/indexer_testutil indirection that adds
// nothing for a function pair smaller than its comment.
type SCCodeFetcherForTest func(idx *Indexer, scid string) (string, int64, error)

// SetSCCodeFetcherForTest is the exported shim around setSCCodeFetcher. Use
// from *_test.go files in downstream packages; in-package tests should call
// setSCCodeFetcher directly.
func SetSCCodeFetcherForTest(f SCCodeFetcherForTest) SCCodeFetcherForTest {
	if f == nil {
		prev := setSCCodeFetcher(defaultSCCodeFetcher)
		return SCCodeFetcherForTest(prev)
	}
	prev := setSCCodeFetcher(sccodeFetcher(f))
	return SCCodeFetcherForTest(prev)
}

// defaultSCCodeFetcher issues DERO.GetSC(code=true) through idx.RPCPool.
func defaultSCCodeFetcher(idx *Indexer, scid string) (string, int64, error) {
	if idx.RPCPool == nil {
		return "", 0, fmt.Errorf("rpc pool not configured")
	}
	var code string
	err := idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
		specs := []jrpc2.Spec{{
			Method: "DERO.GetSC",
			Params: rpc.GetSC_Params{SCID: scid, Code: true},
		}}
		ctx, cancel := context.WithTimeout(context.Background(), sccodeBackfillTimeout)
		defer cancel()
		results, err := c.RPC.Batch(ctx, specs)
		if err != nil {
			return err
		}
		if len(results) == 0 {
			return fmt.Errorf("empty batch result")
		}
		var r rpc.GetSC_Result
		if err := results[0].UnmarshalResult(&r); err != nil {
			return err
		}
		code = r.Code
		return nil
	})
	if err != nil {
		return "", 0, err
	}
	// Prefer the real install height from ClassMeta (populated at scan
	// time) for SCIDs we've already classified. Falls back to chain tip
	// only for genuinely pre-feature SCIDs — a strict improvement over
	// unconditionally using ChainHeight, which produced wrong heights
	// for every classified SCID hit through the lazy path.
	height := idx.ChainHeight.Load()
	if meta, err := idx.Store.GetSCIDClass(scid); err == nil && meta != nil && meta.InstallHeight > 0 {
		height = meta.InstallHeight
	}
	return code, height, nil
}

// GetSCCode returns the install-time DVM code for scid.
//
// Lookup order:
//  1. sccode bucket (O(1) bbolt Get, sub-µs on hot cache).
//  2. On miss: sccodeFetcher (real daemon in production, injectable in tests),
//     persist, return.
//
// Single-flight keyed on scid coalesces concurrent miss requests. Returns
// (nil, nil) when the SCID doesn't exist on-chain (fetcher returns empty
// code + nil err). Errors are transport/DB problems.
func (idx *Indexer) GetSCCode(scid string) (*structures.SCCodeEntry, error) {
	if idx == nil || idx.Store == nil {
		return nil, fmt.Errorf("indexer not initialized")
	}
	if scid == "" {
		return nil, fmt.Errorf("empty scid")
	}

	if entry, err := idx.Store.GetSCCode(scid); err != nil {
		return nil, err
	} else if entry != nil {
		return entry, nil
	}

	loaded, _ := sccodeSingleFlight.LoadOrStore(scid, &sccodeInflight{})
	flight := loaded.(*sccodeInflight)
	flight.once.Do(func() {
		defer sccodeSingleFlight.Delete(scid)

		// Re-check bucket under the Once — another request may have
		// persisted while we were waiting for the Once to claim us.
		if entry, err := idx.Store.GetSCCode(scid); err != nil {
			flight.err = err
			return
		} else if entry != nil {
			flight.entry = entry
			return
		}

		code, installH, err := currentSCCodeFetcher(idx, scid)
		if err != nil {
			flight.err = err
			return
		}
		if code == "" {
			// SCID doesn't exist on-chain or daemon returned empty.
			return
		}
		entry := &structures.SCCodeEntry{Code: code, InstallHeight: installH}
		if err := idx.Store.PutSCCode(scid, entry); err != nil {
			// Persist failed but we have the code; surface the code anyway
			// so the caller can proceed. Log at debug — next call will
			// retry the persist path.
			logger.Debugf("sccode persist for %s: %v (returning entry without cache)", scid, err)
		}
		flight.entry = entry
	})

	return flight.entry, flight.err
}
