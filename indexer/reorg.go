package indexer

import "github.com/deroproject/derohe/block"

// Reorg detection (M1 stub)
//
// This file contains the lightweight reorg *detection* path. It is intentionally
// cheap to run on every batch: the fetcher looks up the stored hash for h-1
// and compares it to the incoming block's Prev_Hash. On a mismatch we log and
// bump a counter but do NOT truncate — full truncate+replay belongs to M2
// (DESIGN.md §6). The single-block stub keeps the hot loop unchanged when the
// chain is linear (the common case) and isolates the "what do we do about it"
// policy in one place.

// CheckReorgAt verifies that the block at `height` chains correctly onto the
// hash we have stored for `height-1`. It returns:
//
//	(true, 0)          — match, OR no stored hash yet (undetermined, treat as ok)
//	(false, height-1)  — stored hash disagrees with parentHash; reorg suspected
//
// The "no stored hash" case covers cold-start / gap scenarios: we can't
// contradict evidence we don't have, so we let the batch proceed. The NEXT
// batch will have a stored predecessor and the check will be meaningful.
//
// This function does a single small bbolt read per call. It is safe to invoke
// from the fetcher goroutine; Storage.GetBlockHash takes a read-only view.
func (idx *Indexer) CheckReorgAt(height int64, parentHash string) (ok bool, storedAtH int64) {
	if height <= 0 {
		// Genesis / pre-genesis: nothing to chain onto.
		return true, 0
	}
	prev, err := idx.Store.GetBlockHash(height - 1)
	if err != nil {
		// A storage error is not proof of a reorg; surface it via "ok" so the
		// caller doesn't treat it as one. Logging happens at the call site if
		// it wants to.
		return true, 0
	}
	if prev == "" {
		// Nothing stored at h-1 yet (e.g. first batch after startup, or a gap
		// from a parse failure). Cannot determine — treat as ok.
		return true, 0
	}
	if prev == parentHash {
		return true, 0
	}
	return false, height - 1
}

// checkReorgForBlock runs M1 reorg detection for a single parsed block at the
// given height. Both the catch-up batch path and the live speculative
// single-block path call this, so a reorg at the live tip — where real DERO
// reorgs happen and where only the single-block path runs — is now visible
// instead of slipping through undetected.
//
// Parent reference: DERO's block.Block has NO Prev_Hash field; its parent(s)
// live in Tips ([]crypto.Hash), because blocks form a DAG. In the linear
// region of the chain a block carries exactly one tip — the direct ancestor
// at h-1 — and we compare that against the hash we stored for h-1.
//
// ASSUMPTION (inherited from the existing batch-path check, not independently
// proven here): Tips[0] equals the stored GetBlockHash(h-1), i.e. a tip
// reference is the parent block's GetHash(). This has not been traced against
// derohe's miniblock/integrator hashing; if it turns out a tip uses a
// different form than the block ID we persist, both call sites would need to
// change together. Near the tip a block may carry >1 tip (DAG); we compare the
// first (the primary parent). Empty Tips (genesis / malformed) → skip.
func (idx *Indexer) checkReorgForBlock(height int64, bl *block.Block) {
	if len(bl.Tips) == 0 {
		return
	}
	if ok, storedAt := idx.CheckReorgAt(height, bl.Tips[0].String()); !ok {
		idx.onReorgDetected(storedAt, height)
	}
}

// onReorgDetected is invoked when CheckReorgAt returns a mismatch. In M1 this
// only logs and increments a counter; the actual truncate+replay lives in M2.
// Keeping the policy in one function means the fetcher's reorg-check site
// stays a single line and M2 can change the response (truncate, pause,
// re-fetch) without touching the hot loop.
//
// Counter semantics: ReorgDetected counts mismatch OBSERVATIONS, not distinct
// reorgs — the live single-block path re-fires on every tip poll while a
// mismatch persists (which is what a real reorg looks like until M2.3
// truncates), so one reorg can bump the counter and the warn log several
// times. M2.3's truncate+replay is what makes both one-shot.
func (idx *Indexer) onReorgDetected(oldTip, newTip int64) {
	idx.ReorgDetected.Add(1)
	logger.Warnf("reorg detected: stored tip=%d incoming tip=%d — TODO(M2): truncate+replay",
		oldTip, newTip)
}

// ReorgDetectedCount returns how many reorg mismatches have been observed so
// far. Surfaced in the API /getstats response so operators can distinguish a
// noisy chain from a broken indexer.
func (idx *Indexer) ReorgDetectedCount() int64 {
	return idx.ReorgDetected.Load()
}

// findForkPoint locates the highest height at which the locally stored block
// hash still agrees with the daemon's — the last common ancestor of the old and
// new chains. Truncating storage to that height and replaying above it converges
// the index onto the new chain.
//
// It walks DOWN from `suspected`. CALLER CONTRACT: pass storedAt — the h-1 side
// of the detection mismatch (what onReorgDetected receives as oldTip) — NOT the
// incoming block's height. Detection fires in the fetcher before the processor
// flushes the incoming block, so the stored hash at the incoming height is
// typically absent and starting there returns ok=false immediately, silently
// degrading every real reorg to a full resync.
//
// For each height it compares the stored hash to the daemon's; the first
// agreement is the fork point. A daemon miss ("" hash) is treated as
// disagreement and the walk continues. It is a PURE function over injected
// lookups (no *Indexer receiver, no RPC/DB access) so the whole fork-finding
// decision is unit-testable without a live daemon — the M2.3 wiring passes
// closures over Store.GetBlockHash and client.GetBlockHash.
//
// Returns:
//
//	(forkH, true, nil)  — last common ancestor found.
//	(0, false, nil)     — cannot determine: a height with no stored hash was hit
//	                      before any agreement (the fastsync floor — FastSync never
//	                      writes block hashes — or a scan-gap hole), or the walk
//	                      exhausted maxDepth without agreement. The caller must
//	                      fall back to a full ResetIndex/resync.
//	(0, false, err)     — a lookup failed; the caller should retry, not truncate.
//
// maxDepth bounds the walk so a pathological state can't scan the whole chain:
// at most maxDepth+1 heights are examined (suspected down to suspected-maxDepth
// inclusive).
func findForkPoint(suspected int64, storedHash, daemonHash func(int64) (string, error), maxDepth int64) (int64, bool, error) {
	if suspected < 1 {
		return 0, false, nil
	}
	lo := suspected - maxDepth
	if lo < 0 {
		lo = 0
	}
	for h := suspected; h >= lo; h-- {
		sh, err := storedHash(h)
		if err != nil {
			return 0, false, err
		}
		if sh == "" {
			// No stored hash at h: below the fastsync floor or a scan-gap hole.
			// We have no local history to roll back to here — fall back.
			return 0, false, nil
		}
		dh, err := daemonHash(h)
		if err != nil {
			return 0, false, err
		}
		if dh != "" && sh == dh {
			return h, true, nil
		}
	}
	return 0, false, nil
}
