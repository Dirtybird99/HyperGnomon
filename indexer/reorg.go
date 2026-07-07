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
