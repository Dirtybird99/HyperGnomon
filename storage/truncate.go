package storage

import (
	"bytes"
	"strconv"

	bolt "go.etcd.io/bbolt"

	"github.com/hypergnomon/hypergnomon/structures"
)

// TruncateToHeight rolls the index back to a prefix-consistent snapshot as of
// height h in a single atomic transaction. See the Storage interface for the
// correctness contract (which state is restored exactly vs left for replay).
//
// The pass order matters: aggregates are recomputed from the height-keyed detail
// BEFORE that detail is deleted.
//
//	Step 0  build the affected-scid set (interaction heights > h, scvars
//	        snapshots > h, installs > h) — this bounds the per-SCID work.
//	Step 1  recompute addr_scids from surviving (<=h) invocation records.
//	Step 2  delete height-keyed detail > h across every bucket.
//	Step 3  drop latest-wins/derived state for scids first installed > h, lower
//	        the scvars_latest pointers, and recompute sc_count + lastindexedheight.
func (s *BboltStore) TruncateToHeight(h int64) error {
	if h < 0 {
		h = 0
	}
	return s.DB.Update(func(tx *bolt.Tx) error {
		// ---- Step 0: affected-scid set + scvars keys to delete ----
		affected := map[string]struct{}{}

		// (a) scids with an interaction height > h.
		if hb := tx.Bucket(bucketHeight); hb != nil {
			c := hb.Cursor()
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				if scid, hh, ok := splitHeightKey(k); ok && hh > h {
					affected[scid] = struct{}{}
				}
			}
		}

		// (b) scids with a scvars snapshot > h. This covers TELA-refresher /
		//     fastsync-probe writes that add a snapshot at chainHeight without an
		//     interaction height, which (a) alone would miss. The delete keys are
		//     collected in the same pass.
		var scvarsToDel [][]byte
		if vb := tx.Bucket(bucketScVars); vb != nil {
			c := vb.Cursor()
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				if scid, hh, ok := splitScVarsKey(k); ok && hh > h {
					affected[scid] = struct{}{}
					scvarsToDel = append(scvarsToDel, cloneKey(k))
				}
			}
		}

		// (c) scids first installed > h — their entire footprint must go.
		installedAbove := map[string]struct{}{}
		if ib := tx.Bucket(bucketInstalls); ib != nil {
			c := ib.Cursor()
			for k, _ := c.Seek(encHeight(h + 1)); k != nil; k, _ = c.Next() {
				if scid, ok := installKeyScid(k); ok {
					installedAbove[scid] = struct{}{}
					affected[scid] = struct{}{}
				}
			}
		}

		// ---- Step 1: recompute addr_scids from surviving (<=h) invocations ----
		survivors := map[addrSCIDKey]*structures.AddrSCIDEntry{}
		for scid := range affected {
			invB := tx.Bucket([]byte(scid))
			if invB == nil {
				continue
			}
			if err := invB.ForEach(func(k, _ []byte) error {
				sender, hh, ok := parseInvocationKey(k)
				if !ok || hh > h {
					return nil
				}
				key := addrSCIDKey{addr: sender, scid: scid}
				if e := survivors[key]; e != nil {
					if hh < e.FirstHeight {
						e.FirstHeight = hh
					}
					if hh > e.LastHeight {
						e.LastHeight = hh
					}
					e.Count++
				} else {
					survivors[key] = &structures.AddrSCIDEntry{FirstHeight: hh, LastHeight: hh, Count: 1}
				}
				return nil
			}); err != nil {
				return err
			}
		}
		if err := applyAddrSCIDRollback(tx, affected, survivors); err != nil {
			return err
		}

		// ---- Step 2: delete height-keyed detail > h ----

		// blockhashes + installs: BE8-height-prefixed, so seek + delete to end.
		if err := deleteFromSeek(tx.Bucket(bucketBlockHash), encHeight(h+1)); err != nil {
			return err
		}
		if err := deleteFromSeek(tx.Bucket(bucketInstalls), encHeight(h+1)); err != nil {
			return err
		}

		// height + invocation buckets: bounded to affected scids by prefix/bucket.
		hb := tx.Bucket(bucketHeight)
		for scid := range affected {
			if hb != nil {
				prefix := append([]byte(scid), ':')
				if err := deletePrefixWhere(hb, prefix, func(k []byte) bool {
					_, hh, ok := splitHeightKey(k)
					return ok && hh > h
				}); err != nil {
					return err
				}
			}
			if invB := tx.Bucket([]byte(scid)); invB != nil {
				if err := deleteWhere(invB, func(k []byte) bool {
					_, hh, ok := parseInvocationKey(k)
					return ok && hh > h
				}); err != nil {
					return err
				}
			}
		}

		// scvars: delete the keys collected in Step 0(b).
		if vb := tx.Bucket(bucketScVars); vb != nil {
			for _, k := range scvarsToDel {
				if err := vb.Delete(k); err != nil {
					return err
				}
			}
		}

		// normaltxwithscid (addr-prefixed) + class prefix (class-prefixed): the
		// height is embedded mid-key, so full-scan on the decoded height.
		if err := deleteWhere(tx.Bucket(bucketNormTx), func(k []byte) bool {
			return normTxHeight(k) > h
		}); err != nil {
			return err
		}
		if err := deleteWhere(tx.Bucket(bucketClass), func(k []byte) bool {
			return classKeyHeight(k) > h
		}); err != nil {
			return err
		}

		// ---- Step 3: latest-wins / derived + aggregate stats ----

		// Entities first created > h: nothing at <=h refers to them.
		scidKeyedBuckets := [][]byte{bucketOwners, bucketClassIdx, bucketSCCode, bucketInvalid}
		for scid := range installedAbove {
			key := []byte(scid)
			for _, name := range scidKeyedBuckets {
				if b := tx.Bucket(name); b != nil {
					if err := b.Delete(key); err != nil {
						return err
					}
				}
			}
		}
		// owner_scids: "<owner>|<scid>" — drop any whose scid was installed > h.
		if err := deleteWhere(tx.Bucket(bucketOwnerSCIDs), func(k []byte) bool {
			if i := bytes.LastIndexByte(k, '|'); i >= 0 {
				_, ok := installedAbove[string(k[i+1:])]
				return ok
			}
			return false
		}); err != nil {
			return err
		}

		// scvars_latest: lower each affected pointer to the max surviving (<=h)
		// snapshot, or drop it entirely. The monotonic putLatestSCVarsHeight
		// helper refuses to lower, so we write the raw value here.
		latest := tx.Bucket(bucketScVarsLatest)
		vb := tx.Bucket(bucketScVars)
		for scid := range affected {
			maxH := maxSurvivingScVarsHeight(vb, scid, h)
			if maxH == 0 {
				if err := latest.Delete([]byte(scid)); err != nil {
					return err
				}
				continue
			}
			val := make([]byte, 8)
			copy(val, encHeight(maxH))
			if err := latest.Put([]byte(scid), val); err != nil {
				return err
			}
		}

		// stats: recompute sc_count from the surviving owners; pin
		// lastindexedheight to h; leave the counted-and-discarded tx-count stats
		// (reg/burn/norm) for replay to recount.
		stats := tx.Bucket(bucketStats)
		var owners int64
		if err := tx.Bucket(bucketOwners).ForEach(func(_, _ []byte) error {
			owners++
			return nil
		}); err != nil {
			return err
		}
		if err := stats.Put([]byte("sc_count"), []byte(strconv.FormatInt(owners, 10))); err != nil {
			return err
		}
		// lastindexedheight: truncate only rolls back, so never raise it above
		// the current value (truncating to a height >= tip is a no-op here).
		newLast := h
		if v := stats.Get([]byte("lastindexedheight")); v != nil {
			if cur, err := strconv.ParseInt(string(v), 10, 64); err == nil && cur < h {
				newLast = cur
			}
		}
		return stats.Put([]byte("lastindexedheight"), []byte(strconv.FormatInt(newLast, 10)))
	})
}

// applyAddrSCIDRollback replaces each affected (addr, scid) addr_scids entry with
// its recomputed <=h survivor, or deletes it when no <=h interaction survived.
func applyAddrSCIDRollback(tx *bolt.Tx, affected map[string]struct{}, survivors map[addrSCIDKey]*structures.AddrSCIDEntry) error {
	parent := tx.Bucket(bucketAddrSCIDs)
	if parent == nil {
		return nil
	}
	var addrs [][]byte
	if err := parent.ForEach(func(name, v []byte) error {
		if v == nil { // sub-bucket
			addrs = append(addrs, cloneKey(name))
		}
		return nil
	}); err != nil {
		return err
	}
	for _, addrName := range addrs {
		sub := parent.Bucket(addrName)
		if sub == nil {
			continue
		}
		addr := string(addrName)
		type op struct {
			scid string
			val  []byte // nil => delete
		}
		var ops []op
		if err := sub.ForEach(func(scidK, _ []byte) error {
			scid := string(scidK)
			if _, aff := affected[scid]; !aff {
				return nil
			}
			if e := survivors[addrSCIDKey{addr: addr, scid: scid}]; e != nil {
				ops = append(ops, op{scid: scid, val: e.MarshalTypedAppend(nil)})
			} else {
				ops = append(ops, op{scid: scid})
			}
			return nil
		}); err != nil {
			return err
		}
		for _, o := range ops {
			if o.val == nil {
				if err := sub.Delete([]byte(o.scid)); err != nil {
					return err
				}
			} else if err := sub.Put([]byte(o.scid), o.val); err != nil {
				return err
			}
		}
	}
	return nil
}

// deleteFromSeek deletes every key at or after `from` (two-phase: collect then
// delete, since bbolt forbids delete under a live cursor).
func deleteFromSeek(b *bolt.Bucket, from []byte) error {
	if b == nil {
		return nil
	}
	var toDel [][]byte
	c := b.Cursor()
	for k, _ := c.Seek(from); k != nil; k, _ = c.Next() {
		toDel = append(toDel, cloneKey(k))
	}
	for _, k := range toDel {
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

// deletePrefixWhere deletes keys sharing `prefix` for which pred is true.
func deletePrefixWhere(b *bolt.Bucket, prefix []byte, pred func(k []byte) bool) error {
	if b == nil {
		return nil
	}
	var toDel [][]byte
	c := b.Cursor()
	for k, _ := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, _ = c.Next() {
		if pred(k) {
			toDel = append(toDel, cloneKey(k))
		}
	}
	for _, k := range toDel {
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

// deleteWhere full-scans b and deletes keys for which pred is true.
func deleteWhere(b *bolt.Bucket, pred func(k []byte) bool) error {
	if b == nil {
		return nil
	}
	var toDel [][]byte
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if v == nil { // sub-bucket: not a data key
			continue
		}
		if pred(k) {
			toDel = append(toDel, cloneKey(k))
		}
	}
	for _, k := range toDel {
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

// maxSurvivingScVarsHeight returns the greatest scvars snapshot height <= h for
// scid, or 0 if none survive.
func maxSurvivingScVarsHeight(vb *bolt.Bucket, scid string, h int64) int64 {
	if vb == nil {
		return 0
	}
	prefix := append([]byte(scid), ':')
	var maxH int64
	c := vb.Cursor()
	for k, _ := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, _ = c.Next() {
		hh, err := strconv.ParseInt(string(k[len(prefix):]), 10, 64)
		if err != nil {
			continue
		}
		if hh <= h && hh > maxH {
			maxH = hh
		}
	}
	return maxH
}

func cloneKey(k []byte) []byte {
	c := make([]byte, len(k))
	copy(c, k)
	return c
}

// --- key decoders (height extraction for truncation predicates) ---

// splitHeightKey parses a "<scid>:<BE8 h>" interaction-height key. Legacy
// whole-SCID msgpack keys (no ':' + 8-byte tail) return ok=false.
func splitHeightKey(k []byte) (scid string, height int64, ok bool) {
	sep := bytes.IndexByte(k, ':')
	if sep < 0 || len(k) != sep+1+8 {
		return "", 0, false
	}
	return string(k[:sep]), decHeight(k[sep+1:]), true
}

// splitScVarsKey parses a "<scid>:<decimal h>" scvars key.
func splitScVarsKey(k []byte) (scid string, height int64, ok bool) {
	sep := bytes.IndexByte(k, ':')
	if sep < 0 {
		return "", 0, false
	}
	hh, err := strconv.ParseInt(string(k[sep+1:]), 10, 64)
	if err != nil {
		return "", 0, false
	}
	return string(k[:sep]), hh, true
}

// installKeyScid parses the scid out of a "<BE8 h>|<scid>" install key.
func installKeyScid(k []byte) (string, bool) {
	if len(k) < 9 || k[8] != '|' {
		return "", false
	}
	return string(k[9:]), true
}

// parseInvocationKey parses "<sender>:<txid>:<decimal h>:<entrypoint>".
func parseInvocationKey(k []byte) (sender string, height int64, ok bool) {
	i1 := bytes.IndexByte(k, ':')
	if i1 < 0 {
		return "", 0, false
	}
	rest := k[i1+1:]
	i2 := bytes.IndexByte(rest, ':')
	if i2 < 0 {
		return "", 0, false
	}
	hpart := rest[i2+1:]
	i3 := bytes.IndexByte(hpart, ':')
	if i3 < 0 {
		return "", 0, false
	}
	hh, err := strconv.ParseInt(string(hpart[:i3]), 10, 64)
	if err != nil {
		return "", 0, false
	}
	return string(k[:i1]), hh, true
}

// normTxHeight extracts the BE8 height from "<addr>:<BE8 h>:<txid>:<scid>".
// Legacy whole-addr blob keys (no ':') return 0 (left in place).
func normTxHeight(k []byte) int64 {
	i1 := bytes.IndexByte(k, ':')
	if i1 < 0 || len(k) < i1+1+8 {
		return 0
	}
	return decHeight(k[i1+1 : i1+9])
}

// classKeyHeight extracts the BE8 install height from
// "<class>|<BE8 h>|<scid>".
func classKeyHeight(k []byte) int64 {
	i1 := bytes.IndexByte(k, '|')
	if i1 < 0 || len(k) < i1+1+8 {
		return 0
	}
	return decHeight(k[i1+1 : i1+9])
}
