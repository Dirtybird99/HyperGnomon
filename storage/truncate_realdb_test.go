package storage

import (
	"bytes"
	"encoding/hex"
	"os"
	"strconv"
	"testing"

	bolt "go.etcd.io/bbolt"
)

// TestTruncateToHeight_RealDB is a belt-and-suspenders check against a REAL
// indexed bbolt DB (real scids/addresses/txids/entrypoints — key-forms the
// synthetic oracle cannot produce). It truncates to H = tip - margin and asserts
// that afterwards NO key anywhere in the store encodes a height > H, then that
// the store still reads back without error.
//
// Gated: set HG_TRUNCATE_REAL_DB=<dir containing HYPERGNOMON.db>. Optional
// HG_TRUNCATE_MARGIN=<blocks below tip> (default 500000). Skips in CI.
func TestTruncateToHeight_RealDB(t *testing.T) {
	dir := os.Getenv("HG_TRUNCATE_REAL_DB")
	if dir == "" {
		t.Skip("set HG_TRUNCATE_REAL_DB=<dir with HYPERGNOMON.db copy> to run")
	}
	margin := int64(500000)
	if m := os.Getenv("HG_TRUNCATE_MARGIN"); m != "" {
		if v, err := strconv.ParseInt(m, 10, 64); err == nil {
			margin = v
		}
	}

	s, err := NewBboltStore(dir, "")
	if err != nil {
		t.Fatalf("open real DB: %v", err)
	}
	defer s.Close()

	n, err := s.GetLastIndexHeight()
	if err != nil {
		t.Fatalf("GetLastIndexHeight: %v", err)
	}
	h := n - margin
	if h < 1 {
		t.Fatalf("tip=%d too low for margin=%d", n, margin)
	}
	scBefore, _ := s.GetSCIDCount()
	t.Logf("real DB: tip=%d, truncating to H=%d (margin=%d), sc_count=%d", n, h, margin, scBefore)

	if err := s.TruncateToHeight(h); err != nil {
		t.Fatalf("TruncateToHeight(%d): %v", h, err)
	}

	// --- orphan scan: no key may encode a height > H ---
	named := map[string]bool{}
	for _, b := range [][]byte{
		bucketStats, bucketOwners, bucketHeaders, bucketClass, bucketTags,
		bucketHeight, bucketScVars, bucketScVarsLatest, bucketNormTx, bucketInvalid,
		bucketBlockHash, bucketInstalls, bucketClassIdx, bucketAddrSCIDs,
		bucketTELAContent, bucketSCCode, bucketOwnerSCIDs,
	} {
		named[string(b)] = true
	}

	var violations, unparsed int
	report := func(bucket string, k []byte, hh int64) {
		violations++
		if violations <= 20 {
			t.Errorf("orphan > H: bucket=%s key=%s height=%d (H=%d)", bucket, hex.EncodeToString(k), hh, h)
		}
	}

	err = s.DB.View(func(tx *bolt.Tx) error {
		// lastindexedheight pinned to H.
		if v := tx.Bucket(bucketStats).Get([]byte("lastindexedheight")); v != nil {
			if got, _ := strconv.ParseInt(string(v), 10, 64); got != h {
				t.Errorf("lastindexedheight = %s, want %d", v, h)
			}
		}

		// blockhashes: whole key is BE8 height.
		_ = tx.Bucket(bucketBlockHash).ForEach(func(k, _ []byte) error {
			if decHeight(k) > h {
				report("blockhashes", k, decHeight(k))
			}
			return nil
		})
		// installs: <BE8 h>|<scid>.
		_ = tx.Bucket(bucketInstalls).ForEach(func(k, _ []byte) error {
			if len(k) >= 8 && decHeight(k[:8]) > h {
				report("installs", k, decHeight(k[:8]))
			}
			return nil
		})
		// height: <scid>:<BE8 h>.
		_ = tx.Bucket(bucketHeight).ForEach(func(k, _ []byte) error {
			if _, hh, ok := splitHeightKey(k); ok {
				if hh > h {
					report("height", k, hh)
				}
			} else {
				unparsed++
			}
			return nil
		})
		// scvars: <scid>:<decimal h>.
		_ = tx.Bucket(bucketScVars).ForEach(func(k, _ []byte) error {
			if _, hh, ok := splitScVarsKey(k); ok {
				if hh > h {
					report("scvars", k, hh)
				}
			} else {
				unparsed++
			}
			return nil
		})
		// scvars_latest: value is BE8 height.
		_ = tx.Bucket(bucketScVarsLatest).ForEach(func(k, v []byte) error {
			if len(v) >= 8 && decHeight(v) > h {
				report("scvars_latest(val)", k, decHeight(v))
			}
			return nil
		})
		// normaltxwithscid: <addr>:<BE8 h>:...
		_ = tx.Bucket(bucketNormTx).ForEach(func(k, _ []byte) error {
			if i := bytes.IndexByte(k, ':'); i >= 0 && len(k) >= i+1+8 {
				if hh := decHeight(k[i+1 : i+9]); hh > h {
					report("normaltxwithscid", k, hh)
				}
			} else {
				unparsed++
			}
			return nil
		})
		// class prefix: <class>|<BE8 InstallHeight>|<scid>. The InstallHeight is
		// NOT a reliable event height: fastsync stamps it to the seed-cache height
		// and TELA re-classification moves it (category-B, documented). So the
		// invariant is NOT key-height <= H; it is that every class-prefix scid must
		// survive (installed <= H → still has an owner). A class row for a dropped
		// scid is the real orphan; a category-B row above the fork is counted, not
		// failed.
		ownersB := tx.Bucket(bucketOwners)
		var classAboveH int
		_ = tx.Bucket(bucketClass).ForEach(func(k, _ []byte) error {
			i1 := bytes.IndexByte(k, '|')
			if i1 < 0 || len(k) < i1+1+8 {
				unparsed++
				return nil
			}
			if decHeight(k[i1+1:i1+9]) > h {
				classAboveH++
			}
			rest := k[i1+9:] // should be "|<scid>"
			if len(rest) < 2 || rest[0] != '|' {
				unparsed++
				return nil
			}
			if ownersB.Get(rest[1:]) == nil {
				report("class(orphan: scid has no surviving owner)", k, decHeight(k[i1+1:i1+9]))
			}
			return nil
		})
		if classAboveH > 0 {
			t.Logf("class prefix: %d rows carry InstallHeight > H (fastsync seed / TELA re-class artifact); kept because their scid survives (category-B)", classAboveH)
		}
		// per-SCID invocation buckets: any top-level bucket not in the named set.
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			if named[string(name)] {
				return nil
			}
			return b.ForEach(func(k, _ []byte) error {
				if _, hh, ok := parseInvocationKey(k); ok {
					if hh > h {
						report("invoke:"+string(name), k, hh)
					}
				} else {
					unparsed++
				}
				return nil
			})
		})
	})
	if err != nil {
		t.Fatalf("orphan scan: %v", err)
	}
	if unparsed > 0 {
		t.Logf("note: %d keys did not parse as height-bearing (legacy/other forms) — inspect if nonzero on a typed-encoding DB", unparsed)
	}
	if violations > 20 {
		t.Errorf("... and %d more orphan keys > H", violations-20)
	}

	// --- readback sanity: the truncated store must still be usable ---
	scAfter, err := s.GetSCIDCount()
	if err != nil {
		t.Fatalf("GetSCIDCount after truncate: %v", err)
	}
	scids, err := s.GetAllSCIDs()
	if err != nil {
		t.Fatalf("GetAllSCIDs after truncate: %v", err)
	}
	if int64(len(scids)) != scAfter {
		t.Logf("sc_count=%d vs GetAllSCIDs len=%d (owners bucket)", scAfter, len(scids))
	}
	t.Logf("post-truncate: sc_count=%d (was %d), owners=%d, violations=%d", scAfter, scBefore, len(scids), violations)
	// Exercise a few real reads for panic/decoder safety.
	for i, scid := range scids {
		if i >= 25 {
			break
		}
		if _, err := s.GetSCIDClass(scid); err != nil {
			t.Errorf("GetSCIDClass(%s): %v", scid, err)
		}
		if _, err := s.GetSCIDInteractionHeights(scid); err != nil {
			t.Errorf("GetSCIDInteractionHeights(%s): %v", scid, err)
		}
	}
}
