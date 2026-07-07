package storage

import (
	"encoding/hex"
	"fmt"
	"testing"

	bolt "go.etcd.io/bbolt"

	"github.com/hypergnomon/hypergnomon/structures"
)

// ---- reorg-truncate test fixture ----------------------------------------
//
// buildReorgChain flushes a deterministic synthetic chain into store for
// heights 1..maxH, one batch per height (mirroring the per-block flush). It is
// CATEGORY-A only: an SC's owner and class-meta are written exactly once, at
// install, and never re-mutated above install — so TruncateToHeight alone is
// expected to restore a fresh ≤H sync byte-for-byte (except the counted-and-
// discarded tx-count stats, which drift by design pending replay).
//
// Layout:
//
//	scidX installed by addrA at h=2; addrA re-invokes it every h>=3 (entrypoint
//	  "rate"), and addrC touches it via a normal-tx every h>=3. Each re-invoke
//	  also writes a fresh per-height scvars snapshot. NO AddOwner/AddClass on
//	  re-invoke → owner/class stay category-A.
//	scidY installed by addrB at h=4, never touched again.
//
// Truncating this chain to H=3 must: keep scidX with addr_scids/heights/scvars/
// scvars_latest recomputed to their ≤3 values, and fully remove scidY (installed
// at 4 > 3) from every bucket.
const (
	trAddrA = "addrAAA"
	trAddrB = "addrBBB"
	trAddrC = "addrCCC"
)

func trHexID(n int) string { return fmt.Sprintf("%064x", n) }

var (
	trScidX = trHexID(0x11)
	trScidY = trHexID(0x22)
	trScidZ = trHexID(0x33)
)

func buildReorgChain(t *testing.T, store *BboltStore, maxH int64) {
	t.Helper()
	for h := int64(1); h <= maxH; h++ {
		b := NewWriteBatch()
		b.LastHeight = h
		b.AddBlockHash(h, trHexID(0xB00+int(h)))
		b.RegTxCount = 1
		b.NormTxCount = 1

		switch h {
		case 2:
			installSC(b, trScidX, trAddrA, h, "TELA-INDEX-1", "AppX", "appx.tela")
		case 4:
			installSC(b, trScidY, trAddrB, h, "TELA-DOC-1", "DocY", "doc.tela")
		}
		if h >= 3 {
			// addrA re-invokes scidX (category-A: no AddOwner/AddClass).
			b.AddInvocation(structures.InvokeRecord{
				Scid: trScidX, Sender: trAddrA, Entrypoint: "rate", Height: h,
				Details: &structures.SCTXParse{
					Txid: trHexID(0x1000 + int(h)), Scid: trScidX,
					Sender: trAddrA, Entrypoint: "rate", Height: h,
				},
			})
			b.AddInteractionHeight(trScidX, h)
			b.AddAddrSCID(trAddrA, trScidX, h)
			b.AddVariables(trScidX, h, []*structures.SCIDVariable{
				{Key: "likes", Value: fmt.Sprintf("%d", h)},
			})
			// Ring-member touch: the pipeline (processNormalTx) records BOTH the
			// addr→scid edge and the normal-tx detail for every ring member — with
			// no invocation record. The paired AddAddrSCID here keeps the fixture
			// faithful so the prefix-equivalence oracle covers that edge source.
			b.AddAddrSCID(trAddrC, trScidX, h)
			b.AddNormalTx(trAddrC, trHexID(0x3000+int(h)), trScidX, 7, h)
		}

		if err := store.FlushBatch(b); err != nil {
			t.Fatalf("flush h=%d: %v", h, err)
		}
	}
}

func installSC(b *WriteBatch, scid, owner string, h int64, class, name, durl string) {
	b.AddOwner(scid, owner)
	b.AddInvocation(structures.InvokeRecord{
		Scid: scid, Sender: owner, Entrypoint: "install", Height: h,
		Details: &structures.SCTXParse{
			Txid: trHexID(0x2000 + int(h)), Scid: scid,
			Sender: owner, Entrypoint: "install", Height: h,
		},
	})
	b.AddInteractionHeight(scid, h)
	b.AddAddrSCID(owner, scid, h)
	b.AddVariables(scid, h, []*structures.SCIDVariable{{Key: "likes", Value: "0"}})
	b.AddInstall(scid, h, &structures.InstallRecord{Owner: owner, Entrypoint: "install", Fees: 100})
	b.AddClass(scid, &structures.ClassMeta{
		Class: class, Tags: []string{"all", "tela"}, Name: name, DURL: durl,
		Version: "1.0.0", InstallHeight: h, LastHeight: h,
	})
	b.AddSCCode(scid, h, "Function Initialize() Uint64\n10 RETURN 0\nEnd Function")
}

// dumpAll walks every bucket (recursing into sub-buckets so addr_scids' per-addr
// buckets and the top-level per-SCID invocation buckets are all captured) into a
// path -> hexkey -> hexval map for structural comparison.
func dumpAll(t *testing.T, s *BboltStore) map[string]map[string]string {
	t.Helper()
	out := map[string]map[string]string{}
	err := s.DB.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			return dumpBucket(hex.EncodeToString(name), b, out)
		})
	})
	if err != nil {
		t.Fatalf("dumpAll: %v", err)
	}
	return out
}

func dumpBucket(path string, b *bolt.Bucket, out map[string]map[string]string) error {
	return b.ForEach(func(k, v []byte) error {
		if v == nil { // sub-bucket
			return dumpBucket(path+"/"+hex.EncodeToString(k), b.Bucket(k), out)
		}
		m := out[path]
		if m == nil {
			m = map[string]string{}
			out[path] = m
		}
		m[hex.EncodeToString(k)] = hex.EncodeToString(v)
		return nil
	})
}

// statsCountKeys are the category-(C) counted-and-discarded aggregates excluded
// from byte-identical comparison (unrecoverable from storage; replay recounts).
var statsCountKeys = map[string]bool{
	hex.EncodeToString([]byte("regtxcount")):  true,
	hex.EncodeToString([]byte("burntxcount")): true,
	hex.EncodeToString([]byte("normtxcount")): true,
}

var statsBucketPath = hex.EncodeToString([]byte("stats"))

// assertDumpsEqual compares two bucket dumps, ignoring the category-(C) tx-count
// stats (which drift by design). Reports both over-deletion (missing) and
// under-deletion (orphan survived).
func assertDumpsEqual(t *testing.T, got, want map[string]map[string]string) {
	t.Helper()
	skip := func(path, k string) bool { return path == statsBucketPath && statsCountKeys[k] }
	for path, kv := range want {
		for k, v := range kv {
			if skip(path, k) {
				continue
			}
			gv, ok := got[path][k]
			if !ok {
				t.Errorf("missing after truncate: bucket=%s key=%s (want val=%s)", path, k, v)
				continue
			}
			if gv != v {
				t.Errorf("value mismatch: bucket=%s key=%s\n got=%s\nwant=%s", path, k, gv, v)
			}
		}
	}
	for path, kv := range got {
		for k, v := range kv {
			if skip(path, k) {
				continue
			}
			if _, ok := want[path][k]; !ok {
				t.Errorf("orphan survived truncate: bucket=%s key=%s val=%s", path, k, v)
			}
		}
	}
}

// TestTruncateToHeight_PrefixEquivalence is the strong oracle: a chain synced to
// N then truncated to H must be byte-identical (every bucket, minus the category-C
// tx-count stats) to a fresh chain synced only to H.
func TestTruncateToHeight_PrefixEquivalence(t *testing.T) {
	const N, H = int64(8), int64(3)

	full := newTestStore(t)
	buildReorgChain(t, full, N)
	if err := full.TruncateToHeight(H); err != nil {
		t.Fatalf("TruncateToHeight: %v", err)
	}
	got := dumpAll(t, full)

	ref := newTestStore(t)
	buildReorgChain(t, ref, H)
	want := dumpAll(t, ref)

	assertDumpsEqual(t, got, want)
}

// TestTruncateToHeight_Idempotent guards the states M2.3 will drive truncate
// from: progressive rollback (5 then 3) and a repeated truncate to the same
// height must both equal a single straight truncate to 3.
func TestTruncateToHeight_Idempotent(t *testing.T) {
	const N = int64(8)

	prog := newTestStore(t)
	buildReorgChain(t, prog, N)
	for _, h := range []int64{5, 3, 3} { // step down, then repeat
		if err := prog.TruncateToHeight(h); err != nil {
			t.Fatalf("TruncateToHeight(%d): %v", h, err)
		}
	}

	ref := newTestStore(t)
	buildReorgChain(t, ref, N)
	if err := ref.TruncateToHeight(3); err != nil {
		t.Fatalf("TruncateToHeight(3): %v", err)
	}

	assertDumpsEqual(t, dumpAll(t, prog), dumpAll(t, ref))
}

// TestTruncateToHeight_AddrSCIDsRecompute pins the hardest aggregate: a cross-fork
// (addr,scid) pair's Count/FirstHeight/LastHeight recomputed to ≤H, and a pair
// whose only activity was >H fully removed.
func TestTruncateToHeight_AddrSCIDsRecompute(t *testing.T) {
	const N, H = int64(8), int64(3)
	s := newTestStore(t)
	buildReorgChain(t, s, N)
	if err := s.TruncateToHeight(H); err != nil {
		t.Fatalf("TruncateToHeight: %v", err)
	}

	// addrA touched scidX at h=2 (install) and h=3 (invoke) within ≤H.
	ax, err := s.GetAddressSCIDs(trAddrA)
	if err != nil {
		t.Fatalf("GetAddressSCIDs(A): %v", err)
	}
	e := ax[trScidX]
	if e == nil {
		t.Fatalf("addr_scids[A][X] missing after truncate")
	}
	if e.Count != 2 || e.FirstHeight != 2 || e.LastHeight != 3 {
		t.Errorf("addr_scids[A][X] = {First:%d Last:%d Count:%d}, want {2 3 2}",
			e.FirstHeight, e.LastHeight, e.Count)
	}

	// addrC touched scidX only as a normal-tx ring member (no invocation
	// records). Its h=3 touch is ≤ H and must survive the recompute.
	cx, err := s.GetAddressSCIDs(trAddrC)
	if err != nil {
		t.Fatalf("GetAddressSCIDs(C): %v", err)
	}
	ce := cx[trScidX]
	if ce == nil {
		t.Fatalf("addr_scids[C][X] missing after truncate (ring-member edge dropped)")
	}
	if ce.Count != 1 || ce.FirstHeight != 3 || ce.LastHeight != 3 {
		t.Errorf("addr_scids[C][X] = {First:%d Last:%d Count:%d}, want {3 3 1}",
			ce.FirstHeight, ce.LastHeight, ce.Count)
	}

	// addrB's only touch (scidY install at h=4) is > H → entry gone entirely.
	by, err := s.GetAddressSCIDs(trAddrB)
	if err != nil {
		t.Fatalf("GetAddressSCIDs(B): %v", err)
	}
	if _, ok := by[trScidY]; ok {
		t.Errorf("addr_scids[B][Y] survived truncate (installed at 4 > %d)", H)
	}
}

// TestTruncateToHeight_ScVarsLatestLowered pins the monotonic-pointer bug: the
// scvars_latest pointer must be lowered to the max surviving ≤H snapshot (the
// existing putLatestSCVarsHeight helper refuses to lower it).
func TestTruncateToHeight_ScVarsLatestLowered(t *testing.T) {
	const N, H = int64(8), int64(3)
	s := newTestStore(t)
	buildReorgChain(t, s, N)
	if err := s.TruncateToHeight(H); err != nil {
		t.Fatalf("TruncateToHeight: %v", err)
	}

	s.DB.View(func(tx *bolt.Tx) error {
		if got := decHeight(tx.Bucket(bucketScVarsLatest).Get([]byte(trScidX))); got != H {
			t.Errorf("scvars_latest[X] = %d, want %d (max surviving snapshot)", got, H)
		}
		// scidY (installed at 4 > H) must have no latest pointer at all.
		if v := tx.Bucket(bucketScVarsLatest).Get([]byte(trScidY)); v != nil {
			t.Errorf("scvars_latest[Y] survived truncate: %x", v)
		}
		return nil
	})

	// The >H snapshot must be gone; the ≤H one must remain readable.
	if v, _ := s.GetSCIDVariableDetailsAtHeight(trScidX, 5); v != nil {
		t.Errorf("scvars[X:5] survived truncate")
	}
	if v, _ := s.GetSCIDVariableDetailsAtHeight(trScidX, H); v == nil {
		t.Errorf("scvars[X:%d] was wrongly deleted", H)
	}
}

// TestTruncateToHeight_LatestWinsBoundary documents the category-B seam: an SC
// whose class-meta is bumped ABOVE the fork retains the >H value after truncate
// (truncate alone cannot restore it — replay does), and a re-flush of the ≤H
// state corrects it.
func TestTruncateToHeight_LatestWinsBoundary(t *testing.T) {
	const H = int64(3)
	s := newTestStore(t)

	b1 := NewWriteBatch()
	b1.LastHeight = 2
	installSC(b1, trScidX, trAddrA, 2, "TELA-INDEX-1", "AppX", "appx.tela")
	if err := s.FlushBatch(b1); err != nil {
		t.Fatalf("flush install: %v", err)
	}
	// Metadata bump at h=5 (> H): same InstallHeight, new Version — category B.
	b2 := NewWriteBatch()
	b2.LastHeight = 5
	b2.AddClass(trScidX, &structures.ClassMeta{
		Class: "TELA-INDEX-1", Tags: []string{"all", "tela"}, Name: "AppX",
		DURL: "appx.tela", Version: "2.0.0", InstallHeight: 2, LastHeight: 5,
	})
	if err := s.FlushBatch(b2); err != nil {
		t.Fatalf("flush bump: %v", err)
	}

	if err := s.TruncateToHeight(H); err != nil {
		t.Fatalf("TruncateToHeight: %v", err)
	}
	meta, err := s.GetSCIDClass(trScidX)
	if err != nil || meta == nil {
		t.Fatalf("GetSCIDClass: %v meta=%v", err, meta)
	}
	if meta.Version != "2.0.0" {
		t.Errorf("class_scid[X].Version = %q after truncate, want %q retained pending replay",
			meta.Version, "2.0.0")
	}
}

// TestTruncateToHeight_RefresherScVarsNoHeight guards Step 0(b): a scvars
// snapshot written above the fork WITHOUT a paired interaction height (as the
// TELA refresher / fastsync probe do at chainHeight) must still be detected and
// deleted, and its latest pointer lowered — the height-bucket-only affected set
// would miss it.
func TestTruncateToHeight_RefresherScVarsNoHeight(t *testing.T) {
	const H = int64(3)
	s := newTestStore(t)

	b1 := NewWriteBatch()
	b1.LastHeight = 2
	installSC(b1, trScidX, trAddrA, 2, "TELA-INDEX-1", "AppX", "appx.tela")
	if err := s.FlushBatch(b1); err != nil {
		t.Fatalf("flush install: %v", err)
	}
	// Refresher-style write: a scvars snapshot at h=5, no interaction height.
	b2 := NewWriteBatch()
	b2.LastHeight = 5
	b2.AddVariables(trScidX, 5, []*structures.SCIDVariable{{Key: "likes", Value: "9"}})
	if err := s.FlushBatch(b2); err != nil {
		t.Fatalf("flush refresh: %v", err)
	}

	if err := s.TruncateToHeight(H); err != nil {
		t.Fatalf("TruncateToHeight: %v", err)
	}
	if v, _ := s.GetSCIDVariableDetailsAtHeight(trScidX, 5); v != nil {
		t.Errorf("refresher scvars[X:5] survived truncate (union-from-scvars missed it)")
	}
	s.DB.View(func(tx *bolt.Tx) error {
		if got := decHeight(tx.Bucket(bucketScVarsLatest).Get([]byte(trScidX))); got != 2 {
			t.Errorf("scvars_latest[X] = %d after truncate, want 2 (max surviving <=%d)", got, H)
		}
		return nil
	})
}

// TestTruncateToHeight_TELAContentDropped covers the durable TELA content cache,
// which the sync fixture (WriteBatch) structurally cannot populate — so it needs
// its own test. Cached bodies for affected scids (touched > H) must be dropped
// (on-chain content above the fork may have changed), while an unaffected scid's
// cache is left intact.
func TestTruncateToHeight_TELAContentDropped(t *testing.T) {
	const H = int64(3)
	s := newTestStore(t)
	buildReorgChain(t, s, 8) // X interacts every h>=3 (affected); Y installed at 4 (affected)

	// Control Z: installed at h=1, no activity > H → not affected.
	zb := NewWriteBatch()
	zb.LastHeight = 8
	installSC(zb, trScidZ, "addrZ", 1, "TELA-INDEX-1", "AppZ", "z.tela")
	if err := s.FlushBatch(zb); err != nil {
		t.Fatalf("flush Z: %v", err)
	}

	body := &structures.TELAContentEntry{Body: []byte("hi"), MIME: "text/html"}
	for _, scid := range []string{trScidX, trScidY, trScidZ} {
		if err := s.PutTELAContent(scid, "index.html", body); err != nil {
			t.Fatalf("PutTELAContent %s: %v", scid, err)
		}
	}

	if err := s.TruncateToHeight(H); err != nil {
		t.Fatalf("TruncateToHeight: %v", err)
	}

	for _, scid := range []string{trScidX, trScidY} {
		if c, _ := s.GetTELAContent(scid, "index.html"); c != nil {
			t.Errorf("tela_content for affected %s survived truncate", scid)
		}
	}
	if c, _ := s.GetTELAContent(trScidZ, "index.html"); c == nil {
		t.Errorf("tela_content for unaffected Z was wrongly dropped")
	}
}

// TestTruncateToHeight_FastsyncOwnerEdgeSurvives guards the fastsync-shaped
// addr_scids source: turbo fastsync writes owner+install+interaction+addr edge
// with NO invocation record (fastsync.go). If such an SC later becomes affected
// (here: a refresher scvars snapshot > H), the recompute must not destroy the
// owner's ≤H edge just because no invocation records exist for it.
func TestTruncateToHeight_FastsyncOwnerEdgeSurvives(t *testing.T) {
	const H = int64(3)
	s := newTestStore(t)

	// Fastsync-style seed at h=2: no AddInvocation, no per-SCID bucket.
	b1 := NewWriteBatch()
	b1.LastHeight = 2
	b1.AddOwner(trScidX, trAddrA)
	b1.AddInteractionHeight(trScidX, 2)
	b1.AddInstall(trScidX, 2, &structures.InstallRecord{Owner: trAddrA})
	b1.AddAddrSCID(trAddrA, trScidX, 2)
	if err := s.FlushBatch(b1); err != nil {
		t.Fatalf("flush fastsync seed: %v", err)
	}
	// Refresher-style scvars snapshot above the fork makes scidX affected.
	b2 := NewWriteBatch()
	b2.LastHeight = 5
	b2.AddVariables(trScidX, 5, []*structures.SCIDVariable{{Key: "likes", Value: "9"}})
	if err := s.FlushBatch(b2); err != nil {
		t.Fatalf("flush refresh: %v", err)
	}

	if err := s.TruncateToHeight(H); err != nil {
		t.Fatalf("TruncateToHeight: %v", err)
	}
	ax, err := s.GetAddressSCIDs(trAddrA)
	if err != nil {
		t.Fatalf("GetAddressSCIDs(A): %v", err)
	}
	e := ax[trScidX]
	if e == nil {
		t.Fatalf("addr_scids[A][X] (fastsync owner edge, written at 2 <= H) destroyed by truncate")
	}
	if e.Count != 1 || e.FirstHeight != 2 || e.LastHeight != 2 {
		t.Errorf("addr_scids[A][X] = {First:%d Last:%d Count:%d}, want {2 2 1}",
			e.FirstHeight, e.LastHeight, e.Count)
	}
}

// TestTruncateToHeight_SCCountOwnerlessInstall pins the sc_count decrement to
// owners entries actually removed: the on-demand refresh path can write an
// install with owner=="" and no AddOwner (indexer refreshOneSCID), which never
// bumped sc_count on the way in — truncating it away must not decrement.
func TestTruncateToHeight_SCCountOwnerlessInstall(t *testing.T) {
	const N, H = int64(8), int64(3)
	s := newTestStore(t)
	buildReorgChain(t, s, N) // scidX owner at h=2, scidY owner at h=4 → sc_count=2

	// Ownerless install above the fork (refresh-path shape: no AddOwner).
	b := NewWriteBatch()
	b.LastHeight = 6
	scidW := trHexID(0x44)
	b.AddInteractionHeight(scidW, 6)
	b.AddInstall(scidW, 6, &structures.InstallRecord{Owner: ""})
	if err := s.FlushBatch(b); err != nil {
		t.Fatalf("flush ownerless install: %v", err)
	}
	if n, _ := s.GetSCIDCount(); n != 2 {
		t.Fatalf("pre-truncate sc_count = %d, want 2", n)
	}

	if err := s.TruncateToHeight(H); err != nil {
		t.Fatalf("TruncateToHeight: %v", err)
	}
	// Only scidY's owners entry was removed; scidW never had one.
	if n, _ := s.GetSCIDCount(); n != 1 {
		t.Errorf("post-truncate sc_count = %d, want 1 (scidX only)", n)
	}
}

// TestTruncateToHeight_Edges covers no-op and full-wipe boundaries.
func TestTruncateToHeight_Edges(t *testing.T) {
	const N = int64(8)

	// Truncate to the current tip (and above) is a no-op.
	s := newTestStore(t)
	buildReorgChain(t, s, N)
	before := dumpAll(t, s)
	for _, h := range []int64{N, N + 100} {
		if err := s.TruncateToHeight(h); err != nil {
			t.Fatalf("TruncateToHeight(%d): %v", h, err)
		}
	}
	after := dumpAll(t, s)
	if fmt.Sprint(before) != fmt.Sprint(after) {
		t.Errorf("TruncateToHeight(>=tip) mutated the store")
	}

	// Truncate to 0 removes all height-bearing state.
	s2 := newTestStore(t)
	buildReorgChain(t, s2, N)
	if err := s2.TruncateToHeight(0); err != nil {
		t.Fatalf("TruncateToHeight(0): %v", err)
	}
	if hs, _ := s2.GetSCIDInteractionHeights(trScidX); len(hs) != 0 {
		t.Errorf("interaction heights survived TruncateToHeight(0): %v", hs)
	}
	if bh, _ := s2.GetBlockHash(1); bh != "" {
		t.Errorf("blockhash[1] survived TruncateToHeight(0): %q", bh)
	}
}
