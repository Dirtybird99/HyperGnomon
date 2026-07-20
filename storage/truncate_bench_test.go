package storage

import (
	"fmt"
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// truncBenchCfg spans the four independent axes TruncateToHeight's cost can
// scale with. The old fixture welded them together (one SC per height, ONE
// constant address), which made two of its scans unobservable: with a single
// address, applyAddrSCIDRollback's outer sub-bucket walk ran over exactly one
// bucket, and distinct-SCs-above-fork was identical to reorg depth.
type truncBenchCfg struct {
	scids  int // total SCs, one installed per height 1..scids (fork = scids)
	addrs  int // distinct addresses round-robined across owners/invokes/normtx
	depth  int // reorg depth: above-fork blocks occupy scids+1..scids+depth
	sAbove int // distinct pre-fork SCs re-touched above the fork ("affected" set)
}

func (c truncBenchCfg) name() string {
	return fmt.Sprintf("scids=%d/addrs=%d/depth=%d/S=%d", c.scids, c.addrs, c.depth, c.sAbove)
}

// buildBenchChain populates store per cfg. Below the fork: one SC installed per
// height h=1..scids with a full record set (owner, invocation, interaction,
// addr_scid, scvars snapshot, install, class, normal-tx), owner = addr[i%addrs].
// Above the fork: NO new installs — SC k (k=0..S-1) is re-touched (interaction,
// scvars, invocation, addr_scid, normal-tx) at height fork+1+(k%depth), so all
// S SCs land above the fork regardless of depth, and every above-fork height
// gets a blockhash. Installs-above-fork stays empty on purpose: that path is
// already an O(depth) seek (truncate.go Step 0(c)), and keeping it out of the
// fixture keeps the affected set exactly S, so growth attributes cleanly to
// the discovery scans and the per-affected-SC recompute. Heights are flushed
// in chunks so setup is fast.
func buildBenchChain(b *testing.B, store *BboltStore, cfg truncBenchCfg) {
	b.Helper()
	const chunk = 1000
	vars := []*structures.SCIDVariable{{Key: "likes", Value: "0"}}
	addrAt := func(i int) string {
		return fmt.Sprintf("dero1qy%057d", i%cfg.addrs)
	}
	scidAt := func(i int) string {
		return fmt.Sprintf("%064x", i)
	}
	for start := 1; start <= cfg.scids; start += chunk {
		end := start + chunk - 1
		if end > cfg.scids {
			end = cfg.scids
		}
		batch := NewWriteBatch()
		for h := start; h <= end; h++ {
			scid, addr, hh := scidAt(h), addrAt(h), int64(h)
			batch.AddBlockHash(hh, fmt.Sprintf("%064x", 0xB0000000+h))
			batch.AddOwner(scid, addr)
			batch.AddInvocation(structures.InvokeRecord{
				Scid: scid, Sender: addr, Entrypoint: "install", Height: hh,
				Details: &structures.SCTXParse{
					Txid: fmt.Sprintf("%064x", 0x70000000+h), Scid: scid,
					Sender: addr, Entrypoint: "install", Height: hh,
				},
			})
			batch.AddInteractionHeight(scid, hh)
			batch.AddAddrSCID(addr, scid, hh)
			batch.AddVariables(scid, hh, vars)
			batch.AddInstall(scid, hh, &structures.InstallRecord{Owner: addr, Entrypoint: "install", Fees: 1})
			batch.AddClass(scid, &structures.ClassMeta{
				Class: "TELA-INDEX-1", Tags: []string{"all", "tela"}, Name: "A",
				InstallHeight: hh, LastHeight: hh,
			})
			batch.AddNormalTx(addr, fmt.Sprintf("%064x", 0x90000000+h), scid, 1, hh)
			batch.RegTxCount++
		}
		batch.LastHeight = int64(end)
		if err := store.FlushBatch(batch); err != nil {
			b.Fatalf("build flush: %v", err)
		}
	}

	// Above-fork activity: S distinct pre-fork SCs re-touched, no new installs.
	fork := cfg.scids
	batch := NewWriteBatch()
	for j := 1; j <= cfg.depth; j++ {
		batch.AddBlockHash(int64(fork+j), fmt.Sprintf("%064x", 0xC0000000+j))
	}
	for k := 0; k < cfg.sAbove; k++ {
		i := 1 + k%cfg.scids // pre-fork SC to re-touch
		scid, addr := scidAt(i), addrAt(i)
		hh := int64(fork + 1 + k%cfg.depth)
		batch.AddInvocation(structures.InvokeRecord{
			Scid: scid, Sender: addr, Entrypoint: "poke", Height: hh,
			Details: &structures.SCTXParse{
				Txid: fmt.Sprintf("%064x", 0xA0000000+k), Scid: scid,
				Sender: addr, Entrypoint: "poke", Height: hh,
			},
		})
		batch.AddInteractionHeight(scid, hh)
		batch.AddAddrSCID(addr, scid, hh)
		batch.AddVariables(scid, hh, vars)
		batch.AddNormalTx(addr, fmt.Sprintf("%064x", 0xD0000000+k), scid, 1, hh)
	}
	batch.LastHeight = int64(fork + cfg.depth)
	if err := store.FlushBatch(batch); err != nil {
		b.Fatalf("above-fork flush: %v", err)
	}
}

// BenchmarkTruncateToHeight sweeps each cost axis with the others pinned.
// allocs/op is the lead metric (deterministic; ~1 alloc per key SPLIT during
// the discovery scans — splitHeightKey/splitScVarsKey materialize a string
// per parsed key BEFORE the height filter — plus the per-affected-SC recompute
// work); ns/op is advisory (~6% jitter).
//
//	scids sweep — linear allocs/op growth = full-scan discovery dominates
//	              (cost scales with total DB size, not reorg work).
//	addrs sweep — growth here isolates applyAddrSCIDRollback's walk over
//	              every address sub-bucket (invisible to the old fixture).
//	S sweep     — the per-affected-SC term: invocation-bucket harvest +
//	              scvars_latest lowering.
//	depth sweep — should be ~flat; if it is, a 1-block reorg really does pay
//	              the same scan bill as a 1000-block one.
//
// Setup rebuilds the DB in the untimed region each iteration, so run with a
// bounded -benchtime (e.g. -benchtime=3x); the default 1s target would rebuild
// the largest fixture for minutes.
func BenchmarkTruncateToHeight(b *testing.B) {
	cases := []truncBenchCfg{
		// DB-size sweep (addrs/depth/S pinned).
		{scids: 2000, addrs: 512, depth: 10, sAbove: 10},
		{scids: 8000, addrs: 512, depth: 10, sAbove: 10},
		{scids: 32000, addrs: 512, depth: 10, sAbove: 10},
		// Address-cardinality sweep (8k scids; addrs=512 point shared above).
		{scids: 8000, addrs: 1, depth: 10, sAbove: 10},
		{scids: 8000, addrs: 8192, depth: 10, sAbove: 10},
		// Affected-SC (S) sweep.
		{scids: 8000, addrs: 512, depth: 100, sAbove: 1},
		{scids: 8000, addrs: 512, depth: 100, sAbove: 100},
		{scids: 8000, addrs: 512, depth: 100, sAbove: 1000},
		// Depth sweep (S pinned).
		{scids: 8000, addrs: 512, depth: 1, sAbove: 1},
		{scids: 8000, addrs: 512, depth: 1000, sAbove: 1},
	}
	for _, cfg := range cases {
		b.Run(cfg.name(), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				store := newTestStore(b)
				buildBenchChain(b, store, cfg)
				b.StartTimer()

				if err := store.TruncateToHeight(int64(cfg.scids)); err != nil {
					b.Fatalf("truncate: %v", err)
				}

				b.StopTimer()
				_ = store.Close()
				b.StartTimer()
			}
			b.ReportMetric(float64(cfg.sAbove), "affectedSCs")
		})
	}
}
