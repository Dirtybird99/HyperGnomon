package storage

import (
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// buildBenchChain populates store with nScids SCs, one installed per height
// h=1..nScids, each with a full record set (owner, invocation, interaction,
// addr_scid, scvars snapshot, install, class, normal-tx). Heights are flushed in
// chunks so setup is fast. This shapes every bucket the truncate touches so the
// benchmark reflects realistic full-bucket scan costs.
func buildBenchChain(b *testing.B, store *BboltStore, nScids int) {
	b.Helper()
	const chunk = 1000
	const addr = "benchAddr"
	vars := []*structures.SCIDVariable{{Key: "likes", Value: "0"}}
	for start := 1; start <= nScids; start += chunk {
		end := start + chunk - 1
		if end > nScids {
			end = nScids
		}
		batch := NewWriteBatch()
		for h := start; h <= end; h++ {
			scid := fmt.Sprintf("%064x", h)
			hh := int64(h)
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
}

// BenchmarkTruncateToHeight measures a shallow (10-block) reorg truncate against
// growing total DB size. If ns/op grows ~linearly with scid count, the cost is
// dominated by the full-bucket scans (scales with DB size); if flat, it is
// bounded by reorg depth.
func BenchmarkTruncateToHeight(b *testing.B) {
	for _, n := range []int{2000, 8000, 32000} {
		b.Run(fmt.Sprintf("scids=%d/reorg=10", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dir := filepath.Join(b.TempDir(), strconv.Itoa(i))
				store, err := NewBboltStore(dir, "")
				if err != nil {
					b.Fatalf("open: %v", err)
				}
				buildBenchChain(b, store, n)
				b.StartTimer()

				if err := store.TruncateToHeight(int64(n - 10)); err != nil {
					b.Fatalf("truncate: %v", err)
				}

				b.StopTimer()
				_ = store.Close()
			}
		})
	}
}
