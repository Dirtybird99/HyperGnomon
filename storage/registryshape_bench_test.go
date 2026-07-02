package storage

import (
	"fmt"
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// BenchmarkFlushBatch_RegistryShape reproduces the FastSync turbo registry
// persist: one FlushBatch carrying ~50k SCIDs' worth of Owners (+ the
// OwnerSCIDs reverse index AddOwner maintains), Heights, Installs, and
// AddrSCIDs — the single 6.7-8.6s bbolt Update that dominates cold TELA
// indexing. Distinct owners are ~a tenth of SCIDs (deployers own many
// contracts), so the OwnerSCIDs/AddrSCIDs shape is realistic. ns/op is the
// lead metric here: the cost is B+tree page-split CPU (NoSync is on), which
// allocs/op cannot see.
func BenchmarkFlushBatch_RegistryShape(b *testing.B) {
	const nSCIDs = 50_000
	const nOwners = 5_000

	scids := make([]string, nSCIDs)
	owners := make([]string, nOwners)
	for i := range owners {
		owners[i] = fmt.Sprintf("dero1qy%057d", i)
	}
	for i := range scids {
		scids[i] = fmt.Sprintf("%016x%048d", i*2654435761, i)
	}

	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		b.StopTimer()
		store := newTestStore(b)
		batch := NewWriteBatch()
		for j, scid := range scids {
			owner := owners[j%nOwners]
			height := int64(7_000_000 + j)
			batch.AddOwner(scid, owner)
			batch.AddInteractionHeight(scid, height)
			batch.AddInstall(scid, height, &structures.InstallRecord{Owner: owner})
			batch.AddAddrSCID(owner, scid, height)
		}
		batch.LastHeight = 7_000_000 + nSCIDs
		b.StartTimer()
		if err := store.FlushBatch(batch); err != nil {
			b.Fatalf("flush: %v", err)
		}
		b.StopTimer()
		PutWriteBatch(batch)
		_ = store.Close()
		b.StartTimer()
	}
	b.ReportMetric(float64(nSCIDs), "scids")
}
