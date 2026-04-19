package storage

import (
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// BenchmarkFlushBatch_AddrSCIDs isolates the addr_scids merge path so C4's
// restructure proposal can be measured against a clear baseline.
// Builds a batch with only AddrSCIDs populated; nothing else.
func BenchmarkFlushBatch_AddrSCIDs(b *testing.B) {
	const nAddrs = 100
	const scidsPerAddr = 10

	addrs := make([]string, nAddrs)
	scids := make([]string, scidsPerAddr)
	for i := range addrs {
		addrs[i] = fakeAddr()
	}
	for i := range scids {
		scids[i] = fakeSCID()
	}

	store := openTestStore(b)
	batch := NewWriteBatch()

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		batch.Reset()
		batch.LastHeight = 1
		for ai, addr := range addrs {
			for si, scid := range scids {
				batch.AddAddrSCID(addr, scid, int64(ai*scidsPerAddr+si))
			}
		}
		// Only addr_scids bucket work runs.
		if err := store.FlushBatch(batch); err != nil {
			b.Fatalf("FlushBatch: %v", err)
		}
	}
	b.ReportMetric(float64(nAddrs*scidsPerAddr), "addr_scid_pairs")

	// Silence unused-import warning in this file since we only use structures
	// indirectly via the batch helpers.
	_ = structures.MethodInstallSC
}
