package storage

import (
	"fmt"
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// BenchmarkAddVariables_Build isolates the batch-BUILD side of variable
// snapshots (FlushBatch_VarSnapshotBurst times only the flush; its AddVariables
// loop runs under StopTimer). Fixture: nSCIDs distinct scids × nHeights
// snapshots each, batch.Reset() per iteration to mirror the flusher recycling
// the batch. Variables is a flat map[VarKey][]*SCIDVariable, so each snapshot is
// a single keyed insert with no per-scid inner map; Reset's clear() retains the
// map's buckets, so re-filling the same keys each round reuses the backing
// storage. Measured: 0 allocs/op — this bench guards that a regression back to a
// per-scid (or any per-insert) allocation surfaces.
func BenchmarkAddVariables_Build(b *testing.B) {
	const nSCIDs = 512
	const nHeights = 2

	scids := make([]string, nSCIDs)
	for i := range scids {
		scids[i] = fmt.Sprintf("%064x", i)
	}
	vars := []*structures.SCIDVariable{
		{Key: "var_header_name", Value: "App"},
		{Key: "dURL", Value: "app.tela"},
	}

	batch := NewWriteBatch()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		batch.Reset()
		for _, scid := range scids {
			for h := int64(0); h < nHeights; h++ {
				batch.AddVariables(scid, 7_000_000+h, vars)
			}
		}
	}
	PutWriteBatch(batch)
	b.ReportMetric(float64(nSCIDs*nHeights), "snapshots")
}
