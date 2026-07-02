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
// the batch. The nested map[scid]map[height] layout pays one inner-map
// allocation per distinct scid per batch cycle — Reset's clear() drops every
// inner map, so they are re-allocated each round.
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
