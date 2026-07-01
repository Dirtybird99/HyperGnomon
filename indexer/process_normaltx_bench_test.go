package indexer

import (
	"fmt"
	"testing"

	"github.com/deroproject/derohe/cryptography/crypto"
	"github.com/deroproject/derohe/rpc"
	"github.com/deroproject/derohe/transaction"

	"github.com/hypergnomon/hypergnomon/storage"
)

// BenchmarkProcessNormalTx isolates the per-normal-TX build path: for each
// payload's ring, processNormalTx interns the addr, records the addr↔scid
// edge, and appends a NormalTXWithSCIDParse. The pre-arena code heap-allocates
// one *NormalTXWithSCIDParse per ring member (bounded by ring size × payloads,
// on >50% of txs), which is what this benchmark measures for the arena change.
//
// Fixture: nPayloads distinct non-zero SCIDs, each with a ringSize ring of
// distinct addresses. batch.Reset() per iteration mirrors the flusher recycling
// the batch, so the arena's [:0] reuse is exercised every round.
func BenchmarkProcessNormalTx(b *testing.B) {
	const nPayloads = 2
	const ringSize = 16

	idx := &Indexer{}

	var tx transaction.Transaction
	tx.Payloads = make([]transaction.AssetPayload, nPayloads)
	for j := range tx.Payloads {
		var h crypto.Hash
		h[0] = byte(j + 1) // non-zero SCID so the zero-hash guard passes
		tx.Payloads[j].SCID = h
	}

	txInfo := rpc.Tx_Related_Info{Ring: make([][]string, nPayloads)}
	for j := range txInfo.Ring {
		ring := make([]string, ringSize)
		for r := range ring {
			ring[r] = fmt.Sprintf("dero1qy%057d", j*ringSize+r)
		}
		txInfo.Ring[j] = ring
	}

	const txid = "feed0001feed0001feed0001feed0001feed0001feed0001feed0001feed0001"

	batch := storage.NewWriteBatch()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		batch.Reset()
		idx.processNormalTx(&tx, txInfo, txid, 1000, batch)
	}
	storage.PutWriteBatch(batch)
	b.ReportMetric(float64(nPayloads*ringSize), "ntx_records")
}
