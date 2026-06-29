package indexer

import (
	"fmt"
	"testing"

	"github.com/deroproject/derohe/rpc"
)

// txTuple is the (index, hash, height) selection processorLoop makes per TX
// before decode. The drop-txmap change must select the identical ordered
// sequence.
type txTuple struct {
	txIdx   int
	hashStr string
	height  int64
}

// buildTxIdxBatch makes a synthetic fetchedBatch whose allTxHashes is the
// in-order concatenation of every block's txHashes, mirroring BOTH fetcher
// paths (fetcherLoop and fetchSingleBlock append to bi.txHashes and allTxHashes
// adjacently under identical filtering). Hashes are unique. Txs_as_hex / Txs are
// aligned to allTxHashes; pass txLen < total to exercise the truncated skip branch.
func buildTxIdxBatch(nBlocks, perBlock, txLen int) *fetchedBatch {
	fb := &fetchedBatch{}
	n := 0
	for b := 0; b < nBlocks; b++ {
		bi := blockInfo{height: int64(b + 1)}
		for k := 0; k < perBlock; k++ {
			h := fmt.Sprintf("%064x", n+1) // unique 64-hex hash
			bi.txHashes = append(bi.txHashes, h)
			fb.allTxHashes = append(fb.allTxHashes, h)
			n++
		}
		fb.blocks = append(fb.blocks, bi)
	}
	txr := &rpc.GetTransaction_Result{
		Txs_as_hex: make([]string, txLen),
		Txs:        make([]rpc.Tx_Related_Info, txLen),
	}
	for i := 0; i < txLen; i++ {
		txr.Txs_as_hex[i] = fmt.Sprintf("hex-%d", i)
	}
	fb.txResult = txr
	return fb
}

// traverseMap is a VERBATIM copy of the PRE-change processorLoop dispatch:
// build map[hash]index over allTxHashes, then per block/hash look the index up,
// skipping when missing or beyond Txs_as_hex. It is the oracle the counter
// traversal must match.
func traverseMap(fb *fetchedBatch) []txTuple {
	var out []txTuple
	txMap := make(map[string]int, len(fb.allTxHashes))
	for i, h := range fb.allTxHashes {
		txMap[h] = i
	}
	for _, bi := range fb.blocks {
		for _, hashStr := range bi.txHashes {
			txIdx, ok := txMap[hashStr]
			if !ok || txIdx >= len(fb.txResult.Txs_as_hex) {
				continue
			}
			out = append(out, txTuple{txIdx, hashStr, bi.height})
		}
	}
	return out
}

// traverseCounter is a VERBATIM copy of the POST-change processorLoop dispatch:
// a running counter replaces the map, advanced on EVERY visited pair (incl. the
// skip branch) to stay aligned with Txs_as_hex. Returns the selected tuples and
// the final counter value.
func traverseCounter(fb *fetchedBatch) ([]txTuple, int) {
	var out []txTuple
	txIdx := 0
	for _, bi := range fb.blocks {
		for _, hashStr := range bi.txHashes {
			ti := txIdx
			txIdx++
			if ti >= len(fb.txResult.Txs_as_hex) {
				continue
			}
			// Belt-and-suspenders: production code does NOT do this check (no
			// hot-path cost); here it pins the fetcher append-order invariant
			// the counter relies on.
			if fb.allTxHashes[ti] != hashStr {
				panic("allTxHashes is not the in-order block/txHashes concat")
			}
			out = append(out, txTuple{ti, hashStr, bi.height})
		}
	}
	return out, txIdx
}

// TestProcessorTxIdxTraversalAgrees gates the drop-txmap-use-counter change:
// the counter dispatch must select the identical ordered (txIdx, hashStr,
// height) tuples as the map dispatch, and must increment on the skip branch
// (final counter == total visited pairs) so Txs_as_hex alignment is preserved.
func TestProcessorTxIdxTraversalAgrees(t *testing.T) {
	cases := []struct {
		nBlocks, perBlock, txLen int
	}{
		{1, 1, 1},  // single tx, full
		{3, 4, 12}, // full: txLen == total
		{4, 5, 7},  // truncated: exercises skip-branch tail
		{2, 3, 0},  // no txs returned: every pair skipped
		{8, 8, 64}, // full, larger
		{5, 5, 3},  // heavy truncation
	}
	for _, c := range cases {
		fb := buildTxIdxBatch(c.nBlocks, c.perBlock, c.txLen)
		want := traverseMap(fb)
		got, finalCounter := traverseCounter(fb)
		total := c.nBlocks * c.perBlock
		if finalCounter != total {
			t.Fatalf("case %+v: counter ended at %d, want %d (must increment on skip branch)", c, finalCounter, total)
		}
		if len(got) != len(want) {
			t.Fatalf("case %+v: tuple count got=%d want=%d", c, len(got), len(want))
		}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("case %+v: tuple[%d] got=%+v want=%+v", c, i, got[i], want[i])
			}
		}
	}
}
