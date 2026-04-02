package pool

import (
	"sync"

	"github.com/hypergnomon/hypergnomon/structures"
)

// SCTXParsePool recycles parsed SC transaction objects.
var SCTXParsePool = sync.Pool{
	New: func() interface{} {
		return &structures.SCTXParse{}
	},
}

// BlockTxnsPool recycles block transaction containers.
var BlockTxnsPool = sync.Pool{
	New: func() interface{} {
		return &structures.BlockTxns{
			TxHashes: make([]string, 0, 64),
		}
	},
}

// WorkItemPool recycles pipeline work items. This is the Go equivalent
// of Jofito's arena page flowing through the processing pipeline.
var WorkItemPool = sync.Pool{
	New: func() interface{} {
		return &structures.WorkItem{
			SCTxs:     make([]structures.SCTXParse, 0, 32),
			NormalTxs: make([]structures.NormalTXWithSCIDParse, 0, 16),
		}
	},
}

// GetSCTXParse gets a recycled SCTXParse from the pool.
func GetSCTXParse() *structures.SCTXParse {
	return SCTXParsePool.Get().(*structures.SCTXParse)
}

// PutSCTXParse returns a SCTXParse to the pool after resetting.
func PutSCTXParse(s *structures.SCTXParse) {
	s.Reset()
	SCTXParsePool.Put(s)
}

// GetBlockTxns gets a recycled BlockTxns from the pool.
func GetBlockTxns() *structures.BlockTxns {
	return BlockTxnsPool.Get().(*structures.BlockTxns)
}

// PutBlockTxns returns a BlockTxns to the pool after resetting.
func PutBlockTxns(b *structures.BlockTxns) {
	b.Reset()
	BlockTxnsPool.Put(b)
}

// GetWorkItem gets a recycled WorkItem from the pool.
func GetWorkItem() *structures.WorkItem {
	return WorkItemPool.Get().(*structures.WorkItem)
}

// PutWorkItem returns a WorkItem to the pool after resetting.
func PutWorkItem(w *structures.WorkItem) {
	w.Reset()
	WorkItemPool.Put(w)
}
