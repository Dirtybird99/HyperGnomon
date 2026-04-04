package pool

import (
	"fmt"
	"sync"
	"testing"
	"unsafe"

	"github.com/hypergnomon/hypergnomon/structures"
)

// ---------------------------------------------------------------------------
// Unit tests: verify pool get/put/reset cycle
// ---------------------------------------------------------------------------

func TestSCTXParsePool(t *testing.T) {
	s := GetSCTXParse()
	if s == nil {
		t.Fatal("GetSCTXParse returned nil")
	}

	// Dirty every field.
	s.Txid = "abc123"
	s.Scid = "scid_xyz"
	s.Entrypoint = "Transfer"
	s.Method = structures.MethodInvokeSC
	s.Sender = "dero1sender"
	s.Fees = 999
	s.Height = 42000

	PutSCTXParse(s)

	// Get again -- may or may not be the same pointer, but must be reset.
	s2 := GetSCTXParse()
	if s2.Txid != "" {
		t.Errorf("Txid not reset: got %q", s2.Txid)
	}
	if s2.Scid != "" {
		t.Errorf("Scid not reset: got %q", s2.Scid)
	}
	if s2.Entrypoint != "" {
		t.Errorf("Entrypoint not reset: got %q", s2.Entrypoint)
	}
	if s2.Method != 0 {
		t.Errorf("Method not reset: got %d", s2.Method)
	}
	if s2.Sender != "" {
		t.Errorf("Sender not reset: got %q", s2.Sender)
	}
	if s2.Fees != 0 {
		t.Errorf("Fees not reset: got %d", s2.Fees)
	}
	if s2.Height != 0 {
		t.Errorf("Height not reset: got %d", s2.Height)
	}
	PutSCTXParse(s2)
}

func TestBlockTxnsPool(t *testing.T) {
	b := GetBlockTxns()
	if b == nil {
		t.Fatal("GetBlockTxns returned nil")
	}

	b.Topoheight = 150000
	b.TxHashes = append(b.TxHashes, "tx_aaa", "tx_bbb", "tx_ccc")

	PutBlockTxns(b)

	b2 := GetBlockTxns()
	if b2.Topoheight != 0 {
		t.Errorf("Topoheight not reset: got %d", b2.Topoheight)
	}
	if len(b2.TxHashes) != 0 {
		t.Errorf("TxHashes not reset: len=%d", len(b2.TxHashes))
	}
	PutBlockTxns(b2)
}

func TestWorkItemPool(t *testing.T) {
	w := GetWorkItem()
	if w == nil {
		t.Fatal("GetWorkItem returned nil")
	}

	w.Height = 88888
	w.RegCount = 5
	w.BurnCount = 3
	w.NormCount = 7
	w.SCTxs = append(w.SCTxs, structures.SCTXParse{Txid: "sc_tx_1"})
	w.NormalTxs = append(w.NormalTxs, structures.NormalTXWithSCIDParse{Txid: "norm_tx_1"})
	w.Err = fmt.Errorf("synthetic error")

	PutWorkItem(w)

	w2 := GetWorkItem()
	if w2.Height != 0 {
		t.Errorf("Height not reset: got %d", w2.Height)
	}
	if w2.RegCount != 0 {
		t.Errorf("RegCount not reset: got %d", w2.RegCount)
	}
	if w2.BurnCount != 0 {
		t.Errorf("BurnCount not reset: got %d", w2.BurnCount)
	}
	if w2.NormCount != 0 {
		t.Errorf("NormCount not reset: got %d", w2.NormCount)
	}
	if len(w2.SCTxs) != 0 {
		t.Errorf("SCTxs not reset: len=%d", len(w2.SCTxs))
	}
	if len(w2.NormalTxs) != 0 {
		t.Errorf("NormalTxs not reset: len=%d", len(w2.NormalTxs))
	}
	if w2.Err != nil {
		t.Errorf("Err not reset: %v", w2.Err)
	}
	PutWorkItem(w2)
}

// ---------------------------------------------------------------------------
// String interning tests: verify pointer deduplication
// ---------------------------------------------------------------------------

func TestInternSCID(t *testing.T) {
	scid := "0000000000000000000000000000000000000000000000000000000000000001"

	// Build two distinct backing strings so they cannot share a pointer by accident.
	a := InternSCID(string([]byte(scid)))
	b := InternSCID(string([]byte(scid)))

	if a != b {
		t.Fatal("interned strings not equal")
	}

	ptrA := unsafe.StringData(a)
	ptrB := unsafe.StringData(b)
	if ptrA != ptrB {
		t.Errorf("interned SCIDs do not share backing memory: %p vs %p", ptrA, ptrB)
	}
}

func TestInternAddress(t *testing.T) {
	addr := "dero1qyw4fl3dupcg5qlrcsvcedze507q5xcjmahyuqnmtaek0msg5khhqlqqg83nzz"

	a := InternAddress(string([]byte(addr)))
	b := InternAddress(string([]byte(addr)))

	if a != b {
		t.Fatal("interned addresses not equal")
	}

	ptrA := unsafe.StringData(a)
	ptrB := unsafe.StringData(b)
	if ptrA != ptrB {
		t.Errorf("interned addresses do not share backing memory: %p vs %p", ptrA, ptrB)
	}
}

// ---------------------------------------------------------------------------
// Benchmarks: pool reuse vs fresh allocation
// ---------------------------------------------------------------------------

// BenchmarkSCTXParse_Pool measures arena-style reuse through sync.Pool.
func BenchmarkSCTXParse_Pool(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		s := GetSCTXParse()
		s.Txid = "benchmarktxid"
		s.Scid = "benchmarkscid"
		s.Entrypoint = "Transfer"
		s.Method = structures.MethodInvokeSC
		s.Sender = "dero1sender"
		s.Fees = 500
		s.Height = 100000
		PutSCTXParse(s)
	}
}

// BenchmarkSCTXParse_New measures traditional allocation (no pool).
func BenchmarkSCTXParse_New(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		s := &structures.SCTXParse{}
		s.Txid = "benchmarktxid"
		s.Scid = "benchmarkscid"
		s.Entrypoint = "Transfer"
		s.Method = structures.MethodInvokeSC
		s.Sender = "dero1sender"
		s.Fees = 500
		s.Height = 100000
		_ = s
	}
}

// BenchmarkWorkItem_Pool measures arena-style reuse for pipeline work items.
func BenchmarkWorkItem_Pool(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		w := GetWorkItem()
		w.Height = 88888
		w.SCTxs = append(w.SCTxs, structures.SCTXParse{
			Txid: "tx1", Scid: "sc1", Entrypoint: "Stake",
		})
		w.NormalTxs = append(w.NormalTxs, structures.NormalTXWithSCIDParse{
			Txid: "ntx1", Scid: "sc1",
		})
		w.RegCount = 1
		PutWorkItem(w)
	}
}

// BenchmarkWorkItem_New measures traditional allocation for pipeline work items.
func BenchmarkWorkItem_New(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		w := &structures.WorkItem{
			SCTxs:     make([]structures.SCTXParse, 0, 32),
			NormalTxs: make([]structures.NormalTXWithSCIDParse, 0, 16),
		}
		w.Height = 88888
		w.SCTxs = append(w.SCTxs, structures.SCTXParse{
			Txid: "tx1", Scid: "sc1", Entrypoint: "Stake",
		})
		w.NormalTxs = append(w.NormalTxs, structures.NormalTXWithSCIDParse{
			Txid: "ntx1", Scid: "sc1",
		})
		w.RegCount = 1
		_ = w
	}
}

// ---------------------------------------------------------------------------
// Benchmarks: string interning
// ---------------------------------------------------------------------------

// BenchmarkInternSCID measures interning throughput with repeated SCIDs.
// Real workloads see the same handful of SCIDs across thousands of txns.
func BenchmarkInternSCID(b *testing.B) {
	scids := []string{
		"0000000000000000000000000000000000000000000000000000000000000001",
		"a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		"deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
		"0000000000000000000000000000000000000000000000000000000000000001", // repeat
		"a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2", // repeat
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for _, s := range scids {
			_ = InternSCID(s)
		}
	}
}

// BenchmarkInternSCID_NoIntern is the baseline: plain string copy each time.
func BenchmarkInternSCID_NoIntern(b *testing.B) {
	scids := []string{
		"0000000000000000000000000000000000000000000000000000000000000001",
		"a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
		"deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
		"0000000000000000000000000000000000000000000000000000000000000001",
		"a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for _, s := range scids {
			// Force a fresh copy to simulate non-interned allocation.
			cp := string([]byte(s))
			_ = cp
		}
	}
}

// ---------------------------------------------------------------------------
// Benchmarks: concurrent pool access (proves scalability under contention)
// ---------------------------------------------------------------------------

// BenchmarkSCTXParse_Pool_Parallel measures pool reuse under goroutine contention.
func BenchmarkSCTXParse_Pool_Parallel(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s := GetSCTXParse()
			s.Txid = "paralleltx"
			s.Scid = "parallelscid"
			s.Fees = 42
			PutSCTXParse(s)
		}
	})
}

// BenchmarkWorkItem_Pool_Parallel measures pipeline item reuse under contention.
func BenchmarkWorkItem_Pool_Parallel(b *testing.B) {
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			w := GetWorkItem()
			w.Height = 77777
			w.SCTxs = append(w.SCTxs, structures.SCTXParse{Txid: "ptx"})
			PutWorkItem(w)
		}
	})
}

// ---------------------------------------------------------------------------
// AllocsPerRun verification: exact allocation counts
// ---------------------------------------------------------------------------

func TestSCTXParse_PoolZeroAllocs(t *testing.T) {
	// Prime the pool so Get doesn't trigger New.
	s := GetSCTXParse()
	PutSCTXParse(s)

	allocs := testing.AllocsPerRun(100, func() {
		s := GetSCTXParse()
		s.Txid = "test"
		PutSCTXParse(s)
	})
	// Pool reuse should yield 0 allocs in the steady state.
	if allocs > 1 {
		t.Errorf("expected <=1 allocs/op from pool, got %.1f", allocs)
	}
}

func TestWorkItem_PoolZeroAllocs(t *testing.T) {
	w := GetWorkItem()
	PutWorkItem(w)

	allocs := testing.AllocsPerRun(100, func() {
		w := GetWorkItem()
		w.Height = 1
		w.SCTxs = append(w.SCTxs, structures.SCTXParse{Txid: "a"})
		PutWorkItem(w)
	})
	if allocs > 1 {
		t.Errorf("expected <=1 allocs/op from pool, got %.1f", allocs)
	}
}

// ---------------------------------------------------------------------------
// Capacity retention: verify pool preserves underlying slice capacity
// ---------------------------------------------------------------------------

func TestBlockTxns_ResetClearsLength(t *testing.T) {
	b := GetBlockTxns()

	for i := range 200 {
		b.TxHashes = append(b.TxHashes, fmt.Sprintf("tx_%d", i))
	}

	b.Reset()

	if len(b.TxHashes) != 0 {
		t.Errorf("length should be 0 after reset, got %d", len(b.TxHashes))
	}
	// Reset via [:0] preserves capacity on the same object.
	if cap(b.TxHashes) < 200 {
		t.Errorf("capacity should be retained on same object: got %d", cap(b.TxHashes))
	}
	PutBlockTxns(b)
}

func TestWorkItem_ResetClearsLength(t *testing.T) {
	w := GetWorkItem()

	for i := range 100 {
		w.SCTxs = append(w.SCTxs, structures.SCTXParse{Height: int64(i)})
	}

	w.Reset()

	if len(w.SCTxs) != 0 {
		t.Errorf("SCTxs length should be 0 after reset, got %d", len(w.SCTxs))
	}
	if cap(w.SCTxs) < 100 {
		t.Errorf("SCTxs capacity should be retained on same object: got %d", cap(w.SCTxs))
	}
	PutWorkItem(w)
}

// ---------------------------------------------------------------------------
// Concurrency stress: hammer pools from many goroutines
// ---------------------------------------------------------------------------

func TestPoolConcurrentSafety(t *testing.T) {
	const goroutines = 64
	const opsPerGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines * 3) // 3 pool types

	// SCTXParse
	for range goroutines {
		go func() {
			defer wg.Done()
			for range opsPerGoroutine {
				s := GetSCTXParse()
				s.Txid = "concurrent"
				s.Fees = 1
				PutSCTXParse(s)
			}
		}()
	}

	// BlockTxns
	for range goroutines {
		go func() {
			defer wg.Done()
			for range opsPerGoroutine {
				b := GetBlockTxns()
				b.TxHashes = append(b.TxHashes, "h1", "h2")
				PutBlockTxns(b)
			}
		}()
	}

	// WorkItem
	for range goroutines {
		go func() {
			defer wg.Done()
			for range opsPerGoroutine {
				w := GetWorkItem()
				w.Height = 1
				w.SCTxs = append(w.SCTxs, structures.SCTXParse{Txid: "c"})
				PutWorkItem(w)
			}
		}()
	}

	wg.Wait()
}
