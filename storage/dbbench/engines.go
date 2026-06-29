package dbbench

// engine is the uniform adapter the benchmark drives so every store runs the
// same logical operation through its own native fast path. Each concrete engine
// lives in its own engine_<name>.go file so an optimize-loop can target one
// store in isolation.
type engine interface {
	name() string
	// batchWrite commits every pair in ONE transaction/commit (the FlushBatch unit).
	batchWrite(pairs []kv) error
	// get is a single point lookup by key.
	get(key []byte) ([]byte, error)
	// rangeScan counts (and touches the value of) every entry in [lo, hi).
	rangeScan(lo, hi []byte) (int, error)
	// scanAll counts (and touches the value of) every entry in the store.
	scanAll() (int, error)
	close() error
}

// sink defends the read benchmarks against dead-code elimination — every scanned
// value length is summed here so the compiler cannot drop the loop body. The
// benchmarks run serially (no b.RunParallel), so an unsynchronized int is fine.
var sink int //nolint:unused

type engineFactory struct {
	name string
	open func(dir string) (engine, error)
}

// engineFactories lists the engines under test, in factory-readiness order.
func engineFactories() []engineFactory {
	return []engineFactory{
		{"bbolt", openBbolt},
		{"sqlite", openSqlite},
		{"graviton", openGraviton},
	}
}
