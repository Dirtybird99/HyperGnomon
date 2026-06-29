// Package dbbench is a standalone, engine-level speed comparison of the three
// key/value stores HyperGnomon's pluggable storage layer names: bbolt (the only
// shipped Storage backend), sqlite (planned), and graviton (deliberately
// unsupported — see storage/factory.go).
//
// It does NOT go through the 39-method Storage interface — sqlite and graviton
// have no Storage implementation. Instead it drives each raw engine through the
// same HyperGnomon-shaped workloads (batched bulk writes like FlushBatch, point
// reads by SCID, and the BE8 height-prefix range scans that are the whole reason
// the factory rejects graviton). The package lives under its own directory so its
// sqlite/graviton imports stay in this test binary and never link into the
// production cmd/hypergnomon build.
//
// The numbers are only trusted if TestEnginesRoundTrip passes first: it proves
// all three engines persist and return byte-identical data, so no engine can fake
// a "win" by silently doing less work.
package dbbench

import "encoding/binary"

// kv is one key/value pair. Keys and values are plain []byte so every engine
// sees byte-identical input.
type kv struct {
	k []byte
	v []byte
}

// durability selects how aggressively each engine fsyncs on commit.
//
//	"bulk"    — mirrors HyperGnomon's initial-sync hot path: bbolt runs with
//	            NoSync until chain tip (storage/bbolt.go), sqlite with
//	            synchronous=OFF. This is the throughput-dominant regime the
//	            request cares about ("who syncs the chain fastest").
//	"durable" — fsync on every commit (bbolt sync, sqlite synchronous=FULL) for a
//	            crash-safe comparison. Flip this constant and re-run for that view.
//
// graviton has no fsync knob; NewDiskStore persists on Commit either way.
const durability = "bulk" // "bulk" | "durable"

// fill writes a deterministic pseudo-random byte pattern derived from seed into
// dst (xorshift64). Deterministic so every engine and every run sees identical
// bytes — no Date.now/random drift, results are reproducible.
func fill(dst []byte, seed uint64) {
	x := seed*0x9E3779B97F4A7C15 + 0x1234567890ABCDEF
	if x == 0 {
		x = 0x9E3779B97F4A7C15
	}
	for i := range dst {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		dst[i] = byte(x >> 24)
	}
}

// scidKey returns a deterministic 32-byte SCID-shaped key for index i.
func scidKey(i int) []byte {
	k := make([]byte, 32)
	fill(k, uint64(i)+0x9E3779B9)
	return k
}

// heightKey returns a deterministic BE8-height-prefixed key for index i: the
// 8-byte big-endian height (== i) followed by a 4-byte SCID-ish tag. Keys sort by
// height, so a window [heightKey(h), heightKey(h+W)) selects exactly W entries —
// the GetInstallsInRange / interaction-height range-scan shape.
func heightKey(i int) []byte {
	k := make([]byte, 12)
	binary.BigEndian.PutUint64(k[:8], uint64(i))
	binary.BigEndian.PutUint32(k[8:], uint32(i)*2654435761)
	return k
}

// makeValue returns a deterministic value of the given size for index i.
func makeValue(i, size int) []byte {
	v := make([]byte, size)
	fill(v, uint64(i)*1000003+7)
	return v
}

// pointPairs builds n point-lookup pairs: 32-byte SCID key → valSize-byte value.
func pointPairs(n, valSize int) []kv {
	out := make([]kv, n)
	for i := range out {
		out[i] = kv{k: scidKey(i), v: makeValue(i, valSize)}
	}
	return out
}

// heightPairs builds n height-prefixed pairs (64-byte values), for range/scan.
func heightPairs(n int) []kv {
	out := make([]kv, n)
	for i := range out {
		out[i] = kv{k: heightKey(i), v: makeValue(i, 64)}
	}
	return out
}

// rotatingBatches builds `count` distinct batches that share nothing — distinct
// keys AND values — so a write benchmark can rotate through them: every commit
// writes genuinely changed data (no no-op "nothing dirty" commits that would flatter
// an engine), while the working set stays bounded at count*n keys so B-tree depth
// does not drift across iterations. valSize sets the value width.
func rotatingBatches(count, n, valSize int) [][]kv {
	batches := make([][]kv, count)
	for b := range batches {
		pairs := make([]kv, n)
		for i := range pairs {
			idx := b*n + i
			pairs[i] = kv{k: scidKey(idx), v: makeValue(idx, valSize)}
		}
		batches[b] = pairs
	}
	return batches
}
