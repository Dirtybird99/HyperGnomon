# storage/dbbench — engine-level DB speed comparison

A standalone benchmark that answers *"which key/value store is fastest for
HyperGnomon's workload?"* across the three engines the pluggable storage layer
names: **bbolt** (the shipped backend), **sqlite** (`modernc.org/sqlite`, pure
Go), and **graviton** (`deroproject/graviton`).

**Why engine-level, not through the `Storage` interface?** Only bbolt has a real
`Storage` implementation — `storage.Open("sqlite", …)` returns
`ErrBackendNotImplemented` and `Open("graviton", …)` returns
`ErrGravitonUnsupported`. So instead of writing two full 39-method backends, this
package drives each raw engine through the same operations, with byte-identical
synthetic data, and compares them directly.

This package is intentionally isolated: its sqlite/graviton imports live only in
this test binary and are **never linked into `cmd/hypergnomon`** (`go build ./...`
stays sqlite-free).

## Results

See **[RESULTS.md](RESULTS.md)** for the latest measured numbers. Short version:
**bbolt wins 4 of 5 operations**; after the optimize-loop pass, tuned sqlite wins
the small-batch write — but that is a single-flat-table artifact that would **not**
survive a real multi-bucket backend (see the caveats in RESULTS.md), so it is not a
backend-selection conclusion. graviton's range scan is ~758× slower than bbolt (no
ordered iteration), and sqlite trails on point reads, range scans, and full scans.

## The workload (`workload.go`)

Data shapes mirror the real hot paths (`storage/storage.go`, `structures/`):

| dataset | key | value | models |
|---|---|---|---|
| point | 32-byte SCID | 80 B | `GetOwner` / `GetSCIDClass` point lookups |
| height-prefixed | `BE8(height)` + tag (12 B) | 64 B | `GetInstallsInRange` / interaction-height range scans |
| batch values | — | 256 B and 2 KB | mixed records vs variable snapshots |

All keys/values are generated deterministically (xorshift64 seeded by index), so
every engine and every run sees identical bytes — results are reproducible.

## Operations (`engine_compare_test.go`)

| benchmark | what it measures | bbolt | sqlite | graviton |
|---|---|---|---|---|
| `BatchWrite/{small,large}` | commit 1000 pairs in one txn (`FlushBatch`) | `Update` + `Put`×N | `BEGIN`/`INSERT`/`COMMIT` | `Put`×N + `Commit` |
| `PointRead` | random lookup over 50k keys | `View` + `Get` | `SELECT … WHERE k=?` | `tree.Get` |
| `RangeScan` | 500-row height window | `Cursor.Seek` (O(window)) | `WHERE k>=? AND k<?` (O(window)) | full `Cursor` + filter (**O(N)**) |
| `ScanAll` | iterate all 50k keys (`GetAllSCIDs`) | `Cursor` | `SELECT v FROM kv` | full `Cursor` |

## Fairness

`TestEnginesRoundTrip` is the gate: every engine must persist and return
byte-identical data for the same workload (present key, absent key, range count,
full count) — and all three must agree with each other. A benchmark number is
only meaningful if this passes (a silently-no-op store would look infinitely
fast). Read benchmarks close and reopen the store before timing, so every engine
is measured against a cold, committed-on-disk view.

**Durability** is a documented constant (`durability` in `workload.go`). Default
is `"bulk"` — bbolt `NoSync`, sqlite `synchronous=OFF`+WAL — matching the
indexer's initial-sync hot path. Set it to `"durable"` for an fsync-on-commit
comparison.

## Run it

```bash
# 1) fairness gate — must pass before trusting any number
go test ./storage/dbbench/ -run TestEnginesRoundTrip -v

# 2) the comparison (median of 6 for stable ns/op)
go test ./storage/dbbench/ -bench=. -benchmem -run=^$ -count=6

# focus a single operation or engine
go test ./storage/dbbench/ -bench=RangeScan -benchmem -run=^$ -count=6
go test ./storage/dbbench/ -bench=BatchWrite/large_2KB/graviton -benchmem -run=^$
```

ns/op carries run-to-run thermal jitter on a laptop — compare **medians**, and
treat the deterministic `B/op` / `allocs/op` as the tie-breakers.
