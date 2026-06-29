# DB Engine Speed Showdown — Results

This is an *engine-level* comparison (raw bbolt vs sqlite vs graviton driven
through identical HyperGnomon-shaped workloads), because only bbolt has a real
`Storage` implementation — sqlite is a not-implemented stub and graviton is
`ErrGravitonUnsupported` by design. See [README.md](README.md) for the workload
and how to reproduce.

The numbers below are **after an optimize-loop pass** over each engine's
adapter, targeting the **BatchWrite (FlushBatch) hot path** under a correctness
gate. The optimization round is summarized at the bottom; the headline is that
tuning flipped the small-batch-write ranking.

## Verdict

**bbolt still wins 4 of 5 operations** (point reads, range scans, full scans, and
large-value writes) — its single-file B+tree fits HyperGnomon's access patterns.
**But after optimization, sqlite now wins the small-batch write hot path**
(1.56 ms vs bbolt's ~3.2 ms) and does it with a third of bbolt's allocations.
graviton remains structurally disqualified — its hash-ordered range scan is still
**~758× slower** than bbolt, the exact reason `storage/factory.go` rejects it.

## Caveats — read before citing these numbers

These are an **engine-floor study**, not a backend-selection verdict. Two limits:

1. **The sqlite small-write win is a single-table artifact and almost certainly
   does not transfer to a real backend.** `engine_sqlite.go` writes ONE flat
   `kv` table via a pure multi-row `ON CONFLICT` upsert with no read-before-write.
   The real `storage/bbolt.go` `FlushBatch` fans one batch across ~13 logical
   tables / 17 buckets with per-row **merge-on-write** the upsert can't express —
   uvarint height-count merge (`bbolt.go:781-790`), AddrSCID min/max/sum merge
   (`bbolt.go:977-1001`), class dual-write + stale-row delete (`bbolt.go:898-912`).
   Those merges break the exact keeper (the 100-row multi-row INSERT) that produced
   1.56 ms, and each read-back costs ~8 µs in sqlite vs 625 ns in bbolt. So a real
   sqlite backend's `FlushBatch` would very likely **meet or exceed** bbolt, not beat
   it. Do **not** quote "sqlite wins small writes" as a backend-selection input.
2. **Only the `bulk` durability profile is published** (sqlite `synchronous=OFF`,
   WAL never checkpointed; `durability` pinned at `workload.go:38`) — the most
   sqlite-favorable regime, and weaker than bbolt `NoSync`. A crash-safe (`durable`)
   comparison is unrun. The rotating-8-batch workload also caps the DB at ~8k keys,
   so B-tree depth never reaches initial-sync scale and the 64 MiB cache trivially
   covers it.

What **is** robust: bbolt winning the read paths and large-value writes (realistic
shapes), and graviton's structural disqualification (hash-ordered cursor, no
`Seek`/`Range` — hardware-independent).

## Setup

| | |
|---|---|
| Host | 13th Gen Intel Core i7-13700HX, windows/amd64 |
| Go | go1.26.0 |
| SQLite driver | `modernc.org/sqlite` v1.53.0 (pure Go, no cgo) |
| Durability profile | **bulk** — bbolt `NoSync`, sqlite `synchronous=OFF`+WAL+64 MiB cache (mirrors the indexer's initial-sync hot path) |
| Samples | `-count=6`; **median** ns/op (ns has ~thermal jitter on a laptop; B/op and allocs/op are deterministic — trust those) |
| Workload | writes: 1000 pairs/commit; reads: 50 000 keys preloaded; range window: 500 rows |
| Gate | `TestEnginesRoundTrip` — all three engines return byte-identical data before any number is trusted |

`vs best` is the slowdown factor relative to the winner of that row (**1.0× = fastest**).

## Per-operation results (median of 6, optimized adapters)

### BatchWrite — 1000 pairs in one transaction (the `FlushBatch` hot path)

256-byte values (mixed records) — **sqlite now wins after optimization**:

| engine | ns/op | B/op | allocs/op | vs best |
|---|--:|--:|--:|--:|
| **sqlite** | **1.56 ms** | 203 KiB | 2 179 | **1.0×** |
| bbolt | 3.32 ms | 895 KiB | 6 688 | 2.1× |
| graviton | 24.72 ms | 6.13 MiB | 22 238 | 15.8× |

2 KB values (variable snapshots) — bbolt wins:

| engine | ns/op | B/op | allocs/op | vs best |
|---|--:|--:|--:|--:|
| **bbolt** | **7.80 ms** | 9.05 MiB | 8 161 | **1.0×** |
| graviton | 34.60 ms | 12.27 MiB | 23 080 | 4.4× |
| sqlite | 56.79 ms | 1.08 MiB | 21 539 | 7.3× |

### PointRead — single lookup by SCID

| engine | ns/op | B/op | allocs/op | vs best |
|---|--:|--:|--:|--:|
| **bbolt** | **625 ns** | 688 B | 11 | **1.0×** |
| graviton | 824 ns | 181 B | 1 | 1.3× |
| sqlite | 8.0 µs | 888 B | 24 | 12.8× |

### RangeScan — 500-row height window — graviton's structural collapse

| engine | ns/op | B/op | allocs/op | vs best |
|---|--:|--:|--:|--:|
| **bbolt** | **4.8 µs** | 608 B | 11 | **1.0×** |
| sqlite | 146.7 µs | 86.6 KiB | 2 022 | 30.6× |
| graviton | 3.64 ms | 678 KiB | 1 708 | **758×** |

### ScanAll — full iteration over 50 000 keys

| engine | ns/op | B/op | allocs/op | vs best |
|---|--:|--:|--:|--:|
| **bbolt** | **337 µs** | 576 B | 9 | **1.0×** |
| graviton | 2.63 ms | 481 KiB | 1 212 | 7.8× |
| sqlite | 11.34 ms | 8.39 MiB | 200 029 | 33.6× |

## Optimization round (optimize-loop)

Each engine adapter ran a separate optimize-loop: **gate** = `TestEnginesRoundTrip`
stays green (byte-identical results), **metric** = lexicographic
`(allocs/op, B/op, median ns/op)` of `BatchWrite/small_256B` — leading with the
*deterministic* allocs and using noisy ns only as a tie-breaker. Baselines below
are the in-loop iteration-0 measurements (same thermal state as the optimized
numbers — the fair before/after). Full trajectory: [`optimize-ledger.tsv`](optimize-ledger.tsv).

| engine | BatchWrite/small ns | allocs/op | result |
|---|--:|--:|---|
| **sqlite** | 11.14 ms → **1.56 ms** (7.1×) | 12 644 → **2 179** (−83%) | 4 keepers |
| bbolt | 3.14 ms → 3.32 ms (noise) | 6 733 → **6 687** (−0.7%) | 1 keeper |
| graviton | 24.96 ms → 24.72 ms (noise) | 22 243 → 22 238 (none) | 0 keepers |

**sqlite — the big win (4 kept changes):**
1. `INSERT OR REPLACE` → `INSERT … ON CONFLICT DO UPDATE` (upsert): avoids the
   delete+insert churn on every overwrite. (allocs −26%, ns −72%)
2. Multi-row `VALUES (?,?),(?,?),…` in chunks of 100: 1000 single-row `Exec`s
   become ~10, gutting the `database/sql` per-`Exec` allocation machinery.
   (allocs −73%)
3. Persistent prepared statement (prepared once at open, reused per batch via
   `tx.Stmt`) instead of re-parsing the SQL each batch.
4. `cache_size=-64MiB` + `temp_store=memory`: the default 2 MB page cache was
   spilling as the DB grew; keeping it resident cut ns a further 43% — and, as a
   free side effect, sped up sqlite's reads too (PointRead 1.34×, ScanAll 1.36×).

   *Discarded:* reusing the variadic args slice (no gain — `database/sql` copies
   internally); chunk size 500 (−4% allocs but +50% ns from giant-statement
   prepare cost).

**bbolt — marginal (1 kept change):** pre-map a 256 MiB region
(`Options{InitialMmapSize}`) so bulk writes never pause to grow+remap — a small
alloc trim (6733→6687). bbolt's per-node allocations are internal to the library,
so config tuning is the only lever and the headroom is small. *Discarded:* manual
Begin/Commit (closure already stack-allocated), `FillPercent=0.9` (random keys
split fuller pages → more allocs), `NoGrowSync` (within jitter).

**graviton — none (0 kept changes):** its ~22k allocs/op and 6.4 MB/op are
*entirely* inside the library (tree-node creation + key hashing); the adapter only
calls `Put`+`Commit`. The one candidate (`tree.Commit()` vs the variadic package
`graviton.Commit`) moved allocs by less than the ±30 run-to-run jitter, so it was
reverted. graviton is at its floor — an honest, expected result that reinforces
why it's `ErrGravitonUnsupported`.

## Bottom line

bbolt remains the right default backend for HyperGnomon's mixed read/write
workload. The optimize-loop showed that a *carefully tuned* pure-Go sqlite can
beat bbolt on small-batch write throughput (the FlushBatch hot path) — a useful
data point if sqlite is ever implemented as a real `Storage` backend — but it
still loses badly on the read paths and large-value writes that dominate the API
surface. graviton stays disqualified.

*Reproduce with the commands in [README.md](README.md); the optimization
trajectory (every keep/discard with metrics) is in [`optimize-ledger.tsv`](optimize-ledger.tsv).*
