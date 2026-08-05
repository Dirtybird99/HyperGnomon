# Benchmarks

Every numeric claim in the README links back here. Each entry names the harness, the hardware, the daemon, and the date — so a reader can reproduce, or flag the number as stale.

## Reproducibility ground rules

1. **Daemon pinned.** Every live-daemon number names the endpoint and its chain height at measurement time. A remote daemon mid-sync will produce very different numbers than one at tip.
2. **Go version pinned.** HyperGnomon requires Go 1.26. Numbers below were taken with Go `1.26.x` — use `go version` to check yours.
3. **Warm vs cold.** bbolt page cache matters. Every bench declares whether it ran against a freshly-opened store or a pre-warmed one.
4. **Three runs, median reported.** Variance on live-daemon numbers can be ±10% from RPC jitter alone.

## Live-daemon wall-clock (mainnet)

**Environment:**

- Daemon: `203.0.113.10:10102` (LAN, mainnet, at tip)
- Host: Windows 11, Go 1.26, local SSD
- Date: April 2026
- Command: `./hypergnomon --fastsync --daemon-rpc-address=203.0.113.10:10102 --timing --timing-every=1`

| Stage | Baseline (pre-v0.8) | v0.9 | Δ | What changed |
|---|---|---|---|---|
| FastSync main | 5.2 s | 2.9 s | −44% | Hex-decode consolidation, typed encoding, keysstrings refresh |
| Phase-1 classify probe | 24.9 s | 11.6 s | −53% | Canonical classifier (G45 family), batch code probe 100/call, pool=8 |
| Phase-2 variable write | 10.7 s | 4.4 s | −59% | Typed ClassMeta encoding, WriteBatch arena reuse, atomic flush |

Baseline is the v0.7 release against the same daemon. Re-run against a remote daemon (e.g. `node.derofoundation.org:11012`) is expected to roughly double these numbers from round-trip latency alone — use LAN for comparability.

## DERO mainnet reorg frequency and depth (July 2026)

Measured with `cmd/reorgwatch` against `192.168.2.251:10102` (LAN, mainnet, at tip,
daemon `3.5.5-142.DEROHE.STARGATE`), July 20 2026, tip ≈ 7,357,000. This is the
number that gates truncate scan-cost work and validates M2.3 wiring assumptions.

| Measurement | Result |
|---|---|
| Protocol depth bound (`topoheight − stableheight`) | **8 blocks** (~2.5 min at 18–20 s/block) |
| Historical scan: 99,992 headers (**23.3 days** of chain) | **0** sideblocks, **0** multi-tip blocks, **0** height≠topoheight |
| Live watch (first 2.2 h): topo-order rewrites | **4 reorg events, all depth 2** (~1 per 100 blocks, ≈1.8/h) |

Reproduce: `./reorgwatch -daemon=<host:port> -scan 100000` (one JSON line, ~50 min at
~30 ms/header over WS) and `./reorgwatch -daemon=<host:port> -watch -out=reorgwatch.jsonl`
(JSONL events + hourly heartbeats; leave running for days).

Two findings worth stating plainly:

1. **Reorgs are shallow but NOT rare.** Every observed event rewrote a single topo
   1–2 below the tip (depth 2, well inside the STABLE_LIMIT=8 bound), but at ~1.8/hour
   — roughly one per 100 blocks — not the "rare" of earlier doc claims. M2.3's
   truncate+replay wiring will be a *routine hourly* path, not an exceptional one:
   correctness and idempotence matter more than its cost. The cost itself is settled —
   with depth ≤ 2 and truncate at ~30 ms even on a 32k-SC store (see the truncate cost
   model in `storage/truncate.go`), ~44 events/day ≈ 1.3 s/day total.
2. **The canonical DAG hides these events entirely.** The same 2.2 h window that
   produced 4 live rewrites shows **zero** sideblocks — and so does the whole 23-day
   history. Displaced tip blocks vanish rather than being absorbed as sideblocks, so
   the chain's own record is a *false negative* for reorg frequency. Only live
   observation (the `blockhashes`-bucket comparison hypergnomon already does, which
   `reorgwatch -watch` replicates) sees them.

The watcher stays running; its JSONL accumulates the long-run rate. Depth events
> 8 would be protocol-anomalous and worth investigation on sight.

## Microbenchmarks

Run from repo root:

```bash
go test ./storage/... -bench=. -benchmem -count=3
go test ./structures/... -bench=. -benchmem -count=3
go test ./api/... -bench=. -benchmem -count=3
go test ./pool/... -bench=. -benchmem -count=3
```

All numbers below were produced by the full-sweep harness in `BENCHMARK_RESULTS.md`:

```
go test ./... -bench=. -benchmem -benchtime=500ms -count=3 -run=^$
```

Hardware at time of measurement: Windows 11, Intel Core i7-13700HX (24 logical cores), Go 1.26. Values shown are medians across the three `-count=3` samples.

### Typed encoding vs msgpack

Re-measured June 2026 after the benchmark-integrity fix†: medians of `-count=3`.

| Bench | Msgpack (ns/op, allocs) | Typed (ns/op, allocs) | Factor |
|---|---|---|---|
| `ClassMeta_Marshal` | 641 / 11 allocs | 133 / 2 allocs | **4.8×** |
| `ClassMeta_MarshalTypedAppend` | 641 / 11 allocs | 24.2 / **0 allocs** | **27×** |
| `ClassMeta_Unmarshal` | 572 / 9 allocs | 125 / 7 allocs | 4.6× |
| `SCIDVariables_Marshal` | 1,043 / 6 allocs | 51.5 / **0 allocs** | **20×** |
| `SCIDVariables_Unmarshal` | 2,103 / 36 allocs | 488 / 25 allocs | 4.3× |
| `InstallRecord_Marshal` | 281 / 5 allocs | 37.0 / 1 alloc | **7.6×** |
| `InstallRecord_MarshalTypedAppend` | 281 / 5 allocs | 6.1 / **0 allocs** | **46×** |
| `InstallRecord_Unmarshal` | 284 / 4 allocs | 48.1 / 2 allocs | 5.9× |
| `SCTXParse_Turbo_Marshal` | 667 / 5 allocs | 108 / 1 alloc | **6.2×** |
| `SCTXParse_Turbo_MarshalTypedAppend` | 667 / 5 allocs | 12.9 / **0 allocs** | **52×** |
| `SCTXParse_Turbo_Unmarshal` | 610 / 6 allocs | 112 / 4 allocs | 5.5× |
| `AddrSCIDEntry_Marshal` | 156 / 2 allocs | 14.8 / 1 alloc | **11×** |
| `AddrSCIDEntry_Marshal_TypedAppend` | 156 / 2 allocs | 1.50 / **0 allocs** | **104×** |
| `AddrSCIDEntry_Unmarshal` | 222 / 2 allocs | 1.47 / **0 allocs** | **151×** |

† Earlier revisions of this table reported sub-nanosecond `AddrSCIDEntry` typed times (0.46–1.08 ns) with multipliers up to ~800×. Those benchmarks discarded their results, so the fully-inlinable fixed-size codecs were dead-code-eliminated — the loop measured nothing. The benchmarks now use `b.Loop()` plus package-level sinks; the ~1.5 ns append/unmarshal rows are genuine (a tag check plus three big-endian word moves against a cache-hot buffer), and the multipliers above are the defensible ones.

Drives the Phase-2 writes improvement: every classified SC writes one `ClassMeta` + N `AddrSCIDEntry` rows per scan cycle. Moving these off msgpack was worth ~59% of Phase-2 wall-clock.

### TELA content cache invalidation

```
BenchmarkTELAContentCache_InvalidatePrefix/fill=64     1.00 µs/op
BenchmarkTELAContentCache_InvalidatePrefix/fill=1024   1.06 µs/op
BenchmarkTELAContentCache_InvalidatePrefix/fill=8192   1.11 µs/op
```

**Flat across total cache fill** — O(|entries for this scid|), not O(fill). The byScid secondary index is the load-bearing structure; a regression in its bookkeeping would show up as fill-dependent growth here. Every event-bus `EventInstall` or `EventVarChange` hits this path; at mainnet cache fill the old linear scan was quadratic.

### TELA content server hot paths (v1.0 canonical-spec work)

| Bench | Median | Allocs | Throughput |
|---|---|---|---|
| `DecompressTELAGzip` (base64→gunzip, 5 KiB body) | 32 µs | 17 | 2.5 MB/s |
| `ExtractDOCBodyFromSource` (3.5 KiB DOC) | 8.9 µs | 1 | 380 MB/s |
| `ExtractDocShardBodyFromSource` (4.8 KiB shard) | 2.3 µs | 1 | 1.8 GB/s |
| `DecodeHexIfPrintableASCII_Printable` (decodes) | 97 ns | 1 | — |
| `DecodeHexIfPrintableASCII_Passthrough` (rejects) | 175 ns | 0 | — |
| `ReadTELASigFields_Signed` | 350 ns | 1 | — |
| `ReadTELASigFields_Unsigned` | 55 ns | 0 | — |

DocShard is strict-framing (no `TrimSpace`), which is why it's ~4× faster than the canonical DOC extractor. `DecompressTELAGzip` is dominated by `compress/gzip`'s inflate — not a target for further optimization without moving off stdlib.

### SCCode cache + lazy backfill

```
BenchmarkGetSCCode_CacheHit/size=256       1.3 µs/op
BenchmarkGetSCCode_CacheHit/size=2048      2.1 µs/op
BenchmarkGetSCCode_CacheHit/size=16384     8.1 µs/op   (dominated by memory copy)
BenchmarkGetSCCode_CacheMiss               124 µs/op   (no simulated latency)
BenchmarkGetSCCode_CacheMiss_WithLatency   1,600 µs/op (+1 ms daemon-probe sim)
```

The single-flight guard means concurrent misses collapse to one RPC — `BenchmarkGetSCCode_ConcurrentMiss_SingleFlight` in the same file confirms N simultaneous callers pay exactly one `CacheMiss_WithLatency` cost, not N.

### Pool + pool primitives

```
BenchmarkSCTXParse_Pool               9.6 ns/op    0 allocs     (serial)
BenchmarkSCTXParse_Pool_Parallel      2.4 ns/op    0 allocs     (GOMAXPROCS=24)
BenchmarkSCTXParse_New                4.2 ns/op    0 allocs
BenchmarkWorkItem_Pool                18 ns/op     0 allocs
BenchmarkWorkItem_Pool_Parallel       2.5 ns/op    0 allocs
BenchmarkWorkItem_New                 2,300 ns/op  5.7 KB / 2 allocs
BenchmarkPool_GetPut                  30 ns/op     0 allocs     (rpc pool, serial)
BenchmarkPool_GetPut_Parallel         45–290 ns    0 allocs     (contested; 24 goroutines on 8-slot pool)
BenchmarkFacadeFieldRefresh (pkg/gnomes)   7 ns/op    0 allocs
BenchmarkInternSCID                   284 ns/op    0 allocs     (with unique.Make intern)
BenchmarkInternSCID_NoIntern          290 ns/op    320 B / 5 allocs
```

`SCTXParse` is cheap enough that the pool overhead is a net loss serially — but under parallel contention the pool's per-op cost amortizes to 2.4 ns. The `New` path matters because it's what we'd pay without the pool; at 24-core fanout across the scan pipeline, paying 2.3 µs per `WorkItem` instead of 2.5 ns would dominate the hot loop.

### bbolt batch vs individual writes

Re-measured June 2026 (medians of `-count=3`) after the composite-key history
layout, `NoFreelistSync`, exact-size variable marshal, and invocation
bucket-handle memoization landed:

```
BenchmarkFlushBatch_100      0.61 ms/op  (100 records per atomic flush = 6.1 µs/record)
BenchmarkFlushBatch_1000     8.0 ms/op   (1000 records per flush = 8.0 µs/record)
BenchmarkFlushBatch_10000    99 ms/op    (10000 records per flush = 9.9 µs/record)
BenchmarkIndividualWrites    8.8 ms/op   (100 records one-at-a-time = 88 µs/record)
```

**14× faster** at n=100 (individual 88 µs/rec vs batched 6.1 µs/rec); **11×
faster** at n=1000. Per-record cost now rises only gently with batch size
(6.1 → 9.9 µs across 100 → 10,000) since `NoFreelistSync` removed the
freelist-serialization cost that used to dominate mega-transactions. 100–1000
remains the sweet spot, which is why `--batch-size` defaults to 100 with
adaptive scaling up to 1000.

### Append-only history layout (O(delta) flushes)

Interaction heights and per-address normal-TX history used to live as one
msgpack blob per SCID/address: every flush decoded the full history, appended
the delta, re-encoded, and rewrote — quadratic over the life of an active
contract during initial sync. They now use composite keys
(`<scid>:<BE8:h>` → uvarint count, `<addr>:<BE8:h>:<txid>` → one record) so a
flush costs O(batch delta) regardless of accumulated history. Legacy blobs
remain readable; readers merge both layouts.

`BenchmarkFlushBatch_HeightsAccumulation` / `_NormalTxAccumulation`
(storage/interaction_history_test.go) pre-seed one key with N records, then
time a constant-size flush against it:

| Pre-seeded history | Heights: blob layout | Heights: composite | NormalTx: blob | NormalTx: composite |
|---|---|---|---|---|
| 1,000 | 173 µs | 116 µs | 1,333 µs | 44 µs |
| 10,000 | 612 µs | 115 µs | 6,709 µs | 44 µs |
| 100,000 | 5,684 µs | 118 µs | — | — |

Flat vs linear: at 100k accumulated heights the composite layout flushes
**48× faster**, and the gap keeps widening with chain history.

### Event bus

```
BenchmarkFilter_Match_Speculative/speculative_event_opted_out     2.5 ns/op
BenchmarkFilter_Match_Speculative/speculative_event_opted_in      7.1 ns/op
BenchmarkFilter_Match_Speculative/finalized_event_default_filter  8.2 ns/op
BenchmarkFilter_Match_Speculative/finalized_event_with_filters    26.6 ns/op
BenchmarkBus_PublishFanOut_Speculative                            21.2 ns/op
```

The opted-out speculative path is deliberately short-circuited — subscribers who don't want speculative events pay 2.5 ns per Publish to be ignored.

### Classifier rule walk

```
BenchmarkClassifySC_TELAIndex   3.0 µs/op    3 allocs / 64 B   (middle-of-table hit)
BenchmarkClassifySC_TELADoc     5.3 µs/op    3 allocs / 64 B
BenchmarkClassifySC_G45NFA      400 ns/op    3 allocs / 64 B   (first-rule hit)
BenchmarkClassifySC_Miss        18.5 µs/op   3 allocs / 64 B   (full-table walk + fallback)
```

(Re-measured July 2026 after the shared per-class Tags slices change — the
former 4-5 allocs included a per-call Tags `make`+`append` that classification
now avoids entirely.)

`ClassifySC` runs once per new SC install; a 14 µs worst-case is invisible in scan wall-clock. This bench exists as a regression guard — if the rule table ever grows to 50+ rules or a rule's pattern becomes expensive, the Miss bench is first to show it.

### G45 corpus classify (July 2026)

A real-mainnet corpus is committed under `indexer/testdata/`, captured raw from
`GetSC` at topoheight 7,389,814 — 45,539 NFT-class + 112 G45-C snapshots, see
`indexer/testdata/corpus_manifest.json`. Every benchmark iteration classifies
the ENTIRE corpus, so allocs/op is the exact allocation total of one full pass —
deterministic, immune to per-op rounding.

```
BenchmarkClassifyCorpus/Full   naive hex decode:  91,759 allocs/op   24.7 MB/op   ~127 ms
                               buffer handover:   46,123 allocs/op   12.4 MB/op   ~121 ms
```

**These are not comparable to the figures published before 2026-07-27**
(1,970,788 → 415 allocs). Those were measured against a corpus whose `metadata`
values had been hex-DECODED by some step outside the capture. derod hex-encodes
every DVM `STORE` string, so that fixture let the scanner return zero-copy
substrings of text the daemon never sends in that form — and the 415 figure was
never reachable in production. Worse, it hid a real bug: on a live chain the
extractors were handed hex, parsed nothing, and left `Name`/`Desc`/`IconURL`
empty for every G45 asset while every gate passed. See `cmd/corpusdump`.

46,123 over 45,651 SCs is ~1.01 allocations per SC, and that is the floor. Hex
input cannot be aliased: each extracted string is a different encoding from the
bytes it came from, so it must be materialized exactly once. Getting there took
handing the decode buffer over via `unsafe.String` instead of copying it a
second time (`ownedBytesToString`), which halved both allocations and bytes.

Reproduce: `bash scripts/measure_classify.sh` — one JSON line with the
median-of-5 metric plus the gates: full `indexer` `-race` suite, a golden
snapshot pinning the full `SCClass` output for all 45,651 SCs byte-identically
(regeneration is a deliberate human act, never part of an optimization), a
map/slice path equivalence gate, and an allocs-determinism tripwire.

Corpus regeneration is likewise a deliberate operator act: `cmd/corpusdump`
needs a synced DB and a live daemon, so it cannot run in CI.
`TestCorpusHoldsRawDaemonShape` is the tripwire that keeps the fixture honest
between regenerations.

The scanner tier still carries the classify path: zero-copy substrings within a
decoded blob, a tri-state verdict that skips decodes proven to set nothing, and
shared precomputed per-class `Tags` slices, all fenced by a differential fuzz
gate against the stdlib decode. Unusual shapes (escaped values, case-fold keys)
still fall back to the ORIGINAL `map[string]interface{}` decode, kept for exact
behavior parity with pre-optimization Gnomon (exact-case key matching,
whole-blob strictness).

### TruncateToHeight reorg rollback (July 2026)

`TruncateToHeight` self-discovers affected SCIDs by scanning entity-major
buckets, so its cost scales with **total DB size, not reorg depth** — a
deliberate tradeoff pinned by these numbers (commit `7910a26` cut the avoidable
scans and rejected a height→scids index as a tax on the hot flush path; the
O(reorg-depth) fix is M2.3's orchestrator passing the touched-SCID set in).
The bench sweeps each cost axis with the others pinned; allocs/op leads (the
discovery scans allocate ~1 string per key parsed, so allocs track keys-touched
deterministically), ns/op is advisory.

```
axis            config (scids/addrs/depth/S)      ns/op (median)   allocs/op
DB size         2000/512/10/10                        1.68 ms         13,516
                8000/512/10/10                        6.4  ms         31,570
                32000/512/10/10                      29.9  ms        104,234
addr count      8000/1/10/10                         10.0  ms         25,278
                8000/8192/10/10                      20.9  ms        121,453
affected SCs    8000/512/100/S=1                      7.5  ms         31,043
                8000/512/100/S=1000                  37.2  ms        123,031
reorg depth     8000/512/depth=1/S=1                  7.3  ms         30,628
                8000/512/depth=1000/S=1               8.1  ms         34,737
```

allocs/op fits `~3.0·scids + ~12·addrs + ~92·affectedSCs + ~4·depth` (the
DB-size fit predicts 103.8k at 32k SCs; measured 104.2k). Read: a 1000× deeper
reorg costs +13%; DB size, address cardinality, and affected-SC count set the
bill. Even the 32k-SC fixture truncates in ~30 ms — orders of magnitude cheaper
than the resync alternative — which is why the residual O(N) is accepted until
M2.3 wiring lands.

Reproduce (the bounded `-benchtime` matters — setup rebuilds the whole DB per
iteration in the untimed region, so the default 1s target would rebuild the
32k fixture for minutes):

```bash
go test ./storage/ -run=^$ -bench=BenchmarkTruncateToHeight -benchmem -benchtime=3x -count=6
```

## TELA correctness

Not a perf bench — a correctness gate. The content server's output must be byte-identical (SHA256) to civilware/tela's `parseDocCode` on every live mainnet TELA contract we can reach.

**Live fixture**: TELA-INDEX SCID `813b020791998dbefafb72e89c812f8cb0b9c04efbe11963a1ca140e2da72eb9`, route `algo4.html` → TELA-DOC SCID `f5f2773902f2ba974a3a87243e87bbc2d313b9466bf9b29570f8c1b9695b2fce`. Body: 3693 bytes.

```bash
# Canonical body hash (civilware/tela parseDocCode equivalent):
curl -s http://203.0.113.10:10102/json_rpc \
    -d '{"jsonrpc":"2.0","id":1,"method":"DERO.GetSC","params":{"scid":"f5f2773902f2ba974a3a87243e87bbc2d313b9466bf9b29570f8c1b9695b2fce","code":true}}' \
  | python -c 'import json,sys,hashlib; c=json.load(sys.stdin)["result"]["code"]; s=c.index("/*"); e=c.rindex("*/"); b=c[s+2:e].strip(); print(hashlib.sha256(b.encode()).hexdigest())'

# HyperGnomon body hash:
curl -s "http://127.0.0.1:8082/tela/813b020791998dbefafb72e89c812f8cb0b9c04efbe11963a1ca140e2da72eb9/algo4.html" \
  | sha256sum | awk '{print $1}'

# The two must match.
```

Synthetic cases (`api/tela_content_test.go`):

- `.gz` asset: base64-gzip-encoded body decompresses to the raw source.
- DocShard (`.shard` / `.shards` suffix): `code[start+3:]` + `TrimSuffix("\n*/")` — no `TrimSpace`, matching `civilware/tela/tela.go:parseDocShardCode`.
- Hex-layer unwrap: daemon-returned STORE strings that are printable-ASCII hex are decoded before class-bucket lookup.

## Head-to-head vs civilware/Gnomon

Shipped in v1.0 as `cmd/benchvs` — a single-target harness the operator runs **sequentially**, once per indexer, against the same daemon. Both runs append to the same markdown file (`--out`, default `bench_vs_civilware.md`), so the comparison accumulates in one document. It deliberately does not run the indexers concurrently: concurrent runs would starve whichever started second via RPC contention, so sequential runs at full daemon bandwidth are the honest comparison. Full usage, including the civilware wrapper recipe, in [`cmd/benchvs/README.md`](../cmd/benchvs/README.md).

```bash
# HyperGnomon side
go run ./cmd/benchvs \
    --name=HyperGnomon \
    --binary=./hypergnomon \
    --daemon=203.0.113.10:10102 \
    --db-dir=/tmp/hg-bench \
    --probe-duration=60s \
    --probe-workers=32 \
    --out=bench_vs_civilware.md

# civilware/Gnomon side: civilware is primarily a Go library, so the
# operator supplies a small wrapper binary that embeds it and exposes
# /api/getinfo + /api/getstats (recipe in cmd/benchvs/README.md), then
# points benchvs at it via --binary / --api-url / --probe-paths.
```

Measured per run:

- **Time-to-tip** — process start until the index height is within `STABLE_LIMIT=8` of daemon tip (polled via `/api/getinfo` + `/api/getstats`)
- **Time-to-ready** — optional, when `--ready-log-pattern` markers are set (e.g. `Classify probe complete`); waited for in the child log after tip
- **DB size** — `--db-dir` is wiped before launch and measured at teardown
- **REST latency p50 / p95 / p99** (and max) per probe path, under `--probe-workers` (default 32) concurrent clients for `--probe-duration` (default 60s)

RSS at steady state is explicitly **not** measured in v1.0: cross-platform child-process memory sampling would need a platform-specific shim or child-side cooperation — see "What benchvs does not measure" in [`cmd/benchvs/README.md`](../cmd/benchvs/README.md).

Output appended to `bench_vs_civilware.md`, pulled into [README §9](../README.md#9-benchmarks). Regenerated per release tag.

## Release A/B matrix

Use `cmd/benchmatrix` when comparing HyperGnomon release tags, `origin/main`,
and the current workspace branch. It is a sequential orchestrator around
`cmd/benchvs`: each trial gets a fresh DB directory, unique API/WS ports, and
the same daemon bandwidth. It never runs targets concurrently.

Default release check:

```bash
go run ./cmd/benchmatrix \
    --daemon=203.0.113.10:10102 \
    --trials=5 \
    --probe-duration=60s
```

LAN tuning sweep:

```bash
go run ./cmd/benchmatrix \
    --daemon=203.0.113.10:10102 \
    --trials=5 \
    --targets="main=origin/main,workspace-pool8=workspace,workspace-pool12=workspace|--rpc-pool-size=12,workspace-pool16=workspace|--rpc-pool-size=16"
```

Defaults:

- Targets: `v1.0.0`, `origin/main`, `workspace`.
- Readiness marker: `Classify probe complete`.
- Target args: `--fastsync --timing --timing-every=10`.
- Output root: timestamped `hypergnomon-benchmatrix` directory under the system temp dir.

Outputs:

- `benchmatrix.jsonl` — one machine-readable row per `benchvs` run.
- `benchmatrix.md` — median/p95 comparison table with deltas versus `origin/main`.
- `benchmatrix-runs.md` — the raw per-run `benchvs` markdown sections.
- Per-run child logs next to the DB directories.

Dry-run command:

```bash
go run ./cmd/benchmatrix --dry-run --trials=1 --probe-duration=5s
```

Interpretation rule: deltas under 5%, or deltas where matched trials do not all
move in the same direction, are labeled as noise. The `workspace` target is
intentionally built from the active checkout, including uncommitted changes,
so it is the right target for validating pending optimization branches.

Target entries support `label=ref|extra args`; the extra args are appended to
the launched indexer only for that target. Use this for pool-size and
classify-batch sweeps without adding wrapper scripts.

### Classify seed cache

Current builds also maintain a cross-DB classify seed cache under the OS user
cache directory, or `--classify-seed-cache-dir` when set. The cache is keyed by
network, GnomonSC SCID, schema version, registry hash, and height. On clean DB
runs, a matching seed writes class metadata, TELA INDEX/DOC variable snapshots,
and persisted TELA install code into the fresh DB before probing only SCIDs
newer than the cached height.

This is a readiness optimization, not a replacement for live verification: a
missing, stale, corrupt, or registry-mismatched seed falls back to the full
classify probe.

`benchmatrix` isolates this cache automatically: for any target binary whose
`--help` advertises `--classify-seed-cache-dir`, each trial gets a fresh seed
directory under the run root, so every trial measures the cold full-probe path
and a seed written by an earlier trial (or a prior operator run in the OS user
cache) cannot skew the comparison. Targets predating the flag launch without
it. To measure the warm seed-hit path on purpose, pin a shared directory via
target extra args: `workspace-seeded=workspace|--classify-seed-cache-dir=C:\seedpin`.

## Retired claims

The earlier README carried speedup multipliers inherited without methodology. The full-sweep harness reproduced what it could; remaining items are retired with explicit reasoning.

| Claim | Status | Detail |
|---|---|---|
| "1,786× faster than Engram cached TELA" | **retired** | No methodology, no harness, no hardware, no date. No path to reproduction; Engram is a different tool class. |
| "8,571× faster than PureWolf cached TELA" | **retired** | Same. |
| "228× faster than individual writes" (batch flush) | **measured: 14× at n=100, 11× at n=1000** (June 2026, post composite-key + NoFreelistSync layout) | See `BenchmarkFlushBatch_100` vs `BenchmarkIndividualWrites` above. The original 228× likely compared batched WriteBatch against pre-v0.7 one-Put-per-txn code that no longer exists. Current multiplier is honest and reproducible. |
| "96× faster than `new` (WorkItem pool)" | **measured: 127× (exceeds original claim)** | `BenchmarkWorkItem_Pool` = 18 ns/op; `BenchmarkWorkItem_New` = 2,300 ns/op. The original 96× was conservative; the current number is higher. |
| "3,240× faster (Buffer256K pool)" | **retired** | No `BenchmarkBuffer256K_*` harness exists in the current tree (`grep Buffer256K` returns nothing). Either the bench was removed in a refactor and the claim lived on, or the number was invented. No path to reproduction. |

The reproduced numbers flow into the microbench tables above; no "pending reproduction" rows remain.
