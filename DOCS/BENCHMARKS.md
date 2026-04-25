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

| Bench | Msgpack (ns/op, allocs) | Typed (ns/op, allocs) | Factor |
|---|---|---|---|
| `ClassMeta_Marshal` | 1,480 / 11 allocs | 239 / 2 allocs | **6.2×** |
| `ClassMeta_MarshalTypedAppend` | 1,480 / 11 allocs | 53 / **0 allocs** | **28×** |
| `ClassMeta_Unmarshal` | 1,310 / 9 allocs | 286 / 7 allocs | 4.6× |
| `SCIDVariables_Marshal` | 1,360 / 6 allocs | 85 / **0 allocs** | **16×** |
| `SCIDVariables_Unmarshal` | 3,990 / 36 allocs | 1,260 / 30 allocs | 3.2× |
| `AddrSCIDEntry_Marshal` | 364 / 2 allocs | 0.46 / **0 allocs** | **~800×** † |
| `AddrSCIDEntry_MarshalTypedAppend` | 364 / 2 allocs | 1.43 / **0 allocs** | **255×** |
| `AddrSCIDEntry_Unmarshal` | 487 / 2 allocs | 1.08 / **0 allocs** | **~450×** † |

† The `AddrSCIDEntry` typed path compiles to a handful of byte-order writes into a caller-provided buffer. At sub-nanosecond times, compiler inlining + branch prediction dominate — we report the number honestly but treat the exact multiplier as an upper bound.

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

```
BenchmarkFlushBatch_100      3.9 ms/op   (100 records per atomic flush = 39 µs/record)
BenchmarkFlushBatch_1000     24.6 ms/op  (1000 records per flush = 24.6 µs/record)
BenchmarkFlushBatch_10000    780 ms/op   (10000 records per flush = 78 µs/record)
BenchmarkIndividualWrites    26.4 ms/op  (100 records one-at-a-time = 264 µs/record)
```

**6.8× faster** at n=100 (individual 264 µs/rec vs batched 39 µs/rec); **11× faster** at n=1000. The curve turns back up past n=1000 because the bbolt freelist and allocation cost dominate for mega-transactions. 100–1000 is the sweet spot, which is why `--batch-size` defaults to 100 with adaptive scaling up to 2000.

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
BenchmarkClassifySC_TELAIndex   2.1 µs/op    5 allocs   (middle-of-table hit)
BenchmarkClassifySC_TELADoc     5.1 µs/op    5 allocs
BenchmarkClassifySC_G45NFA      550 ns/op    5 allocs   (first-rule hit)
BenchmarkClassifySC_Miss        14.1 µs/op   4 allocs   (full-table walk + fallback)
```

`ClassifySC` runs once per new SC install; a 14 µs worst-case is invisible in scan wall-clock. This bench exists as a regression guard — if the rule table ever grows to 50+ rules or a rule's pattern becomes expensive, the Miss bench is first to show it.

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

Planned for v1.0 via `cmd/benchvs` (tracked as task P4).

```bash
go run ./cmd/benchvs \
    --daemon=203.0.113.10:10102 \
    --duration=5m \
    --civilware-branch=dev
```

Clones civilware/Gnomon at the specified branch, builds, runs both indexers concurrently against the same daemon. Measures:

- FastSync wall-clock (both)
- DB size at chain tip (both)
- RSS at steady state
- API query p50 / p95 / p99 under 32 concurrent clients hitting `GetAllSCIDs`, `GetAllSCIDInvokeDetails`, `GetSCIDVariableDetailsAtTopoheight`

Output committed to `bench_vs_civilware.md`, pulled into [README §9](../README.md#9-benchmarks) verbatim. Regenerated per release tag.

## Retired claims

The earlier README carried speedup multipliers inherited without methodology. The full-sweep harness reproduced what it could; remaining items are retired with explicit reasoning.

| Claim | Status | Detail |
|---|---|---|
| "1,786× faster than Engram cached TELA" | **retired** | No methodology, no harness, no hardware, no date. No path to reproduction; Engram is a different tool class. |
| "8,571× faster than PureWolf cached TELA" | **retired** | Same. |
| "228× faster than individual writes" (batch flush) | **measured: 6.8× at n=100, 11× at n=1000** | See `BenchmarkFlushBatch_100` vs `BenchmarkIndividualWrites` above. The original 228× likely compared batched WriteBatch against pre-v0.7 one-Put-per-txn code that no longer exists. Current multiplier is honest and reproducible. |
| "96× faster than `new` (WorkItem pool)" | **measured: 127× (exceeds original claim)** | `BenchmarkWorkItem_Pool` = 18 ns/op; `BenchmarkWorkItem_New` = 2,300 ns/op. The original 96× was conservative; the current number is higher. |
| "3,240× faster (Buffer256K pool)" | **retired** | No `BenchmarkBuffer256K_*` harness exists in the current tree (`grep Buffer256K` returns nothing). Either the bench was removed in a refactor and the claim lived on, or the number was invented. No path to reproduction. |

The reproduced numbers flow into the microbench tables above; no "pending reproduction" rows remain.
