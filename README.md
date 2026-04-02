# HyperGnomon

Arena-accelerated DERO blockchain indexer. Finds all TELA apps on the entire blockchain in under 5 seconds.

Built from scratch in Go 1.26, inspired by [Jody Bruchon's Jofito](https://codeberg.org/jbruchon/jofito) arena-based memory philosophy: **the fastest code is code that doesn't run.**

## Performance

| Scenario | HyperGnomon | Engram | PureWolf |
|---|---|---|---|
| **TELA discovery (cached)** | **14ms** | ~25s | ~120s |
| **TELA discovery (cold)** | **~5s** | ~60-91s | ~120s |
| **Full chain scan** | 143 blk/s | ~20-30 blk/s | ~20-30 blk/s |
| **Speedup** | — | **1,786x cached** | **8,571x cached** |

### TELA Discovery Breakdown

```
0.0s  Binary starts
0.01s 8 parallel RPC connections established
0.05s TELA cache loaded (14ms path) — OR —
2.5s  GnomonSC registry fetched (49,924 SCIDs)
3.5s  Batch code probe finds 645 TELA apps (92 INDEX + 553 DOC)
4.5s  Variable fetch for 92 INDEX apps
5.0s  TELA apps ready via REST + WebSocket API
```

## Quick Start

```bash
# Build
go build -o hypergnomon ./cmd/hypergnomon/

# Discover TELA apps (fastest mode)
./hypergnomon --fastsync --turbo --tela-only

# Full indexer with TELA discovery
./hypergnomon --fastsync --turbo

# Custom daemon
./hypergnomon --daemon-rpc-address=node.derofoundation.org:11012 --fastsync --turbo --tela-only
```

Double-click the binary — it auto-connects to local daemon, LAN nodes, or public nodes with interactive fallback.

## Flags

```
--daemon-rpc-address    Daemon RPC address (default: auto-detect with fallback)
--fastsync              Bootstrap from GnomonSC registry (skip historical scan)
--turbo                 Skip SC variable fetching during scan (fetch post-scan)
--tela-only             Discover TELA apps then exit (no chain scanning)
--num-parallel-blocks   Parallel block fetchers (default: 20)
--batch-size            Blocks per DB flush (default: 100, adaptive up to 2000)
--rpc-pool-size         WebSocket connection pool (default: 8)
--recent-blocks         Scan only last N blocks from tip (0 = scan all)
--search-filter         SC code filter, ;;; separated
--fastsync              Bootstrap from on-chain GnomonSC registry
--testnet               Use testnet GnomonSC SCID
--segment-sync          MapReduce parallel initial sync
--adapt-batch           Auto-tune batch size (default: true)
--mem-limit             GOMEMLIMIT in bytes (0 = auto)
--pprof-address         pprof endpoint (e.g. 127.0.0.1:6060)
--api-address           REST API listen address (default: 127.0.0.1:8082)
--ws-address            WebSocket server address (default: 127.0.0.1:9190)
--db-dir                Database directory (default: gnomondb)
--debug                 Enable debug logging
```

## API

### REST (default :8082)

```
GET /api/getinfo         Daemon info (cached)
GET /api/getstats        SC count, TX counts, TELA count, index height
GET /api/getscids        All indexed SCIDs
GET /api/indexedscs      All SCIDs with owners
GET /api/indexbyscid     Invocation details (?scid=X)
GET /api/scvarsbyheight  SC variables (?scid=X&height=N)
GET /api/tela            All TELA apps with metadata
GET /api/tela/count      TELA app count (lightweight polling)
GET /api/invalidscids    Failed SC deploys
```

### WebSocket JSON-RPC (default :9190/ws)

```
GetAllOwnersAndSCIDs                    → map[scid]owner
GetAllSCIDs                             → []scid
GetSCIDVariableDetailsAtTopoheight      → variables at height
GetSCIDInteractionHeight                → interaction heights
GetAllSCIDInvokeDetails                 → invocation details
```

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                    HyperGnomon                        │
├──────────┬──────────┬───────────┬────────────────────┤
│ FastSync │ TELA     │ 3-Stage   │ REST + WebSocket   │
│ GnomonSC │ Probe    │ Pipeline  │ API                │
│ Registry │ (batch)  │ fetch→    │                    │
│          │          │ process→  │ /api/tela          │
│          │          │ flush     │ /api/getstats      │
├──────────┴──────────┴───────────┴────────────────────┤
│              Arena-Inspired Memory Layer              │
│  sync.Pool │ Pre-alloc │ unique.Make │ Batch Flush   │
├──────────────────────────────────────────────────────┤
│  RPC Pool (8 parallel WS) │ BoltDB (msgpack, NoSync) │
└──────────────────────────────────────────────────────┘
```

### Key Optimizations

**Jofito Arena Patterns (memory)**
- `sync.Pool` for all hot-path structs — 96x faster than `new`
- Pre-allocated slices with `[:0]` reset per cycle
- `unique.Make` string interning for SCIDs and addresses
- Batch `WriteBatch` flush — 228x faster than individual writes

**Nuclear RPC (network)**
- `BatchGetBlocks` — 50 blocks per JSON-RPC batch call
- Mega-batch `GetTransaction` — all TXs from all blocks in 1 call
- 8 parallel WebSocket connections with 64KB buffers
- 2 round trips per 50 blocks instead of 150

**TELA Discovery (intelligence)**
- GnomonSC registry bootstrap — skip 6.8M block scan
- Batch code probe — 100 `GetSC(code=true)` per batch call
- Height-descending sort — find TELA apps in first 10% of SCIDs
- Early-exit — stop probing after 3000 dry SCIDs
- INDEX/DOC split — only fetch variables for ~92 INDEX apps, not all 645
- Jofito cache — save TELA list to disk, instant load on subsequent starts
- Cache height tracking — updates every block, always fresh

**Pipeline (throughput)**
- 3-stage prefetch pipeline: fetcher → processor → flusher
- Adaptive batch sizing: 100→2000 based on flush latency
- Turbo mode: skip GetSC during scan, defer to post-scan
- BoltDB NoSync + NoGrowSync during initial sync
- msgpack encoding (2-3x faster than JSON)

## Benchmarks

```bash
# Storage: batch vs individual writes
go test ./storage/ -bench=. -benchmem
# FlushBatch_100:  5.2ms    (1 transaction)
# Individual_100:  1,188ms  (400 transactions) → 228x faster

# Pool: arena reuse vs fresh allocation
go test ./pool/ -bench=. -benchmem
# WorkItem_Pool:   9.2ns  0 allocs
# WorkItem_New:    887ns  2 allocs → 96x faster
# Buffer256K_Pool: 8.4ns  0 B
# Buffer256K_New:  27µs   262KB → 3,240x faster
```

## How It Works

### TELA Discovery (--fastsync --turbo --tela-only)

1. **Check cache** — if `tela_cache.bin` exists and is <1000 blocks old, load it (14ms)
2. **Fetch GnomonSC registry** — 1 RPC call returns 49,924 SCIDs with owners + heights (2.5s)
3. **Sort by height descending** — newest SCIDs first (TELA apps cluster in recent deployments)
4. **Batch code probe** — JSON-RPC batch of 100 `GetSC(code=true)` per call across 8 connections
5. **Check for `STORE("telaVersion"` / `STORE("docVersion"`** in SC code
6. **Early exit** — stop after 3000 consecutive non-TELA SCIDs
7. **Fetch variables** — batch `GetSC(variables=true)` for only the ~92 INDEX apps
8. **Save cache** — write TELA SCID list to `tela_cache.bin` for instant next startup

### Full Chain Scan (--turbo)

1. **3-stage pipeline**: fetcher → processor → flusher (all concurrent)
2. **Fetcher**: `BatchGetBlocks(50 heights)` → 1 JSON-RPC batch per 50 blocks
3. **Processor**: decode TXs, classify SC installs/invokes, accumulate to WriteBatch
4. **Flusher**: atomic BoltDB commit every N blocks, adaptive batch sizing
5. **Turbo mode**: skip GetSC during scan, fetch variables in post-scan pass

## Project Structure

```
cmd/hypergnomon/main.go    CLI, flags, interactive fallback, GOMEMLIMIT
indexer/indexer.go          3-stage pipeline, batch RPC, turbo mode
indexer/fastsync.go         GnomonSC bootstrap, TELA probe, Jofito cache
indexer/classify.go         G45/NFA/TELA/NAMESERVICE classification
indexer/segment.go          MapReduce parallel initial sync
rpc/client.go               WS JSON-RPC, BatchGetBlocks, GetBlockByHeight
rpc/pool.go                 Parallel connection pool
rpc/rwc/rwc.go              WebSocket ReadWriteCloser adapter
storage/storage.go          Storage interface, WriteBatch (arena pattern)
storage/bbolt.go            BoltDB backend, msgpack, FlushBatch
pool/pools.go               sync.Pool for hot-path structs
pool/intern.go              unique.Make string interning
structures/types.go         Core types with Reset() for recycling
structures/globals.go       Constants, version, TELACount
api/http.go                 REST API (9 endpoints)
api/ws.go                   WebSocket JSON-RPC server
```

## References

- [Jofito](https://codeberg.org/jbruchon/jofito) — Jody Bruchon's arena-based file tool
- [jdupes](https://codeberg.org/jbruchon/jdupes) — 32x speedup via arena allocation
- [civilware/Gnomon](https://github.com/civilware/Gnomon) — Original DERO indexer
- [simple-gnomon](https://github.com/secretnamebasis/simple-gnomon) — Simplified indexer
- [Kelvin](https://arxiv.org/abs/2504.06151) — Zero-copy data pipelines
- [Xor Filters](https://arxiv.org/abs/1912.08258) — Faster than Bloom filters
- [simdjson](https://arxiv.org/abs/1902.08318) — Gigabytes of JSON per second

## License

MIT
