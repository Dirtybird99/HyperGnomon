# HyperGnomon

Arena-accelerated DERO blockchain indexer. Byte-correct TELA content server, WebSocket push API, civilware/Gnomon-shape Go library for embedding.

Built for **TELA dApp operators**, **HOLOGRAM** backend, and the **dReams / Engram / TELA-CLI** Go-library embedders who import `civilware/Gnomon` today. Drop-in source compatibility via `pkg/gnomes` — migration is a three-sed search-and-replace plus one constructor swap.

Status: v1.1.0. All numeric claims below are reproducible — see [DOCS/BENCHMARKS.md](DOCS/BENCHMARKS.md) for methodology, hardware, and dates.

## Table of contents

1. [Who it's for](#1-who-its-for)
2. [Install](#2-install)
3. [Quickstart](#3-quickstart)
4. [Comparison — HyperGnomon vs civilware/Gnomon vs simple-gnomon](#4-comparison)
5. [Flags](#5-flags)
6. [HTTP API](#6-http-api)
7. [WebSocket API](#7-websocket-api)
8. [Library mode (`pkg/gnomes`)](#8-library-mode)
9. [Benchmarks](#9-benchmarks)
10. [Known limitations](#10-known-limitations)

## 1. Who it's for

- **TELA dApp operators** serving `.html` / `.js` / `.css` / `.gz` / DocShard assets from on-chain SC code. HyperGnomon's content server is canonical-spec compliant (base64→gunzip for `.gz`, DocShard strict parsing, optional `fileCheckC/S` signature reporting).
- **HOLOGRAM** and other Go-library embedders (dReams, dPrediction, Duels, Dero-Baccarat, Engram, TELA-CLI) who import `github.com/civilware/Gnomon` today and want a drop-in replacement. See §8.
- **Operators** who want a typed WebSocket push API, byte-identical TELA serving against mainnet contracts, and honest benchmarks.

Non-goals: not a chain node, not a SQL engine, not a long-term code archive.

## 2. Install

```bash
# go install (recommended for Go users)
go install github.com/hypergnomon/hypergnomon/cmd/hypergnomon@latest

# Build from source
git clone https://github.com/hypergnomon/hypergnomon && cd hypergnomon
go build -o hypergnomon ./cmd/hypergnomon/

# Docker
docker build -t hypergnomon:local .
docker run --rm -p 8082:8082 -p 9190:9190 -v hg-data:/data \
    hypergnomon:local \
    --daemon-rpc-address=host.docker.internal:10102 \
    --api-address=0.0.0.0:8082 --ws-address=0.0.0.0:9190 \
    --db-dir=/data/gnomondb --fastsync

# Pre-built binaries (Linux/macOS/Windows, amd64/arm64)
# GoReleaser produces archives on every tag push.
# Grab one from https://github.com/hypergnomon/hypergnomon/releases
# and extract:
#   tar -xzf hypergnomon_<ver>_<os>_<arch>.tar.gz
#   ./hypergnomon --help
```

## 3. Quickstart

```bash
# TELA-only: discover all TELA apps on mainnet, exit when ready
./hypergnomon --fastsync --tela-only

# Full indexer + API servers
./hypergnomon --fastsync --daemon-rpc-address=127.0.0.1:10102

# Remote daemon
./hypergnomon --fastsync --daemon-rpc-address=node.derofoundation.org:11012
```

Once running, REST API listens on `127.0.0.1:8082` and WebSocket on `127.0.0.1:9190/ws`.

Double-clicking the binary auto-detects local / LAN / public daemons with interactive fallback.

## 4. Comparison

| | **HyperGnomon** | civilware/Gnomon | simple-gnomon |
|---|---|---|---|
| Backends | bbolt (selectable; sqlite deferred) | bbolt, graviton | bbolt _(secretnamebasis)_ / sqlite _(siteraiser fork)_ |
| TELA content server | yes, canonical-spec | no | no |
| TELA `.gz` (base64+gunzip) | yes | n/a | n/a |
| TELA DocShard `.shard`/`.shards` | yes | n/a | n/a |
| `fileCheckC/S` header reporting | yes (v1.0) | no | no |
| Cryptographic signature verification | v1.2 (stub through v1.1) | no | no |
| Typed binary encoding hot-path | yes | msgpack everywhere | msgpack |
| WebSocket push (subscribe/event) | yes | partial | no |
| Reorg detection (STABLE_LIMIT=8 DAG) | yes, `SafeHeight` exposed | no | no |
| Reorg truncate-replay | v1.x (deferred — see §10) | no (manual `pop`) | no |
| Go-library drop-in (`pkg/gnomes`) | yes, three-sed migration | n/a (is the original) | partial |
| Unit + integration tests | yes | partial | partial |
| Published benchmark methodology | yes ([DOCS/BENCHMARKS.md](DOCS/BENCHMARKS.md)) | no | no |
| Live-daemon head-to-head harness | `cmd/benchvs` (v1.0) | no | no |

The comparison is scoped to indexer concerns. civilware/Gnomon remains the canonical reference for the DERO indexer surface — HyperGnomon's goal is not to replace it but to be the best fit for consumers who need a byte-correct TELA content layer, a typed push API, and an embeddable Go library with the same import shape.

## 5. Flags

The flags below are the ones worth knowing. For the full list with rationale for each default, see [DOCS/FLAGS.md](DOCS/FLAGS.md).

| Flag | Default | Purpose |
|---|---|---|
| `--daemon-rpc-address` | `127.0.0.1:10102` | DERO daemon RPC endpoint |
| `--fastsync` | `false` | Bootstrap from GnomonSC on-chain registry instead of full scan |
| `--turbo` | `true` | Skip per-SC variable fetch during scan |
| `--postscan-vars` | `lazy` | Turbo follow-up variable policy: `lazy` skips all-SCID sweep, `all` keeps it |
| `--classify-seed-cache-dir` | `""` | Optional cross-DB classify seed cache location (empty = OS user cache) |
| `--tela-only` | `false` | Discover TELA apps, print, and exit |
| `--num-parallel-blocks` | `8` | Parallel block fetchers. Higher risks daemon rate-limit (tested ceiling per TELA-CLI guide: ~16) |
| `--rpc-pool-size` | `8` | WebSocket connection pool size |
| `--batch-size` | `100` | Blocks per atomic bbolt flush. Adaptive up to 1000 when `--adapt-batch` is on |
| `--persist-install-code` | `tela` | sccode bucket policy: `none`, `tela` (TELA-INDEX/DOC/MOD only), or `all` |
| `--tela-verify-sigs` | `false` | Report `X-TELA-Verify` header on `/tela/…` responses (v1.0/v1.1: presence only, v1.2: cryptographic check) |
| `--search-filter` | `""` | Substring filter on SC code, separated by literal `;;;` |
| `--recent-blocks` | `0` | Scan only last N blocks from tip (0 = all) |
| `--segment-sync` | `false` | MapReduce parallel initial sync |
| `--adapt-batch` | `true` | Auto-tune batch size based on flush latency |
| `--mem-limit` | `0` | `GOMEMLIMIT` in bytes (0 = no explicit limit; Go runtime default) |
| `--api-address` | `127.0.0.1:8082` | REST API listen address |
| `--ws-address` | `127.0.0.1:9190` | WebSocket listen address |

## 6. HTTP API

Default listen address: `127.0.0.1:8082`.

```
GET /api/getinfo                  Daemon info (cached)
GET /api/getstats                 SC count, TX counts, TELA count, index height
GET /api/getscids                 All indexed SCIDs
GET /api/indexedscs               All SCIDs with owners
GET /api/indexbyscid?scid=X       Invocation details for one SCID
GET /api/scvarsbyheight?scid=X&height=N   SC variables snapshot
GET /api/initialscidcode?scid=X   Install-time SC code
GET /api/scidprivtx?address=A     Normal (non-SC-invoke) TXs carrying SCID payloads for an address
GET /api/tela                     All TELA apps with metadata
GET /api/tela/count               TELA app count (lightweight polling)
GET /api/tela/{scid}/ratings      On-chain TELA ratings for one SCID (per-rater scores, count, average)
GET /api/assets                   Asset/NFT catalog (NFA, G45 incl. collections, legacy DERO assets)
GET /api/assets/{scid}            Asset/NFT metadata for one SCID (incl. media URLs)
GET /api/address/{addr}/created-assets  Asset contracts deployed by address
GET /api/address/{addr}/touched-assets  Asset contracts the address interacted with
GET /api/address/{addr}/scs       All SCIDs the address interacted with
GET /api/invalidscids             Failed SC deploys

GET /tela/{scid}/{path…}          TELA content server (INDEX routing, DOC body, .gz, DocShard)
```

The `/tela/{scid}/…` endpoint is the content server. It resolves TELA-INDEX routes, fetches referenced TELA-DOC source, strips the `/* … */` body, decompresses `.gz` assets (base64→gunzip), and dispatches DocShard strict parsing when the dURL suffix is `.shard`/`.shards`. With `--tela-verify-sigs`, every response carries `X-TELA-Verify: disabled|unsigned|signed-unverified|passed|failed`.

Asset endpoints are catalog and activity views. True "my held assets" should be wallet-assisted: fetch candidate asset SCIDs from `/api/assets`, then have the wallet check decrypted balances for each SCID at the latest topoheight. `/created-assets` means deployer/registry ownership; `/touched-assets` means interaction history. Neither endpoint claims current wallet balance.

Asset entries carry the media URLs declared in the contract's `metadata` blob — `image` (an NFT's artwork, or a collection's `backdropImage`), `alt_image`, `audio`, `video`, and `images` (the raw JSON object of named secondary artwork). Each is omitted when empty. Note that G45 metadata does not use the `icon` key, so `icon_url` is empty for G45 assets and `image` is the field to render.

**These are URLs, not content.** HyperGnomon never fetches, caches, or proxies the bytes behind them — it is an indexer, not a CDN. On current mainnet 45,347 of them are `ipfs://` and 52 are `https`, so a consumer needs a gateway or a local IPFS node to resolve them.

Populating them requires the contract's variables, which turbo mode skips during scan. Run with `--postscan-vars=all`, or call `RefreshClassVars("G45-NFT")` from the Go library, to fill asset metadata; under the default `--postscan-vars=lazy` the class bucket holds the class name alone. This is pre-existing behavior for `Name`/`Desc` too, not specific to media.

## 7. WebSocket API

Default listen address: `127.0.0.1:9190/ws`. JSON-RPC 2.0.

Core methods (civilware-shape):

```
GetAllOwnersAndSCIDs                      → map[scid]owner
GetAllSCIDs                               → []scid
GetSCIDVariableDetailsAtTopoheight        → variables at height
GetSCIDInteractionHeight                  → []height
GetAllSCIDInvokeDetails                   → invocation detail list
GetSCIDValuesByKey / GetSCIDKeysByValue   → variable lookups
```

HyperGnomon-native methods:

```
subscribe {events, filters}     → typed push: install, invoke, var_change, reorg, safe, class_match
unsubscribe {id}
```

See [DESIGN.md §4](DESIGN.md) for the subscription protocol.

## 8. Library mode

Go consumers who import `github.com/civilware/Gnomon/{indexer,storage,structures}` today can migrate with three sed commands and one constructor swap. Full walkthrough in [DOCS/MIGRATION_FROM_GNOMON.md](DOCS/MIGRATION_FROM_GNOMON.md).

```go
import (
    compatindexer    "github.com/hypergnomon/hypergnomon/pkg/gnomes/indexer"
    compatstructures "github.com/hypergnomon/hypergnomon/pkg/gnomes/structures"
)

cfg := &compatstructures.FastSyncConfig{
    Enabled: true, SkipFSRecheck: true, ForceFastSyncDiff: 100,
}
idx, err := compatindexer.NewIndexerWithDBDir(
    "./gnomondb", "telaVersion;;;docVersion", "127.0.0.1:10102", "daemon",
    cfg, nil,
)
if err != nil { return err }
defer idx.Close()
idx.StartDaemonMode(8)
```

See [`pkg/gnomes/example/main.go`](pkg/gnomes/example/main.go) for a complete 30-line program.

Note: `NewIndexerWithDBDir` accepts the `FastSyncConfig` argument for signature compatibility with civilware's shape, but does not apply it in v1.0 — to fastsync, run the `hypergnomon` binary with `--fastsync`, or call the native indexer's `Indexer.FastSync` method (`github.com/hypergnomon/hypergnomon/indexer`).

Compat scope: bbolt backend, `daemon` runmode, civilware-shape `Indexer` fields (`LastIndexedHeight`, `ChainHeight`, `DBType`, `GravDBBackend`, `BBSBackend`) and store methods (`GetLastIndexHeight`, `GetAllOwnersAndSCIDs`, `GetAllSCIDVariableDetails`, `GetSCIDValuesByKey`, `GetSCIDKeysByValue`, `GetSCIDInteractionHeight`). `dbType="gravdb"` is accepted but maps to bbolt with a warning (so a gravdb-defaulting consumer like HOLOGRAM runs unchanged); `NewIndexer(gravDB, boltDB, …)` injects your pre-opened bbolt store. Non-daemon runmodes return a dead indexer — see §10.

## 9. Benchmarks

Every number below has a reproducible harness. Hardware, date, daemon endpoint, and command in [DOCS/BENCHMARKS.md](DOCS/BENCHMARKS.md).

**Live-daemon wall-clock** (mainnet 203.0.113.10:10102, April 2026):

| Stage | Baseline | v0.9 | Δ |
|---|---|---|---|
| FastSync main | 5.2 s | 2.9 s | −44% |
| Phase-1 classify probe | 24.9 s | 11.6 s | −53% |
| Phase-2 variable write | 10.7 s | 4.4 s | −59% |

**Microbenchmarks** (medians from `go test ./... -bench=. -benchmem -benchtime=500ms -count=3`):

| Bench | Msgpack / baseline | Typed / optimized | Factor |
|---|---|---|---|
| `ClassMeta_Marshal` | 641 ns / 11 allocs | 133 ns / 2 allocs | **4.8×** |
| `ClassMeta_MarshalTypedAppend` | 641 ns / 11 allocs | 24 ns / 0 allocs | **27×** |
| `SCIDVariables_Marshal` | 1,043 ns / 6 allocs | 51.5 ns / 0 allocs | **20×** |
| `AddrSCIDEntry_Marshal_TypedAppend` | 156 ns / 2 allocs | 1.5 ns / 0 allocs | **104×** |
| `TELAContentCache_InvalidatePrefix` (fill=8192) | O(fill) quadratic | 1.1 µs, flat across fills | O(k) |
| `WorkItem_Pool` vs `New` | 2,300 ns / 5.7 KB | 18 ns / 0 B | **127×** |
| `FlushBatch_100` vs individual writes | 88 µs/record | 6.1 µs/record | **14×** |
| `FlushBatch` vs 100k-interaction history (heights) | 5,684 µs (blob layout) | 118 µs (composite keys) | **48×, flat in history size** |
| `ClassifyCorpus/Full` — 45,589 real mainnet G45 SCs, golden-gated (July 2026) | 1,970,788 allocs / 79.3 MB | 415 allocs / 34 KB | **4,749× fewer allocs** |

Full table with reproduction command, hardware, and methodology: [DOCS/BENCHMARKS.md](DOCS/BENCHMARKS.md).

**TELA correctness**: content server output is byte-identical (SHA256) to civilware/tela `parseDocCode` for `.html`, `.js`, `.css`, `.gz`, and DocShard paths. Live-fixture test: `api/tela_content_test.go`.

The head-to-head harness vs civilware/Gnomon ships as `cmd/benchvs`: sequential single-target runs — one per indexer, with an operator-supplied civilware wrapper binary — appending to `bench_vs_civilware.md`. See [`cmd/benchvs/README.md`](cmd/benchvs/README.md) for the civilware recipe and the full flag list (RSS is not measured in v1.0).

```bash
go run ./cmd/benchvs \
    --name=HyperGnomon \
    --binary=./hypergnomon \
    --daemon=203.0.113.10:10102 \
    --db-dir=/tmp/hg-bench \
    --probe-duration=60s \
    --probe-workers=32 \
    --out=bench_vs_civilware.md
# Appends: time-to-tip, time-to-ready, DB size, REST p50/p95/p99 per probe path.
```

For multi-target A/B matrices across release tags, `origin/main`, and the current workspace, `cmd/benchmatrix` orchestrates repeated sequential `benchvs` trials and aggregates the results — see [`cmd/benchmatrix/README.md`](cmd/benchmatrix/README.md).

Numbers reported as "228×", "96×", "3,240×", "1,786×", "8,571×" in earlier README revisions were inherited without reproducible methodology and have been retired. If you have a repro case we should own, file an issue.

## 10. Known limitations

- **Storage backend**: selectable via `--storage-backend` (`storage.Open` factory). `bbolt` is the default and only shipped engine; `sqlite` is deferred — the selector + a backend conformance suite exist, but it returns "not implemented yet", and implementing it is gated on a named consumer need plus a full-schema prototype (the engine benchmark does not settle it — see `storage/dbbench/RESULTS.md`); `graviton` is unsupported.
- **Graviton backend**: not a real backend, by design. The **CLI** `--storage-backend=graviton` errors (an operator who typed it should know). The **library** `storage.NewGravDB` and `pkg/gnomes` `dbType="gravdb"` instead **warn and fall back to bbolt** (so a civilware consumer that defaults to gravdb — e.g. HOLOGRAM — drops in unchanged and runs on bbolt). Rationale: graviton iterates in hash byte-sorted order with no prefix/range queries, so it cannot serve the key-ordered Route B scans (class / install / owner / interaction-height) HyperGnomon is built on; emulating them needs per-SCID index trees — the path behind civilware/Gnomon issue #24 (graviton bloat, open 2+ years; both simple-gnomon forks dropped it). bbolt's batch-flush also outperforms it in our measurements.
- **Runmodes**: only `daemon` is implemented. `wallet` and `asset` return a dead indexer. No consumer has needed them yet.
- **External-store injection**: supported. `NewIndexer(gravDB, boltDB, …)` injects a pre-opened bbolt store (via `indexer.Config.Store`) and **borrows** it — the caller keeps ownership; the facade's `Close()` releases it. `NewIndexerWithDBDir(path, …)` remains for callers who prefer to pass a path and let HyperGnomon open its own store.
- **Reorg truncate-replay (M2)**: detection is implemented (`SafeHeight` atomic, `CheckReorgAt` stub); automatic rewind is not. Rationale: DERO's `STABLE_LIMIT=8` consensus bound plus DAG side-block absorption means real reorgs are rare and shallow. civilware/Gnomon has no reorg handling at all; operators recover via `pop <N>` CLI. Our `resync` subcommand serves the same operator path. Full truncate-replay is tracked for v1.x.
- **Signature verification** (`fileCheckC/S` Schnorr on bn256): v1.0 and v1.1 report presence only via `X-TELA-Verify`. Cryptographic verification ships in v1.2; the header surface is stable.
- **Mempool speculative path (M6)**: designed, not yet implemented.

## References

- [civilware/Gnomon](https://github.com/civilware/Gnomon) — canonical DERO indexer; HyperGnomon's `pkg/gnomes` is a drop-in compat layer for its public surface
- [DHEBP/HOLOGRAM](https://github.com/DHEBP/HOLOGRAM) — reference embedded-indexer consumer
- [civilware/tela](https://github.com/civilware/tela) — canonical TELA spec; HyperGnomon's content server is verified byte-identical against it
- [DESIGN.md](DESIGN.md) — subscription protocol, data model, milestones
- [DOCS/MIGRATION_FROM_GNOMON.md](DOCS/MIGRATION_FROM_GNOMON.md) — migration guide for civilware consumers
- [DOCS/FLAGS.md](DOCS/FLAGS.md) — full flag reference with default rationale
- [DOCS/BENCHMARKS.md](DOCS/BENCHMARKS.md) — methodology, hardware, dates

## License

MIT
