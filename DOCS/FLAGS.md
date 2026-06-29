# Flag reference

Defaults ship conservative. Every non-default value below is here so an operator can tune without reading source.

## Daemon + network

### `--daemon-rpc-address` (default `127.0.0.1:10102`)

DERO daemon RPC endpoint. Accepts bare `host:port`, `http://…`, `https://…`, or `ws://…` / `wss://…`. Schemes are stripped before dialing; HyperGnomon selects WS on the inferred port.

Public nodes that work out of the box: `node.derofoundation.org:11012`. LAN / local nodes default to `:10102` (mainnet) or `:40402` (testnet).

### `--rpc-pool-size` (default `8`)

Number of concurrent WebSocket connections to the daemon. Higher values increase probe and batch throughput; the TELA-CLI operator guide cautions against exceeding **16** on remote daemons — they rate-limit per-connection. `8` is the conservative default that performs well against local and LAN daemons and stays under the documented ceiling.

Rationale: v0.8 shipped `4`; measured probe-phase stalls on mainnet pushed this to `8` in v0.9. Values above `16` have shown rate-limit rejections from public nodes.

Do not raise this expecting a faster classify probe: a `cmd/benchmatrix` sweep
against the LAN mainnet daemon (June 2026, 2 matched trials per size) measured
time-to-ready medians of 21.3 s / 22.0 s / 24.0 s for pool sizes 8 / 12 / 16 —
cumulative phase-1 RPC wait grew proportionally with the connection count
(48 s → 74 s → 97 s), i.e. the daemon serializes the work server-side and
extra connections only add queueing.

### `--rpc-compression` (default `true`)

Enable WebSocket compression on new daemon RPC connections. Applies to every connection in the pool. Set `--rpc-compression=false` to disable, e.g. when diagnosing daemon-side framing issues or benchmarking raw transport cost.

### `--num-parallel-blocks` (default `8`)

Block fetchers in the 3-stage pipeline. Higher values increase memory pressure and RPC concurrency. `8` pairs with the default `--rpc-pool-size=8` for one fetcher per connection.

### `--batch-size` (default `100`)

Blocks per atomic bbolt flush. Bigger batches amortize the commit cost but increase RAM during the flush cycle. With `--adapt-batch=true` (default), HyperGnomon auto-tunes up to `1000` when flushes stay fast.

### `--recent-blocks` (default `0`)

Scan only the last `N` blocks from chain tip. `0` means scan all. Useful for operator re-indexes when only recent state is needed.

### `--segment-sync` (default `false`)

MapReduce parallel initial sync. Splits the chain into segments processed by parallel fetchers. Faster for cold starts on fast daemons; adds RAM.

### `--mem-limit` (default `0`)

`GOMEMLIMIT` in bytes. `0` means no explicit limit (Go runtime default) — `debug.SetMemoryLimit` is only called when the value is above `0`. Set this if you're running HyperGnomon in a container with a hard memory cap.

## Sync + probe

### `--fastsync` (default `false`)

Bootstrap from the GnomonSC on-chain registry instead of scanning every block from genesis. Highly recommended for TELA-focused workflows — skips ~6.8M blocks of chain scan.

### `--classify-probe-batch-size` (default `400`)

SCIDs packed into each phase-1 classify `GetSC(code=true)` JSON-RPC batch during FastSync. Values `<= 0` reset to the default `400`; values above `1000` are capped at `1000`. LAN benchmarking showed `400` keeps startup close to the 1000-SCID-ceiling throughput without making each response frame as large.

### `--turbo` (default `true`)

Turbo scan mode. During initial sync, skip per-SC variable fetching. This is the default because it's consistently faster on every tested daemon. `--turbo=false` is a diagnostic replay mode — also launches `probeTELA` so `--tela-only --turbo=false` doesn't hang (fix landed in v1.0).

### `--postscan-vars` (default `lazy`)

Turbo follow-up variable policy after the indexer reaches tip:

- `lazy` — skip the historical all-SCID variable sweep. TELA variables still come from the classify/TELA probe and refresher; broad metadata is filled by targeted/on-demand paths.
- `all` — fetch variables for every indexed SCID after startup. This preserves the old cold-start behavior but adds roughly a minute on current mainnet against the LAN benchmark daemon.

### `--classify-seed-cache-dir` (default `""`)

Directory for the cross-DB classify seed cache. Empty uses the OS user cache
directory. The seed cache is keyed by network, GnomonSC SCID, schema version,
registry hash, and height; a clean FastSync can seed class metadata, TELA
variables, and persisted TELA install code from a verified prior run instead
of repeating the full `GetSC(code=true)` classify probe.

Set this only when you want benchmark isolation or an operator-controlled cache
location. If the cache is missing, stale, corrupt, or registry-mismatched,
HyperGnomon falls back to the normal live classify probe.

### `--tela-only` (default `false`)

Discover all TELA apps, print a summary, then exit. No chain scanning beyond the probe. Designed for CI / cache-warming jobs.

### `--testnet` (default `false`)

Use the testnet GnomonSC SCID (`c9d2…`). Default is the mainnet SCID (`a053…`).

### `--search-filter` (default `""`)

Substring filter on SC code, `;;;` separated. Matches if any pattern is present in the code. Passed through to the classifier.

### `--sf-scid-exclusions` (default `""`)

SCIDs to exclude from indexing, `;;;` separated.

## TELA

### `--persist-install-code` (default `tela`)

Policy for the `sccode` bucket (install-time SC code persistence):

- `none` — disables forward-populate entirely. Code is lazy-filled on each `GetInitialSCIDCode` read via a single-flight backfill.
- `tela` (**default**) — persists only `TELA-INDEX-1`, `TELA-DOC-1`, `TELA-MOD-1`. These are the classes whose content server + `GetInitialSCIDCode` consumers actually read. Adds ~15 MB to mainnet DB.
- `all` — persists every SC's install code. Matches the pre-class-aware behavior; adds ~134 MB to mainnet DB.

The legacy `--skip-tela-doc-code` flag has been removed in favor of `--persist-install-code` (passing it now makes the binary exit at flag parsing). Its old behavior maps to `--persist-install-code=none`.

### `--tela-verify-sigs` (default `false`)

Emit an `X-TELA-Verify` response header on `/tela/…` responses. In v1.0 the header reports signature **presence**:

- `disabled` — flag is off
- `unsigned` — contract has no `fileCheckC` / `fileCheckS` fields
- `signed-unverified` — fields present but cryptographic check not yet implemented (v1.0)
- `passed` / `failed` — reserved for v1.2+ when bn256 Schnorr verification ships

The header surface is stable. Operators can already use `signed-unverified` vs `unsigned` to distinguish TELA v1.0.0 (no fileCheck fields) from v1.1.0 contracts.

### `--tela-cache-mb` (default `128`)

In-memory cap for the TELA content 2-tier cache. Second tier is disk-backed and uncapped.

## Servers

### `--api-address` (default `127.0.0.1:8082`)

REST API listen address. See [README §6](../README.md#6-http-api) for endpoints.

### `--ws-address` (default `127.0.0.1:9190`)

WebSocket JSON-RPC 2.0 listen address, served at `/ws`. See [README §7](../README.md#7-websocket-api) + [DESIGN.md §4](../DESIGN.md).

### `--pprof-address` (default `""`)

`net/http/pprof` listen address (e.g. `127.0.0.1:6060`). Empty disables.

## Debug + ops

### `--debug` (default `false`)

Enable debug logging. Prints per-scan-cycle detail; noisy.

### `--adapt-batch` (default `true`)

Auto-tune `--batch-size` based on flush latency.

### `--timing` (default `false`) / `--timing-every` (default `10`)

Emit per-stage (fetcher, processor, flusher) timing summaries every N batches. Operator visibility for pipeline contention.

### `--storage-backend` (default `bbolt`)

Selects the storage engine via the `storage.Open` factory. `bbolt` is the
default and only shipped backend — the arena/batch-flush design the rest of
these docs describe. `sqlite` is **deferred** (not promised): the selector and a
backend conformance suite already exist, but `--storage-backend=sqlite` exits with
"not implemented yet". Implementing it is gated on (a) a named consumer with a
concrete SQL/operability need and (b) a full-schema prototype that passes a
hardened conformance suite and re-benchmarks the real `FlushBatch` at or below
bbolt — see `storage/dbbench/RESULTS.md` for why the engine benchmark does not
settle this. `graviton` is **unsupported**
and exits with a clear error — graviton iterates in hash byte-sorted order with
no prefix/range queries, which cannot serve HyperGnomon's key-ordered Route B
scans (class / install / owner / interaction-height prefix scans built on
big-endian height keys); see civilware/Gnomon issue #24. Unknown values exit at
startup with the list of valid backends.

### `--db-dir` (default `gnomondb`)

bbolt database directory. Created if missing.

On Windows the database file is pre-extended to **256 MiB** at open (64-bit
builds): bbolt's memory map is reserved up front so the file never re-maps as
it grows. Re-mapping on Windows tears down and recreates the file mapping
with transactions quiesced, which measurably stalled large write bursts — the
classify-probe variable flush took 4.1–4.3 s against a growing DB vs
0.41–0.44 s with the reservation (live mainnet daemon, June 2026). On
Linux/macOS only virtual address space is reserved and the file stays at its
data size. Once the data outgrows 256 MiB the reservation has no further
effect.

### Maintenance subcommands

Run these with the indexer stopped — each takes the bbolt lock and fails fast if
the store is in use:

- `hypergnomon resync [--db-dir=…]` — drop every index bucket so the next start
  rescans from height 0. Block-hash history is kept for reorg-detection replay.
- `hypergnomon clean <mainnet|testnet|simulator> [--db-dir=…] [--force]` — remove
  the db dir entirely. `mainnet` requires `--force`.
- `hypergnomon compact [--db-dir=…]` — rewrite the bbolt store via `bbolt.Compact`,
  dropping free/fragmented pages to reclaim disk and improve page locality
  (measured ~94% reclaim on a heavily-churned DB). The original is kept as
  `HYPERGNOMON.db.bak` for rollback — delete it once the compacted DB starts
  cleanly. On Windows the next start re-applies the 256 MiB mmap reservation, so
  the on-disk win is largest for stores that grew well past 256 MiB; on
  Linux/macOS the file stays at its compacted size.
