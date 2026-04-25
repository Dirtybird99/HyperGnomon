# Flag reference

Defaults ship conservative. Every non-default value below is here so an operator can tune without reading source.

## Daemon + network

### `--daemon-rpc-address` (default `127.0.0.1:10102`)

DERO daemon RPC endpoint. Accepts bare `host:port`, `http://…`, `https://…`, or `ws://…` / `wss://…`. Schemes are stripped before dialing; HyperGnomon selects WS on the inferred port.

Public nodes that work out of the box: `node.derofoundation.org:11012`. LAN / local nodes default to `:10102` (mainnet) or `:40402` (testnet).

### `--rpc-pool-size` (default `8`)

Number of concurrent WebSocket connections to the daemon. Higher values increase probe and batch throughput; the TELA-CLI operator guide cautions against exceeding **16** on remote daemons — they rate-limit per-connection. `8` is the conservative default that performs well against local and LAN daemons and stays under the documented ceiling.

Rationale: v0.8 shipped `4`; measured probe-phase stalls on mainnet pushed this to `8` in v0.9. Values above `16` have shown rate-limit rejections from public nodes.

### `--num-parallel-blocks` (default `8`)

Block fetchers in the 3-stage pipeline. Higher values increase memory pressure and RPC concurrency. `8` pairs with the default `--rpc-pool-size=8` for one fetcher per connection.

### `--batch-size` (default `100`)

Blocks per atomic bbolt flush. Bigger batches amortize the commit cost but increase RAM during the flush cycle. With `--adapt-batch=true` (default), HyperGnomon auto-tunes up to `2000` when flushes stay fast.

### `--recent-blocks` (default `0`)

Scan only the last `N` blocks from chain tip. `0` means scan all. Useful for operator re-indexes when only recent state is needed.

### `--segment-sync` (default `false`)

MapReduce parallel initial sync. Splits the chain into segments processed by parallel fetchers. Faster for cold starts on fast daemons; adds RAM.

### `--mem-limit` (default `0`)

`GOMEMLIMIT` in bytes. `0` means auto-detect (60% of system RAM). Set this if you're running HyperGnomon in a container with a hard memory cap.

## Sync + probe

### `--fastsync` (default `false`)

Bootstrap from the GnomonSC on-chain registry instead of scanning every block from genesis. Highly recommended for TELA-focused workflows — skips ~6.8M blocks of chain scan.

### `--turbo` (default `true`)

Turbo scan mode. During initial sync, skip per-SC variable fetching. This is the default because it's consistently faster on every tested daemon. `--turbo=false` is a diagnostic replay mode — also launches `probeTELA` so `--tela-only --turbo=false` doesn't hang (fix landed in v1.0).

### `--postscan-vars` (default `lazy`)

Turbo follow-up variable policy after the indexer reaches tip:

- `lazy` — skip the historical all-SCID variable sweep. TELA variables still come from the classify/TELA probe and refresher; broad metadata is filled by targeted/on-demand paths.
- `all` — fetch variables for every indexed SCID after startup. This preserves the old cold-start behavior but adds roughly a minute on current mainnet against the LAN benchmark daemon.

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
- `all` — persists every SC's install code. Matches the pre-class-aware behaviour; adds ~134 MB to mainnet DB.

Legacy `--skip-tela-doc-code` is still accepted and coerces the policy toward `none` for `TELA-DOC-1`.

### `--tela-verify-sigs` (default `false`)

Emit an `X-TELA-Verify` response header on `/tela/…` responses. In v1.0 the header reports signature **presence**:

- `disabled` — flag is off
- `unsigned` — contract has no `fileCheckC` / `fileCheckS` fields
- `signed-unverified` — fields present but cryptographic check not yet implemented (v1.0)
- `passed` / `failed` — reserved for v1.1+ when bn256 Schnorr verification ships

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

### `--db-dir` (default `gnomondb`)

bbolt database directory. Created if missing.
