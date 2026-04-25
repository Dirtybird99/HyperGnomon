# Changelog

All notable changes to HyperGnomon. Dates in UTC.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/) and this project adheres to [SemVer](https://semver.org/) from v1.0 onward.

## [1.0.0] — 2026-04-25

Migration-driving release: canonical TELA spec compliance, civilware/Gnomon drop-in Go-library surface, reproducible benchmarks, honest README.

### Added

- `pkg/gnomes/{indexer,storage,structures}` — civilware/Gnomon-shape compat surface. A Go consumer migrates via three sed commands plus one constructor swap. `pkg/gnomes/example/main.go` is the 30-line reference consumer; `pkg/gnomes/compat_test.go` pins the HOLOGRAM call shape as a guard.
- `DOCS/MIGRATION_FROM_GNOMON.md` — migration guide with full API surface matrix.
- `DOCS/FLAGS.md`, `DOCS/BENCHMARKS.md` — full flag reference + benchmark methodology.
- `--tela-verify-sigs` flag. Emits `X-TELA-Verify: disabled|unsigned|signed-unverified|passed|failed` on `/tela/…` responses. v1.0 reports signature presence; cryptographic bn256 Schnorr verification ships in v1.1.
- TELA content server: `.gz` asset decompression (`base64.StdEncoding → gzip.NewReader`, matching `civilware/tela/compression.go`), DocShard strict parsing (`.shard`/`.shards` dURL suffix; no `TrimSpace`, matching `civilware/tela/tela.go:parseDocShardCode`), `fileCheckC/S` field extraction with hex-decode.
- `decodeHexIfPrintableASCII` hex-layer unwrap on DOC# route values and `var_header_name` / `nameHdr` filename lookups.

### Changed

- TELA `var_header_name` lookup now falls back to legacy `nameHdr` key (TELA-INDEX-1 v1.0.0 contracts use the short name). Hex-decodes both before matching.
- `classKeys` no longer fast-paths `TELA-INDEX-1` — now forces `Variables=true` fetch so DOC# routes and both v1.0.0 (`nameHdr`) and v1.1.0 (`var_header_name`) key families are captured. Previously missed either DOC# entries or the v1.0.0 name key depending on which family was compiled in.
- `extractDOCBodyFromSource` uses `strings.TrimSpace` on the `/* … */` body (matches `civilware/tela/tela.go:parseDocCode`). Earlier single-newline trim produced a 3716-byte body vs the canonical 3693 bytes on live `algo4.html`.
- Non-turbo FastSync branch now launches `probeTELA` so `--tela-only --turbo=false` no longer hangs.
- Default flag values are now canonically documented: `--num-parallel-blocks=8`, `--rpc-pool-size=8`, `--batch-size=100`, `--persist-install-code=tela`, `--turbo=true`. CLAUDE.md and DESIGN.md reconciled with shipped values.

### Removed

- Unsubstantiated "1,786× / 8,571× faster than Engram/PureWolf", "228× / 96× / 3,240×" speedup claims from README. Replaced with live-daemon measurements (FastSync main −44%, Phase-1 classify −53%, Phase-2 writes −59%) and microbenchmark numbers that every number links to a reproducible harness in `DOCS/BENCHMARKS.md`.

### Fixed

- TELA content server 404s on live mainnet contracts caused by: (1) missing hex-decode on daemon-returned STORE strings, (2) class-key fast-path skipping v1.0.0 `nameHdr` and DOC# entries, (3) double-hex-encoded DOC# values.
- Stale content-server cache after code changes — now invalidated via `TELAContentCache.InvalidatePrefix` on re-probe.

## [0.9.0] — 2026-04-20

- Default RPC pool size increased to mitigate probe-phase stalls under contention.
- `keysstrings`-backed TELA refresh path; stage timers (`--timing`, `--timing-every`) for operator visibility.
- Event bus hooks for `safe_height` transitions; `CheckReorgAt` detection stub.
- Typed binary encoding for `ClassMeta` hot path (3.2× marshal vs msgpack, 1 alloc vs 11).
- `TELAContentCache.InvalidatePrefix` O(1) per entry (250× improvement at fill=8192).

## [0.8.0] — 2026-04-19

- Route B foundation: `class_scid`, `installs`, `addr_scids`, `owner_scids` buckets with binary big-endian height keys for range-scan support.
- WebSocket JSON-RPC 2.0 server with `subscribe` / `unsubscribe` + typed event notifications.
- REST API: `/api/initialscidcode`, `/api/tela/count`, `/api/invalidscids`.
- bbolt backend with adaptive batch flush (100 → 2000 blocks based on flush latency).
- Arena-style sync.Pool recycling for `SCTXParse`, `BlockTxns`, `WorkItem`, and byte buffers.
- `unique.Make` string interning for SCIDs and addresses.

## [0.7.0] — initial release

- Arena-accelerated DERO blockchain scanner baseline.
