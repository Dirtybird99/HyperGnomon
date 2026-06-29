# Changelog

All notable changes to HyperGnomon. Dates in UTC.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/) and this project adheres to [SemVer](https://semver.org/) from v1.0 onward.

## [1.1.0] — 2026-06-29

Pluggable storage backends + a large allocation-reduction pass, with a true civilware/Gnomon drop-in for HOLOGRAM.

### Added

- Pluggable storage-backend factory (`storage/factory.go`): `storage.Open(backend, dbDir, searchFilter)` selects a backend by name (`bbolt` default + `bolt`/`boltdb`/`bbs` aliases; `sqlite` → `ErrBackendNotImplemented`; `graviton` → `ErrGravitonUnsupported`), plus `ValidateBackend`/`SupportedBackends`. New `--storage-backend` CLI flag, validated up front.
- `hypergnomon compact` subcommand (`storage/compact.go`): offline `bbolt.Compact` rewrite with atomic swap, `.bak` rollback, and a reclaimed-space report; opens the source read-only and fails fast if the indexer holds the lock.
- External store injection (`indexer.Config.Store`): the indexer borrows a caller-provided open store instead of opening its own — the seam the `pkg/gnomes` compat shim uses.
- `pkg/gnomes` is now a real civilware/Gnomon drop-in for DHEBP/HOLOGRAM: 12-arg `NewIndexer`, `AddSCIDToIndex`, `FastSyncImport`, `GetOwner`, and a `GravDBBackend` that delegates to bbolt.
- SWAP (`StartSwap`) and EPOCH (`epochEnabled`/`crowd_mining`) classification tags.
- Typed binary encoders for `NormalTXWithSCIDParse` (tag 0x08) and `TELAContentEntry` (tag 0x07) with byte[0] tag-dispatch and backward-compatible legacy-msgpack reads.
- `DOCS/ECOSYSTEM_GNOMON.md` — Gnomon storage-ecosystem study backing the bbolt-only / defer-sqlite / reject-graviton decisions.

### Changed

- `go.mod`: `modernc.org/sqlite v1.53.0` (pure-Go) added as a direct dep used only by the isolated `storage/dbbench` harness — never linked into `cmd/hypergnomon`; graviton promoted indirect→direct; `golang.org/x/sync` 0.1.0→0.20.0.
- Updated `README.md`, `DOCS/FLAGS.md`, `DOCS/MIGRATION_FROM_GNOMON.md`; pushed the cryptographic-signature-verification milestone from v1.1 to v1.2.

### Performance

All gated and measured (allocs/op):
- `storage`: `GetOwnersForSCIDs` 510→114; `parseTELARatingValue` SplitN→IndexByte + `hexDecodeIfHex` unsafe.String scratch 4035→2037@1000; `FlushBatch` class-key `keyBuf` reuse; `AddrSCIDEntry` arena 10940→9939; typed normaltx/telacontent encode.
- `api`: assets single query-parse 42→27; sccode typed-struct response 101→91; `gzip.Reader` `sync.Pool` 17→2; ws `safeWrite` reused encoder + `forwardEvents` notif hoist.
- `indexer`: classify Tags cap-2; `processorLoop` txMap→counter; `segment` argString port.

### Fixed

- Borrowed-store lifecycle: `indexer.New` no longer closes a store it does not own when construction fails after accepting an injected `Config.Store`.
- Typed-decoder use-after-free safety: hot-path decoders return strings backed by a decoder-owned buffer, never aliasing the bbolt page the View txn frees — locked by an anti-aliasing gate.

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
