# Changelog

All notable changes to HyperGnomon. Dates in UTC.

The format is loosely based on [Keep a Changelog](https://keepachangelog.com/) and this project adheres to [SemVer](https://semver.org/) from v1.0 onward.

## [Unreleased]

### Added

- **NFA media extraction.** The classifier now reads the Artificer NFA standard's direct media variables — `fileURL` → `image`, `coverURL` → `alt_image` (live-sampled: 21/21 NFAs carry both, largely GitHub-hosted) — plus the free-form `image_url` token-logo variable (8 bridged-token contracts in the corpus). All 2,626 mainnet NFAs now surface artwork on `/api/assets` and `/api/media`, and `cmd/mediawarm` warms them by default. GitHub `blob/` page URLs are rewritten to their `raw.githubusercontent.com` form at fetch time so the cache holds image bytes, not repository HTML.

- **Wayback Machine recovery in `cmd/mediawarm`.** The G45 minting platform's own gateway (`ipfs.deronfts.com`) is DNS-dead, but the Internet Archive crawled it: 497 unique image captures across 95 root CIDs, retrievable verbatim via `id_` snapshot URLs (verified byte-for-byte against a 1.1 MB PNG). After the live pass, mediawarm queries the CDX index for each still-lost root — serial, politely paced — and lands recovered bytes at the exact cache path `/api/media` serves. The census marks these `via: wayback`, and now also inventories every G45-C collection's off-chain `links` as the manual follow-up list for content nothing automated can reach.

- **`GET /api/media/{scid}` — asset media served from a local fetch-once cache.** New `media` package: deterministic URL→path mapping (the filesystem is the index), hedged multi-gateway race (local kubo first via `--ipfs-gateway`, then the surviving public gateways, 800 ms hedging, 429 cooldown, 50 MB cap, atomic temp+rename writes), immutable/`nosniff`/CSP-sandboxed responses. On-demand fetching is opt-in (`--media-fetch`); default serves pre-cached bytes only, so the API cannot be used as an open proxy. Non-`ipfs`/`https` schemes in on-chain metadata are refused — those URLs are attacker-controlled strings.

- **`cmd/mediawarm`** — bulk archival warm + availability census. Groups all media URLs by root CID (most-referenced first), probes each root via gateways, consults the local kubo DHT (`routing/findprovs`, bounded) when gateways miss, fetches everything reachable into the shared cache, and writes `media-census.json`. Motivated by measurement, not hypothetical rot: as of 2026-07-27, 7 of the top 10 corpus roots (~19k images) had **no live public-gateway copy**, and spot-checked gateway-MISS roots had **zero DHT providers** — content cached today may be unobtainable tomorrow.

  First full census (2026-07-27, mainnet, local kubo + 4 public gateways): of **45,740 media files across 2,352 root CIDs, 12,600 (27.5%, 8.07 GB) were retrievable; 2,131 roots holding 32,544 files (71%) had no reachable source anywhere** — including five of the six largest collections (Dero Heist 4,164, 4,139/3,354/3,037/2,647-file roots). The Dero Ducks root (3,379) is known to survive on Pinata's gateway but was rate-limited during the run; re-running mediawarm resumes for free and only touches misses.

- G45 media URLs on the asset API. `ClassifySC` now lifts `image`, `backdropImage`, `alt-image`, `alt-backdropImage`, `audio`, `video`, and `images` out of the G45 `metadata` blob into new `ClassMeta` fields, surfaced as `image` / `alt_image` / `audio` / `video` / `images` on `/api/assets` and `/api/assets/{scid}` (omitted when empty). These are **URLs only** — HyperGnomon does not fetch, cache, or proxy the bytes, and 99.9% of them are `ipfs://`.

  Motivation: the extractor only ever read `icon`, which appears **zero times** across the 45,651-SC mainnet corpus, so `ClassMeta.IconURL` — the only media-ish field the asset API exposed — was empty for every G45 asset. `image` is present on 45,414 of 45,539 NFT-class contracts and `backdropImage` on 87 of 112 collections.

  `ImagesJSON` carries the `images` object as **verbatim on-chain JSON text**, not re-encoded: key order and spacing are whatever the minter wrote.

- `G45-C` (collections) added to the `/api/assets` catalog and to `isAssetClass`, so a collection's `backdropImage` is reachable. This grows the default `/api/assets` response by the ~75 collection contracts on mainnet.

- The new fields ride the `/ws` class-assignment event automatically — `publishBatchEvents` sends the whole `ClassMeta` as the event payload.

### Changed

- `ClassMeta` typed v1 encoding gained an optional five-string media tail, appended after `Version` with **no tag bump**. Forward-compat: the v1 reader always discarded trailing bytes, so a pre-media binary reads a media record. Backward-compat: a truncated or absent tail now decodes as empty rather than `ErrInvalidClassMeta`. Records without media encode byte-identically to before, so stored records and size benchmarks are untouched.

- Turbo's post-scan variable sweep (`--postscan-vars=all`) now re-classifies from the variables it fetches instead of discarding them. Turbo's scan-time classify runs with nil vars (code only), so previously the sweep paid for every `GetSC` and still left `Name`/`Desc`/`IconURL` empty for non-TELA classes. Fixes pre-existing empty G45/NFA metadata, not just the new media fields.

### Fixed

- **Fastsync no longer wipes populated asset metadata on restart.** Fastsync's "other classes" path blind-wrote a bare ClassMeta (class + tags only) over every non-TELA record on every startup, and the classify seed cache's store-seeding did the same with cached (usually bare) snapshots — so a `--postscan-vars=all` sweep or an operator's `RefreshClassVars` was silently undone by the next restart. Caught live: a restarted node served an asset catalog whose every `name` and media field had reverted to empty. Both sites are now populate-only — they write a record only when none exists, since an existing record is always at least as good as the bare placeholder.

- **The G45 `metadata` variable is now hex-decoded before parsing.** derod returns `STORE`'d strings hex-encoded, and every other string var in `extractClassVars` goes through `decodeHexIfPrintable` — `metadata` did not, at either read site. The JSON extractors were handed hex, parsed nothing, and left `Name`/`Desc`/`IconURL` empty for every G45 asset on a live chain. This was invisible to the entire suite because `indexer/testdata/nfts.json.gz` holds `metadata` already decoded, a shape the daemon never sends; only a live sync exposed it.

- Every `ClassMeta` construction site now funnels through one `classMetaFrom` projection. The projection had been copy-pasted across seven sites in `indexer.go`, `fastsync.go`, and `tela_refresher.go`, so adding a field to `SCClass` populated it only on the paths the author remembered — with no failing test, because each path belongs to a different sync mode.

- `reclassifyFromVars` wrote a `ClassMeta.LastHeight` that disagreed with the height its paired `AddVariables` used. `GetSCIDVariableDetailsAtHeight` builds an exact `"<scid>:<height>"` key with no floor scan, so the snapshot the post-scan sweep had just written was unreachable.

- **The classify corpus is regenerated from raw `GetSC` output** and no longer holds decoded values. New `cmd/corpusdump` enumerates G45 SCIDs from a synced DB and captures every variable verbatim at one pinned topoheight (7,389,814: 45,539 NFT-class + 112 G45-C, recorded in `indexer/testdata/corpus_manifest.json`). The previous fixture held `metadata` decoded in both files and `type` decoded in `nfts.json.gz` — a shape derod never sends, which is why the hex bug above passed every gate.

  The re-capture is verified faithful rather than merely different: across all 45,586 SCIDs present in both the old and new corpora, `ClassifySCVars` output is **identical**. `TestCorpusHoldsRawDaemonShape` now pins the hex shape so the fixture cannot drift back.

  Three corpus-iterating tests had to start decoding before parsing. `TestG45ScanDifferentialCorpus` in particular was **vacuous** on hex input — both paths saw non-JSON, set nothing, and agreed on nothing; `TestG45ScanCorpusFireRate` is what caught it.

### Changed (benchmarks)

- **`BenchmarkClassifyCorpus/Full` is rebased and is not comparable to earlier releases.** The published `1,970,788 → 415 allocs` was measured on the decoded fixture, where the scanner could return zero-copy substrings of text the daemon never sends in that form; 415 was never reachable in production. Against the raw corpus the honest figures are **91,759 allocs / 24.7 MB**, cut to **46,123 allocs / 12.4 MB** by handing the hex-decode buffer over via `unsafe.String` instead of copying it (`ownedBytesToString`). That is ~1.01 allocations per SC — the floor, since hex input cannot be aliased.

### Verified

Against a DERO mainnet daemon at height 7,389,740 (`--fastsync --turbo --postscan-vars=all`, 50,245 SCIDs, 77s sweep), both on a fresh DB and on a DB written by the previous binary (the mixed-version upgrade path):

| | `name` | `icon_url` | `image` | `alt_image` | `audio` | `video` | `images` |
|---|---|---|---|---|---|---|---|
| G45-NFT (45,516) | 45,503 | **0** | 45,401 | 239 | 295 | 148 | 23 |
| G45-C (112) | 88 | **0** | 87 | 2 | 0 | 0 | 0 |

`icon_url` is zero across all 45,628 G45 assets — the premise of the change. The media counts match the independently-derived corpus oracle exactly (`image` differs by the 2 NFTs minted since the corpus snapshot).

### Performance

- Adding the media fields costs nothing on the classify path. `images` is routed through the scanner's **skip** branch rather than made a target key — its value is an object, and a non-string target value forces the whole blob down the fallback map decode (measured on the pre-regeneration fixture: +1,111 allocs, +62 KB, for the 23 blobs that carry it). The skip branch has already walked the value's extent, so capturing the raw text is free. Absolute figures for this benchmark moved when the corpus was regenerated — see *Changed (benchmarks)* above; the media fields are not what moved them.

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
