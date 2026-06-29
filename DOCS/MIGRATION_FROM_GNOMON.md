# Migrating from civilware/Gnomon to HyperGnomon

This guide walks a Go consumer of `github.com/civilware/Gnomon` through the v1.0 port to `github.com/hypergnomon/hypergnomon/pkg/gnomes`. The compat layer reproduces civilware's public surface; most migrations are **three sed commands plus one constructor swap**.

Target audiences (Go-library embedders who import `civilware/Gnomon/structures` today):

- TELA-CLI
- HOLOGRAM (github.com/DHEBP/HOLOGRAM)
- dReams + spinoffs (dPrediction, Duels, Dero-Baccarat)
- Engram
- Anyone else using `gnomes.NewGnomes()`-style wrappers on top of civilware

## What HyperGnomon gives you

- **Same types** (`structures.SCIDVariable`, `structures.FastSyncConfig`): type aliased to HyperGnomon's internals so pointers round-trip.
- **Same method names** on the store (`GetLastIndexHeight`, `GetAllOwnersAndSCIDs`, `GetSCIDValuesByKey`, `GetSCIDKeysByValue`, `GetSCIDInteractionHeight`, `GetAllSCIDVariableDetails`).
- **Same Indexer shape** (`LastIndexedHeight`, `ChainHeight`, `DBType`, `GravDBBackend`, `BBSBackend` fields; `StartDaemonMode(n)`, `Close()` methods).
- **Same TELA variable-key semantics** — canonical spec compliance (`var_header_*` + legacy `nameHdr` fallback, hex-decode of stored strings, correct `fileCheckC/S` parsing).
- **Faster**: typed-encoded ClassMeta (3.2× marshal), O(1) TELA cache invalidation, turbo-mode scan default.

## What HyperGnomon does differently (but still drop-in)

- `dbType="gravdb"` — **accepted, but maps to bbolt** with a one-time warning. HyperGnomon is bbolt-only (graviton iterates in hash byte-sorted order with no prefix/range queries, so it can't serve the key-ordered Route B scans; civilware/Gnomon #24). A consumer that defaults to `gravdb` (e.g. HOLOGRAM) therefore runs unchanged, on bbolt. `storage.NewGravDB` no longer errors — it warns and returns a vestigial store so the caller's own error check passes; `storage.ErrGravDBNotSupported` is kept (deprecated) for source compatibility.
- **External-store injection now works.** `NewIndexer(gravDB, boltDB, …)` is wired end-to-end: the bbolt store you pre-open with `storage.NewBBoltDB(path, name)` is injected into the indexer, which **borrows** it — you keep ownership, and the facade's `Close()` releases it. `NewIndexerWithDBDir(path, …)` remains for callers who'd rather pass a path and let HyperGnomon open its own store.

## What HyperGnomon does NOT give you (yet)

- `runmode="wallet"` / `runmode="asset"` — only `"daemon"` is implemented. Other runmodes return a dead-indexer with `DBType==""`.

## The port in three sed commands

Against a repo whose imports today look like:

```go
import (
    "github.com/civilware/Gnomon/indexer"
    "github.com/civilware/Gnomon/storage"
    "github.com/civilware/Gnomon/structures"
)
```

Run these from the repo root (bash):

```bash
find . -name '*.go' -not -path './vendor/*' -exec sed -i \
    -e 's|github.com/civilware/Gnomon/indexer|github.com/hypergnomon/hypergnomon/pkg/gnomes/indexer|g' \
    -e 's|github.com/civilware/Gnomon/storage|github.com/hypergnomon/hypergnomon/pkg/gnomes/storage|g' \
    -e 's|github.com/civilware/Gnomon/structures|github.com/hypergnomon/hypergnomon/pkg/gnomes/structures|g' \
    {} +
```

(or use your editor's project-wide replace; the three substrings above are literal.)

Then update `go.mod`:

```bash
go mod edit -droprequire github.com/civilware/Gnomon
go get github.com/hypergnomon/hypergnomon@latest
go mod tidy
```

## The constructor

Which path you take depends on **which civilware/Gnomon `NewIndexer` your code targets** — its
signature changed between the frozen `main` line and the current feature line, and Go can't overload,
so one facade signature can't serve both. `NewIndexerWithDBDir` is the universal one-line swap that
works regardless.

### Already on the current 12-arg `NewIndexer` (e.g. HOLOGRAM) — import rewrite only

The compat `NewIndexer` mirrors civilware's **current** 12-arg signature exactly (`searchFilter []string`,
`mbllookup`, `fsc *FastSyncConfig`, `storeIntegrators`), so this call compiles and runs after only the
import rewrite — no constructor swap:

```go
gravDB, _ := storage.NewGravDB(path+"/gravdb", "25ms")
boltDB, _ := storage.NewBBoltDB(path+"/bolt", "name")
idx := indexer.NewIndexer(gravDB, boltDB, "gravdb",
    searchFilter /* []string */, 0, endpoint, "daemon",
    false /*mbllookup*/, false /*closeOnDisconnect*/, fastSyncConfig, exclusions, false /*storeIntegrators*/)
go idx.StartDaemonMode(5)
```

HyperGnomon has no graviton engine, so `dbType="gravdb"` is accepted **with a warning and runs on the
bbolt store you passed** as `boltDB` (it *borrows* it — you keep ownership; the facade's `Close()`
releases it). `idx.DBType` then reports `"boltdb"`, and `idx.GravDBBackend` is wired as a delegating
handle over that same bbolt store — so a consumer that reads through *either* `BBSBackend` or
`GravDBBackend` (HOLOGRAM does both, and hardcodes `GravDBBackend.GetSCIDInteractionHeight`) gets real
data. `AddSCIDToIndex(map[string]*FastSyncImport, …)` and `GetOwner` are present. The only hard
requirement: pass a pre-opened `boltDB` — a nil store, or a non-`"daemon"` runmode, yields a dead
indexer signalled by `DBType==""`.

### On civilware `main`'s 11-arg `NewIndexer` (dReams / Engram / TELA-CLI) — one-line swap

`main`'s `NewIndexer` has **11** args with `fastsync bool` at position 10 (no `*FastSyncConfig`, no
`storeIntegrators`). That signature is incompatible with the 12-arg form above, so do the documented
one-line swap to the native constructor (which also opens the store for you):

```go
idx, err := indexer.NewIndexerWithDBDir(path, filter, endpoint, "daemon", fastSyncConfig, exclusions)
if err != nil {
    return err
}
idx.StartDaemonMode(8)
defer idx.Close()
```

Notes:

- Recommended `parallelBlocks` is 8 (default). Civilware's 5 works; 16+ trips rate limits on remote daemons.
- `fastSyncConfig` is accepted for signature compatibility but **not applied in v1.0** — the constructor ignores it. To fastsync, run the `hypergnomon` binary with `--fastsync`, or call the native indexer's `Indexer.FastSync` method (`github.com/hypergnomon/hypergnomon/indexer`).

## Verifying the port

```bash
# 1. Rebuild.
go build ./...

# 2. Spot-check a few queries against a running daemon.
# Replace 203.0.113.10:10102 with your own.
go run ./path/to/your/main -daemon=203.0.113.10:10102

# 3. The indexer's scan log should look similar to civilware's —
# height progression, classify-probe counts, TELA refresh lines.
```

If something doesn't compile or behaves differently, open an issue at
https://github.com/hypergnomon/hypergnomon/issues with the civilware
import paths you were using and the error. v1.0 coverage is the
HOLOGRAM surface; any gap for another consumer is a release-blocker
bug, not an enhancement request.

## API surface matrix

| civilware/Gnomon symbol | HyperGnomon compat | Notes |
|---|---|---|
| `indexer.NewIndexer(…)` | ✓ fully wired to civilware's **current 12-arg** shape; `gravdb` accepted → runs on bbolt; non-`daemon` runmodes → dead indexer | import-only drop-in for current-API callers (HOLOGRAM); `main`'s 11-arg callers use `NewIndexerWithDBDir` |
| `indexer.NewIndexerWithDBDir(…)` | — | HyperGnomon-native entry |
| `indexer.InitLog(…)` | no-op accept | HyperGnomon uses logrus internally; inject via future Config.Logger |
| `indexer.Indexer.LastIndexedHeight` | present | atomic refresh at ~10 Hz |
| `indexer.Indexer.ChainHeight` | present | atomic refresh at ~10 Hz |
| `indexer.Indexer.DBType` | present | "boltdb" or "" (dead indexer) |
| `indexer.Indexer.GravDBBackend` | present but always nil | graviton is not supported |
| `indexer.Indexer.BBSBackend` | present, nil in v1.0 | access inner store via `Inner()` escape hatch |
| `storage.NewBBoltDB(path, name)` | ✓ | opens HyperGnomon's arena-optimized bbolt store |
| `storage.NewGravDB(path, interval)` | errors with `ErrGravDBNotSupported` | — |
| `storage.BboltStore.GetLastIndexHeight` | ✓ | |
| `storage.BboltStore.GetAllOwnersAndSCIDs` | ✓ | |
| `storage.BboltStore.GetAllSCIDVariableDetails` | ✓ | reads latest-height snapshot |
| `storage.BboltStore.GetSCIDValuesByKey` | ✓ | `any=true` walks all interaction heights |
| `storage.BboltStore.GetSCIDKeysByValue` | ✓ | |
| `storage.BboltStore.GetSCIDInteractionHeight` | ✓ | |
| `structures.FastSyncConfig` | ✓ (same struct shape) | accepted by constructors for signature compatibility; not applied in v1.0 — use `--fastsync` / native `Indexer.FastSync` |
| `structures.SCIDVariable` | ✓ (type alias) | |

Anything not in this matrix hasn't been needed by a known consumer. If your code imports something missing, open an issue with the import path + call site.
