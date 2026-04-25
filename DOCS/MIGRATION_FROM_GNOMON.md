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

## What HyperGnomon does NOT give you (yet)

- `dbType="gravdb"` — the graviton backend is bbolt-only here. Calls return `storage.ErrGravDBNotSupported` rather than crashing; see civilware/Gnomon issue #24 for why.
- `runmode="wallet"` / `runmode="asset"` — only `"daemon"` is implemented. Other runmodes return a dead-indexer with `DBType==""`.
- External-store injection — civilware's `NewIndexer(gravDB, boltDB, …)` expects the caller to pre-open the store. HyperGnomon's internal indexer opens its own; for v1.0 use `NewIndexerWithDBDir(…)` which takes a path. A v1.1 release will wire `NewIndexer(…)` end-to-end.

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

## The one constructor change

Civilware consumers construct the indexer like this:

```go
gravDB, _ := storage.NewGravDB(path+"/gravdb", "25ms")
boltDB, _ := storage.NewBBoltDB(path+"/bolt", "name")
idx := indexer.NewIndexer(gravDB, boltDB, "gravdb",
    filter, 0, endpoint, "daemon", false, false, fastSyncConfig, exclusions)
go idx.StartDaemonMode(5)
```

Under the compat layer that code still compiles — but `NewIndexer` detects `dbType="gravdb"` and returns a dead indexer. Switch to the HyperGnomon-native path:

```go
idx, err := indexer.NewIndexerWithDBDir(path, filter, endpoint, "daemon", fastSyncConfig, exclusions)
if err != nil {
    return err
}
idx.StartDaemonMode(8)
defer idx.Close()
```

Differences:

- No pre-opened store arguments — HyperGnomon opens its own bbolt store at the given `dbDir`.
- Error return — civilware's shape panics on misconfiguration; HyperGnomon returns an error you can handle.
- Recommended `parallelBlocks` is 8 (default). Civilware's 5 works; 16+ trips rate limits on remote daemons.

If you need to share a pre-opened store with another process (civilware callers sometimes do this), file an issue — external-store injection is tracked for v1.1.

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
| `indexer.NewIndexer(…)` | present but dead-stubs `gravdb` and non-daemon runmodes | use `NewIndexerWithDBDir` for v1.0 |
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
| `structures.FastSyncConfig` | ✓ (same struct shape) | |
| `structures.SCIDVariable` | ✓ (type alias) | |

Anything not in this matrix hasn't been needed by a known consumer. If your code imports something missing, open an issue with the import path + call site.
