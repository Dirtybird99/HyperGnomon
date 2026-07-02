# HyperGnomon

Arena-accelerated DERO blockchain indexer. See [README.md](README.md) for the full feature surface, comparison tables, and reproducible benchmarks — this file is the short operator cheat-sheet.

## Build

```bash
go build -o hypergnomon ./cmd/hypergnomon/
```

## Run

```bash
./hypergnomon --daemon-rpc-address=127.0.0.1:10102 --fastsync
```

## Key flags (shipped defaults)

- `--daemon-rpc-address` — DERO daemon RPC (default: `127.0.0.1:10102`)
- `--num-parallel-blocks` — Parallel block fetchers (default: `8`)
- `--batch-size` — Blocks per DB flush (default: `100`; adaptive up to `1000`)
- `--rpc-pool-size` — WebSocket connection pool (default: `8`)
- `--persist-install-code` — sccode policy: `none|tela|all` (default: `tela`)
- `--tela-verify-sigs` — Emit `X-TELA-Verify` response header on `/tela/…` (default: `false`)
- `--search-filter` — SC code filter (`;;;` separated)
- `--fastsync` — Bootstrap from GnomonSC registry
- `--turbo` — Skip per-SC variable fetch during scan (default: `true`)
- `--mem-limit` — `GOMEMLIMIT` in bytes
- `--cpuprofile` — Whole-run CPU profile file, flushed on shutdown (PGO refresh source)
- `--debug` — Enable debug logging

Full flag reference with rationale for each default: [DOCS/FLAGS.md](DOCS/FLAGS.md).

## Architecture

Arena-inspired patterns applied to Go:
- `sync.Pool` object recycling (`SCTXParse`, `BlockTxns`, `WorkItem`, byte buffers)
- Pre-allocated slices with `[:0]` reset per cycle
- Batch DB writes (N blocks → 1 atomic bbolt transaction)
- RPC connection pool (8 WebSocket connections)
- Batch `GetTransaction` (1 RPC call per block, not per TX)
- `sync.Map` for validated SCs, `map[string]struct{}` for exclusions
- `GOMEMLIMIT` tuning for GC optimization
- `unique.Make` string interning for SCIDs/addresses
- Typed binary encoding for ClassMeta / SCIDVariables / SCCodeEntry / AddrSCIDEntry / InstallRecord / turbo SCTXParse hot paths

## Test

```bash
go test ./... -v -race
go test ./storage/ -bench=. -benchmem
go test ./pool/ -bench=. -benchmem
go test ./structures/ -bench=. -benchmem
```

## Library mode

Go consumers migrating from `civilware/Gnomon` import `github.com/hypergnomon/hypergnomon/pkg/gnomes/{indexer,storage,structures}` — three-sed migration plus one constructor swap. See [DOCS/MIGRATION_FROM_GNOMON.md](DOCS/MIGRATION_FROM_GNOMON.md).
