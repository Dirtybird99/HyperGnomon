# HyperGnomon

Arena-accelerated DERO blockchain indexer.

## Build

```bash
go build -o hypergnomon ./cmd/hypergnomon/
```

## Run

```bash
./hypergnomon --daemon-rpc-address=127.0.0.1:10102
```

## Key flags

- `--daemon-rpc-address` - DERO daemon RPC (default: 127.0.0.1:10102)
- `--num-parallel-blocks` - Parallel block fetchers (default: 5)
- `--batch-size` - Blocks per DB flush (default: 100)
- `--rpc-pool-size` - WebSocket connection pool (default: 4)
- `--search-filter` - SC code filter (;;; separated)
- `--fastsync` - Bootstrap from GnomonSC registry
- `--mem-limit` - GOMEMLIMIT in bytes
- `--debug` - Enable debug logging

## Architecture

Arena-inspired patterns applied to Go:
- sync.Pool object recycling (SCTXParse, BlockTxns, WorkItem, byte buffers)
- Pre-allocated slices with [:0] reset per cycle
- Batch DB writes (100 blocks → 1 atomic BoltDB transaction)
- RPC connection pool (4 WebSocket connections)
- Batch GetTransaction (1 RPC call per block, not per TX)
- sync.Map for validated SCs, map[string]struct{} for exclusions
- GOMEMLIMIT tuning for GC optimization
- unique.Make string interning for SCIDs/addresses

## Test

```bash
go test ./... -v
go test ./storage/ -bench=. -benchmem
go test ./pool/ -bench=. -benchmem
```
