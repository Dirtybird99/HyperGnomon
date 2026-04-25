# benchvs — single-target DERO indexer benchmark

Measures one indexer's time-to-tip, DB size at tip, and API latency under concurrent probe load. Designed to be run **twice** against the same daemon — once per indexer — so the results accumulate in one markdown document (`bench_vs_civilware.md`).

Running two indexers concurrently against the same daemon would starve whichever started second via RPC contention; sequential runs at full daemon bandwidth are the honest comparison.

## Build

```bash
go build -o benchvs ./cmd/benchvs
```

## HyperGnomon side

```bash
./benchvs \
    --name=HyperGnomon \
    --binary=./hypergnomon \
    --daemon=192.168.2.251:10102 \
    --db-dir=/tmp/hg-bench \
    --probe-duration=60s \
    --probe-workers=32 \
    --out=bench_vs_civilware.md
```

This launches HyperGnomon with `--daemon-rpc-address=<daemon> --db-dir=<db-dir>` plus any extra args you pass via `--args`. It polls `/api/getinfo` + `/api/getstats` until the indexer is within `STABLE_LIMIT=8` of daemon tip, then runs 32 concurrent probe workers for the configured duration, then appends a markdown section to `--out`.

## civilware/Gnomon side

civilware/Gnomon is primarily a Go library — there's no turnkey `cmd/gnomon` binary that mirrors HyperGnomon's flags. The operator supplies a small wrapper program that embeds civilware and exposes an HTTP surface with the same two JSON fields benchvs reads: `/api/getinfo.TopoHeight` and `/api/getstats.index_height`.

Minimal wrapper (`cmd/civilware-runner/main.go` in your fork):

```go
package main

import (
    "encoding/json"
    "flag"
    "net/http"

    "github.com/civilware/Gnomon/indexer"
    "github.com/civilware/Gnomon/storage"
    "github.com/civilware/Gnomon/structures"
)

func main() {
    daemon := flag.String("daemon-rpc-address", "127.0.0.1:10102", "")
    dbDir  := flag.String("db-dir", "./cw-db", "")
    flag.Parse()

    boltDB, _ := storage.NewBBoltDB(*dbDir+"/bolt", "scids")
    cfg := &structures.FastSyncConfig{Enabled: true, SkipFSRecheck: true}
    idx := indexer.NewIndexer(nil, boltDB, "boltdb",
        "telaVersion;;;docVersion", 0, *daemon, "daemon",
        false, false, cfg, nil)
    go idx.StartDaemonMode(8)

    http.HandleFunc("/api/getinfo", func(w http.ResponseWriter, _ *http.Request) {
        _ = json.NewEncoder(w).Encode(map[string]int64{"TopoHeight": idx.ChainHeight})
    })
    http.HandleFunc("/api/getstats", func(w http.ResponseWriter, _ *http.Request) {
        _ = json.NewEncoder(w).Encode(map[string]int64{"index_height": idx.LastIndexedHeight})
    })
    _ = http.ListenAndServe("127.0.0.1:8083", nil)
}
```

Then point benchvs at it on a different API port:

```bash
./benchvs \
    --name="civilware/Gnomon@dev" \
    --binary=./civilware-runner \
    --daemon=192.168.2.251:10102 \
    --db-dir=/tmp/cw-bench \
    --api-url=http://127.0.0.1:8083 \
    --probe-paths=/api/getinfo,/api/getstats \
    --out=bench_vs_civilware.md
```

The same `--out` file accumulates both sections.

## What benchvs does not measure (v1.0)

- **RSS** at steady state. Cross-platform child-process memory sampling would require either a platform-specific shim (Linux: `/proc/<pid>/status`; macOS: `proc_pidinfo`; Windows: `GetProcessMemoryInfo`) or child-side cooperation (subprocess exposes its own `runtime.MemStats` on an HTTP endpoint). Both are tractable; neither is a v1.0 blocker. Track in a follow-up.
- **Concurrent indexer runs.** The harness intentionally measures one at a time — see the rationale at the top.

## Flags

Run `benchvs -h` for the full list. The knobs most operators will touch:

| Flag | Default | Purpose |
|---|---|---|
| `--name` | `HyperGnomon` | Label for this run in the markdown |
| `--binary` | `./hypergnomon` | Path to the indexer binary |
| `--args` | `""` | Space-separated extra args appended to the binary's argv |
| `--daemon` | `127.0.0.1:10102` | Daemon RPC address (injected via `--daemon-flag`) |
| `--db-dir` | _required_ | Fresh DB directory (removed + recreated before launch; measured at teardown) |
| `--api-url` | `http://127.0.0.1:8082` | Indexer API base URL |
| `--probe-paths` | `/api/getinfo,/api/getstats,/api/getscids` | Paths probed for latency |
| `--probe-workers` | `32` | Concurrent probe workers |
| `--probe-duration` | `60s` | Probe window after reaching tip |
| `--tip-timeout` | `15m` | Max wait for indexer to reach tip |
| `--out` | `bench_vs_civilware.md` | Markdown file (appended) |
| `--daemon-flag` | `--daemon-rpc-address` | Name of the daemon flag on the target binary |
| `--db-dir-flag` | `--db-dir` | Name of the db-dir flag on the target binary |
