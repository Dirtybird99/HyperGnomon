# benchmatrix — release A/B matrix orchestrator

Use `cmd/benchmatrix` when comparing HyperGnomon release tags, `origin/main`,
and the current workspace branch. It is a sequential orchestrator around
[`cmd/benchvs`](../benchvs/README.md): each trial gets a fresh DB directory,
unique API/WS ports, and the same daemon bandwidth. It never runs targets
concurrently.

Methodology background and result interpretation live in
[DOCS/BENCHMARKS.md — "Release A/B matrix"](../../DOCS/BENCHMARKS.md#release-ab-matrix).

## Usage

Default release check:

```bash
go run ./cmd/benchmatrix \
    --daemon=203.0.113.10:10102 \
    --trials=5 \
    --probe-duration=60s
```

LAN tuning sweep:

```bash
go run ./cmd/benchmatrix \
    --daemon=203.0.113.10:10102 \
    --trials=5 \
    --targets="main=origin/main,workspace-pool8=workspace,workspace-pool12=workspace|--rpc-pool-size=12,workspace-pool16=workspace|--rpc-pool-size=16"
```

Dry-run (prints the build/run plan without fetching, building, or running):

```bash
go run ./cmd/benchmatrix --dry-run --trials=1 --probe-duration=5s
```

## Defaults

- Targets: `v1.0.0`, `origin/main`, `workspace`.
- Readiness marker: `Classify probe complete` (passed to benchvs as `--ready-log-pattern`).
- Target args: `--fastsync --timing --timing-every=10` (plus per-trial `--api-address` / `--ws-address`).
- Output root: timestamped `hypergnomon-benchmatrix` directory under the system temp dir (`--out-dir` overrides).

Non-`workspace` targets are built from detached git worktrees of the named
ref; the `workspace` target is intentionally built from the active checkout,
including uncommitted changes, so it is the right target for validating
pending optimization branches.

Target entries support `label=ref|extra args`; the extra args are appended to
the launched indexer only for that target. Use this for pool-size and
classify-batch sweeps without adding wrapper scripts.

## Outputs

- `benchmatrix.jsonl` — one machine-readable row per `benchvs` run.
- `benchmatrix.md` — median/p95 comparison table with deltas versus `origin/main`.
- `benchmatrix-runs.md` — the raw per-run `benchvs` markdown sections.
- Per-run child logs next to the DB directories.

Interpretation rule: deltas under 5%, or deltas where matched trials do not
all move in the same direction, are labeled as noise.

## Classify seed cache isolation

Current HyperGnomon builds maintain a cross-DB classify seed cache under the
OS user cache directory (or `--classify-seed-cache-dir` when set). On clean DB
runs, a matching seed can skip most of the classify probe — which would skew
an A/B comparison if one trial silently hit a seed written by an earlier
trial or a prior operator run.

`benchmatrix` isolates this cache automatically: for any target binary whose
`--help` advertises `--classify-seed-cache-dir`, each trial gets a fresh seed
directory under the run root, so every trial measures the cold full-probe
path. Targets predating the flag (e.g. `v1.0.0`) launch without it. To
measure the warm seed-hit path on purpose, pin a shared directory via target
extra args: `workspace-seeded=workspace|--classify-seed-cache-dir=C:\seedpin`.

## Flags

| Flag | Default | Purpose |
|---|---|---|
| `--daemon` | `203.0.113.10:10102` | DERO daemon RPC address |
| `--trials` | `5` | Clean sequential trials per target |
| `--probe-duration` | `60s` | API probe duration per trial |
| `--probe-workers` | `32` | Concurrent API probe workers |
| `--probe-paths` | `/api/getinfo,/api/getstats,/api/getscids` | Comma-separated API paths to probe |
| `--ready-log-pattern` | `Classify probe complete` | Child-log marker before API probes |
| `--ready-timeout` | `5m` | Max wait for readiness marker |
| `--tip-timeout` | `15m` | Max wait for target to reach tip |
| `--out-dir` | `""` | Output/temp root (empty = timestamped dir under system temp) |
| `--api-port-start` | `18082` | First API port assigned to trial targets |
| `--ws-port-start` | `19190` | First WS port assigned to trial targets |
| `--targets` | `v1.0.0,origin/main,workspace` | Comma-separated refs; `label=ref\|extra args` supported |
| `--dry-run` | `false` | Print the plan without building or running |
