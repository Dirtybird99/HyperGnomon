#!/usr/bin/env bash
# measure_classify.sh — measurement harness for the classify-corpus-g45
# optimization loop. Emits ONE JSON object on stdout:
#
#   {"allocs_op":N,"b_op":N,"ns_op":N,"allocs_deterministic":B,
#    "gates":{"tests_pass":B,"golden_pass":B},"ok":B}
#
# Gates run under -race; the metric run does NOT (the race runtime adds its
# own allocations and a 5-20x slowdown, corrupting both allocs/op and ns/op).
# allocs_op / b_op are the MEDIAN across repetitions; the determinism tripwire
# requires the allocs/op spread (max-min) <= 32 — the measured runtime noise
# floor is ~7 allocs on a ~2M-alloc op (stack growth / GC bookkeeping), while
# history-dependent caching shifts counts by orders of magnitude more.
# ns_op is the MIN across repetitions (~6% run-to-run jitter; diagnostic only).
set -u
cd "$(dirname "$0")/.."

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# Gate 1: full indexer package under -race (classify helpers are used by
# fastsync/tela paths in-package).
go test ./indexer/ -race -count=1 >"$TMP/tests.log" 2>&1
tests_pass=$?

# Gate 2: golden + equivalence in isolation, so a golden failure is
# distinguishable from an unrelated package-test failure in gate 1.
go test ./indexer/ -run '^(TestClassifyCorpusGolden|TestClassifyCorpusMapSliceEquivalence)$' -count=1 >"$TMP/golden.log" 2>&1
golden_pass=$?

# Metric: 10 full corpus passes per rep, 5 reps, no -race.
go test ./indexer/ -run '^$' -bench '^BenchmarkClassifyCorpus/Full$' -benchmem -benchtime=10x -count=5 >"$TMP/bench.log" 2>&1
bench_status=$?

awk -v tests_pass="$tests_pass" -v golden_pass="$golden_pass" -v bench_status="$bench_status" '
  $1 ~ /^BenchmarkClassifyCorpus\/Full-[0-9]+$/ {
    for (i = 2; i <= NF; i++) {
      if ($i == "ns/op")     { ns[++nns] = $(i-1) }
      if ($i == "B/op")      { bo[++nbo] = $(i-1) }
      if ($i == "allocs/op") { al[++nal] = $(i-1) }
    }
  }
  function median(arr, n,   i, j, tmp) {
    for (i = 2; i <= n; i++) { tmp = arr[i]; j = i - 1; while (j >= 1 && arr[j] + 0 > tmp + 0) { arr[j+1] = arr[j]; j-- } arr[j+1] = tmp }
    return arr[int((n + 1) / 2)]
  }
  END {
    det = 1
    if (nal == 0 || nal != nbo || nal != nns) det = 0
    minal = al[1]; maxal = al[1]
    for (i = 2; i <= nal; i++) {
      if (al[i] + 0 < minal + 0) minal = al[i]
      if (al[i] + 0 > maxal + 0) maxal = al[i]
    }
    if (nal > 0 && maxal - minal > 32) det = 0
    minns = (nns > 0) ? ns[1] : 0
    for (i = 2; i <= nns; i++) if (ns[i] + 0 < minns + 0) minns = ns[i]
    medal = (nal > 0) ? median(al, nal) : 0
    medbo = (nbo > 0) ? median(bo, nbo) : 0
    tp = (tests_pass == 0) ? "true" : "false"
    gp = (golden_pass == 0) ? "true" : "false"
    dt = (det == 1) ? "true" : "false"
    ok = (tests_pass == 0 && golden_pass == 0 && bench_status == 0 && det == 1 && nal >= 5 && medal + 0 > 0) ? "true" : "false"
    printf "{\"allocs_op\":%d,\"b_op\":%d,\"ns_op\":%d,\"allocs_deterministic\":%s,\"gates\":{\"tests_pass\":%s,\"golden_pass\":%s},\"ok\":%s}\n",
      medal, medbo, minns + 0, dt, tp, gp, ok
  }
' "$TMP/bench.log"

# Surface failure logs on stderr so a failed measurement is diagnosable
# without rerunning.
if [ "$tests_pass" -ne 0 ]; then echo "--- tests.log (gate 1 FAILED) ---" >&2; tail -50 "$TMP/tests.log" >&2; fi
if [ "$golden_pass" -ne 0 ]; then echo "--- golden.log (gate 2 FAILED) ---" >&2; tail -50 "$TMP/golden.log" >&2; fi
if [ "$bench_status" -ne 0 ]; then echo "--- bench.log (bench FAILED) ---" >&2; tail -50 "$TMP/bench.log" >&2; fi
