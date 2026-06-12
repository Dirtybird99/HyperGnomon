package main

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDefaultTargetsCompareReleaseMainAndWorkspace(t *testing.T) {
	got := defaultTargets()
	want := []string{"v1.0.0", "origin/main", "workspace"}
	if len(got) != len(want) {
		t.Fatalf("defaultTargets len = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i].Ref != want[i] {
			t.Fatalf("defaultTargets[%d].Ref = %q, want %q", i, got[i].Ref, want[i])
		}
	}
}

func TestParseTargetsSupportsLabelAndExtraArgs(t *testing.T) {
	targets, err := parseTargets("main=origin/main,workspace-pool16=workspace|--rpc-pool-size=16 --classify-probe-batch-size=800")
	if err != nil {
		t.Fatalf("parseTargets: %v", err)
	}
	if len(targets) != 2 {
		t.Fatalf("targets len = %d, want 2", len(targets))
	}
	if targets[0].Name != "main" || targets[0].Ref != "origin/main" || targets[0].Workspace {
		t.Fatalf("main target = %+v, want named origin/main remote target", targets[0])
	}
	if targets[1].Name != "workspace-pool16" || targets[1].Ref != "workspace" || !targets[1].Workspace {
		t.Fatalf("workspace target = %+v, want named workspace target", targets[1])
	}
	wantArgs := []string{"--rpc-pool-size=16", "--classify-probe-batch-size=800"}
	if len(targets[1].ExtraArgs) != len(wantArgs) {
		t.Fatalf("extra args len = %d, want %d: %+v", len(targets[1].ExtraArgs), len(wantArgs), targets[1].ExtraArgs)
	}
	for i, want := range wantArgs {
		if targets[1].ExtraArgs[i] != want {
			t.Fatalf("ExtraArgs[%d] = %q, want %q", i, targets[1].ExtraArgs[i], want)
		}
	}
}

func TestBuildRunSpecsUsesFreshDBsAndUniquePorts(t *testing.T) {
	cfg := Config{
		Daemon:          "203.0.113.10:10102",
		Trials:          2,
		ProbeDuration:   time.Minute,
		ProbeWorkers:    32,
		ProbePaths:      "/api/getinfo,/api/getstats",
		ReadyLogPattern: "Classify probe complete",
		ReadyTimeout:    5 * time.Minute,
		TipTimeout:      15 * time.Minute,
		RootDir:         `C:\tmp\matrix`,
		APIPortStart:    18082,
		WSPortStart:     19190,
	}
	target := Target{Ref: "workspace", Name: "workspace", BinaryPath: `C:\tmp\matrix\bin\hypergnomon-workspace.exe`}

	specs := buildRunSpecs(cfg, []Target{target})
	if len(specs) != 2 {
		t.Fatalf("run spec count = %d, want 2", len(specs))
	}
	if specs[0].DBDir == specs[1].DBDir {
		t.Fatalf("DBDir reused across trials: %q", specs[0].DBDir)
	}
	if specs[0].APIPort == specs[1].APIPort || specs[0].WSPort == specs[1].WSPort {
		t.Fatalf("ports reused across trials: first=%+v second=%+v", specs[0], specs[1])
	}
	joined := strings.Join(specs[0].BenchvsArgs, " ")
	for _, want := range []string{
		"--target-ref=workspace",
		"--trial=1",
		"--ready-log-pattern=Classify probe complete",
		"--args=--fastsync --timing --timing-every=10 --api-address=127.0.0.1:18082 --ws-address=127.0.0.1:19190",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("benchvs args missing %q:\n%s", want, joined)
		}
	}
}

func TestBuildRunSpecsAppendsTargetExtraArgs(t *testing.T) {
	cfg := Config{
		Daemon:          "203.0.113.10:10102",
		Trials:          1,
		ProbeDuration:   time.Minute,
		ProbeWorkers:    32,
		ProbePaths:      "/api/getinfo",
		ReadyLogPattern: "Classify probe complete",
		ReadyTimeout:    5 * time.Minute,
		TipTimeout:      15 * time.Minute,
		RootDir:         `C:\tmp\matrix`,
		APIPortStart:    18082,
		WSPortStart:     19190,
	}
	target := Target{
		Ref:        "workspace",
		Name:       "workspace-pool16",
		BinaryPath: `C:\tmp\matrix\bin\hypergnomon-workspace.exe`,
		Workspace:  true,
		ExtraArgs:  []string{"--rpc-pool-size=16", "--classify-probe-batch-size=800"},
	}

	specs := buildRunSpecs(cfg, []Target{target})
	if len(specs) != 1 {
		t.Fatalf("run spec count = %d, want 1", len(specs))
	}
	joined := strings.Join(specs[0].BenchvsArgs, " ")
	want := "--args=--fastsync --timing --timing-every=10 --api-address=127.0.0.1:18082 --ws-address=127.0.0.1:19190 --rpc-pool-size=16 --classify-probe-batch-size=800"
	if !strings.Contains(joined, want) {
		t.Fatalf("benchvs args missing target extras:\nwant substring: %s\nactual: %s", want, joined)
	}
}

func TestBuildRunSpecsIsolatesSeedCachePerTrial(t *testing.T) {
	cfg := Config{
		Daemon:          "203.0.113.10:10102",
		Trials:          2,
		ProbeDuration:   time.Minute,
		ProbeWorkers:    32,
		ProbePaths:      "/api/getinfo",
		ReadyLogPattern: "Classify probe complete",
		ReadyTimeout:    5 * time.Minute,
		TipTimeout:      15 * time.Minute,
		RootDir:         `C:\tmp\matrix`,
		APIPortStart:    18082,
		WSPortStart:     19190,
	}
	seedCapable := Target{
		Ref:                  "workspace",
		Name:                 "workspace",
		BinaryPath:           `C:\tmp\matrix\bin\hypergnomon-workspace.exe`,
		Workspace:            true,
		SupportsSeedCacheDir: true,
	}
	legacy := Target{Ref: "v1.0.0", Name: "v1.0.0", BinaryPath: `C:\tmp\matrix\bin\hypergnomon-v1.0.0.exe`}

	specs := buildRunSpecs(cfg, []Target{seedCapable, legacy})
	if len(specs) != 4 {
		t.Fatalf("run spec count = %d, want 4", len(specs))
	}

	first := strings.Join(specs[0].BenchvsArgs, " ")
	second := strings.Join(specs[1].BenchvsArgs, " ")
	for i, joined := range []string{first, second} {
		if !strings.Contains(joined, "--classify-seed-cache-dir=") {
			t.Fatalf("seed-capable trial %d missing seed isolation arg:\n%s", i+1, joined)
		}
	}
	seed1 := filepath.Join(cfg.RootDir, "seed", "workspace", "trial-01")
	seed2 := filepath.Join(cfg.RootDir, "seed", "workspace", "trial-02")
	if !strings.Contains(first, seed1) || !strings.Contains(second, seed2) {
		t.Fatalf("seed dirs not per-trial:\ntrial1: %s\ntrial2: %s", first, second)
	}

	for _, spec := range specs[2:] {
		joined := strings.Join(spec.BenchvsArgs, " ")
		if strings.Contains(joined, "--classify-seed-cache-dir=") {
			t.Fatalf("legacy target got seed flag it cannot parse:\n%s", joined)
		}
	}
}

func TestHelpMentionsFlag(t *testing.T) {
	const help = `Usage of hypergnomon:
  -classify-seed-cache-dir string
        Cross-DB classify seed cache directory (empty = OS user cache)
  -daemon-rpc-address string
`
	if !helpMentionsFlag(help, "classify-seed-cache-dir") {
		t.Fatal("flag present in help but not detected")
	}
	if helpMentionsFlag("Usage of hypergnomon:\n  -daemon-rpc-address string\n", "classify-seed-cache-dir") {
		t.Fatal("flag absent from help but detected")
	}
}

func TestParseJSONLRunsAndSummarize(t *testing.T) {
	const body = `
{"name":"origin/main","target_ref":"origin/main","trial":1,"time_to_tip_ms":1000,"time_to_ready_ms":100000,"db_bytes":200000000,"latency":[{"path":"/api/getinfo","p50_ms":1,"p95_ms":7,"p99_ms":14}]}
{"name":"origin/main","target_ref":"origin/main","trial":2,"time_to_tip_ms":1100,"time_to_ready_ms":110000,"db_bytes":200000000,"latency":[{"path":"/api/getinfo","p50_ms":1,"p95_ms":8,"p99_ms":14}]}
{"name":"origin/main","target_ref":"origin/main","trial":3,"time_to_tip_ms":1200,"time_to_ready_ms":120000,"db_bytes":200000000,"latency":[{"path":"/api/getinfo","p50_ms":1,"p95_ms":9,"p99_ms":15}]}
{"name":"origin/main","target_ref":"origin/main","trial":4,"time_to_tip_ms":1300,"time_to_ready_ms":130000,"db_bytes":200000000,"latency":[{"path":"/api/getinfo","p50_ms":2,"p95_ms":10,"p99_ms":16}]}
{"name":"origin/main","target_ref":"origin/main","trial":5,"time_to_tip_ms":1400,"time_to_ready_ms":140000,"db_bytes":200000000,"latency":[{"path":"/api/getinfo","p50_ms":2,"p95_ms":11,"p99_ms":17}]}
{"name":"workspace","target_ref":"workspace","trial":1,"time_to_tip_ms":900,"time_to_ready_ms":90000,"db_bytes":100000000,"latency":[{"path":"/api/getinfo","p50_ms":1,"p95_ms":6,"p99_ms":12}]}
{"name":"workspace","target_ref":"workspace","trial":2,"time_to_tip_ms":950,"time_to_ready_ms":95000,"db_bytes":100000000,"latency":[{"path":"/api/getinfo","p50_ms":1,"p95_ms":7,"p99_ms":13}]}
{"name":"workspace","target_ref":"workspace","trial":3,"time_to_tip_ms":1000,"time_to_ready_ms":100000,"db_bytes":100000000,"latency":[{"path":"/api/getinfo","p50_ms":1,"p95_ms":8,"p99_ms":14}]}
{"name":"workspace","target_ref":"workspace","trial":4,"time_to_tip_ms":1050,"time_to_ready_ms":105000,"db_bytes":100000000,"latency":[{"path":"/api/getinfo","p50_ms":2,"p95_ms":9,"p99_ms":15}]}
{"name":"workspace","target_ref":"workspace","trial":5,"time_to_tip_ms":1100,"time_to_ready_ms":110000,"db_bytes":100000000,"latency":[{"path":"/api/getinfo","p50_ms":2,"p95_ms":10,"p99_ms":16}]}
{"name":"v1.0.0","target_ref":"v1.0.0","trial":1,"time_to_tip_ms":1160,"time_to_ready_ms":116000,"db_bytes":200000000}
{"name":"v1.0.0","target_ref":"v1.0.0","trial":2,"time_to_tip_ms":1180,"time_to_ready_ms":118000,"db_bytes":200000000}
{"name":"v1.0.0","target_ref":"v1.0.0","trial":3,"time_to_tip_ms":1200,"time_to_ready_ms":120000,"db_bytes":200000000}
{"name":"v1.0.0","target_ref":"v1.0.0","trial":4,"time_to_tip_ms":1220,"time_to_ready_ms":122000,"db_bytes":200000000}
{"name":"v1.0.0","target_ref":"v1.0.0","trial":5,"time_to_tip_ms":1240,"time_to_ready_ms":124000,"db_bytes":200000000}
`
	records, err := parseJSONLRuns(strings.NewReader(body))
	if err != nil {
		t.Fatalf("parseJSONLRuns: %v", err)
	}
	if len(records) != 15 {
		t.Fatalf("record count = %d, want 15", len(records))
	}

	summaries := summarizeRuns(records, "origin/main")
	workspace := findSummary(t, summaries, "workspace")
	if workspace.ReadyMS.Median != 100000 {
		t.Fatalf("workspace ready median = %.0f, want 100000", workspace.ReadyMS.Median)
	}
	if workspace.ReadyMS.P95 != 110000 {
		t.Fatalf("workspace ready p95 = %.0f, want 110000", workspace.ReadyMS.P95)
	}
	if workspace.ReadyMS.Noise {
		t.Fatalf("workspace ready delta should be signal, got noise")
	}
	if workspace.DBBytes.DeltaPct != -50 {
		t.Fatalf("workspace DB delta = %.1f, want -50.0", workspace.DBBytes.DeltaPct)
	}
	if got := workspace.Latency["/api/getinfo"].P95MSMedian; got != 8 {
		t.Fatalf("workspace /api/getinfo p95 median = %.0f, want 8", got)
	}

	release := findSummary(t, summaries, "v1.0.0")
	if !release.ReadyMS.Noise {
		t.Fatalf("v1.0.0 ready delta should be labeled as noise")
	}
}

func TestSummarizeRunsMatchesBaselineByTargetRef(t *testing.T) {
	records := []RunRecord{
		{Name: "main", TargetRef: "origin/main", Trial: 1, TimeToReadyMS: 1000, DBBytes: 2000},
		{Name: "main", TargetRef: "origin/main", Trial: 2, TimeToReadyMS: 1000, DBBytes: 2000},
		{Name: "workspace", TargetRef: "workspace", Trial: 1, TimeToReadyMS: 500, DBBytes: 1000},
		{Name: "workspace", TargetRef: "workspace", Trial: 2, TimeToReadyMS: 500, DBBytes: 1000},
	}

	summaries := summarizeRuns(records, "origin/main")
	workspace := findSummary(t, summaries, "workspace")
	if workspace.ReadyMS.DeltaPct != -50 {
		t.Fatalf("workspace ready delta = %.1f, want -50.0", workspace.ReadyMS.DeltaPct)
	}
	if workspace.DBBytes.DeltaPct != -50 {
		t.Fatalf("workspace DB delta = %.1f, want -50.0", workspace.DBBytes.DeltaPct)
	}
}

func findSummary(t *testing.T, summaries []TargetSummary, name string) TargetSummary {
	t.Helper()
	for _, summary := range summaries {
		if summary.Name == name {
			return summary
		}
	}
	t.Fatalf("summary %q not found in %+v", name, summaries)
	return TargetSummary{}
}
