package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAppendJSONReportWritesMachineReadableRun(t *testing.T) {
	path := filepath.Join(t.TempDir(), "benchmatrix.jsonl")
	result := Result{
		Name:          "workspace",
		TargetRef:     "workspace",
		Trial:         3,
		Binary:        "hypergnomon.exe",
		Daemon:        "203.0.113.10:10102",
		GoVersion:     "go1.26.0",
		HostOS:        "windows/amd64",
		Timestamp:     "2026-05-07T12:00:00Z",
		TipReached:    true,
		TimeToTip:     1500 * time.Millisecond,
		ReadyPattern:  "Classify probe complete",
		ReadyReached:  true,
		TimeToReady:   2500 * time.Millisecond,
		DBBytes:       137460609,
		ProbeDuration: 60 * time.Second,
		ProbeWorkers:  32,
		ProbePaths:    []string{"/api/getinfo"},
		Latency: []PathLatency{{
			Path:   "/api/getinfo",
			N:      10,
			Errors: 1,
			P50:    time.Millisecond,
			P95:    8 * time.Millisecond,
			P99:    13 * time.Millisecond,
			Max:    20 * time.Millisecond,
		}},
		ClassifyTiming: "Classify probe timings: phase1_rpc=3m19.902s",
		LogPath:        `C:\tmp\benchvs.log`,
	}

	if err := appendJSONReport(path, result); err != nil {
		t.Fatalf("appendJSONReport: %v", err)
	}

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) != 1 {
		t.Fatalf("JSONL line count = %d, want 1; body=%q", len(lines), body)
	}

	var got struct {
		Name          string `json:"name"`
		TargetRef     string `json:"target_ref"`
		Trial         int    `json:"trial"`
		Binary        string `json:"binary"`
		Daemon        string `json:"daemon"`
		TimeToTipMS   int64  `json:"time_to_tip_ms"`
		TimeToReadyMS int64  `json:"time_to_ready_ms"`
		DBBytes       int64  `json:"db_bytes"`
		ProbeWorkers  int    `json:"probe_workers"`
		LogPath       string `json:"log_path"`
		Latency       []struct {
			Path  string `json:"path"`
			P95MS int64  `json:"p95_ms"`
		} `json:"latency"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &got); err != nil {
		t.Fatalf("Unmarshal JSONL row: %v\nrow=%s", err, lines[0])
	}

	if got.Name != "workspace" || got.TargetRef != "workspace" || got.Trial != 3 {
		t.Fatalf("identity fields = (%q, %q, %d), want workspace/workspace/3", got.Name, got.TargetRef, got.Trial)
	}
	if got.TimeToTipMS != 1500 || got.TimeToReadyMS != 2500 {
		t.Fatalf("duration ms = (%d, %d), want (1500, 2500)", got.TimeToTipMS, got.TimeToReadyMS)
	}
	if got.DBBytes != 137460609 || got.ProbeWorkers != 32 || got.LogPath != result.LogPath {
		t.Fatalf("metadata mismatch: %+v", got)
	}
	if len(got.Latency) != 1 || got.Latency[0].Path != "/api/getinfo" || got.Latency[0].P95MS != 8 {
		t.Fatalf("latency row = %+v, want /api/getinfo p95=8ms", got.Latency)
	}
}

func TestAppendJSONReportAppendsRowsInOrder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "benchmatrix.jsonl")
	for trial := 1; trial <= 2; trial++ {
		if err := appendJSONReport(path, Result{Name: "workspace", Trial: trial}); err != nil {
			t.Fatalf("appendJSONReport trial %d: %v", trial, err)
		}
	}

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) != 2 {
		t.Fatalf("JSONL line count = %d, want 2; body=%q", len(lines), body)
	}
	for i, line := range lines {
		var got struct {
			Trial int `json:"trial"`
		}
		if err := json.Unmarshal([]byte(line), &got); err != nil {
			t.Fatalf("row %d unparseable: %v\nrow=%s", i, err, line)
		}
		if got.Trial != i+1 {
			t.Fatalf("row %d trial = %d, want %d (append order)", i, got.Trial, i+1)
		}
	}
}

func TestWriteReportAppendsHeaderOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bench.md")
	for _, name := range []string{"hypergnomon", "gnomon"} {
		if err := writeReport(path, Result{Name: name, Binary: name + ".exe"}); err != nil {
			t.Fatalf("writeReport %s: %v", name, err)
		}
	}

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	text := string(body)
	if got := strings.Count(text, "# Head-to-head: HyperGnomon vs civilware/Gnomon"); got != 1 {
		t.Fatalf("top-level header count = %d, want exactly 1:\n%s", got, text)
	}
	for _, section := range []string{"## hypergnomon", "## gnomon"} {
		if got := strings.Count(text, section+"\n"); got != 1 {
			t.Fatalf("section %q count = %d, want exactly 1:\n%s", section, got, text)
		}
	}
}

func TestWriteReportStillWritesMarkdownOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bench.md")
	if err := writeReport(path, Result{Name: "workspace", Binary: "hypergnomon.exe"}); err != nil {
		t.Fatalf("writeReport: %v", err)
	}

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	text := string(body)
	if !strings.Contains(text, "# Head-to-head: HyperGnomon vs civilware/Gnomon") {
		t.Fatalf("markdown header missing:\n%s", text)
	}
	if strings.Contains(text, "target_ref") {
		t.Fatalf("markdown report unexpectedly contains JSON field names:\n%s", text)
	}
}
