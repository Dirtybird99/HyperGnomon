package main

import "time"

type Config struct {
	Daemon          string
	Trials          int
	ProbeDuration   time.Duration
	ProbeWorkers    int
	ProbePaths      string
	ReadyLogPattern string
	ReadyTimeout    time.Duration
	TipTimeout      time.Duration
	RootDir         string
	APIPortStart    int
	WSPortStart     int
	BenchvsBinary   string
	JSONOut         string
	RunMarkdownOut  string
	SummaryOut      string
	DryRun          bool
}

type Target struct {
	Ref        string
	Name       string
	BinaryPath string
	WorkDir    string
	Workspace  bool
	ExtraArgs  []string
	// SupportsSeedCacheDir reports whether the built binary accepts
	// --classify-seed-cache-dir. Targets that do get a fresh per-trial seed
	// directory so the cross-DB classify seed cache cannot leak state between
	// trials (or from prior operator runs) and skew the A/B comparison.
	SupportsSeedCacheDir bool
}

type RunSpec struct {
	Target      Target
	Trial       int
	DBDir       string
	APIPort     int
	WSPort      int
	BenchvsArgs []string
}

type RunRecord struct {
	Name          string          `json:"name"`
	TargetRef     string          `json:"target_ref"`
	Trial         int             `json:"trial"`
	TimeToTipMS   float64         `json:"time_to_tip_ms"`
	TimeToReadyMS float64         `json:"time_to_ready_ms"`
	DBBytes       float64         `json:"db_bytes"`
	Latency       []LatencyRecord `json:"latency"`
}

type LatencyRecord struct {
	Path  string  `json:"path"`
	P50MS float64 `json:"p50_ms"`
	P95MS float64 `json:"p95_ms"`
	P99MS float64 `json:"p99_ms"`
}

type TargetSummary struct {
	Name    string
	Count   int
	TipMS   MetricSummary
	ReadyMS MetricSummary
	DBBytes MetricSummary
	Latency map[string]LatencySummary
}

type MetricSummary struct {
	Median   float64
	Min      float64
	Max      float64
	P95      float64
	DeltaPct float64
	Noise    bool
}

type LatencySummary struct {
	P50MSMedian float64
	P95MSMedian float64
	P99MSMedian float64
}
