package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"
)

func parseJSONLRuns(r io.Reader) ([]RunRecord, error) {
	scanner := bufio.NewScanner(r)
	var records []RunRecord
	for lineNo := 1; scanner.Scan(); lineNo++ {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var record RunRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNo, err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return records, nil
}

func summarizeRuns(records []RunRecord, baselineName string) []TargetSummary {
	groups := groupRuns(records)
	names := make([]string, 0, len(groups))
	for name := range groups {
		names = append(names, name)
	}
	sort.Strings(names)

	baseline := baselineRuns(records, groups, baselineName)
	out := make([]TargetSummary, 0, len(names))
	for _, name := range names {
		runs := groups[name]
		sort.Slice(runs, func(i, j int) bool { return runs[i].Trial < runs[j].Trial })
		summary := TargetSummary{
			Name:    name,
			Count:   len(runs),
			TipMS:   summarizeMetric(runs, baseline, func(r RunRecord) float64 { return r.TimeToTipMS }),
			ReadyMS: summarizeMetric(runs, baseline, func(r RunRecord) float64 { return r.TimeToReadyMS }),
			DBBytes: summarizeMetric(runs, baseline, func(r RunRecord) float64 { return r.DBBytes }),
			Latency: summarizeLatency(runs),
		}
		out = append(out, summary)
	}
	return out
}

func baselineRuns(records []RunRecord, groups map[string][]RunRecord, baselineName string) []RunRecord {
	if runs := groups[baselineName]; len(runs) > 0 {
		return runs
	}
	for _, record := range records {
		if record.TargetRef != baselineName {
			continue
		}
		name := record.Name
		if name == "" {
			name = record.TargetRef
		}
		if runs := groups[name]; len(runs) > 0 {
			return runs
		}
	}
	return nil
}

func groupRuns(records []RunRecord) map[string][]RunRecord {
	groups := make(map[string][]RunRecord)
	for _, record := range records {
		name := record.Name
		if name == "" {
			name = record.TargetRef
		}
		groups[name] = append(groups[name], record)
	}
	return groups
}

func summarizeMetric(runs, baseline []RunRecord, value func(RunRecord) float64) MetricSummary {
	values := make([]float64, 0, len(runs))
	for _, run := range runs {
		values = append(values, value(run))
	}
	stats := metricStats(values)
	if len(baseline) == 0 {
		stats.Noise = true
		return stats
	}
	baseValues := make([]float64, 0, len(baseline))
	for _, run := range baseline {
		baseValues = append(baseValues, value(run))
	}
	baseMedian := metricStats(baseValues).Median
	if baseMedian == 0 {
		stats.Noise = true
		return stats
	}
	stats.DeltaPct = (stats.Median - baseMedian) / baseMedian * 100
	stats.Noise = math.Abs(stats.DeltaPct) < 5 || !trialsAgreeDirection(runs, baseline, stats.DeltaPct, value)
	return stats
}

func metricStats(values []float64) MetricSummary {
	if len(values) == 0 {
		return MetricSummary{Noise: true}
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	return MetricSummary{
		Median: median(sorted),
		Min:    sorted[0],
		Max:    sorted[len(sorted)-1],
		P95:    nearestRank(sorted, 0.95),
	}
}

func median(sorted []float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	mid := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[mid]
	}
	return (sorted[mid-1] + sorted[mid]) / 2
}

func nearestRank(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func trialsAgreeDirection(runs, baseline []RunRecord, deltaPct float64, value func(RunRecord) float64) bool {
	if deltaPct == 0 {
		return false
	}
	byTrial := make(map[int]RunRecord, len(baseline))
	for _, run := range baseline {
		byTrial[run.Trial] = run
	}
	for _, run := range runs {
		base, ok := byTrial[run.Trial]
		if !ok {
			return false
		}
		diff := value(run) - value(base)
		if deltaPct < 0 && diff >= 0 {
			return false
		}
		if deltaPct > 0 && diff <= 0 {
			return false
		}
	}
	return true
}

func summarizeLatency(runs []RunRecord) map[string]LatencySummary {
	byPath := make(map[string][]LatencyRecord)
	for _, run := range runs {
		for _, lat := range run.Latency {
			byPath[lat.Path] = append(byPath[lat.Path], lat)
		}
	}
	out := make(map[string]LatencySummary, len(byPath))
	for path, rows := range byPath {
		var p50, p95, p99 []float64
		for _, row := range rows {
			p50 = append(p50, row.P50MS)
			p95 = append(p95, row.P95MS)
			p99 = append(p99, row.P99MS)
		}
		out[path] = LatencySummary{
			P50MSMedian: metricStats(p50).Median,
			P95MSMedian: metricStats(p95).Median,
			P99MSMedian: metricStats(p99).Median,
		}
	}
	return out
}

func writeSummaryMarkdown(path string, summaries []TargetSummary, baselineName string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600) // #nosec G304 -- report path is operator-selected output.
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintln(f, "# Release A/B Benchmark Matrix")
	fmt.Fprintln(f)
	fmt.Fprintf(f, "Baseline: `%s`. Deltas under 5%%, or deltas without same-direction agreement across matched trials, are labeled as noise.\n\n", baselineName)
	fmt.Fprintln(f, "| target | runs | ready median | ready p95 | ready delta | tip median | DB median | DB delta |")
	fmt.Fprintln(f, "|---|---:|---:|---:|---:|---:|---:|---:|")
	for _, summary := range summaries {
		fmt.Fprintf(f, "| `%s` | %d | %s | %s | %s | %s | %s | %s |\n",
			summary.Name,
			summary.Count,
			formatMS(summary.ReadyMS.Median),
			formatMS(summary.ReadyMS.P95),
			formatDelta(summary.ReadyMS),
			formatMS(summary.TipMS.Median),
			humanBytes(int64(summary.DBBytes.Median)),
			formatDelta(summary.DBBytes),
		)
	}

	fmt.Fprintln(f)
	fmt.Fprintln(f, "## API Latency")
	for _, summary := range summaries {
		if len(summary.Latency) == 0 {
			continue
		}
		fmt.Fprintf(f, "\n### %s\n\n", summary.Name)
		fmt.Fprintln(f, "| path | median p50 | median p95 | median p99 |")
		fmt.Fprintln(f, "|---|---:|---:|---:|")
		paths := make([]string, 0, len(summary.Latency))
		for path := range summary.Latency {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		for _, path := range paths {
			lat := summary.Latency[path]
			fmt.Fprintf(f, "| `%s` | %s | %s | %s |\n",
				path,
				formatMS(lat.P50MSMedian),
				formatMS(lat.P95MSMedian),
				formatMS(lat.P99MSMedian),
			)
		}
	}
	return nil
}

func formatMS(ms float64) string {
	if ms == 0 {
		return "n/a"
	}
	if ms < 1000 {
		return fmt.Sprintf("%.0f ms", ms)
	}
	return fmt.Sprintf("%.1f s", ms/1000)
}

func formatDelta(metric MetricSummary) string {
	suffix := ""
	if metric.Noise {
		suffix = " noise"
	}
	return fmt.Sprintf("%+.1f%%%s", metric.DeltaPct, suffix)
}

func humanBytes(n int64) string {
	const (
		KB = 1 << 10
		MB = 1 << 20
		GB = 1 << 30
	)
	switch {
	case n >= GB:
		return fmt.Sprintf("%.2f GiB", float64(n)/GB)
	case n >= MB:
		return fmt.Sprintf("%.1f MiB", float64(n)/MB)
	case n >= KB:
		return fmt.Sprintf("%.1f KiB", float64(n)/KB)
	default:
		return fmt.Sprintf("%d B", n)
	}
}
