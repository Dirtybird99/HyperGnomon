// Command benchvs measures a single DERO indexer's time-to-tip, DB
// size, and API latency under concurrent probe load. Designed to be
// run once per indexer under comparison — the operator runs it
// against HyperGnomon, then against civilware/Gnomon (or whatever
// binary exposes a comparable API), and the tool appends both
// results to the same markdown file.
//
// The deliberate choice NOT to run both indexers concurrently is to
// avoid RPC contention against the shared daemon, which would make
// whichever ran first look artificially slow. Sequential runs at
// full daemon bandwidth are the honest comparison.
//
// Usage:
//
//	# HyperGnomon side
//	./benchvs --name=HyperGnomon \
//	          --binary=./hypergnomon \
//	          --daemon=203.0.113.10:10102 \
//	          --db-dir=/tmp/hg-bench
//
//	# civilware side (operator builds civilware's cmd or a wrapper)
//	./benchvs --name="civilware/Gnomon@dev" \
//	          --binary=./civilware-runner \
//	          --daemon=203.0.113.10:10102 \
//	          --db-dir=/tmp/cw-bench \
//	          --api-url=http://127.0.0.1:8083
//
// Both invocations append to the same --out file (default
// bench_vs_civilware.md) so the comparison table builds up.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

func main() {
	name := flag.String("name", "HyperGnomon", "label for this run in the output markdown")
	binary := flag.String("binary", "./hypergnomon", "path to indexer binary")
	argsRaw := flag.String("args", "", "additional args passed to the binary (space-separated; --daemon + --db-dir are injected automatically)")
	daemon := flag.String("daemon", "127.0.0.1:10102", "DERO daemon RPC address")
	dbDir := flag.String("db-dir", "", "DB directory the binary will write to; also measured for size (required)")
	apiURL := flag.String("api-url", "http://127.0.0.1:8082", "indexer API base URL (for tip detection + probes)")
	tipTimeout := flag.Duration("tip-timeout", 15*time.Minute, "max wait for indexer to reach daemon tip")
	probeDur := flag.Duration("probe-duration", 60*time.Second, "time to run API probes after reaching tip")
	probeWorkers := flag.Int("probe-workers", 32, "concurrent probe workers")
	probePaths := flag.String("probe-paths", "/api/getinfo,/api/getstats,/api/getscids", "comma-separated paths to probe")
	outFile := flag.String("out", "bench_vs_civilware.md", "markdown output file (appended)")
	daemonFlag := flag.String("daemon-flag", "--daemon-rpc-address", "name of the daemon flag on the target binary")
	dbDirFlag := flag.String("db-dir-flag", "--db-dir", "name of the db-dir flag on the target binary")
	readyLogPattern := flag.String("ready-log-pattern", "", "optional comma-separated child-log markers to wait for before API probes")
	readyTimeout := flag.Duration("ready-timeout", 5*time.Minute, "max wait for --ready-log-pattern after reaching tip")
	flag.Parse()

	if *dbDir == "" {
		fmt.Fprintln(os.Stderr, "benchvs: --db-dir is required")
		os.Exit(2)
	}
	if _, err := os.Stat(*binary); err != nil {
		fmt.Fprintf(os.Stderr, "benchvs: --binary %q: %v\n", *binary, err)
		os.Exit(2)
	}

	// Fresh DB dir per run. Measuring size is meaningless otherwise
	// because leftovers from a prior run would inflate the total.
	if err := os.RemoveAll(*dbDir); err != nil {
		fmt.Fprintf(os.Stderr, "benchvs: cleanup of --db-dir failed: %v\n", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(*dbDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "benchvs: mkdir --db-dir: %v\n", err)
		os.Exit(1)
	}

	// Compose the target's full argv. Injected flags come first so an
	// explicit override from --args wins.
	extra := strings.Fields(*argsRaw)
	args := append([]string{
		*daemonFlag + "=" + *daemon,
		*dbDirFlag + "=" + *dbDir,
	}, extra...)

	fmt.Printf("benchvs: %s → %s  (daemon=%s, db=%s)\n", *name, *binary, *daemon, *dbDir)
	fmt.Printf("benchvs: argv: %s %s\n", *binary, strings.Join(args, " "))

	result := Result{
		Name:          *name,
		Binary:        filepath.Base(*binary),
		Daemon:        *daemon,
		GoVersion:     runtime.Version(),
		HostOS:        runtime.GOOS + "/" + runtime.GOARCH,
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
		ProbeDuration: *probeDur,
		ProbeWorkers:  *probeWorkers,
		ProbePaths:    strings.Split(*probePaths, ","),
		ReadyPattern:  *readyLogPattern,
	}

	// 1. Start the indexer subprocess.
	proc, err := startIndexer(*binary, args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchvs: start indexer: %v\n", err)
		os.Exit(1)
	}
	defer proc.Stop()

	// 2. Wait for it to reach tip. Time from process start to tip.
	fmt.Printf("benchvs: waiting for tip (up to %s)...\n", *tipTimeout)
	tipStart := proc.StartedAt
	tipReached, err := waitForTip(*apiURL, *tipTimeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchvs: tip wait failed: %v\n", err)
		// Continue anyway — partial numbers beat no numbers.
		result.TipReached = false
	} else {
		result.TipReached = true
		result.TimeToTip = tipReached.Sub(tipStart)
		fmt.Printf("benchvs: reached tip in %s\n", result.TimeToTip.Round(100*time.Millisecond))
	}

	// 3. Run concurrent probes for probe-duration.
	if result.TipReached {
		markers := splitCSV(*readyLogPattern)
		if len(markers) > 0 {
			fmt.Printf("benchvs: waiting for readiness marker(s) %v in %s (up to %s)...\n",
				markers, proc.LogPath, *readyTimeout)
			readyAt, err := waitForLogPattern(proc.LogPath, markers, *readyTimeout)
			if err != nil {
				fmt.Fprintf(os.Stderr, "benchvs: readiness wait failed: %v\n", err)
				result.ReadyReached = false
			} else {
				result.ReadyReached = true
				result.TimeToReady = readyAt.Sub(tipStart)
				fmt.Printf("benchvs: ready in %s\n", result.TimeToReady.Round(100*time.Millisecond))
			}
		}
		fmt.Printf("benchvs: probing for %s with %d workers across paths %v...\n",
			*probeDur, *probeWorkers, result.ProbePaths)
		result.Latency = runProbes(*apiURL, result.ProbePaths, *probeWorkers, *probeDur)
	}

	// 4. Stop indexer cleanly.
	proc.Stop()

	// 5. Measure DB size.
	size, err := dirSize(*dbDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "benchvs: dbDir size: %v\n", err)
	}
	result.DBBytes = size
	result.ClassifyTiming = lastLogLineContaining(proc.LogPath, "Classify probe timings:")

	// 6. Append to markdown.
	if err := writeReport(*outFile, result); err != nil {
		fmt.Fprintf(os.Stderr, "benchvs: write report: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("benchvs: appended result to %s\n", *outFile)
}

func splitCSV(raw string) []string {
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
