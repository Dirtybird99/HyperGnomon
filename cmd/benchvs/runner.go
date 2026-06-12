package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Proc wraps a running indexer subprocess. Stop is idempotent so the
// caller can safely defer Stop and also call it explicitly after
// the probe phase.
type Proc struct {
	cmd       *exec.Cmd
	StartedAt time.Time
	LogPath   string
	stopped   bool
}

// startIndexer spawns the target binary with the given argv, writing
// its stdout/stderr to a log file alongside the indexer's db-dir so
// failures are diagnosable. Returns once the process is launched;
// does NOT wait for readiness — that's the tip-detection loop's job.
func startIndexer(bin string, args []string) (*Proc, error) {
	cmd := exec.Command(bin, args...) // #nosec G204 -- benchvs intentionally launches an operator-selected benchmark binary.
	logPath := logPathForRun(bin, args)
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600) // #nosec G304 -- log path is derived from operator-selected benchmark paths.
	if err != nil {
		return nil, fmt.Errorf("open log %s: %w", logPath, err)
	}
	cmd.Stdout = f
	cmd.Stderr = f
	if err := cmd.Start(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Proc{cmd: cmd, StartedAt: time.Now(), LogPath: logPath}, nil
}

func logPathForRun(bin string, args []string) string {
	dbDir := findDBDir(args)
	if dbDir == "" {
		return filepath.Join(filepath.Dir(bin), "benchvs.log")
	}
	return filepath.Join(filepath.Dir(dbDir), filepath.Base(dbDir)+"-benchvs.log")
}

// Stop terminates the subprocess. Best-effort: sends interrupt first
// (graceful shutdown lets the indexer flush), escalates to kill if
// the process is still alive after 10 s.
func (p *Proc) Stop() {
	if p == nil || p.cmd == nil || p.cmd.Process == nil || p.stopped {
		return
	}
	p.stopped = true

	// os.Interrupt maps to a clean shutdown on Unix; on Windows it
	// returns "not supported" for child processes, so we fall
	// through to Kill after the timeout. Either way, Wait reaps.
	_ = p.cmd.Process.Signal(os.Interrupt)

	done := make(chan error, 1)
	go func() { done <- p.cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		_ = p.cmd.Process.Kill()
		<-done
	}
}

// findDBDir extracts the --db-dir value from argv so we can place
// the subprocess log next to it. Recognizes both `--db-dir=path`
// and `--db-dir path`. Returns "" if not found.
func findDBDir(args []string) string {
	const prefix = "--db-dir="
	for i, a := range args {
		if a == "--db-dir" && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(a, prefix) {
			return a[len(prefix):]
		}
	}
	return ""
}

// waitForTip polls /api/getinfo (daemon chain state) and /api/getstats
// (indexer state) until indexer index_height is within STABLE_LIMIT
// (8) of daemon TopoHeight. Returns the moment the condition became
// true, or an error on timeout.
//
// The two-endpoint design means this works for HyperGnomon out of
// the box and also works for any indexer that exposes an API
// surface with the same JSON field names (getinfo.TopoHeight +
// getstats.index_height). Indexers using different names can wrap
// with a reverse proxy — see cmd/benchvs/README.md.
func waitForTip(apiURL string, timeout time.Duration) (time.Time, error) {
	deadline := time.Now().Add(timeout)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	for time.Now().Before(deadline) {
		<-tick.C
		tip, err := fetchTopoHeight(apiURL + "/api/getinfo")
		if err != nil {
			continue
		}
		idx, err := fetchIndexHeight(apiURL + "/api/getstats")
		if err != nil {
			continue
		}
		if tip > 0 && idx > 0 && tip-idx <= 8 {
			return time.Now(), nil
		}
	}
	return time.Time{}, fmt.Errorf("timed out after %s", timeout)
}

// waitForLogPattern polls the subprocess log until any of the supplied
// marker strings appears. This lets benchvs wait for indexer-specific
// background readiness, such as HyperGnomon's classify/TELA probe, before
// starting API latency probes.
func waitForLogPattern(logPath string, markers []string, timeout time.Duration) (time.Time, error) {
	if logPath == "" || len(markers) == 0 {
		return time.Now(), nil
	}
	deadline := time.Now().Add(timeout)
	tick := time.NewTicker(500 * time.Millisecond)
	defer tick.Stop()

	for time.Now().Before(deadline) {
		body, err := readBenchmarkLog(logPath)
		if err == nil {
			text := string(body)
			for _, marker := range markers {
				if marker != "" && strings.Contains(text, marker) {
					return time.Now(), nil
				}
			}
		}
		<-tick.C
	}
	return time.Time{}, fmt.Errorf("timed out after %s waiting for %q in %s", timeout, strings.Join(markers, " | "), logPath)
}

func lastLogLineContaining(logPath, needle string) string {
	if logPath == "" || needle == "" {
		return ""
	}
	body, err := readBenchmarkLog(logPath)
	if err != nil {
		return ""
	}
	lines := strings.Split(string(body), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if strings.Contains(line, needle) {
			return line
		}
	}
	return ""
}

func readBenchmarkLog(path string) ([]byte, error) {
	return os.ReadFile(path) // #nosec G304 -- benchvs reads only the operator-selected child log.
}

func fetchTopoHeight(url string) (int64, error) {
	var body struct {
		TopoHeight int64 `json:"TopoHeight"`
	}
	if err := getJSON(url, &body); err != nil {
		return 0, err
	}
	return body.TopoHeight, nil
}

func fetchIndexHeight(url string) (int64, error) {
	var body struct {
		IndexHeight int64 `json:"index_height"`
	}
	if err := getJSON(url, &body); err != nil {
		return 0, err
	}
	return body.IndexHeight, nil
}

func getJSON(url string, dst interface{}) error {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return fmt.Errorf("http %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(dst)
}

// dirSize walks a directory tree summing file sizes. Returns 0 + nil
// for a missing path — a target that crashed on startup won't have
// written anything, so reporting 0 is honest.
func dirSize(path string) (int64, error) {
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return 0, nil
	}
	var total int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total, err
}
