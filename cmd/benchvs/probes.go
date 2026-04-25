package main

import (
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
)

// PathLatency holds timings collected against one API path. Times
// are stored as time.Duration and sorted once at aggregation so
// percentile extraction is O(1).
type PathLatency struct {
	Path   string
	N      int
	Errors int
	P50    time.Duration
	P95    time.Duration
	P99    time.Duration
	Max    time.Duration
}

// runProbes spawns `workers` goroutines that round-robin across
// `paths`, issuing GET requests for `duration`, and returns
// per-path latency percentiles. A shared httpClient keeps
// connections warm — we're measuring server latency, not TLS /
// TCP setup overhead.
func runProbes(baseURL string, paths []string, workers int, duration time.Duration) []PathLatency {
	if len(paths) == 0 || workers <= 0 || duration <= 0 {
		return nil
	}

	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        workers * 2,
			MaxIdleConnsPerHost: workers * 2,
			IdleConnTimeout:     30 * time.Second,
		},
	}

	type sample struct {
		pathIdx int
		dur     time.Duration
		err     bool
	}

	samples := make(chan sample, workers*256)
	type aggregate struct {
		durations [][]time.Duration
		errors    []int
	}
	aggDone := make(chan aggregate, 1)
	go func() {
		durations := make([][]time.Duration, len(paths))
		errors := make([]int, len(paths))
		for s := range samples {
			if s.pathIdx < 0 || s.pathIdx >= len(paths) {
				continue
			}
			if s.err {
				errors[s.pathIdx]++
			}
			durations[s.pathIdx] = append(durations[s.pathIdx], s.dur)
		}
		aggDone <- aggregate{durations: durations, errors: errors}
	}()

	deadline := time.Now().Add(duration)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; time.Now().Before(deadline); i++ {
				idx := (workerID + i) % len(paths)
				url := baseURL + paths[idx]
				start := time.Now()
				resp, err := client.Get(url)
				lat := time.Since(start)
				var failed bool
				if err != nil {
					failed = true
				} else {
					if resp.StatusCode >= 400 {
						failed = true
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					_ = resp.Body.Close()
				}
				samples <- sample{pathIdx: idx, dur: lat, err: failed}
			}
		}(w)
	}
	wg.Wait()
	close(samples)
	agg := <-aggDone

	// Aggregate per-path.
	out := make([]PathLatency, len(paths))
	for i, durations := range agg.durations {
		sort.Slice(durations, func(a, b int) bool { return durations[a] < durations[b] })

		out[i] = PathLatency{
			Path:   paths[i],
			N:      len(durations),
			Errors: agg.errors[i],
		}
		if len(durations) > 0 {
			out[i].P50 = pct(durations, 0.50)
			out[i].P95 = pct(durations, 0.95)
			out[i].P99 = pct(durations, 0.99)
			out[i].Max = durations[len(durations)-1]
		}
	}
	return out
}

// pct returns the value at percentile p (0.0 to 1.0) from a
// pre-sorted slice. Uses nearest-rank with (n-1)*p indexing —
// standard enough for operator-facing latency reports.
func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}
