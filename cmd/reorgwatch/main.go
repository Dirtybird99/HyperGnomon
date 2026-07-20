// Command reorgwatch measures DERO mainnet reorg frequency and depth against a
// live daemon. It answers the one number gating truncate scan-cost work and
// M2.3 wiring assumptions: how often does the topo order actually rewrite, and
// how deep?
//
// Two modes:
//
//	-scan N   historical: walk the last N topoheights' headers and count
//	          sideblocks / multi-tip blocks / height≠topoheight — the DAG's own
//	          record of divergence. Lower-bound proxy: truly orphaned branches
//	          vanish from the canonical DAG. One JSON summary line to stdout.
//
//	-watch    live: poll GetInfo, keep a rolling topoheight→hash window, and
//	          re-verify the youngest -recheck topos every poll. A recorded hash
//	          that changed is exactly the event hypergnomon's blockhashes-bucket
//	          detection would see. Events + hourly heartbeats append to -out as
//	          JSONL. Dedupe is by convergence: after an event the window is
//	          updated to the daemon's current hashes, so one reorg emits one
//	          event (the indexer's reorg_detected counter conflates repeated
//	          observations of one reorg — see indexer/reorg.go:83-87 — and this
//	          tool deliberately does not repeat that).
//
// Like cmd/wstest this is a human-driven measurement tool, not a test.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	derorpc "github.com/deroproject/derohe/rpc"

	hgrpc "github.com/hypergnomon/hypergnomon/rpc"
)

func main() {
	daemon := flag.String("daemon", "127.0.0.1:10102", "daemon RPC endpoint (host:port)")
	scanN := flag.Int64("scan", 0, "historical mode: scan the last N topoheights and exit")
	watch := flag.Bool("watch", false, "live mode: watch for topo-order rewrites until killed")
	out := flag.String("out", "reorgwatch.jsonl", "watch mode: JSONL output file (appended)")
	poll := flag.Duration("poll", 9*time.Second, "watch mode: poll interval (~half a block time)")
	window := flag.Int64("window", 64, "watch mode: retained topoheight→hash window")
	recheck := flag.Int64("recheck", 16, "watch mode: youngest topos re-verified per poll (> STABLE_LIMIT=8)")
	flag.Parse()

	if (*scanN > 0) == *watch {
		fmt.Fprintln(os.Stderr, "exactly one of -scan N or -watch is required")
		flag.Usage()
		os.Exit(1)
	}

	c, err := hgrpc.NewClient(*daemon)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect %s: %v\n", *daemon, err)
		os.Exit(1)
	}
	defer c.Close()

	if *scanN > 0 {
		if err := runScan(c, *scanN); err != nil {
			fmt.Fprintf(os.Stderr, "scan: %v\n", err)
			os.Exit(1)
		}
		return
	}
	if err := runWatch(c, *out, *poll, *window, *recheck); err != nil {
		fmt.Fprintf(os.Stderr, "watch: %v\n", err)
		os.Exit(1)
	}
}

// retry calls fn up to 3 times, redialing the WebSocket between attempts so a
// dropped daemon connection heals instead of killing a multi-day run.
func retry[T any](c *hgrpc.Client, fn func() (T, error)) (T, error) {
	var last error
	for attempt := 0; attempt < 3; attempt++ {
		v, err := fn()
		if err == nil {
			return v, nil
		}
		last = err
		time.Sleep(time.Duration(attempt+1) * 2 * time.Second)
		_ = c.Connect() // best-effort redial; next attempt reports if still down
	}
	var zero T
	return zero, last
}

// ---- scan mode ----

func runScan(c *hgrpc.Client, n int64) error {
	info, err := retry(c, c.GetInfo)
	if err != nil {
		return fmt.Errorf("GetInfo: %w", err)
	}
	tip, stable := info.TopoHeight, info.StableHeight
	from, to := tip-n, tip-(tip-stable) // stop at the stability boundary
	if from < 1 {
		from = 1
	}

	var side, multiTip, heightNeTopo, scanned int64
	var sideTopos []int64
	start := time.Now()
	for t := from; t < to; t++ {
		topo := t
		h, err := retry(c, func() (*derorpc.GetBlockHeaderByHeight_Result, error) {
			return c.GetBlockHeaderByTopoHeight(uint64(topo))
		})
		if err != nil {
			return fmt.Errorf("header at %d: %w", topo, err)
		}
		bh := h.Block_Header
		scanned++
		if bh.SideBlock {
			side++
			sideTopos = append(sideTopos, topo)
		}
		if len(bh.Tips) > 1 {
			multiTip++
		}
		if bh.Height != bh.TopoHeight {
			heightNeTopo++
		}
		if scanned%10000 == 0 {
			fmt.Fprintf(os.Stderr, "scanned %d/%d (%.0f/s)\n",
				scanned, to-from, float64(scanned)/time.Since(start).Seconds())
		}
	}

	if len(sideTopos) > 200 { // keep the summary line bounded
		sideTopos = sideTopos[len(sideTopos)-200:]
	}
	days := float64(scanned) * float64(info.AverageBlockTime50) / 86400
	return json.NewEncoder(os.Stdout).Encode(map[string]any{
		"type": "scan", "ts": time.Now().UTC().Format(time.RFC3339),
		"tip": tip, "stableheight": stable, "from": from, "to": to,
		"scanned": scanned, "span_days": round2(days),
		"avg_block_time_s": info.AverageBlockTime50,
		"sideblocks":       side, "multi_tip_blocks": multiTip,
		"height_ne_topo": heightNeTopo,
		"sideblocks_per_day": round2(perDay(side, days)),
		"sideblock_topos":    sideTopos,
		"elapsed_s":          round2(time.Since(start).Seconds()),
	})
}

// ---- watch mode ----

func runWatch(c *hgrpc.Client, outPath string, poll time.Duration, window, recheck int64) error {
	f, err := os.OpenFile(outPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	emit := func(v map[string]any) {
		v["ts"] = time.Now().UTC().Format(time.RFC3339)
		_ = enc.Encode(v)
		if v["type"] != "heartbeat" {
			_ = json.NewEncoder(os.Stderr).Encode(v)
		}
	}

	hashes := map[int64]string{} // rolling topoheight -> hash window
	var lastTip, polls, blocksSeen, sideSeen, multiTipSeen, rpcErrs, events int64
	depthHist := map[int64]int64{}
	start := time.Now()
	lastBeat := start

	getHeader := func(topo int64) (*derorpc.GetBlockHeaderByHeight_Result, error) {
		return retry(c, func() (*derorpc.GetBlockHeaderByHeight_Result, error) {
			return c.GetBlockHeaderByTopoHeight(uint64(topo))
		})
	}

	emit(map[string]any{"type": "start", "daemon": c.Endpoint,
		"poll_s": poll.Seconds(), "window": window, "recheck": recheck})

	for {
		info, err := retry(c, c.GetInfo)
		if err != nil {
			rpcErrs++
			time.Sleep(poll)
			continue
		}
		tip := info.TopoHeight
		polls++

		// Ingest new topos (seed the full window on first poll).
		low := lastTip + 1
		if lastTip == 0 || low < tip-window+1 {
			low = tip - window + 1
		}
		for t := low; t <= tip; t++ {
			h, err := getHeader(t)
			if err != nil {
				rpcErrs++
				break
			}
			hashes[t] = h.Block_Header.Hash
			blocksSeen++
			if h.Block_Header.SideBlock {
				sideSeen++
			}
			if len(h.Block_Header.Tips) > 1 {
				multiTipSeen++
			}
		}
		lastTip = tip

		// Re-verify the youngest recheck topos below the ones just fetched: a
		// recorded hash that changed is a topo-order rewrite (indexer-visible
		// reorg). Walk down to the fork point, emit one event, converge.
		changed, forkTopo, derr := divergence(low-1, recheck,
			func(t int64) (string, bool) { h, ok := hashes[t]; return h, ok },
			func(t int64) (string, error) {
				h, err := getHeader(t)
				if err != nil {
					return "", err
				}
				return h.Block_Header.Hash, nil
			})
		if derr != nil {
			rpcErrs++ // abort this poll's walk; stale entries re-diff next poll
		}
		if len(changed) > 0 {
			events++
			depth := tip - forkTopo
			if forkTopo < 0 {
				depth = -1 // ran past the window without finding agreement
			}
			depthHist[depth]++
			ev := make([]map[string]any, len(changed))
			for i, ch := range changed {
				ev[i] = map[string]any{"topo": ch.Topo, "old": ch.Old, "new": ch.New}
				hashes[ch.Topo] = ch.New // converge: one reorg emits one event
			}
			emit(map[string]any{"type": "reorg", "tip": tip,
				"stableheight": info.StableHeight, "fork_topo": forkTopo,
				"depth": depth, "changed": ev})
		}

		for t := range hashes { // prune below the window
			if t < tip-window {
				delete(hashes, t)
			}
		}

		if time.Since(lastBeat) >= time.Hour {
			lastBeat = time.Now()
			emit(map[string]any{"type": "heartbeat", "tip": tip,
				"uptime_h": round2(time.Since(start).Hours()), "polls": polls,
				"blocks_seen": blocksSeen, "sideblocks": sideSeen,
				"multi_tip_blocks": multiTipSeen, "reorg_events": events,
				"depth_hist": depthHist, "rpc_errors": rpcErrs})
		}
		time.Sleep(poll)
	}
}

// changedHash is one rewritten topo: the hash we recorded vs the daemon's now.
type changedHash struct {
	Topo     int64
	Old, New string
}

// divergence walks DOWN from topo start, re-checking up to limit recorded
// hashes against the daemon, and returns the rewritten entries plus the fork
// topo — the first topo below the diverged range where recorded and daemon
// agree — or -1 if the walk ran past the recorded window or exhausted limit
// without agreement (caller treats depth as unknown). Mirrors the unexported
// indexer/reorg.go findForkPoint contract: a daemon miss ("" hash) counts as
// DISAGREEMENT and the walk continues (the daemon not serving a topo is not
// evidence of agreement), but a miss is not recorded as a new hash; a recorded
// miss (below our window) means the fork point is unknowable. An RPC error
// aborts the walk with what was found so far — the caller retries next poll.
func divergence(start, limit int64, recorded func(int64) (string, bool), daemon func(int64) (string, error)) ([]changedHash, int64, error) {
	var changed []changedHash
	for i := int64(0); i < limit; i++ {
		t := start - i
		old, seen := recorded(t)
		if t < 1 || !seen {
			return changed, -1, nil // below window: unknowable
		}
		cur, err := daemon(t)
		if err != nil {
			return changed, -1, err
		}
		if cur == old {
			if len(changed) == 0 {
				return nil, t, nil // no divergence at all
			}
			return changed, t, nil // first agreement below the diverged range
		}
		if cur != "" { // daemon miss stays a disagreement but records nothing
			changed = append(changed, changedHash{Topo: t, Old: old, New: cur})
		}
	}
	return changed, -1, nil // limit exhausted without agreement
}

func perDay(n int64, days float64) float64 {
	if days <= 0 {
		return 0
	}
	return float64(n) / days
}

func round2(f float64) float64 { return float64(int64(f*100)) / 100 }
