package indexer

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

type stageID int

const (
	stageFetchBlocksRPC stageID = iota
	stageFetchBlockDecode
	stageFetchTxsRPC
	stageFetchSendWait
	stageProcRecvWait
	stageProcTxDecode
	stageProcSCTx
	stageProcSCTxGetSC
	stageProcNormal
	stageProcSendWait
	stageFlushRecvWait
	stageFlushBBolt
	numStages
)

var stageNames = [numStages]string{
	stageFetchBlocksRPC:   "blocks_rpc",
	stageFetchBlockDecode: "block_decode",
	stageFetchTxsRPC:      "txs_rpc",
	stageFetchSendWait:    "send_wait",
	stageProcRecvWait:     "recv_wait",
	stageProcTxDecode:     "tx_decode",
	stageProcSCTx:         "sctx",
	stageProcSCTxGetSC:    "sctx.getsc",
	stageProcNormal:       "normal",
	stageProcSendWait:     "send_wait",
	stageFlushRecvWait:    "recv_wait",
	stageFlushBBolt:       "bbolt",
}

type stageTimer struct {
	enabled bool
	every   int64
	nanos   [numStages]atomic.Int64
	batches atomic.Int64
}

func newStageTimer(enabled bool, every int) *stageTimer {
	if every <= 0 {
		every = 10
	}
	return &stageTimer{enabled: enabled, every: int64(every)}
}

func (s *stageTimer) record(id stageID, d time.Duration) {
	if !s.enabled {
		return
	}
	s.nanos[id].Add(d.Nanoseconds())
}

func (s *stageTimer) onBatch() {
	if !s.enabled {
		return
	}
	n := s.batches.Add(1)
	if n%s.every == 0 {
		s.emit(n)
	}
}

func (s *stageTimer) emit(n int64) {
	var snap [numStages]time.Duration
	for i := range s.nanos {
		snap[i] = time.Duration(s.nanos[i].Swap(0))
	}
	fetcher := group("fetcher", snap[:], []stageID{
		stageFetchBlocksRPC, stageFetchBlockDecode, stageFetchTxsRPC, stageFetchSendWait,
	})
	processor := groupProcSCTx(snap[:])
	flusher := group("flusher", snap[:], []stageID{
		stageFlushRecvWait, stageFlushBBolt,
	})
	logger.Infof("timing[%d batches] %s", s.every, fetcher)
	logger.Infof("timing[%d batches] %s", s.every, processor)
	logger.Infof("timing[%d batches] %s", s.every, flusher)
	_ = n
}

func group(label string, snap []time.Duration, ids []stageID) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s:", label)
	for _, id := range ids {
		fmt.Fprintf(&b, " %s=%s", stageNames[id], round(snap[id]))
	}
	return b.String()
}

// groupProcSCTx renders the processor line with sctx showing its nested getsc
// time in parens — e.g. sctx=3.4s(getsc=2.9s) — so the RPC-vs-non-RPC split
// inside processSCTx is legible at a glance.
func groupProcSCTx(snap []time.Duration) string {
	var b strings.Builder
	b.WriteString("processor:")
	fmt.Fprintf(&b, " %s=%s", stageNames[stageProcRecvWait], round(snap[stageProcRecvWait]))
	fmt.Fprintf(&b, " %s=%s", stageNames[stageProcTxDecode], round(snap[stageProcTxDecode]))
	fmt.Fprintf(&b, " %s=%s(getsc=%s)",
		stageNames[stageProcSCTx], round(snap[stageProcSCTx]), round(snap[stageProcSCTxGetSC]))
	fmt.Fprintf(&b, " %s=%s", stageNames[stageProcNormal], round(snap[stageProcNormal]))
	fmt.Fprintf(&b, " %s=%s", stageNames[stageProcSendWait], round(snap[stageProcSendWait]))
	return b.String()
}

func round(d time.Duration) time.Duration {
	switch {
	case d >= time.Second:
		return d.Round(10 * time.Millisecond)
	case d >= 100*time.Millisecond:
		return d.Round(time.Millisecond)
	case d >= time.Millisecond:
		return d.Round(100 * time.Microsecond)
	default:
		return d.Round(time.Microsecond)
	}
}
