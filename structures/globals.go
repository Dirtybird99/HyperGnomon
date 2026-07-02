package structures

import (
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

// Version is set via -ldflags -X at release build time (goreleaser).
// Default value below is what source builds report without release ldflags.
var Version = "1.1.0"

const (
	AppName = "HyperGnomon"

	// Mainnet GnomonSC SCID for fastsync
	GnomonSCID_Mainnet = "a05395bb0cf77adc850928b0db00eb5ca7a9ccbafd9a38d021c8d299ad5ce1a4"
	// Testnet GnomonSC SCID for fastsync
	GnomonSCID_Testnet = "c9d23d2fc4ced00dcbc3e88b9e4e4a39a650fda82dcfab66116027e3e53b0f18"
	// DERO Nameservice SCID
	NameServiceSCID = "0000000000000000000000000000000000000000000000000000000000000001"

	// Default batch size for DB writes (number of blocks to accumulate)
	DefaultBatchSize = 100
	// DefaultClassifyProbeBatchSize is the number of SCIDs packed into each
	// phase-1 GetSC(code=true) JSON-RPC batch during fastsync classification.
	// LAN benchmarking showed 400 keeps startup close to the 1000-SCID ceiling
	// without making each response frame as large.
	DefaultClassifyProbeBatchSize = 400
	// DefaultPostScanVarsMode controls turbo-mode behavior after the indexer
	// catches up. "lazy" skips the old all-SCID variable sweep; richer
	// metadata is filled by targeted probes/refresher paths.
	DefaultPostScanVarsMode = "lazy"
	// Default RPC connection pool size.
	//
	// Previous: 16. LAN-measured knee was 16 (pool=8 took 104s vs 51s at 16)
	// — but derod's default rate limit is 10 rps with 2× burst, so pool=16
	// trips remote daemons. The TELA-CLI guide warns that >5 parallel on a
	// remote node "triggers timeouts/rate-limits." 8 keeps headroom under
	// that ceiling while preserving most of the LAN throughput win.
	// Operators with their own LAN daemon can still override to 16 via
	// --rpc-pool-size.
	DefaultPoolSize = 8
	// Default parallel block fetchers.
	//
	// Same rate-limit consideration applies: 20 concurrent block fetches
	// × up to a few batched RPC calls each = burst spikes well past the
	// 10 rps ceiling. 8 mirrors the pool default so an operator running
	// against a public node doesn't get throttled by default.
	DefaultParallelBlocks = 8
	// Default blocks-behind-tip considered safe from reorg.
	// DERO stabilizes in ~8 blocks; 10 is a pragmatic safety margin.
	// Exposed as SafeHeight in API responses.
	DefaultFinalityDepth = 10
)

// DefaultExclusions are large/known non-TELA SCIDs to skip during probing.
// These contracts are massive (e.g. SC Market at 47MB) and definitely not TELA apps.
// Excluding them saves expensive GetSC calls during fastsync probe.
var DefaultExclusions = []string{
	NameServiceSCID,    // Nameservice
	GnomonSCID_Mainnet, // GnomonSC itself
}

// Logger is the package-level logger
var Logger = logrus.New()

// TELACount tracks the number of TELA apps discovered during fastsync probe.
var TELACount atomic.Int64

// TELAProbeSettled flips true once TELA discovery has fully finished AND its
// results are durable: a full classify probe returned (including its
// cache/seed saves), a cache hit's gating delta probe finished flushing any
// newly-deployed SCIDs, or a fresh cache hit made a probe unnecessary. Unlike
// TELACount — set as soon as apps are counted — this is the signal
// `--tela-only` waits on to exit without racing cache or batch writes.
// Terminal classify paths settle via indexer's settleTELADiscovery helper,
// which couples this flag with the readiness log marker so neither can be
// added without the other. Never reset at runtime; tests that drive a settle
// path must Store(false) in setup or later tests pass vacuously.
var TELAProbeSettled atomic.Bool
