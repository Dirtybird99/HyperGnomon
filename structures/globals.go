package structures

import (
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

const (
	Version = "0.7.0"
	AppName = "HyperGnomon"

	// Mainnet GnomonSC SCID for fastsync
	GnomonSCID_Mainnet = "a05395bb0cf77adc850928b0db00eb5ca7a9ccbafd9a38d021c8d299ad5ce1a4"
	// Testnet GnomonSC SCID for fastsync
	GnomonSCID_Testnet = "c9d23d2fc4ced00dcbc3e88b9e4e4a39a650fda82dcfab66116027e3e53b0f18"
	// DERO Nameservice SCID
	NameServiceSCID = "0000000000000000000000000000000000000000000000000000000000000001"

	// Default batch size for DB writes (number of blocks to accumulate)
	DefaultBatchSize = 100
	// Default RPC connection pool size
	DefaultPoolSize = 8
	// Default parallel block fetchers
	DefaultParallelBlocks = 20
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
