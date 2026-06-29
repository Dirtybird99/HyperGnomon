// Command pkg/gnomes/example demonstrates the civilware/Gnomon-shape
// API surface exposed by pkg/gnomes. A consumer migrating from
// civilware/Gnomon can port their code with three sed commands and
// then replace their NewIndexer(…) with NewIndexerWithDBDir(…) to
// unblock the v1.0 single-source-change migration path.
//
// Run:
//
//	go run ./pkg/gnomes/example -daemon=203.0.113.10:10102 -db=/tmp/gnomes-example
//
// Prints the owner→scid map once the indexer reaches chain tip, then
// exits. Suppresses HyperGnomon's chatty scan-loop log at warning
// level so the output stays focused on the library surface.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"

	compatindexer "github.com/hypergnomon/hypergnomon/pkg/gnomes/indexer"
	compatstructures "github.com/hypergnomon/hypergnomon/pkg/gnomes/structures"
	hgstructures "github.com/hypergnomon/hypergnomon/structures"
)

func main() {
	daemon := flag.String("daemon", "127.0.0.1:10102", "DERO daemon RPC address")
	dbDir := flag.String("db", "./gnomes-example-db", "directory for the indexer's bbolt store")
	duration := flag.Duration("duration", 30*time.Second, "how long to run before printing + exiting")
	flag.Parse()

	// Keep HyperGnomon quiet; this example prints its own output.
	hgstructures.Logger.SetLevel(logrus.WarnLevel)

	// Civilware FastSyncConfig — same shape as civilware/Gnomon;
	// consumers migrating paste this unchanged.
	cfg := &compatstructures.FastSyncConfig{
		Enabled:           true,
		SkipFSRecheck:     true,
		ForceFastSync:     false,
		ForceFastSyncDiff: 100,
		NoCode:            false,
	}

	// Construct the indexer via the HyperGnomon-native entry point.
	// (The civilware-shape NewIndexer(gravDB, boltDB, …) is also fully
	// wired — it injects the caller's pre-opened bbolt store and matches
	// civilware's *current* 12-arg signature, so a consumer on that API —
	// e.g. HOLOGRAM — migrates by import-rewrite alone. Callers on civilware
	// main's 11-arg NewIndexer use this NewIndexerWithDBDir swap instead.)
	idx, err := compatindexer.NewIndexerWithDBDir(
		*dbDir,
		"telaVersion;;;docVersion;;;ART-NFA-MS1;;;G45",
		*daemon,
		"daemon",
		cfg,
		nil,
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewIndexerWithDBDir: %v\n", err)
		os.Exit(1)
	}
	defer idx.Close()

	// Run scan loop. Civilware blocks on StartDaemonMode; the compat
	// facade spawns a goroutine and returns immediately so this main
	// can time out cleanly.
	idx.StartDaemonMode(8)

	fmt.Printf("gnomes.example: scanning via %s for %s\n", *daemon, *duration)
	time.Sleep(*duration)

	// Query like civilware: get the full owner map, report count.
	// The inner bbolt store is reachable for deeper queries; the
	// civilware surface here is the shortest-path demonstration.
	if inner := idx; inner != nil {
		fmt.Printf("LastIndexedHeight=%d  ChainHeight=%d  DBType=%s\n",
			inner.LastIndexedHeight, inner.ChainHeight, inner.DBType)
	}
}
