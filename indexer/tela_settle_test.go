package indexer

import (
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// resetTELASettle guards the never-reset-at-runtime global for tests: any test
// that drives a settle path must start from false or later tests pass
// vacuously (see the TELAProbeSettled doc in structures/globals.go).
func resetTELASettle(t *testing.T) {
	t.Helper()
	structures.TELAProbeSettled.Store(false)
	t.Cleanup(func() { structures.TELAProbeSettled.Store(false) })
}

// TestSettleTELADiscovery_SetsFlag pins the invariant behind --tela-only's
// exit gate: the helper both flips TELAProbeSettled and (by construction)
// emits the readiness marker. Two hangs shipped from hand-placing the flag
// and the marker separately; this keeps the helper honest.
func TestSettleTELADiscovery_SetsFlag(t *testing.T) {
	resetTELASettle(t)
	if structures.TELAProbeSettled.Load() {
		t.Fatal("precondition: TELAProbeSettled must start false")
	}
	settleTELADiscovery("unit test", 0)
	if !structures.TELAProbeSettled.Load() {
		t.Fatal("settleTELADiscovery did not set TELAProbeSettled")
	}
}
