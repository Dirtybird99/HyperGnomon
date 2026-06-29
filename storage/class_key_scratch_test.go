package storage

import (
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// TestClassMeta_FlushBatch_MultiSCID_NoKeyBufClobber flushes ONE batch with
// two distinct class SCIDs (different Class + InstallHeight) and reads each
// back via BOTH GetSCIDClass (classIdx) and GetClassInstalls (classPrefix).
// If the shared keyBuf key scratch were clobbered across loop iterations, a
// SCID would land under the wrong class key or have a corrupted scid key —
// caught here.
func TestClassMeta_FlushBatch_MultiSCID_NoKeyBufClobber(t *testing.T) {
	store := openTestStore(t)

	scidA := fakeSCID()
	scidB := fakeSCID()

	batch := NewWriteBatch()
	batch.AddClass(scidA, &structures.ClassMeta{
		Class:         "TELA-INDEX-1",
		Tags:          []string{"all", "tela"},
		Name:          "App A",
		InstallHeight: 1000,
		LastHeight:    1000,
	})
	batch.AddClass(scidB, &structures.ClassMeta{
		Class:         "NFA",
		Tags:          []string{"all", "nfa"},
		Name:          "App B",
		InstallHeight: 2000,
		LastHeight:    2000,
	})
	batch.LastHeight = 2000
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}
	PutWriteBatch(batch)

	// classIdx round-trip: each SCID resolves to its OWN class/name/height.
	gotA, err := store.GetSCIDClass(scidA)
	if err != nil || gotA == nil {
		t.Fatalf("GetSCIDClass(A): meta=%v err=%v", gotA, err)
	}
	if gotA.Class != "TELA-INDEX-1" || gotA.Name != "App A" || gotA.InstallHeight != 1000 {
		t.Fatalf("scidA decoded wrong: %+v", gotA)
	}
	gotB, err := store.GetSCIDClass(scidB)
	if err != nil || gotB == nil {
		t.Fatalf("GetSCIDClass(B): meta=%v err=%v", gotB, err)
	}
	if gotB.Class != "NFA" || gotB.Name != "App B" || gotB.InstallHeight != 2000 {
		t.Fatalf("scidB decoded wrong: %+v", gotB)
	}

	// classPrefix round-trip: each class scan returns exactly its own SCID.
	tela, err := store.GetClassInstalls("TELA-INDEX-1", 0)
	if err != nil {
		t.Fatalf("GetClassInstalls(TELA): %v", err)
	}
	if !classInstallsContain(tela, scidA) || classInstallsContain(tela, scidB) {
		t.Fatalf("TELA scan wrong: %+v (want only scidA)", tela)
	}
	nfa, err := store.GetClassInstalls("NFA", 0)
	if err != nil {
		t.Fatalf("GetClassInstalls(NFA): %v", err)
	}
	if !classInstallsContain(nfa, scidB) || classInstallsContain(nfa, scidA) {
		t.Fatalf("NFA scan wrong: %+v (want only scidB)", nfa)
	}
}

// TestClassMeta_FlushBatch_StaleDelete_KeyBuf re-flushes the SAME scid at a
// new (Class, InstallHeight) in a second batch and asserts the stale
// classPrefix row is gone. The stale-delete key is built into the shared
// keyBuf scratch from the OLD meta; a clobbered/miscomputed key would leave
// the old row behind.
func TestClassMeta_FlushBatch_StaleDelete_KeyBuf(t *testing.T) {
	store := openTestStore(t)
	scid := fakeSCID()

	b1 := NewWriteBatch()
	b1.AddClass(scid, &structures.ClassMeta{
		Class:         "NFA",
		Tags:          []string{"all", "nfa"},
		Name:          "Before",
		InstallHeight: 100,
		LastHeight:    100,
	})
	b1.LastHeight = 100
	if err := store.FlushBatch(b1); err != nil {
		t.Fatalf("FlushBatch b1: %v", err)
	}
	PutWriteBatch(b1)

	nfa0, err := store.GetClassInstalls("NFA", 0)
	if err != nil {
		t.Fatalf("GetClassInstalls(NFA) pre: %v", err)
	}
	if !classInstallsContain(nfa0, scid) {
		t.Fatalf("scid missing from NFA scan after first flush: %+v", nfa0)
	}

	// Re-classify same SCID under a new class AND new install height.
	b2 := NewWriteBatch()
	b2.AddClass(scid, &structures.ClassMeta{
		Class:         "TELA-INDEX-1",
		Tags:          []string{"all", "tela"},
		Name:          "After",
		InstallHeight: 200,
		LastHeight:    200,
	})
	b2.LastHeight = 200
	if err := store.FlushBatch(b2); err != nil {
		t.Fatalf("FlushBatch b2: %v", err)
	}
	PutWriteBatch(b2)

	// Stale NFA|100|scid row must be deleted (this is the keyBuf Delete path).
	nfa1, err := store.GetClassInstalls("NFA", 0)
	if err != nil {
		t.Fatalf("GetClassInstalls(NFA) post: %v", err)
	}
	if classInstallsContain(nfa1, scid) {
		t.Fatalf("stale NFA row not deleted: %+v", nfa1)
	}

	// New TELA|200|scid row must be present at the new height.
	tela, err := store.GetClassInstalls("TELA-INDEX-1", 0)
	if err != nil {
		t.Fatalf("GetClassInstalls(TELA) post: %v", err)
	}
	if !classInstallsContain(tela, scid) {
		t.Fatalf("new TELA row missing: %+v", tela)
	}
	for _, ci := range tela {
		if ci.SCID == scid && ci.InstallHeight != 200 {
			t.Fatalf("TELA row has wrong height: %+v", ci)
		}
	}

	// classIdx must point at the new class/height.
	got, err := store.GetSCIDClass(scid)
	if err != nil || got == nil {
		t.Fatalf("GetSCIDClass post: %v %v", got, err)
	}
	if got.Class != "TELA-INDEX-1" || got.InstallHeight != 200 {
		t.Fatalf("classIdx not updated: %+v", got)
	}
}

func classInstallsContain(list []structures.ClassInstall, scid string) bool {
	for _, ci := range list {
		if ci.SCID == scid {
			return true
		}
	}
	return false
}
