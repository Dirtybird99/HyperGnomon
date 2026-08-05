package storage

import (
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

func TestResetIndex(t *testing.T) {
	store := openTestStore(t)

	// Seed a handful of buckets.
	scid := fakeSCID()
	if err := store.StoreOwner(scid, fakeAddr()); err != nil {
		t.Fatalf("StoreOwner: %v", err)
	}
	if err := store.StoreLastIndexHeight(12345); err != nil {
		t.Fatalf("StoreLastIndexHeight: %v", err)
	}
	if err := store.PutTELAContent(scid, "index.html", &structures.TELAContentEntry{
		Body: []byte("<html/>"), MIME: "text/html",
	}); err != nil {
		t.Fatalf("PutTELAContent: %v", err)
	}

	// Confirm seed data is present.
	if got, _ := store.GetOwner(scid); got == "" {
		t.Fatalf("owner missing before reset")
	}
	if got, _ := store.GetTELAContent(scid, "index.html"); got == nil {
		t.Fatalf("tela content missing before reset")
	}

	if err := store.ResetIndex(); err != nil {
		t.Fatalf("ResetIndex: %v", err)
	}

	// Data buckets should be empty.
	if got, _ := store.GetOwner(scid); got != "" {
		t.Fatalf("owner survived reset: %q", got)
	}
	if got, _ := store.GetTELAContent(scid, "index.html"); got != nil {
		t.Fatalf("tela content survived reset: %+v", got)
	}
	h, err := store.GetLastIndexHeight()
	if err != nil {
		t.Fatalf("GetLastIndexHeight post-reset: %v", err)
	}
	if h != 0 {
		t.Fatalf("last index height not reset: got %d, want 0", h)
	}

	// The buckets themselves should still exist — subsequent writes must work.
	if err := store.StoreOwner(scid, fakeAddr()); err != nil {
		t.Fatalf("StoreOwner after reset: %v", err)
	}
}

// TestResetIndex_DropsPerSCIDInvokeBuckets pins the dynamic-bucket leak: the
// per-SCID invocation buckets are top-level but named by raw SCID, so the old
// fixed drop list never touched them and a resync left every historical
// invocation bucket on disk.
func TestResetIndex_DropsPerSCIDInvokeBuckets(t *testing.T) {
	store := openTestStore(t)

	scid := fakeSCID()
	if err := store.StoreInvokeDetails(scid, fakeAddr(), "Initialize", 42,
		&structures.SCTXParse{Txid: fakeSCID(), Scid: scid, Entrypoint: "Initialize", Height: 42},
	); err != nil {
		t.Fatalf("StoreInvokeDetails: %v", err)
	}
	if got, _ := store.GetInvokeDetailsBySCID(scid); len(got) != 1 {
		t.Fatalf("invoke details missing before reset: got %d records", len(got))
	}
	// Blockhash history must survive reset (reorg detection compares against it).
	if err := store.StoreBlockHash(7, fakeSCID()); err != nil {
		t.Fatalf("StoreBlockHash: %v", err)
	}

	if err := store.ResetIndex(); err != nil {
		t.Fatalf("ResetIndex: %v", err)
	}

	if got, _ := store.GetInvokeDetailsBySCID(scid); len(got) != 0 {
		t.Fatalf("per-SCID invoke bucket survived reset: %d records", len(got))
	}
	if got, _ := store.GetBlockHash(7); got == "" {
		t.Fatalf("blockhash history did not survive reset")
	}
}
