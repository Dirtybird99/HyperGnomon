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
