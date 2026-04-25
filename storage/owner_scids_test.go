package storage

import (
	"fmt"
	"testing"
)

// TestGetSCIDsByOwner validates the reverse owner→SCIDs index built from
// WriteBatch.AddOwner calls. The index should be prefix-scannable and
// return the full set for a given owner.
func TestGetSCIDsByOwner(t *testing.T) {
	store := openTestStore(t)

	alice := "dero1qyalice00000000000000000000000000000000000000000000000000000000aa"
	bob := "dero1qybob00000000000000000000000000000000000000000000000000000000000bb"

	aliceSCIDs := []string{fakeSCID(), fakeSCID(), fakeSCID()}
	bobSCIDs := []string{fakeSCID(), fakeSCID()}

	batch := NewWriteBatch()
	for _, scid := range aliceSCIDs {
		batch.AddOwner(scid, alice)
	}
	for _, scid := range bobSCIDs {
		batch.AddOwner(scid, bob)
	}
	batch.LastHeight = 1000
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}
	PutWriteBatch(batch)

	got, err := store.GetSCIDsByOwner(alice)
	if err != nil {
		t.Fatalf("GetSCIDsByOwner(alice): %v", err)
	}
	if len(got) != len(aliceSCIDs) {
		t.Fatalf("alice: got %d scids, want %d", len(got), len(aliceSCIDs))
	}
	for _, want := range aliceSCIDs {
		found := false
		for _, g := range got {
			if g == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("alice scid %s missing from results", want)
		}
	}

	got, err = store.GetSCIDsByOwner(bob)
	if err != nil {
		t.Fatalf("GetSCIDsByOwner(bob): %v", err)
	}
	if len(got) != len(bobSCIDs) {
		t.Fatalf("bob: got %d scids, want %d", len(got), len(bobSCIDs))
	}

	// Unknown owner returns empty slice + nil error.
	got, err = store.GetSCIDsByOwner("dero1qyunknown00000000000000000000000000000000000000000000000000000000")
	if err != nil {
		t.Fatalf("GetSCIDsByOwner(unknown): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("unknown owner: got %d scids, want 0", len(got))
	}

	// Empty owner short-circuits.
	got, _ = store.GetSCIDsByOwner("")
	if got != nil {
		t.Fatalf("empty owner: got %v, want nil", got)
	}
}

// TestGetSCIDsByOwner_PrefixIsolation ensures scids for `alice` don't leak
// into results for an owner whose address is a prefix of `alice`. Our
// bucket key layout uses '|' as separator, so the prefix scan terminates
// cleanly when the separator differs.
func TestGetSCIDsByOwner_PrefixIsolation(t *testing.T) {
	store := openTestStore(t)

	// A pair where owner1 is a prefix of owner2's first characters.
	owner1 := "dero1qyp"
	owner2 := "dero1qyp00000000000000"

	scid1 := fakeSCID()
	scid2 := fakeSCID()

	batch := NewWriteBatch()
	batch.AddOwner(scid1, owner1)
	batch.AddOwner(scid2, owner2)
	batch.LastHeight = 1
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}
	PutWriteBatch(batch)

	got1, _ := store.GetSCIDsByOwner(owner1)
	if len(got1) != 1 || got1[0] != scid1 {
		t.Fatalf("owner1: got %v, want [%s]", got1, scid1)
	}
	got2, _ := store.GetSCIDsByOwner(owner2)
	if len(got2) != 1 || got2[0] != scid2 {
		t.Fatalf("owner2: got %v, want [%s]", got2, scid2)
	}
}

// TestGetSCIDsByOwner_Dedup confirms re-adding the same (owner,scid) pair
// across multiple batches doesn't produce duplicate results.
func TestGetSCIDsByOwner_Dedup(t *testing.T) {
	store := openTestStore(t)
	owner := "dero1qydedup0000000000000000000000000000000000000000000000000000000000"
	scid := fakeSCID()

	for i := 0; i < 3; i++ {
		batch := NewWriteBatch()
		batch.AddOwner(scid, owner)
		batch.LastHeight = int64(i)
		if err := store.FlushBatch(batch); err != nil {
			t.Fatalf("FlushBatch iter %d: %v", i, err)
		}
		PutWriteBatch(batch)
	}
	got, _ := store.GetSCIDsByOwner(owner)
	if len(got) != 1 {
		t.Fatalf("got %d scids after 3 dupe writes, want 1: %v", len(got), got)
	}
}

// BenchmarkGetSCIDsByOwner measures the prefix scan at realistic owner
// populations. Confirms cost is O(|scids-for-this-owner|) independent of
// total bucket size.
func BenchmarkGetSCIDsByOwner(b *testing.B) {
	for _, n := range []int{1, 16, 128, 1024} {
		b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
			store := openTestStore(b)
			owner := "dero1qybench00000000000000000000000000000000000000000000000000000000"
			// Seed one owner with n SCIDs + some background noise.
			batch := NewWriteBatch()
			for i := 0; i < n; i++ {
				batch.AddOwner(fakeSCID(), owner)
			}
			// 100 noise owners with a random SCID each, so the prefix scan
			// has a realistic "must skip over other owners" cost.
			for i := 0; i < 100; i++ {
				batch.AddOwner(fakeSCID(), fakeAddr())
			}
			batch.LastHeight = 1
			if err := store.FlushBatch(batch); err != nil {
				b.Fatal(err)
			}
			PutWriteBatch(batch)

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				got, err := store.GetSCIDsByOwner(owner)
				if err != nil {
					b.Fatal(err)
				}
				if len(got) != n {
					b.Fatalf("got %d, want %d", len(got), n)
				}
			}
		})
	}
}
