package storage

import (
	"testing"

	"github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"

	"github.com/hypergnomon/hypergnomon/structures"
)

// TestAddrSCIDs_ReadsLegacyMsgpack writes a legacy msgpack-encoded
// AddrSCIDEntry directly into the bucket (bypassing FlushBatch) and
// asserts GetAddressSCIDs decodes it correctly. This guards the
// backward-compat upgrade path: databases from before the v1 typed
// rollout must remain readable.
func TestAddrSCIDs_ReadsLegacyMsgpack(t *testing.T) {
	store := openTestStore(t)

	addr := fakeAddr()
	scid := fakeSCID()
	legacy := structures.AddrSCIDEntry{FirstHeight: 100, LastHeight: 500, Count: 42}
	mp, err := msgpack.Marshal(&legacy)
	if err != nil {
		t.Fatal(err)
	}

	// Write the legacy msgpack blob directly, bypassing FlushBatch.
	err = store.DB.Update(func(tx *bolt.Tx) error {
		parent := tx.Bucket(bucketAddrSCIDs)
		sub, e := parent.CreateBucketIfNotExists([]byte(addr))
		if e != nil {
			return e
		}
		return sub.Put([]byte(scid), mp)
	})
	if err != nil {
		t.Fatalf("write legacy: %v", err)
	}

	// Read via GetAddressSCIDs — should decode via msgpack path.
	got, err := store.GetAddressSCIDs(addr)
	if err != nil {
		t.Fatalf("GetAddressSCIDs: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(got))
	}
	g := got[scid]
	if g == nil {
		t.Fatal("expected entry for scid, missing")
	}
	if g.FirstHeight != 100 || g.LastHeight != 500 || g.Count != 42 {
		t.Fatalf("legacy decode drift: got %+v", *g)
	}
}

// TestAddrSCIDs_UpgradesOnWrite asserts that after writing a legacy
// msgpack blob and then flushing a delta through FlushBatch, the stored
// blob is now v1-typed (25 bytes, tag 0x01).
func TestAddrSCIDs_UpgradesOnWrite(t *testing.T) {
	store := openTestStore(t)

	addr := fakeAddr()
	scid := fakeSCID()
	legacy := structures.AddrSCIDEntry{FirstHeight: 100, LastHeight: 500, Count: 42}
	mp, _ := msgpack.Marshal(&legacy)

	err := store.DB.Update(func(tx *bolt.Tx) error {
		parent := tx.Bucket(bucketAddrSCIDs)
		sub, _ := parent.CreateBucketIfNotExists([]byte(addr))
		return sub.Put([]byte(scid), mp)
	})
	if err != nil {
		t.Fatal(err)
	}

	// FlushBatch with a delta that will merge + rewrite.
	batch := NewWriteBatch()
	batch.AddAddrSCID(addr, scid, 600) // new height
	batch.LastHeight = 600
	if err := store.FlushBatch(batch); err != nil {
		t.Fatal(err)
	}

	// Read raw bytes — expect v1 typed format (tag 0x01, 25 bytes total).
	var raw []byte
	_ = store.DB.View(func(tx *bolt.Tx) error {
		sub := tx.Bucket(bucketAddrSCIDs).Bucket([]byte(addr))
		v := sub.Get([]byte(scid))
		raw = append([]byte(nil), v...) // copy; slice only valid in-txn
		return nil
	})
	if len(raw) != structures.EncodedAddrSCIDEntrySize {
		t.Fatalf("expected typed size %d after flush, got %d (tag=0x%02x)",
			structures.EncodedAddrSCIDEntrySize, len(raw), raw[0])
	}
	if raw[0] != structures.TagAddrSCIDEntryV1 {
		t.Fatalf("expected tag 0x%02x, got 0x%02x", structures.TagAddrSCIDEntryV1, raw[0])
	}

	// And the merged semantics: first=min(100,600)=100, last=max(500,600)=600, count=42+1=43.
	got, _ := store.GetAddressSCIDs(addr)
	g := got[scid]
	if g == nil {
		t.Fatal("entry missing post-flush")
	}
	if g.FirstHeight != 100 || g.LastHeight != 600 || g.Count != 43 {
		t.Fatalf("merge drift: got %+v want {100 600 43}", *g)
	}
}
