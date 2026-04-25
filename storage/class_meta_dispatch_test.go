package storage

import (
	"testing"

	"github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"

	"github.com/hypergnomon/hypergnomon/structures"
)

// TestClassMeta_BucketDispatch_MixedEncodings seeds bucketClassIdx with a
// legacy msgpack record AND a typed v1 record, then reads both back through
// GetSCIDClass (which uses decodeClassMeta). Proves the tag-byte dispatch
// doesn't regress legacy readers during the upgrade window.
func TestClassMeta_BucketDispatch_MixedEncodings(t *testing.T) {
	store := openTestStore(t)

	typedSCID := "typed-scid"
	legacySCID := "legacy-scid"

	typedMeta := &structures.ClassMeta{
		Class:         "TELA-INDEX-1",
		Tags:          []string{"all", "tela"},
		Name:          "Typed App",
		DURL:          "typed.tela",
		Version:       "2.0.0",
		InstallHeight: 1000,
		LastHeight:    1500,
	}
	legacyMeta := &structures.ClassMeta{
		Class:         "NFA",
		Tags:          []string{"all", "nfa"},
		Name:          "Legacy NFA",
		InstallHeight: 42,
		LastHeight:    43,
	}

	// Seed bucketClassIdx directly: typed via MarshalTyped, legacy via
	// msgpack.Marshal. Simulates a DB that was populated by an older
	// binary and now gets read by the new decoder.
	err := store.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketClassIdx)
		if err := b.Put([]byte(typedSCID), typedMeta.MarshalTyped()); err != nil {
			return err
		}
		blob, err := msgpack.Marshal(legacyMeta)
		if err != nil {
			return err
		}
		return b.Put([]byte(legacySCID), blob)
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}

	gotTyped, err := store.GetSCIDClass(typedSCID)
	if err != nil {
		t.Fatalf("GetSCIDClass(typed): %v", err)
	}
	if gotTyped == nil || gotTyped.Class != typedMeta.Class || gotTyped.DURL != typedMeta.DURL {
		t.Fatalf("typed read mismatch: got %+v, want %+v", gotTyped, typedMeta)
	}

	gotLegacy, err := store.GetSCIDClass(legacySCID)
	if err != nil {
		t.Fatalf("GetSCIDClass(legacy): %v", err)
	}
	if gotLegacy == nil || gotLegacy.Class != legacyMeta.Class || gotLegacy.Name != legacyMeta.Name {
		t.Fatalf("legacy read mismatch: got %+v, want %+v", gotLegacy, legacyMeta)
	}
}

// TestClassMeta_FlushBatch_WritesTyped asserts that FlushBatch produces
// typed v1 records (not msgpack) so legacy DBs upgrade-in-place on the
// next class write.
func TestClassMeta_FlushBatch_WritesTyped(t *testing.T) {
	store := openTestStore(t)

	scid := fakeSCID()
	batch := NewWriteBatch()
	batch.AddClass(scid, &structures.ClassMeta{
		Class:         "TELA-INDEX-1",
		Tags:          []string{"all", "tela"},
		Name:          "Test",
		InstallHeight: 500,
		LastHeight:    500,
	})
	batch.LastHeight = 500
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}
	PutWriteBatch(batch)

	// Poke the raw bytes and confirm the tag byte is the typed marker.
	err := store.DB.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketClassIdx).Get([]byte(scid))
		if v == nil {
			t.Fatal("classIdx record missing after flush")
		}
		if !structures.IsClassMetaTyped(v) {
			t.Fatalf("FlushBatch produced non-typed record: tag=0x%02x", v[0])
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
}
