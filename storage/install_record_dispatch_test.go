package storage

import (
	"testing"

	"github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"

	"github.com/hypergnomon/hypergnomon/structures"
)

func TestInstallRecord_BucketDispatch_MixedEncodings(t *testing.T) {
	store := openTestStore(t)

	typedSCID := fakeSCID()
	legacySCID := fakeSCID()
	typed := &structures.InstallRecord{Owner: "typed-owner", Entrypoint: "Initialize", Fees: 10}
	legacy := &structures.InstallRecord{Owner: "legacy-owner", Entrypoint: "Initialize", Fees: 20}

	err := store.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketInstalls)
		if err := b.Put(installKey(10, typedSCID), typed.MarshalTyped()); err != nil {
			return err
		}
		blob, err := msgpack.Marshal(legacy)
		if err != nil {
			return err
		}
		return b.Put(installKey(20, legacySCID), blob)
	})
	if err != nil {
		t.Fatalf("seed installs: %v", err)
	}

	got, err := store.GetInstallsInRange(0, 30, 0)
	if err != nil {
		t.Fatalf("GetInstallsInRange: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("installs len = %d, want 2: %+v", len(got), got)
	}
	if got[0].SCID != typedSCID || got[0].InstallHeight != 10 {
		t.Fatalf("typed install mismatch: %+v", got[0])
	}
	if got[1].SCID != legacySCID || got[1].InstallHeight != 20 {
		t.Fatalf("legacy install mismatch: %+v", got[1])
	}

	// GetInstallsInRange only uses the value decode as a sanity check, so
	// assert the actual Owner/Entrypoint/Fees round-trip for BOTH encodings
	// through the dispatching decoder itself.
	err = store.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketInstalls)
		for _, c := range []struct {
			key  []byte
			want *structures.InstallRecord
		}{
			{installKey(10, typedSCID), typed},
			{installKey(20, legacySCID), legacy},
		} {
			rec, err := decodeInstallRecord(b.Get(c.key))
			if err != nil {
				t.Fatalf("decodeInstallRecord(%s): %v", c.key, err)
			}
			if rec == nil || *rec != *c.want {
				t.Fatalf("install value drift: got=%+v want=%+v", rec, c.want)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("verify install values: %v", err)
	}
}

func TestInstallRecord_FlushBatch_WritesTyped(t *testing.T) {
	store := openTestStore(t)

	scid := fakeSCID()
	batch := NewWriteBatch()
	batch.AddInstall(scid, 500, &structures.InstallRecord{
		Owner:      "owner",
		Entrypoint: "Initialize",
		Fees:       99,
	})
	batch.LastHeight = 500
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}
	PutWriteBatch(batch)

	err := store.DB.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketInstalls).Get(installKey(500, scid))
		if v == nil {
			t.Fatal("install record missing after flush")
		}
		if !structures.IsInstallRecordTyped(v) {
			t.Fatalf("FlushBatch produced non-typed install record: tag=0x%02x", v[0])
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read install record: %v", err)
	}
}
