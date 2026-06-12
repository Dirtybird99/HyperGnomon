package storage

import (
	"testing"

	"github.com/deroproject/derohe/rpc"
	"github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"

	"github.com/hypergnomon/hypergnomon/structures"
)

func TestInvokeDetails_BucketDispatch_MixedEncodings(t *testing.T) {
	store := openTestStore(t)
	scid := fakeSCID()
	typed := &structures.SCTXParse{
		Txid:       fakeTxid(),
		Scid:       scid,
		Entrypoint: "InputStr",
		Method:     structures.MethodInvokeSC,
		Sender:     fakeAddr(),
		Fees:       1,
		Height:     10,
	}
	legacy := &structures.SCTXParse{
		Txid:       fakeTxid(),
		Scid:       scid,
		Entrypoint: "Transfer",
		Method:     structures.MethodInvokeSC,
		Sender:     fakeAddr(),
		Fees:       2,
		Height:     20,
	}

	err := store.DB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(scid))
		if err != nil {
			return err
		}
		if err := b.Put([]byte("typed"), typed.MarshalTurboTyped()); err != nil {
			return err
		}
		blob, err := msgpack.Marshal(legacy)
		if err != nil {
			return err
		}
		return b.Put([]byte("legacy"), blob)
	})
	if err != nil {
		t.Fatalf("seed invokes: %v", err)
	}

	got, err := store.GetInvokeDetailsBySCID(scid)
	if err != nil {
		t.Fatalf("GetInvokeDetailsBySCID: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("invoke len = %d, want 2: %+v", len(got), got)
	}
	// Full value round-trip per encoding, not just Txid membership.
	byTxid := map[string]*structures.SCTXParse{}
	for _, inv := range got {
		if inv == nil {
			t.Fatalf("nil record in results: %+v", got)
		}
		byTxid[inv.Txid] = inv
	}
	for _, want := range []*structures.SCTXParse{typed, legacy} {
		rec, ok := byTxid[want.Txid]
		if !ok {
			t.Fatalf("mixed invoke decode missing record %s: got=%+v", want.Txid, got)
		}
		if rec.Entrypoint != want.Entrypoint || rec.Sender != want.Sender ||
			rec.Fees != want.Fees || rec.Height != want.Height || rec.Method != want.Method {
			t.Fatalf("decoded record drift:\n got=%+v\nwant=%+v", rec, want)
		}
	}
}

func TestInvokeDetails_EmptyValueSkipped(t *testing.T) {
	store := openTestStore(t)
	scid := fakeSCID()
	typed := &structures.SCTXParse{
		Txid: fakeTxid(), Scid: scid, Entrypoint: "InputStr",
		Method: structures.MethodInvokeSC, Sender: fakeAddr(), Fees: 1, Height: 10,
	}

	err := store.DB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(scid))
		if err != nil {
			return err
		}
		if err := b.Put([]byte("real"), typed.MarshalTurboTyped()); err != nil {
			return err
		}
		// Empty value: must be skipped, not surfaced as a nil record.
		return b.Put([]byte("empty"), []byte{})
	})
	if err != nil {
		t.Fatalf("seed invokes: %v", err)
	}

	got, err := store.GetInvokeDetailsBySCID(scid)
	if err != nil {
		t.Fatalf("GetInvokeDetailsBySCID: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("invoke len = %d, want 1 (empty value skipped): %+v", len(got), got)
	}
	if got[0] == nil || got[0].Txid != typed.Txid {
		t.Fatalf("surviving record wrong: %+v", got[0])
	}
}

func TestInvokeDetails_FlushBatch_NonTurboKeepsArgs(t *testing.T) {
	store := openTestStore(t)
	scid := fakeSCID()
	sender := fakeAddr()
	txid := fakeTxid()

	details := &structures.SCTXParse{
		Txid:       txid,
		Scid:       scid,
		Entrypoint: "Transfer",
		Method:     structures.MethodInvokeSC,
		ScArgs: rpc.Arguments{
			{Name: "entrypoint", DataType: rpc.DataString, Value: "Transfer"},
			{Name: "amount", DataType: rpc.DataUint64, Value: uint64(42)},
		},
		Sender: sender,
		Fees:   7,
		Height: 33,
	}
	if details.CanMarshalTurboTyped() {
		t.Fatal("fixture with ScArgs must not be turbo-typed eligible")
	}

	batch := NewWriteBatch()
	batch.AddInvocation(structures.InvokeRecord{
		Scid: scid, Sender: sender, Entrypoint: "Transfer", Height: 33, Details: details,
	})
	batch.LastHeight = 33
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}
	PutWriteBatch(batch)

	// Stored blob must be the msgpack fallback, not turbo-typed.
	err := store.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(scid))
		if b == nil {
			t.Fatal("invoke bucket missing")
		}
		_, v := b.Cursor().First()
		if v == nil {
			t.Fatal("invoke record missing after flush")
		}
		if structures.IsSCTXParseTurboTyped(v) {
			t.Fatal("non-turbo record (ScArgs present) stored as turbo-typed — args would be lost")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read invoke record: %v", err)
	}

	got, err := store.GetInvokeDetailsBySCID(scid)
	if err != nil {
		t.Fatalf("GetInvokeDetailsBySCID: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("invoke len = %d, want 1", len(got))
	}
	rec := got[0]
	if len(rec.ScArgs) != 2 {
		t.Fatalf("ScArgs lost through flush+read: %+v", rec)
	}
	if rec.Entrypoint != "Transfer" || rec.Fees != 7 || rec.Height != 33 || rec.Sender != sender {
		t.Fatalf("non-turbo record drift: %+v", rec)
	}
}

func TestInvokeDetails_FlushBatch_WritesTurboTyped(t *testing.T) {
	store := openTestStore(t)
	scid := fakeSCID()
	sender := fakeAddr()
	txid := fakeTxid()

	batch := NewWriteBatch()
	batch.AddInvocation(structures.InvokeRecord{
		Scid:       scid,
		Sender:     sender,
		Entrypoint: "InputStr",
		Height:     10,
		Details: &structures.SCTXParse{
			Txid:       txid,
			Scid:       scid,
			Entrypoint: "InputStr",
			Method:     structures.MethodInvokeSC,
			Sender:     sender,
			Fees:       1,
			Height:     10,
		},
	})
	batch.LastHeight = 10
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}
	PutWriteBatch(batch)

	err := store.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(scid))
		if b == nil {
			t.Fatal("invoke bucket missing")
		}
		c := b.Cursor()
		_, v := c.First()
		if v == nil {
			t.Fatal("invoke record missing after flush")
		}
		if !structures.IsSCTXParseTurboTyped(v) {
			t.Fatalf("FlushBatch produced non-typed turbo invoke: tag=0x%02x", v[0])
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read invoke record: %v", err)
	}
}
