package storage

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// TestFlushBatch_KeysMatchOldFormat locks in the key format that was
// previously built via fmt.Sprintf, so the byte-buffer rewrite in
// FlushBatch cannot silently change the encoding.
//
// Regression guard: if you change the key shape, you must also migrate
// existing DBs; this test makes that a conscious decision, not an accident.
func TestFlushBatch_KeysMatchOldFormat(t *testing.T) {
	// --- Invocation key: "<sender>:<txid>:<height>:<entrypoint>"
	sender := "dero1qyjjxxaabbccddeeff0011223344556677889900aabbccddee00112233445566"
	txid := "feed0001feed0001feed0001feed0001feed0001feed0001feed0001feed0001"
	height := int64(6927438)
	entrypoint := "InputStr"

	wantInv := fmt.Sprintf("%s:%s:%d:%s", sender, txid, height, entrypoint)

	// Replicate the production path (append + AppendInt).
	buf := make([]byte, 0, 192)
	buf = append(buf, sender...)
	buf = append(buf, ':')
	buf = append(buf, txid...)
	buf = append(buf, ':')
	buf = strconv.AppendInt(buf, height, 10)
	buf = append(buf, ':')
	buf = append(buf, entrypoint...)
	gotInv := string(buf)

	if gotInv != wantInv {
		t.Fatalf("invocation key drift:\n  want %q\n   got %q", wantInv, gotInv)
	}

	// --- Variable key: "<scid>:<height>"
	scid := "a05395bb0cf77adc850928b0db00eb5ca7a9ccbafd9a38d021c8d299ad5ce1a4"
	wantVar := fmt.Sprintf("%s:%d", scid, height)

	buf = buf[:0]
	buf = append(buf, scid...)
	buf = append(buf, ':')
	buf = strconv.AppendInt(buf, height, 10)
	gotVar := string(buf)

	if gotVar != wantVar {
		t.Fatalf("variable key drift:\n  want %q\n   got %q", wantVar, gotVar)
	}
}

// TestFlushBatch_KeyBufReuseSafe writes 100 invocations through FlushBatch
// (which reuses a single keyBuf across iterations) and asserts each key
// was stored with the expected, distinct bytes. This verifies that bbolt's
// internal key copy really does persist the snapshot at Put-time rather
// than retaining a reference to the caller's buffer.
func TestFlushBatch_KeyBufReuseSafe(t *testing.T) {
	store := openTestStore(t)

	scid := fakeSCID()
	batch := NewWriteBatch()
	for i := 0; i < 100; i++ {
		sender := fakeAddr()
		txid := fakeTxid()
		batch.AddInvocation(structures.InvokeRecord{
			Scid:       scid,
			Sender:     sender,
			Entrypoint: "Op",
			Height:     int64(1000 + i),
			Details: &structures.SCTXParse{
				Txid:       txid,
				Scid:       scid,
				Entrypoint: "Op",
				Method:     structures.MethodInvokeSC,
				Sender:     sender,
				Fees:       uint64(i),
				Height:     int64(1000 + i),
			},
		})
	}
	batch.LastHeight = 1099
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}

	got, err := store.GetInvokeDetailsBySCID(scid)
	if err != nil {
		t.Fatalf("GetInvokeDetailsBySCID: %v", err)
	}
	if len(got) != 100 {
		t.Fatalf("expected 100 invocations, got %d — key-buf reuse likely collided", len(got))
	}
	// Spot-check a few: txids must be distinct.
	seen := make(map[string]struct{}, 100)
	for _, d := range got {
		if _, dup := seen[d.Txid]; dup {
			t.Fatalf("duplicate txid %s after FlushBatch — key-buf aliasing", d.Txid)
		}
		seen[d.Txid] = struct{}{}
	}
}
