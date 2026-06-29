package storage

import (
	"bytes"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
	bolt "go.etcd.io/bbolt"

	"github.com/hypergnomon/hypergnomon/structures"
)

// TestGetTELAContent_TypedRoundTrip verifies the typed Put/Get path: a typed
// record written by PutTELAContent decodes back byte-identically via GetTELAContent.
func TestGetTELAContent_TypedRoundTrip(t *testing.T) {
	s := openTestStore(t)
	want := &structures.TELAContentEntry{
		Body:   []byte("<html><body>typed</body></html>"),
		MIME:   "text/html; charset=utf-8",
		ETag:   "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef0",
		Height: 6927400,
	}
	if err := s.PutTELAContent("scidA", "index.html", want); err != nil {
		t.Fatalf("PutTELAContent: %v", err)
	}
	got, err := s.GetTELAContent("scidA", "index.html")
	if err != nil || got == nil {
		t.Fatalf("GetTELAContent: %v (got=%v)", err, got)
	}
	if !bytes.Equal(got.Body, want.Body) || got.MIME != want.MIME || got.ETag != want.ETag || got.Height != want.Height {
		t.Errorf("round-trip mismatch: got %+v want %+v", got, want)
	}
}

// TestGetTELAContent_LegacyMsgpackFallback seeds a legacy msgpack record under
// the content bucket and asserts GetTELAContent's byte[0] dispatch still decodes
// it (backward-compat for records written before the typed switch).
func TestGetTELAContent_LegacyMsgpackFallback(t *testing.T) {
	s := openTestStore(t)
	want := &structures.TELAContentEntry{Body: []byte("legacy body"), MIME: "text/plain", ETag: "abc123", Height: 100}
	enc, err := msgpack.Marshal(want)
	if err != nil {
		t.Fatalf("msgpack.Marshal: %v", err)
	}
	if err := s.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketTELAContent).Put(telaContentKey("scidL", "p.html"), enc)
	}); err != nil {
		t.Fatalf("seed legacy: %v", err)
	}
	got, err := s.GetTELAContent("scidL", "p.html")
	if err != nil || got == nil {
		t.Fatalf("GetTELAContent: %v (got=%v)", err, got)
	}
	if !bytes.Equal(got.Body, want.Body) || got.MIME != want.MIME || got.ETag != want.ETag || got.Height != want.Height {
		t.Errorf("legacy decode mismatch: got %+v want %+v", got, want)
	}
}
