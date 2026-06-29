package structures

import (
	"bytes"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

var benchTELA = TELAContentEntry{
	Body:   []byte("<!DOCTYPE html><html><head><title>TELA</title></head><body><h1>Hello</h1></body></html>"),
	MIME:   "text/html; charset=utf-8",
	ETag:   "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
	Height: 6927400,
}

func TestTELAContent_RoundTrip(t *testing.T) {
	cases := []TELAContentEntry{
		benchTELA,
		{Body: []byte("body-only"), MIME: "", ETag: "", Height: 1}, // empty MIME + ETag (msgpack omitempty)
		{Body: nil, MIME: "text/plain", ETag: "abc", Height: 42},   // empty body
		{Body: []byte{}, MIME: "", ETag: "deadbeef", Height: 7},    // empty MIME, present ETag
	}
	for i, want := range cases {
		b := want.MarshalTyped()
		var got TELAContentEntry
		if err := got.UnmarshalTyped(b); err != nil {
			t.Fatalf("case %d UnmarshalTyped: %v", i, err)
		}
		if !bytes.Equal(got.Body, want.Body) {
			t.Errorf("case %d Body: got %q want %q", i, got.Body, want.Body)
		}
		if got.MIME != want.MIME {
			t.Errorf("case %d MIME: got %q want %q", i, got.MIME, want.MIME)
		}
		// ETag must be byte-identical (load-bearing for the zero-alloc 304 path).
		if got.ETag != want.ETag {
			t.Errorf("case %d ETag: got %q want %q", i, got.ETag, want.ETag)
		}
		if got.Height != want.Height {
			t.Errorf("case %d Height: got %d want %d", i, got.Height, want.Height)
		}
	}
}

// TestTELAContent_NoAlias is the mandatory anti-alias gate: after decode, scribble
// 0xFF over the whole source buffer and assert Body/MIME/ETag still equal the
// known literals (Body must be a copy; MIME/ETag must view owned memory, never b).
func TestTELAContent_NoAlias(t *testing.T) {
	const (
		wantMIME = "text/html; charset=utf-8"
		wantETag = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
	)
	wantBody := []byte("<html><body>scribble me</body></html>")
	in := TELAContentEntry{Body: wantBody, MIME: wantMIME, ETag: wantETag, Height: 6927400}
	b := in.MarshalTyped()

	var got TELAContentEntry
	if err := got.UnmarshalTyped(b); err != nil {
		t.Fatalf("UnmarshalTyped: %v", err)
	}
	if !bytes.Equal(got.Body, wantBody) || got.MIME != wantMIME || got.ETag != wantETag {
		t.Fatalf("decode drift before scribble: %+v", got)
	}

	scribble(b) // 0xFF over the entire source buffer (helper from zz_loop_antialias_test.go)

	if !bytes.Equal(got.Body, wantBody) {
		t.Errorf("Body aliases source buffer b: got %q want %q", got.Body, wantBody)
	}
	if got.MIME != wantMIME {
		t.Errorf("MIME aliases source buffer b: got %q want %q", got.MIME, wantMIME)
	}
	if got.ETag != wantETag {
		t.Errorf("ETag aliases source buffer b: got %q want %q", got.ETag, wantETag)
	}
}

func TestTELAContent_TypedDetect(t *testing.T) {
	typed := benchTELA.MarshalTyped()
	if !IsTELAContentTyped(typed) {
		t.Errorf("typed record not detected as typed")
	}
	legacy, err := msgpack.Marshal(&benchTELA)
	if err != nil {
		t.Fatalf("msgpack.Marshal: %v", err)
	}
	if IsTELAContentTyped(legacy) {
		t.Errorf("legacy msgpack record (first byte %#x) misdetected as typed", legacy[0])
	}
}

func BenchmarkTELAContentEntry_Marshal_Msgpack(b *testing.B) {
	e := benchTELA
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = msgpack.Marshal(&e)
	}
}

func BenchmarkTELAContentEntry_Marshal_Typed(b *testing.B) {
	e := benchTELA
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = e.MarshalTyped()
	}
}

func BenchmarkTELAContentEntry_Unmarshal_Msgpack(b *testing.B) {
	enc, _ := msgpack.Marshal(&benchTELA)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var e TELAContentEntry
		_ = msgpack.Unmarshal(enc, &e)
	}
}

func BenchmarkTELAContentEntry_Unmarshal_Typed(b *testing.B) {
	enc := benchTELA.MarshalTyped()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var e TELAContentEntry
		_ = e.UnmarshalTyped(enc)
	}
}
