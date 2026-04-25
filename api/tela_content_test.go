package api

import (
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

func TestTELAContentCache_PutGet(t *testing.T) {
	c := newTELAContentCache(1024)
	entry := &structures.TELAContentEntry{Body: []byte("hello"), MIME: "text/plain"}
	c.Put("scid1|index.html", entry)

	got := c.Get("scid1|index.html")
	if got == nil || string(got.Body) != "hello" {
		t.Fatalf("Get returned %+v, want body=hello", got)
	}
	if c.Get("missing") != nil {
		t.Fatalf("Get on missing key returned non-nil")
	}
}

func TestTELAContentCache_SizeBound(t *testing.T) {
	// Cap at 100 bytes. Insert four 50-byte entries; oldest two must evict.
	c := newTELAContentCache(100)
	for i, k := range []string{"a|p", "b|p", "c|p", "d|p"} {
		c.Put(k, &structures.TELAContentEntry{
			Body: make([]byte, 50),
			MIME: "application/octet-stream",
		})
		_ = i
	}
	// Expect the newest two to survive.
	if c.Get("a|p") != nil {
		t.Fatalf("oldest entry a|p should have been evicted")
	}
	if c.Get("b|p") != nil {
		t.Fatalf("entry b|p should have been evicted")
	}
	if c.Get("c|p") == nil || c.Get("d|p") == nil {
		t.Fatalf("newest entries should have survived")
	}
}

func TestTELAContentCache_InvalidatePrefix(t *testing.T) {
	c := newTELAContentCache(1024)
	c.Put("scid1|a", &structures.TELAContentEntry{Body: []byte("1a")})
	c.Put("scid1|b", &structures.TELAContentEntry{Body: []byte("1b")})
	c.Put("scid2|a", &structures.TELAContentEntry{Body: []byte("2a")})

	c.InvalidatePrefix("scid1")

	if c.Get("scid1|a") != nil {
		t.Fatalf("scid1|a not invalidated")
	}
	if c.Get("scid1|b") != nil {
		t.Fatalf("scid1|b not invalidated")
	}
	if c.Get("scid2|a") == nil {
		t.Fatalf("scid2|a incorrectly invalidated")
	}
}

func TestExtractDOCBody_Single(t *testing.T) {
	vars := []*structures.SCIDVariable{
		{Key: "code", Value: "hello world"},
	}
	if got := string(extractDOCBody(vars)); got != "hello world" {
		t.Fatalf("single-chunk body: got %q want hello world", got)
	}
}

// TestExtractDOCBodyFromSource validates the canonical TELA-DOC-1 body
// extraction: body lives in the trailing /* ... */ comment of the SC
// source (not a STORE variable). Replaces the previous _Chunked test
// which covered a non-canonical CODE_N variable layout.
func TestExtractDOCBodyFromSource(t *testing.T) {
	src := `Function InitializePrivate() Uint64
	10 RETURN 0
End Function
/*
<html><body>hello tela</body></html>
*/`
	body, err := extractDOCBodyFromSource(src)
	if err != nil {
		t.Fatalf("extractDOCBodyFromSource: %v", err)
	}
	if string(body) != "<html><body>hello tela</body></html>" {
		t.Fatalf("body = %q, want '<html><body>hello tela</body></html>'", body)
	}
}

// TestExtractDOCBodyFromSource_MissingComment rejects source without the
// trailing /* */ block.
func TestExtractDOCBodyFromSource_MissingComment(t *testing.T) {
	src := `Function X() Uint64
	10 RETURN 0
End Function`
	if _, err := extractDOCBodyFromSource(src); err == nil {
		t.Fatal("expected error on missing /* */ block")
	}
}

// TestMIMEForDocType covers the canonical docType → MIME mapping.
func TestMIMEForDocType(t *testing.T) {
	cases := map[string]string{
		"TELA-HTML-1":   "text/html; charset=utf-8",
		"TELA-CSS-1":    "text/css; charset=utf-8",
		"TELA-JS-1":     "application/javascript; charset=utf-8",
		"TELA-JSON-1":   "application/json; charset=utf-8",
		"TELA-MD-1":     "text/markdown; charset=utf-8",
		"TELA-GO-1":     "text/x-go; charset=utf-8",
		"TELA-STATIC-1": "application/octet-stream",
		"TELA-UNKNOWN":  "",
	}
	for docType, want := range cases {
		if got := mimeForDocType(docType); got != want {
			t.Errorf("mimeForDocType(%q) = %q, want %q", docType, got, want)
		}
	}
}

func TestExtractDOCBody_Empty(t *testing.T) {
	vars := []*structures.SCIDVariable{
		{Key: "unrelated", Value: "nope"},
	}
	if got := extractDOCBody(vars); got != nil {
		t.Fatalf("no body keys: got %q want nil", got)
	}
}
