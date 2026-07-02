package api

import (
	"net/http"
	"testing"
)

// nopResponseWriter isolates writeJSON's own allocations from any recorder
// machinery: headers are a small fixed map, writes are counted and dropped.
type nopResponseWriter struct{ h http.Header }

func (w *nopResponseWriter) Header() http.Header         { return w.h }
func (w *nopResponseWriter) Write(b []byte) (int, error) { return len(b), nil }
func (w *nopResponseWriter) WriteHeader(int)             {}

// BenchmarkWriteJSON measures the per-response cost of the shared writeJSON
// helper (every REST handler funnels through it). The payload is a typed
// struct so the encoder alloc — the target — dominates over boxing noise.
func BenchmarkWriteJSON(b *testing.B) {
	type resp struct {
		Scid   string `json:"scid"`
		Owner  string `json:"owner"`
		Height int64  `json:"height"`
		Count  int    `json:"count"`
	}
	v := &resp{
		Scid:   "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899",
		Owner:  "dero1qyjjxxaabbccddeeff0011223344556677889900aabbccddee0011223344",
		Height: 7_270_000,
		Count:  42,
	}
	w := &nopResponseWriter{h: make(http.Header, 4)}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		writeJSON(w, http.StatusOK, v)
	}
}
