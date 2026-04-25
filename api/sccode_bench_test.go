package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/gorilla/mux"

	"github.com/hypergnomon/hypergnomon/eventbus"
	"github.com/hypergnomon/hypergnomon/indexer"
	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

func newBenchHTTPServer(b *testing.B) (*httptest.Server, *indexer.Indexer) {
	b.Helper()
	store, err := storage.NewBboltStore(b.TempDir(), "")
	if err != nil {
		b.Fatalf("NewBboltStore: %v", err)
	}
	b.Cleanup(func() { store.Close() })
	idx := &indexer.Indexer{Store: store}
	idx.ChainHeight.Store(9999)
	s := &Server{store: store, tela: idx, telaCache: newTELAContentCache(1024)}

	r := mux.NewRouter()
	r.HandleFunc("/api/initialscidcode", s.handleGetInitialSCIDCode).Methods(http.MethodGet)
	srv := httptest.NewServer(r)
	b.Cleanup(srv.Close)
	return srv, idx
}

// BenchmarkHTTP_GetInitialSCIDCode — end-to-end REST latency on a cache hit.
// Includes HTTP parse + route + handler + JSON encode + HTTP write. Proves
// the overhead above the pure bucket-get is dominated by the HTTP stack
// (which is outside our control), not by anything we introduced.
func BenchmarkHTTP_GetInitialSCIDCode(b *testing.B) {
	for _, sz := range []int{256, 2048, 16384} {
		b.Run(fmt.Sprintf("size=%d", sz), func(b *testing.B) {
			srv, idx := newBenchHTTPServer(b)
			scid := strings.Repeat("a", 64)
			if err := idx.Store.PutSCCode(scid, &structures.SCCodeEntry{
				Code: strings.Repeat("x", sz), InstallHeight: 1,
			}); err != nil {
				b.Fatal(err)
			}
			url := srv.URL + "/api/initialscidcode?scid=" + scid

			client := srv.Client()
			b.ReportAllocs()
			b.SetBytes(int64(sz))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				resp, err := client.Get(url)
				if err != nil {
					b.Fatal(err)
				}
				if resp.StatusCode != 200 {
					b.Fatalf("status %d", resp.StatusCode)
				}
				// Drain body for connection reuse.
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		})
	}
}

// BenchmarkWS_GetInitialSCIDCode_Dispatch measures the WS dispatch path
// without the websocket transport layer — we call ws.dispatch directly with
// a pre-serialized request. This is the fair baseline against the HTTP bench
// above (neither includes network RTT).
func BenchmarkWS_GetInitialSCIDCode_Dispatch(b *testing.B) {
	store, err := storage.NewBboltStore(b.TempDir(), "")
	if err != nil {
		b.Fatalf("NewBboltStore: %v", err)
	}
	b.Cleanup(func() { store.Close() })
	idx := &indexer.Indexer{Store: store}
	idx.ChainHeight.Store(9999)

	scid := strings.Repeat("a", 64)
	if err := idx.Store.PutSCCode(scid, &structures.SCCodeEntry{
		Code: strings.Repeat("x", 2048), InstallHeight: 1,
	}); err != nil {
		b.Fatal(err)
	}

	bus := eventbus.New(16)
	go bus.Run()
	b.Cleanup(bus.Close)
	var safe atomic.Int64
	ws := NewWSServer("", store, &safe, bus, idx)

	reqBytes, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "GetInitialSCIDCode",
		"params":  map[string]interface{}{"scid": scid},
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp := ws.dispatch(nil, reqBytes)
		if resp.Error != nil {
			b.Fatalf("err: %+v", resp.Error)
		}
	}
}
