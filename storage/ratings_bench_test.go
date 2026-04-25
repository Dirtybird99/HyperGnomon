package storage

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// seedRatings builds a scvars snapshot at `height` for `scid` populated
// with `n` canonical TELA rating entries. Per civilware/tela spec: STORE
// key is a DERO address literal; value is hex-encoded "<score>_<height>".
// Also seeds a few non-rating headers + likes/dislikes aggregates.
func seedRatings(tb testing.TB, store *BboltStore, scid string, height int64, n int) {
	tb.Helper()
	vars := make([]*structures.SCIDVariable, 0, n+6)
	vars = append(vars,
		&structures.SCIDVariable{Key: "var_header_name", Value: "Test App"},
		&structures.SCIDVariable{Key: "var_header_description", Value: "bench"},
		&structures.SCIDVariable{Key: "dURL", Value: "app.tela"},
		// Aggregate counters the Rate() entrypoint maintains.
		&structures.SCIDVariable{Key: "likes", Value: uint64(n / 2)},
		&structures.SCIDVariable{Key: "dislikes", Value: uint64(n / 4)},
	)
	for i := 0; i < n; i++ {
		// Synthesize a DERO-like address key that passes looksLikeDEROAddress.
		// Real addresses are ~66 chars Base58; we produce a plausibly-shaped
		// fixture that's unique per i.
		addr := "dero1qyjjxxaabbccddeeff" + fmt.Sprintf("%044d", i)
		// Value is hex-encoded "<score>_<height>".
		raw := fmt.Sprintf("%d_%d", i%100, height)
		vars = append(vars, &structures.SCIDVariable{
			Key:   addr,
			Value: hex.EncodeToString([]byte(raw)),
		})
	}
	batch := NewWriteBatch()
	batch.AddVariables(scid, height, vars)
	batch.LastHeight = height
	if err := store.FlushBatch(batch); err != nil {
		tb.Fatalf("FlushBatch: %v", err)
	}
	PutWriteBatch(batch)

	// strconv import retained for callers that build addresses differently.
	_ = strconv.Itoa
}

// BenchmarkGetRatingsForSCID parameterized by rater count. Confirms the
// scvars prefix scan + pair-up stays linear in n as popular TELA apps
// accumulate ratings. If this goes non-linear, revisit the flat-variable
// layout (candidate: a separate ratings bucket keyed by scid|addr).
func BenchmarkGetRatingsForSCID(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(fmt.Sprintf("raters=%d", n), func(b *testing.B) {
			store := openTestStore(b)
			scid := fakeSCID()
			seedRatings(b, store, scid, 1000, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rs, err := store.GetRatingsForSCID(scid, 1000)
				if err != nil {
					b.Fatal(err)
				}
				if len(rs) != n {
					b.Fatalf("got %d ratings, want %d", len(rs), n)
				}
			}
		})
	}
}

// BenchmarkGetRatingsForSCID_LatestHeight measures the code path that picks
// the latest height on behalf of the caller (height<=0). The scvars_latest
// pointer should keep this bounded even as snapshots accumulate.
func BenchmarkGetRatingsForSCID_LatestHeight(b *testing.B) {
	store := openTestStore(b)
	scid := fakeSCID()
	// Seed multiple height snapshots so the max-height scan has work.
	for _, h := range []int64{900, 950, 1000, 1050, 1100} {
		seedRatings(b, store, scid, h, 100)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, err := store.GetRatingsForSCID(scid, 0)
		if err != nil {
			b.Fatal(err)
		}
		if len(rs) == 0 {
			b.Fatal("empty ratings")
		}
	}
}

func BenchmarkGetRatingsAndSummaryForSCID_LatestHeight(b *testing.B) {
	store := openTestStore(b)
	scid := fakeSCID()
	for _, h := range []int64{900, 950, 1000, 1050, 1100} {
		seedRatings(b, store, scid, h, 100)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rs, summary, err := store.GetRatingsAndSummaryForSCID(scid, 0)
		if err != nil {
			b.Fatal(err)
		}
		if len(rs) == 0 || summary == nil || summary.Height != 1100 {
			b.Fatalf("bad combined read: ratings=%d summary=%+v", len(rs), summary)
		}
	}
}
