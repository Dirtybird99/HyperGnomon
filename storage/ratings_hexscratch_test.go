package storage

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// TestRatings_HexScratchNoAlias is the anti-alias/fallback gate for the reused
// hex-decode scratch (unsafe.String view). It seeds raters with DISTINCT
// (score, height) of VARYING digit length — so a shorter value decoded after a
// longer one would read stale tail bytes if the scratch view were mis-sized —
// PLUS one raw-ASCII (non-hex) value that must take the fallback path, and one
// oversize-hex value that forces the scratch to grow. Every returned Rating's
// Score AND Height must match exactly, proving the reused-scratch view is
// consumed (by ParseFloat/ParseInt) before the next rater overwrites it.
func TestRatings_HexScratchNoAlias(t *testing.T) {
	store := openTestStore(t)
	scid := fakeSCID()

	type rr struct {
		rater     string
		score     int
		height    int64
		rawNonHex bool // store the "<score>_<height>" string raw (non-hex) → fallback
	}
	raters := []rr{
		{"dero1q" + strings.Repeat("a", 60), 5, 900, false},              // short
		{"dero1q" + strings.Repeat("b", 60), 99, 6937500, false},         // long
		{"dero1q" + strings.Repeat("c", 60), 7, 12, false},               // shorter again (stale-tail trap)
		{"dero1q" + strings.Repeat("d", 60), 42, 6937520, true},          // raw ASCII → fallback branch
		{"dero1q" + strings.Repeat("e", 60), 1, 9007199254740991, false}, // long height → wider scratch
	}

	vars := make([]*structures.SCIDVariable, 0, len(raters))
	want := make(map[string]rr, len(raters))
	for _, r := range raters {
		raw := fmt.Sprintf("%d_%d", r.score, r.height)
		val := raw
		if !r.rawNonHex {
			val = hex.EncodeToString([]byte(raw))
		}
		vars = append(vars, &structures.SCIDVariable{Key: r.rater, Value: val})
		want[r.rater] = r
	}

	batch := NewWriteBatch()
	batch.AddVariables(scid, 7000000, vars)
	batch.LastHeight = 7000000
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}
	PutWriteBatch(batch)

	got, err := store.GetRatingsForSCID(scid, 7000000)
	if err != nil {
		t.Fatalf("GetRatingsForSCID: %v", err)
	}
	if len(got) != len(raters) {
		t.Fatalf("got %d ratings, want %d: %+v", len(got), len(raters), got)
	}
	for _, g := range got {
		w, ok := want[g.Rater]
		if !ok {
			t.Fatalf("unexpected rater %q", g.Rater)
		}
		if g.Score != float64(w.score) {
			t.Fatalf("rater %q Score = %v, want %d (scratch leak / mis-size?)", g.Rater, g.Score, w.score)
		}
		if g.Height != w.height {
			t.Fatalf("rater %q Height = %d, want %d (scratch leak / mis-size?)", g.Rater, g.Height, w.height)
		}
	}
}
