package storage

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// TestRatings_CanonicalKeyFormat verifies the TELA canonical format:
// STORE key is the rater's DERO address, value is hex-encoded "<score>_<height>".
// The parser must ignore non-address keys (headers, docType, etc.) and
// return the rater set sorted by address.
func TestRatings_CanonicalKeyFormat(t *testing.T) {
	store := openTestStore(t)
	scid := fakeSCID()

	// Two canonical ratings + noise keys that must be skipped.
	raterA := "dero1qyjjxxaabbccddeeffaa00001122334455667788990000112233445566778890aa"
	raterB := "deto1qyjjxxaabbccddeeffbb00001122334455667788990000112233445566778890bb"

	vars := []*structures.SCIDVariable{
		{Key: "var_header_name", Value: "Test"},
		{Key: "dURL", Value: "test.tela"},
		{Key: "docType", Value: "TELA-HTML-1"},
		{Key: raterA, Value: hex.EncodeToString([]byte(fmt.Sprintf("%d_%d", 75, 6937500)))},
		{Key: raterB, Value: hex.EncodeToString([]byte(fmt.Sprintf("%d_%d", 20, 6937520)))},
		// Non-address-shaped key must not be treated as a rating.
		{Key: "noise_key", Value: hex.EncodeToString([]byte("99_1"))},
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
	if len(got) != 2 {
		t.Fatalf("got %d ratings, want 2 — got=%+v", len(got), got)
	}
	// Sorted by rater address — raterA starts "dero1", raterB starts "deto1".
	// 'deto1' > 'dero1' lexicographically.
	if got[0].Rater != raterA || got[1].Rater != raterB {
		t.Fatalf("rater ordering: got [%s, %s], want [%s, %s]", got[0].Rater, got[1].Rater, raterA, raterB)
	}
	if got[0].Score != 75 {
		t.Fatalf("raterA score = %v, want 75", got[0].Score)
	}
	if got[0].Height != 6937500 {
		t.Fatalf("raterA height = %d, want 6937500", got[0].Height)
	}
	if got[1].Score != 20 {
		t.Fatalf("raterB score = %v, want 20", got[1].Score)
	}
}

// TestRatings_Summary reads the likes/dislikes aggregate counters.
func TestRatings_Summary(t *testing.T) {
	store := openTestStore(t)
	scid := fakeSCID()

	vars := []*structures.SCIDVariable{
		{Key: "var_header_name", Value: "Liked"},
		{Key: "likes", Value: uint64(42)},
		{Key: "dislikes", Value: uint64(7)},
	}
	batch := NewWriteBatch()
	batch.AddVariables(scid, 6900000, vars)
	batch.LastHeight = 6900000
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}
	PutWriteBatch(batch)

	got, err := store.GetRatingSummary(scid, 6900000)
	if err != nil {
		t.Fatalf("GetRatingSummary: %v", err)
	}
	if got == nil {
		t.Fatal("summary nil")
	}
	if got.Likes != 42 {
		t.Fatalf("Likes = %d, want 42", got.Likes)
	}
	if got.Dislikes != 7 {
		t.Fatalf("Dislikes = %d, want 7", got.Dislikes)
	}
	if got.Height != 6900000 {
		t.Fatalf("Height = %d, want 6900000", got.Height)
	}
}

func TestRatings_CombinedLatest(t *testing.T) {
	store := openTestStore(t)
	scid := fakeSCID()
	raterOld := "dero1qyjjxxaabbccddeeffaa00001122334455667788990000112233445566778890aa"
	raterNew := "dero1qyjjxxaabbccddeeffbb00001122334455667788990000112233445566778890bb"

	batch := NewWriteBatch()
	batch.AddVariables(scid, 100, []*structures.SCIDVariable{
		{Key: "likes", Value: uint64(1)},
		{Key: raterOld, Value: hex.EncodeToString([]byte("51_100"))},
	})
	batch.AddVariables(scid, 200, []*structures.SCIDVariable{
		{Key: "likes", Value: uint64(9)},
		{Key: "dislikes", Value: uint64(3)},
		{Key: raterNew, Value: hex.EncodeToString([]byte("75_200"))},
	})
	batch.LastHeight = 200
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}
	PutWriteBatch(batch)

	ratings, summary, err := store.GetRatingsAndSummaryForSCID(scid, 0)
	if err != nil {
		t.Fatalf("GetRatingsAndSummaryForSCID: %v", err)
	}
	if summary == nil || summary.Height != 200 || summary.Likes != 9 || summary.Dislikes != 3 {
		t.Fatalf("summary = %+v, want height=200 likes=9 dislikes=3", summary)
	}
	if len(ratings) != 1 || ratings[0].Rater != raterNew || ratings[0].Height != 200 {
		t.Fatalf("ratings = %+v, want latest rater only", ratings)
	}
}

// TestRatings_InvalidScoreDropped ignores ratings with out-of-range scores.
func TestRatings_InvalidScoreDropped(t *testing.T) {
	store := openTestStore(t)
	scid := fakeSCID()

	rater := "dero1qyjjxxaabbccddeeffaa00001122334455667788990000112233445566778890aa"
	vars := []*structures.SCIDVariable{
		{Key: rater, Value: hex.EncodeToString([]byte("200_1"))}, // score out of range
	}
	batch := NewWriteBatch()
	batch.AddVariables(scid, 100, vars)
	batch.LastHeight = 100
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}
	PutWriteBatch(batch)

	got, err := store.GetRatingsForSCID(scid, 100)
	if err != nil {
		t.Fatalf("GetRatingsForSCID: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d ratings, want 0 (out-of-range score should be dropped)", len(got))
	}
}

// TestRatings_NoUnderscoreDropped locks the no-"_" reject branch — the one
// parseTELARatingValue edge the SplitN→IndexByte change touches. A
// DERO-address-keyed value lacking the "_" separator must be dropped, not
// parsed as a rating (baseline: SplitN len!=2; after: IndexByte i<0).
func TestRatings_NoUnderscoreDropped(t *testing.T) {
	store := openTestStore(t)
	scid := fakeSCID()

	rater := "dero1qyjjxxaabbccddeeffaa00001122334455667788990000112233445566778890aa"
	vars := []*structures.SCIDVariable{
		// hex of "75" — a DERO-address rating value with NO "_" separator.
		{Key: rater, Value: hex.EncodeToString([]byte("75"))},
	}
	batch := NewWriteBatch()
	batch.AddVariables(scid, 100, vars)
	batch.LastHeight = 100
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}
	PutWriteBatch(batch)

	got, err := store.GetRatingsForSCID(scid, 100)
	if err != nil {
		t.Fatalf("GetRatingsForSCID: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("got %d ratings, want 0 (no-underscore value must be dropped)", len(got))
	}
}
