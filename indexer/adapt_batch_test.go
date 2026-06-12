package indexer

import (
	"testing"
	"time"
)

func TestAdaptiveBatchSizeCapsAtMeasuredSweetSpot(t *testing.T) {
	if got := nextAdaptiveBatchSize(800*time.Millisecond, 800); got != 1000 {
		t.Fatalf("grow from 800 = %d, want 1000", got)
	}
	if got := nextAdaptiveBatchSize(800*time.Millisecond, 1000); got != 1000 {
		t.Fatalf("grow from cap = %d, want 1000", got)
	}
}

func TestAdaptiveBatchSizeShrinksSlowFlush(t *testing.T) {
	if got := nextAdaptiveBatchSize(6*time.Second, 1000); got != 500 {
		t.Fatalf("shrink from 1000 = %d, want 500", got)
	}
	if got := nextAdaptiveBatchSize(6*time.Second, 10); got != 10 {
		t.Fatalf("shrink from floor = %d, want 10", got)
	}
}

// TestAdaptiveBatchSizeBoundaries pins the exact comparison semantics:
// growth requires elapsed STRICTLY below 1s, shrink requires STRICTLY above
// 5s, everything in the [1s, 5s] hold band keeps the current size, and the
// shrink path clamps at the floor.
func TestAdaptiveBatchSizeBoundaries(t *testing.T) {
	cases := []struct {
		name    string
		elapsed time.Duration
		size    int
		want    int
	}{
		{name: "mid hold band keeps size", elapsed: 3 * time.Second, size: 500, want: 500},
		{name: "exactly 1s is not < 1s, holds", elapsed: time.Second, size: 500, want: 500},
		{name: "exactly 5s is not > 5s, holds", elapsed: 5 * time.Second, size: 500, want: 500},
		{name: "fast flush doubles", elapsed: 800 * time.Millisecond, size: 100, want: 200},
		{name: "slow flush clamps at floor", elapsed: 6 * time.Second, size: 15, want: 10},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := nextAdaptiveBatchSize(tc.elapsed, tc.size); got != tc.want {
				t.Fatalf("nextAdaptiveBatchSize(%v, %d) = %d, want %d", tc.elapsed, tc.size, got, tc.want)
			}
		})
	}
}
