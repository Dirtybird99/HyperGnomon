package main

import (
	"errors"
	"testing"
)

// TestDivergence pins the fork-walk contract (mirroring the unexported
// indexer/reorg.go findForkPoint): daemon miss = disagreement that keeps
// walking, recorded miss = unknowable, bounded by limit, first agreement below
// the diverged range = fork point.
func TestDivergence(t *testing.T) {
	rec := func(m map[int64]string) func(int64) (string, bool) {
		return func(t int64) (string, bool) { h, ok := m[t]; return h, ok }
	}
	dmn := func(m map[int64]string) func(int64) (string, error) {
		return func(t int64) (string, error) { return m[t], nil }
	}

	cases := []struct {
		name         string
		start, limit int64
		recorded     map[int64]string
		daemon       map[int64]string
		wantChanged  int
		wantFork     int64
	}{
		{
			name: "no divergence stops at first agreement",
			start: 100, limit: 16,
			recorded:    map[int64]string{100: "a"},
			daemon:      map[int64]string{100: "a"},
			wantChanged: 0, wantFork: 100,
		},
		{
			name: "two-deep rewrite finds fork below",
			start: 100, limit: 16,
			recorded:    map[int64]string{100: "a", 99: "b", 98: "c"},
			daemon:      map[int64]string{100: "A", 99: "B", 98: "c"},
			wantChanged: 2, wantFork: 98,
		},
		{
			name: "daemon miss is disagreement, walk continues, not recorded",
			start: 100, limit: 16,
			recorded:    map[int64]string{100: "a", 99: "b", 98: "c"},
			daemon:      map[int64]string{100: "A", 99: "", 98: "c"},
			wantChanged: 1, wantFork: 98, // 99's "" walked past but not recorded
		},
		{
			name: "recorded miss below window is unknowable",
			start: 100, limit: 16,
			recorded:    map[int64]string{100: "a", 99: "b"}, // nothing at 98
			daemon:      map[int64]string{100: "A", 99: "B", 98: "c"},
			wantChanged: 2, wantFork: -1,
		},
		{
			name: "limit exhausted without agreement",
			start: 100, limit: 2,
			recorded:    map[int64]string{100: "a", 99: "b", 98: "c"},
			daemon:      map[int64]string{100: "A", 99: "B", 98: "C"},
			wantChanged: 2, wantFork: -1,
		},
		{
			name: "topo floor is unknowable",
			start: 1, limit: 16,
			recorded:    map[int64]string{1: "a", 0: "x"},
			daemon:      map[int64]string{1: "A"},
			wantChanged: 1, wantFork: -1, // t=0 guard fires before agreement
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			changed, fork, err := divergence(tc.start, tc.limit, rec(tc.recorded), dmn(tc.daemon))
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(changed) != tc.wantChanged {
				t.Errorf("changed = %d, want %d (%v)", len(changed), tc.wantChanged, changed)
			}
			if fork != tc.wantFork {
				t.Errorf("fork = %d, want %d", fork, tc.wantFork)
			}
		})
	}
}

// TestDivergence_RPCErrorAborts pins that an error aborts with partial results
// so the caller can retry next poll instead of recording bad state.
func TestDivergence_RPCErrorAborts(t *testing.T) {
	boom := errors.New("conn reset")
	recorded := map[int64]string{100: "a", 99: "b"}
	changed, fork, err := divergence(100, 16,
		func(t int64) (string, bool) { h, ok := recorded[t]; return h, ok },
		func(t int64) (string, error) {
			if t == 99 {
				return "", boom
			}
			return "A", nil
		})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want %v", err, boom)
	}
	if len(changed) != 1 || fork != -1 {
		t.Errorf("changed=%v fork=%d, want 1 partial change and fork=-1", changed, fork)
	}
}
