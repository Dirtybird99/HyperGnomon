package indexer

import (
	"errors"
	"fmt"
	"testing"
)

func constHashFn(m map[int64]string) func(int64) (string, error) {
	return func(h int64) (string, error) { return m[h], nil }
}

// commonChain builds a stored/daemon hash pair that agrees on [90,100] and
// diverges (old vs new chain) on [101,105] — i.e. a reorg with fork point 100.
func commonChain() (stored, daemon map[int64]string) {
	stored, daemon = map[int64]string{}, map[int64]string{}
	for h := int64(90); h <= 100; h++ {
		v := fmt.Sprintf("c%d", h)
		stored[h], daemon[h] = v, v
	}
	for h := int64(101); h <= 105; h++ {
		stored[h] = fmt.Sprintf("old%d", h)
		daemon[h] = fmt.Sprintf("new%d", h)
	}
	return stored, daemon
}

func TestFindForkPoint(t *testing.T) {
	cases := []struct {
		name                string
		suspected, maxDepth int64
		mutate              func(stored, daemon map[int64]string)
		wantFork            int64
		wantOK              bool
	}{
		{name: "clean fork at 100", suspected: 105, maxDepth: 1000, wantFork: 100, wantOK: true},
		{name: "fork one below tip", suspected: 105, maxDepth: 1000, wantFork: 104, wantOK: true,
			mutate: func(s, d map[int64]string) {
				for h := int64(101); h <= 104; h++ { // make 101..104 agree, only 105 diverges
					v := fmt.Sprintf("c%d", h)
					s[h], d[h] = v, v
				}
			}},
		{name: "already consistent at suspected", suspected: 100, maxDepth: 1000, wantFork: 100, wantOK: true},
		{name: "fastsync floor below fork", suspected: 105, maxDepth: 1000, wantFork: 0, wantOK: false,
			mutate: func(s, d map[int64]string) {
				for h := int64(90); h <= 100; h++ { // no stored hash below the divergence
					delete(s, h)
				}
			}},
		{name: "hole mid-run", suspected: 105, maxDepth: 1000, wantFork: 0, wantOK: false,
			mutate: func(s, d map[int64]string) { delete(s, 103) }},
		{name: "maxDepth exceeded", suspected: 105, maxDepth: 3, wantFork: 0, wantOK: false},
		{name: "suspected below genesis", suspected: 0, maxDepth: 1000, wantFork: 0, wantOK: false},
		// Pins the caller contract: the incoming block's height has no stored
		// hash yet (detection runs before its flush), so starting the walk there
		// bails immediately — the caller MUST pass storedAt (the h-1 side).
		{name: "no stored hash at suspected (incoming height passed by mistake)",
			suspected: 106, maxDepth: 1000, wantFork: 0, wantOK: false},
		// Pins the daemon-miss rule: an empty daemon hash is disagreement, not a
		// hole — the walk continues down and still finds the fork point.
		{name: "daemon misses above fork", suspected: 105, maxDepth: 1000, wantFork: 100, wantOK: true,
			mutate: func(s, d map[int64]string) {
				for h := int64(101); h <= 105; h++ {
					delete(d, h)
				}
			}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stored, daemon := commonChain()
			if tc.mutate != nil {
				tc.mutate(stored, daemon)
			}
			fork, ok, err := findForkPoint(tc.suspected, constHashFn(stored), constHashFn(daemon), tc.maxDepth)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}
			if fork != tc.wantFork || ok != tc.wantOK {
				t.Fatalf("findForkPoint(%d) = (%d, %v), want (%d, %v)", tc.suspected, fork, ok, tc.wantFork, tc.wantOK)
			}
		})
	}
}

func TestFindForkPoint_LookupError(t *testing.T) {
	boom := func(int64) (string, error) { return "", errors.New("rpc down") }
	present := constHashFn(map[int64]string{100: "x"})

	if _, _, err := findForkPoint(100, present, boom, 10); err == nil {
		t.Error("want error propagated from daemon lookup")
	}
	if _, _, err := findForkPoint(100, boom, present, 10); err == nil {
		t.Error("want error propagated from stored lookup")
	}
}
