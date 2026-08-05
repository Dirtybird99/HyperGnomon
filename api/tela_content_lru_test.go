package api

import (
	"fmt"
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// These are white-box tests for the intrusive-LRU recycle machinery in
// telaContentCache (free-node list, byScid key-slice recycling, node scrub).
// They run single-threaded, so reading the unexported fields without holding
// c.mu is safe — every cache method returns before the next call begins.

// TestTELAContentCache_ReuseAfterEvict pins that an evicted node lands on the
// free list and the very next Put reuses that exact node instead of allocating.
func TestTELAContentCache_ReuseAfterEvict(t *testing.T) {
	// Cap holds exactly one 50-byte entry, so each new Put evicts the prior.
	c := newTELAContentCache(50)
	c.Put("a|p", &structures.TELAContentEntry{Body: make([]byte, 50)})
	c.Put("b|p", &structures.TELAContentEntry{Body: make([]byte, 50)}) // evicts a|p

	if c.freeCount != 1 {
		t.Fatalf("freeCount = %d after one eviction, want 1", c.freeCount)
	}
	freed := c.free // the recycled node

	c.Put("c|p", &structures.TELAContentEntry{Body: make([]byte, 50)}) // reuses freed, evicts b|p
	if c.items["c|p"] != freed {
		t.Fatalf("Put allocated a fresh node instead of reusing the recycled one")
	}
	if c.freeCount != 1 {
		t.Fatalf("freeCount = %d, want 1 (evicting b|p refills the free list)", c.freeCount)
	}
	if g := c.Get("c|p"); g == nil || len(g.Body) != 50 {
		t.Fatalf("c|p not retrievable after node reuse: %+v", g)
	}
}

// TestTELAContentCache_InvalidateThenRePut covers the invalidate → re-Put cycle:
// invalidation must empty the cache and recycle the byScid key-slice, and a
// subsequent Put of the same scid must re-index cleanly and stay invalidatable.
func TestTELAContentCache_InvalidateThenRePut(t *testing.T) {
	c := newTELAContentCache(4096)
	c.Put("scid1|index.html", &structures.TELAContentEntry{Body: []byte("v1")})
	c.Put("scid1|app.js", &structures.TELAContentEntry{Body: []byte("js1")})

	c.InvalidatePrefix("scid1") // frees both nodes + recycles the byScid slice
	if len(c.items) != 0 || len(c.byScid) != 0 {
		t.Fatalf("cache not empty after invalidate: items=%d byScid=%d", len(c.items), len(c.byScid))
	}
	if len(c.freeKeys) == 0 {
		t.Fatalf("byScid key-slice was not recycled on invalidate")
	}

	// Re-Put the same scid: fresh value, re-indexed byScid, reused key-slice.
	c.Put("scid1|index.html", &structures.TELAContentEntry{Body: []byte("v2")})
	if g := c.Get("scid1|index.html"); g == nil || string(g.Body) != "v2" {
		t.Fatalf("re-Put value = %v, want v2", g)
	}
	if keys := c.byScid["scid1"]; len(keys) != 1 || keys[0] != "scid1|index.html" {
		t.Fatalf("byScid not re-indexed after re-Put: %v", keys)
	}

	// The re-Put entry must still be reachable by the invalidator.
	c.InvalidatePrefix("scid1")
	if c.Get("scid1|index.html") != nil {
		t.Fatalf("re-Put entry not invalidated on the second pass")
	}
}

// TestTELAContentCache_FreeListCapWrap forces more freed nodes than the recycle
// list can hold and verifies freeNodeLocked caps the list at maxFreeNodes rather
// than growing unbounded — and that the cache still serves after the wrap.
func TestTELAContentCache_FreeListCapWrap(t *testing.T) {
	c := newTELAContentCache(1 << 30) // effectively unbounded: no eviction
	const n = maxFreeNodes + 50
	for i := 0; i < n; i++ {
		scid := fmt.Sprintf("%064x", i)
		c.Put(scid+"|p", &structures.TELAContentEntry{Body: []byte("x")})
	}
	// Invalidate each distinct scid: every call frees one node without a Put to
	// consume it, so the free list fills past its cap.
	for i := 0; i < n; i++ {
		c.InvalidatePrefix(fmt.Sprintf("%064x", i))
	}

	if len(c.items) != 0 {
		t.Fatalf("items not empty after mass invalidate: %d", len(c.items))
	}
	if c.freeCount != maxFreeNodes {
		t.Fatalf("freeCount = %d, want exactly the cap %d", c.freeCount, maxFreeNodes)
	}

	c.Put("fresh|p", &structures.TELAContentEntry{Body: []byte("ok")})
	if g := c.Get("fresh|p"); g == nil || string(g.Body) != "ok" {
		t.Fatalf("cache broken after cap-wrap: %+v", g)
	}
}

// TestTELAContentCache_ScrubNoBleed verifies a released node is scrubbed so no
// old key/scid/body survives, and reusing it exposes only the new payload.
func TestTELAContentCache_ScrubNoBleed(t *testing.T) {
	c := newTELAContentCache(50) // one entry
	c.Put("aaa|x", &structures.TELAContentEntry{Body: make([]byte, 50)})
	c.Put("bbb|y", &structures.TELAContentEntry{Body: make([]byte, 50)}) // evicts + scrubs the aaa node

	if c.free == nil {
		t.Fatal("expected a recycled node on the free list")
	}
	if c.free.key != "" || c.free.scid != "" || c.free.entry != nil {
		t.Fatalf("freed node not scrubbed: key=%q scid=%q entry=%v", c.free.key, c.free.scid, c.free.entry)
	}
	if c.Get("aaa|x") != nil {
		t.Fatal("evicted key aaa|x still retrievable")
	}
	if _, ok := c.byScid["aaa"]; ok {
		t.Fatal("byScid still indexes the evicted scid aaa")
	}

	// Reusing the scrubbed node for a new key must carry only the new identity.
	c.Put("ccc|z", &structures.TELAContentEntry{Body: make([]byte, 50)})
	n := c.items["ccc|z"]
	if n == nil || n.key != "ccc|z" || n.scid != "ccc" {
		t.Fatalf("reused node has wrong identity: %+v", n)
	}
}
