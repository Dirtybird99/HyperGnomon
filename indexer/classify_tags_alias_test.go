package indexer

import "testing"

// Anti-alias gate for the precomputed, package-level Tags slices.
//
// After the alloc-elision change, ClassifySC / ClassifySCVars /
// ClassifySCVarsWithClass no longer allocate a fresh Tags slice per call —
// every returned SCClass.Tags aliases one of a handful of shared, immutable
// backing arrays built once in init(). Two invariants keep that safe:
//
//  1. len==cap on every shared slice (slice literals guarantee it). Because
//     there is zero spare capacity, ANY caller `append(sc.Tags, …)` must
//     allocate a new backing array — it can never write into the shared one.
//     This is the ONLY mutation callers perform (verified by audit).
//  2. No consumer does in-place element assignment (`Tags[i] = …`) or sorts
//     Tags. The repo-wide audit found none: the sole `.Tags[i] =` is in
//     structures/typed_encoding.go on a freshly make()d decode buffer, and
//     every other consumer either reads Tags or copies it out into a new
//     slice. So the shared arrays are never mutated in place.
//
// These tests lock in both invariants. If a future refactor reintroduces
// spare capacity (breaking invariant 1) or an append into a shared array,
// the assertions below fail.

func assertTagsEq(t *testing.T, label string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: Tags = %v, want %v", label, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s: Tags = %v, want %v", label, got, want)
		}
	}
}

// Same-class pair shares ONE backing array — the strongest aliasing test.
// Appending to the first result must reallocate (len==cap) and leave the
// second result, a fresh classification, and tagsForClass all unchanged.
func TestClassifyTagsSharedBackingAppendSafe(t *testing.T) {
	const g45Code = "G45-NFT"
	want := []string{"all", "g45"}

	a := ClassifySC("scidA", g45Code, nil)
	b := ClassifySC("scidB", g45Code, nil)
	assertTagsEq(t, "a", a.Tags, want)
	assertTagsEq(t, "b", b.Tags, want)

	// Invariant 1: no spare capacity, so append cannot touch the shared array.
	if len(a.Tags) != cap(a.Tags) {
		t.Fatalf("len(Tags)=%d cap(Tags)=%d; spare capacity makes append() corrupt the shared backing array", len(a.Tags), cap(a.Tags))
	}

	// Mutating append on a: must not disturb b or a fresh classification.
	_ = append(a.Tags, "mutant")
	assertTagsEq(t, "b after append(a.Tags)", b.Tags, want)
	c := ClassifySC("scidC", g45Code, nil)
	assertTagsEq(t, "fresh c after append(a.Tags)", c.Tags, want)

	// tagsForClass hands back the same shared slice; repeated calls are stable
	// and an append to one caller's copy never disturbs another's.
	t1 := tagsForClass("G45-NFT")
	t2 := tagsForClass("G45-NFT")
	assertTagsEq(t, "tagsForClass #1", t1, want)
	assertTagsEq(t, "tagsForClass #2", t2, want)
	if len(t1) != cap(t1) {
		t.Fatalf("tagsForClass len=%d cap=%d; spare capacity breaks append-safety", len(t1), cap(t1))
	}
	_ = append(t1, "mutant")
	assertTagsEq(t, "tagsForClass #2 after append(#1)", t2, want)
	assertTagsEq(t, "fresh tagsForClass after append", tagsForClass("G45-NFT"), want)
}

// Different-class pair must not share backing arrays: appending to a G45
// result must never leak into a SWAP result, and vice versa.
func TestClassifyTagsCrossClassIndependent(t *testing.T) {
	g45 := ClassifySC("scidG", "G45-NFT", nil)
	swap := ClassifySC("scidS", "StartSwap", nil)
	assertTagsEq(t, "g45", g45.Tags, []string{"all", "g45"})
	assertTagsEq(t, "swap", swap.Tags, []string{"all", "swap"})

	_ = append(g45.Tags, "mutant")
	_ = append(swap.Tags, "mutant")
	assertTagsEq(t, "g45 after cross appends", g45.Tags, []string{"all", "g45"})
	assertTagsEq(t, "swap after cross appends", swap.Tags, []string{"all", "swap"})
}

// The UNKNOWN path returns the shared bare ["all"] slice; it must be
// len==cap and unaffected by a caller append.
func TestClassifyTagsUnknownShared(t *testing.T) {
	u := ClassifySC("scidU", "no markers here", nil)
	assertTagsEq(t, "unknown", u.Tags, []string{"all"})
	if len(u.Tags) != cap(u.Tags) {
		t.Fatalf("unknown Tags len=%d cap=%d; append-safety broken", len(u.Tags), cap(u.Tags))
	}
	_ = append(u.Tags, "mutant")
	assertTagsEq(t, "fresh unknown after append", ClassifySC("scidU2", "", nil).Tags, []string{"all"})
}

// The []*SCIDVariable hot path (ClassifySCVars) and the seeded-class path
// (ClassifySCVarsWithClass) share the same backing arrays as ClassifySC.
func TestClassifyVarsTagsShared(t *testing.T) {
	a := ClassifySCVars("scidA", "G45-NFT", nil)
	b := ClassifySCVars("scidB", "G45-NFT", nil)
	assertTagsEq(t, "vars a", a.Tags, []string{"all", "g45"})
	if len(a.Tags) != cap(a.Tags) {
		t.Fatalf("ClassifySCVars len=%d cap=%d; append-safety broken", len(a.Tags), cap(a.Tags))
	}
	_ = append(a.Tags, "mutant")
	assertTagsEq(t, "vars b after append(a)", b.Tags, []string{"all", "g45"})

	seeded := ClassifySCVarsWithClass("scidC", "SWAP", nil)
	assertTagsEq(t, "seeded", seeded.Tags, []string{"all", "swap"})
	if len(seeded.Tags) != cap(seeded.Tags) {
		t.Fatalf("ClassifySCVarsWithClass len=%d cap=%d; append-safety broken", len(seeded.Tags), cap(seeded.Tags))
	}
}

// The way storage/eventbus consumers treat ClassMeta.Tags is strictly
// read-only: they either read it or copy it out into a NEW slice (e.g.
// seed_cache's `append([]string(nil), meta.Tags...)`). This test documents
// that a copy-out-then-mutate leaves the shared source untouched — which is
// exactly why sharing the backing arrays across classifications is safe.
func TestClassifyTagsCopyOutIsReadOnly(t *testing.T) {
	src := ClassifySC("scid", "G45-NFT", nil)
	assertTagsEq(t, "src", src.Tags, []string{"all", "g45"})

	// Consumer copy-out pattern (mirrors classify_seed_cache.go:420).
	cp := append([]string(nil), src.Tags...)
	cp[1] = "MUTATED" // callers own their copy; the shared source must not move.

	assertTagsEq(t, "src after copy mutated", src.Tags, []string{"all", "g45"})
	assertTagsEq(t, "fresh after copy mutated", ClassifySC("scid2", "G45-NFT", nil).Tags, []string{"all", "g45"})
}
