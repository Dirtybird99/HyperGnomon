package indexer

import (
	"testing"

	"github.com/deroproject/derohe/rpc"

	"github.com/hypergnomon/hypergnomon/structures"
)

func TestClassifySC_TELAIndex_DURLAndVersion(t *testing.T) {
	code := "some DVM code that includes telaVersion somewhere"
	vars := map[string]interface{}{
		"telaVersion": "1.2.3",
		"dURL":        "hypergnomon.tela",
		"nameHdr":     "Hypergnomon Home",
		"descrHdr":    "root TELA app",
	}
	sc := ClassifySC("scid-abc", code, vars)
	if sc.Class != "TELA-INDEX-1" {
		t.Fatalf("class = %q, want TELA-INDEX-1", sc.Class)
	}
	if sc.DURL != "hypergnomon.tela" {
		t.Fatalf("DURL = %q, want hypergnomon.tela", sc.DURL)
	}
	if sc.Version != "1.2.3" {
		t.Fatalf("Version = %q, want 1.2.3", sc.Version)
	}
	if sc.Name != "Hypergnomon Home" {
		t.Fatalf("Name = %q, want Hypergnomon Home", sc.Name)
	}
}

func TestClassifySC_TELADoc_DocVersion(t *testing.T) {
	code := "code containing docVersion"
	vars := map[string]interface{}{
		"docVersion": "0.9.0",
	}
	sc := ClassifySC("scid-doc", code, vars)
	if sc.Class != "TELA-DOC-1" {
		t.Fatalf("class = %q, want TELA-DOC-1", sc.Class)
	}
	if sc.Version != "0.9.0" {
		t.Fatalf("Version = %q, want 0.9.0", sc.Version)
	}
}

func TestClassifySC_NonTELA_NoVersion(t *testing.T) {
	// Arbitrary SCID (non-hardcoded) + no TELA marker in code → class UNKNOWN.
	// ClassifySC must leave DURL + Version empty on non-TELA classes.
	vars := map[string]interface{}{
		"telaVersion": "should-be-ignored",
		"dURL":        "also-ignored",
	}
	sc := ClassifySC("some-scid", "no markers here", vars)
	if sc.Class != "UNKNOWN" {
		t.Fatalf("class = %q, want UNKNOWN", sc.Class)
	}
	// dURL is class-agnostic in our extractor (TELA CLI stores it on any
	// classified TELA SCID), so we only guarantee Version stays empty.
	if sc.Version != "" {
		t.Fatalf("Version = %q, want empty for non-TELA class", sc.Version)
	}
}

// TestClassifySC_TELAIndex_Canonical mirrors the real mainnet TELA-INDEX-1
// shape discovered during live-daemon verification — canonical var_header_*
// keys + literal "TELA-INDEX-1" token in source + comma-separated mods.
func TestClassifySC_TELAIndex_Canonical(t *testing.T) {
	code := "Function Initialize() ... TELA-INDEX-1 ..."
	vars := map[string]interface{}{
		"var_header_name":        "Algorithm of Faith",
		"var_header_description": "A Decentralized Guide",
		"var_header_icon":        "https://example.test/icon.png",
		"dURL":                   "algorithm-of-faith",
		"mods":                   "VSOO, TXDWA",
		"DOC1":                   "abc123",
	}
	sc := ClassifySC("scid-real", code, vars)
	if sc.Class != "TELA-INDEX-1" {
		t.Fatalf("class = %q, want TELA-INDEX-1", sc.Class)
	}
	if sc.Name != "Algorithm of Faith" {
		t.Fatalf("Name = %q, want 'Algorithm of Faith' (from var_header_name)", sc.Name)
	}
	if sc.Desc != "A Decentralized Guide" {
		t.Fatalf("Desc = %q, want canonical var_header_description value", sc.Desc)
	}
	if sc.DURL != "algorithm-of-faith" {
		t.Fatalf("DURL = %q, want algorithm-of-faith", sc.DURL)
	}
	if len(sc.Mods) != 2 || sc.Mods[0] != "VSOO" || sc.Mods[1] != "TXDWA" {
		t.Fatalf("Mods = %v, want [VSOO TXDWA]", sc.Mods)
	}
}

// TestClassifySC_TELADoc_Canonical covers TELA-DOC-1 with docType enum.
func TestClassifySC_TELADoc_Canonical(t *testing.T) {
	code := "Function Initialize() ... TELA-DOC-1 ..."
	vars := map[string]interface{}{
		"var_header_name": "index.html",
		"dURL":            "algorithm-of-faith",
		"docType":         "TELA-HTML-1",
		"subDir":          "",
	}
	sc := ClassifySC("scid-doc", code, vars)
	if sc.Class != "TELA-DOC-1" {
		t.Fatalf("class = %q, want TELA-DOC-1", sc.Class)
	}
	if sc.DocType != "TELA-HTML-1" {
		t.Fatalf("DocType = %q, want TELA-HTML-1", sc.DocType)
	}
	if sc.Name != "index.html" {
		t.Fatalf("Name = %q, want index.html", sc.Name)
	}
}

// TestClassifySC_TELAMod detects the new TELA-MOD-1 class.
func TestClassifySC_TELAMod(t *testing.T) {
	code := "Function DoSomething() ... TELA-MOD-1 ... VSOO ..."
	sc := ClassifySC("scid-mod", code, map[string]interface{}{
		"var_header_name": "MOD Helper",
	})
	if sc.Class != "TELA-MOD-1" {
		t.Fatalf("class = %q, want TELA-MOD-1", sc.Class)
	}
}

// TestClassifySC_DEROAsset_Fallback catches pre-G45 asset contracts.
func TestClassifySC_DEROAsset_Fallback(t *testing.T) {
	code := `Function InitializePrivate() Uint64
		10 SEND_ASSET_TO_ADDRESS(SIGNER(), 100, SCID())
		20 RETURN 0
	End Function`
	sc := ClassifySC("scid-old-asset", code, nil)
	if sc.Class != "DERO-ASSET" {
		t.Fatalf("class = %q, want DERO-ASSET", sc.Class)
	}
	if len(sc.Tags) < 2 || sc.Tags[1] != "asset" {
		t.Fatalf("Tags = %v, want [all asset]", sc.Tags)
	}
}

// TestClassifySC_Swap detects DEX / atomic-swap contracts via the StartSwap
// entrypoint-name marker (parity with siteraiser/simple-gnomon's `swaps`
// filter; HyperGnomon had no swap detection before).
func TestClassifySC_Swap(t *testing.T) {
	code := `Function StartSwap(amount Uint64) Uint64
		10 RETURN 0
	End Function`
	sc := ClassifySC("scid-swap", code, nil)
	if sc.Class != "SWAP" {
		t.Fatalf("class = %q, want SWAP", sc.Class)
	}
	if len(sc.Tags) != 2 || sc.Tags[0] != "all" || sc.Tags[1] != "swap" {
		t.Fatalf("Tags = %v, want [all swap]", sc.Tags)
	}

	// Class-seeded path (fastsync phase 2 / RefreshClassVars) tags SWAP via
	// tagsForClass, which reads the rules table — no separate switch case.
	seeded := ClassifySCVarsWithClass("scid-swap", "SWAP", nil)
	if len(seeded.Tags) != 2 || seeded.Tags[0] != "all" || seeded.Tags[1] != "swap" {
		t.Fatalf("ClassifySCVarsWithClass Tags = %v, want [all swap]", seeded.Tags)
	}
}

// TestClassifySC_Swap_BeatsAssetFallback proves the StartSwap rule (a step-2
// code-pattern rule) takes precedence over the step-3 DERO-ASSET fallback for
// a swap contract that also disburses assets — StartSwap contracts commonly
// contain InitializePrivate + SEND_ASSET_TO_ADDRESS, so before this rule they
// were classified DERO-ASSET. The reclassification is the intended behavior.
func TestClassifySC_Swap_BeatsAssetFallback(t *testing.T) {
	code := `Function InitializePrivate() Uint64
		10 RETURN 0
	End Function
	Function StartSwap(amount Uint64) Uint64
		10 SEND_ASSET_TO_ADDRESS(SIGNER(), 100, SCID())
		20 RETURN 0
	End Function`
	sc := ClassifySC("scid-swap-asset", code, nil)
	if sc.Class != "SWAP" {
		t.Fatalf("class = %q, want SWAP (StartSwap rule must precede the DERO-ASSET fallback)", sc.Class)
	}
	if len(sc.Tags) < 2 || sc.Tags[1] != "swap" {
		t.Fatalf("Tags = %v, want [all swap]", sc.Tags)
	}
}

// TestClassifySCVars_Swap is the differential check: the slice-based classifier
// must agree with the map-based one on the swap case (both share the rules table).
func TestClassifySCVars_Swap(t *testing.T) {
	code := `Function StartSwap(amount Uint64) Uint64
		10 RETURN 0
	End Function`
	got := ClassifySCVars("scid-swap", code, nil)
	want := ClassifySC("scid-swap", code, nil)
	if got.Class != want.Class || got.Class != "SWAP" {
		t.Fatalf("differential mismatch: ClassifySCVars=%q ClassifySC=%q, want both SWAP", got.Class, want.Class)
	}
	if len(got.Tags) != 2 || got.Tags[1] != "swap" {
		t.Fatalf("Tags = %v, want [all swap]", got.Tags)
	}
}

// TestClassifySC_Epoch detects EPOCH fair-mining contracts (civilware/epoch)
// via the crowd_mining / epochEnabled markers — the one tag HOLOGRAM's
// gnomon_tags.go has that HyperGnomon lacked.
func TestClassifySC_Epoch(t *testing.T) {
	for _, marker := range []string{"crowd_mining", "epochEnabled"} {
		code := "Function Initialize() Uint64\n10 STORE(\"" + marker + "\", 1)\n20 RETURN 0\nEnd Function"
		sc := ClassifySC("scid-epoch", code, nil)
		if sc.Class != "EPOCH" {
			t.Fatalf("marker %q: class = %q, want EPOCH", marker, sc.Class)
		}
		if len(sc.Tags) != 2 || sc.Tags[0] != "all" || sc.Tags[1] != "epoch" {
			t.Fatalf("marker %q: Tags = %v, want [all epoch]", marker, sc.Tags)
		}
	}
	// Class-seeded path tags EPOCH via tagsForClass (reads the rules table).
	seeded := ClassifySCVarsWithClass("scid-epoch", "EPOCH", nil)
	if len(seeded.Tags) != 2 || seeded.Tags[1] != "epoch" {
		t.Fatalf("ClassifySCVarsWithClass Tags = %v, want [all epoch]", seeded.Tags)
	}
}

// TestClassifySC_Epoch_BeatsAssetFallback: a standalone epoch contract that
// also disburses assets classifies EPOCH (rule), not the DERO-ASSET fallback.
func TestClassifySC_Epoch_BeatsAssetFallback(t *testing.T) {
	code := `Function InitializePrivate() Uint64
		10 STORE("crowd_mining", 1)
		20 SEND_ASSET_TO_ADDRESS(SIGNER(), 100, SCID())
		30 RETURN 0
	End Function`
	sc := ClassifySC("scid-epoch-asset", code, nil)
	if sc.Class != "EPOCH" {
		t.Fatalf("class = %q, want EPOCH (epoch rule must precede the DERO-ASSET fallback)", sc.Class)
	}
}

// TestClassifySC_Epoch_DoesNotStealTELA: epoch is a lower-priority add-on, so a
// TELA app that also enables epoch mining stays a TELA class (epoch rules sit at
// the end of the table, after the TELA family). Documents the single-class model.
func TestClassifySC_Epoch_DoesNotStealTELA(t *testing.T) {
	code := "Function Initialize() ... TELA-INDEX-1 ... epochEnabled ..."
	sc := ClassifySC("scid-tela-epoch", code, map[string]interface{}{"dURL": "x.tela"})
	if sc.Class != "TELA-INDEX-1" {
		t.Fatalf("class = %q, want TELA-INDEX-1 (TELA must win over epoch)", sc.Class)
	}
}

// TestClassifySCVars_Epoch is the differential check for the epoch case.
func TestClassifySCVars_Epoch(t *testing.T) {
	code := "Function Initialize() ... crowd_mining ..."
	got := ClassifySCVars("scid-epoch", code, nil)
	want := ClassifySC("scid-epoch", code, nil)
	if got.Class != want.Class || got.Class != "EPOCH" {
		t.Fatalf("differential mismatch: ClassifySCVars=%q ClassifySC=%q, want both EPOCH", got.Class, want.Class)
	}
	if len(got.Tags) != 2 || got.Tags[1] != "epoch" {
		t.Fatalf("Tags = %v, want [all epoch]", got.Tags)
	}
}

// TestClassifySC_DocShard covers the .shard / .shards dURL suffix detection.
func TestClassifySC_DocShard(t *testing.T) {
	sc := ClassifySC("scid-shard-doc", "code with TELA-DOC-1", map[string]interface{}{
		"dURL": "bigapp.shard",
	})
	if !sc.DocShard {
		t.Fatalf("DocShard = false for .shard dURL")
	}
	sc = ClassifySC("scid-shard-index", "code with TELA-INDEX-1", map[string]interface{}{
		"dURL": "bigapp.shards",
	})
	if !sc.DocShard {
		t.Fatalf("DocShard = false for .shards dURL")
	}
}

// TestClassifySC_G45_NoCollision verifies the reordered rules don't
// misclassify G45-FAT (the pre-fix ordering put G45-AT before G45-FAT;
// canonically both are valid but most-specific-first avoids ambiguity).
func TestClassifySC_G45_NoCollision(t *testing.T) {
	sc := ClassifySC("scid-fat", "code with G45-FAT somewhere", nil)
	if sc.Class != "G45-FAT" {
		t.Fatalf("class = %q, want G45-FAT", sc.Class)
	}
	sc = ClassifySC("scid-at", "code with G45-AT token", nil)
	if sc.Class != "G45-AT" {
		t.Fatalf("class = %q, want G45-AT", sc.Class)
	}
}

func TestTELAFieldsForClass(t *testing.T) {
	vars := map[string]interface{}{
		"dURL":        "x.tela",
		"telaVersion": "9",
		"docVersion":  "1",
	}
	durl, version := telaFieldsForClass("TELA-INDEX-1", vars)
	if durl != "x.tela" || version != "9" {
		t.Fatalf("TELA-INDEX-1: got (%q, %q), want (x.tela, 9)", durl, version)
	}
	durl, version = telaFieldsForClass("TELA-DOC-1", vars)
	if durl != "x.tela" || version != "1" {
		t.Fatalf("TELA-DOC-1: got (%q, %q), want (x.tela, 1)", durl, version)
	}
	durl, version = telaFieldsForClass("NFA", vars)
	if durl != "x.tela" || version != "" {
		t.Fatalf("NFA: got (%q, %q), want (x.tela, empty)", durl, version)
	}
}

func TestClassifySCVarsMatchesMapClassifier(t *testing.T) {
	code := "Function Initialize() ... TELA-INDEX-1 ..."
	vars := []*structures.SCIDVariable{
		{Key: "var_header_name", Value: "Algorithm of Faith"},
		{Key: "var_header_description", Value: "A Decentralized Guide"},
		{Key: "var_header_icon", Value: "https://example.test/icon.png"},
		{Key: "dURL", Value: "algorithm-of-faith.shards"},
		{Key: "mods", Value: "VSOO, TXDWA"},
		{Key: uint64(7), Value: "ignored"},
	}
	asMap := varsToMap(vars)

	got := ClassifySCVars("scid-real", code, vars)
	want := ClassifySC("scid-real", code, asMap)

	if got.Class != want.Class || got.Name != want.Name || got.Desc != want.Desc ||
		got.IconURL != want.IconURL || got.DURL != want.DURL || got.DocShard != want.DocShard ||
		got.Version != want.Version {
		t.Fatalf("ClassifySCVars mismatch:\n got=%+v\nwant=%+v", got, want)
	}
	if len(got.Mods) != len(want.Mods) {
		t.Fatalf("Mods len = %d, want %d", len(got.Mods), len(want.Mods))
	}
	for i := range got.Mods {
		if got.Mods[i] != want.Mods[i] {
			t.Fatalf("Mods[%d] = %q, want %q", i, got.Mods[i], want.Mods[i])
		}
	}

	// Tags must match between classifiers AND equal the absolute expected value
	// — pins Tags after the cap-2 presize (current tests don't assert Tags[0]).
	if len(got.Tags) != len(want.Tags) {
		t.Fatalf("Tags len: got %d (%v) want %d (%v)", len(got.Tags), got.Tags, len(want.Tags), want.Tags)
	}
	for i := range got.Tags {
		if got.Tags[i] != want.Tags[i] {
			t.Fatalf("Tags[%d]: got %q want %q", i, got.Tags[i], want.Tags[i])
		}
	}
	if len(got.Tags) != 2 || got.Tags[0] != "all" || got.Tags[1] != "tela" {
		t.Fatalf("Tags = %v, want [all tela]", got.Tags)
	}

	// Absolute anchors: differential equality alone would let a common-mode
	// bug in BOTH classifiers pass silently. Pin the expected values.
	if got.Class != "TELA-INDEX-1" {
		t.Fatalf("Class = %q, want TELA-INDEX-1", got.Class)
	}
	if !got.DocShard {
		t.Fatal("DocShard = false, want true (.shards dURL suffix)")
	}
	if got.Name != "Algorithm of Faith" {
		t.Fatalf("Name = %q, want 'Algorithm of Faith'", got.Name)
	}
}

// TestClassifySCVarsWithClass covers the single-pass class-seeded variant
// used by fastsync phase 2 and RefreshClassVars, including a uint64-valued
// docVersion (exercises varString's numeric path) and a hex-encoded dURL
// (exercises decodeHexIfPrintable).
func TestClassifySCVarsWithClass(t *testing.T) {
	// hex("algorithm-of-faith.shard") — derod returns STORE strings hex-encoded.
	hexDURL := "616c676f726974686d2d6f662d66616974682e7368617264"
	vars := []*structures.SCIDVariable{
		{Key: "var_header_name", Value: "index.html"},
		{Key: "docVersion", Value: uint64(1)},
		{Key: "dURL", Value: hexDURL},
		{Key: "docType", Value: "TELA-HTML-1"},
		{Key: "likes", Value: uint64(42)},
	}

	sc := ClassifySCVarsWithClass("scid-doc", "TELA-DOC-1", vars)
	if sc.Class != "TELA-DOC-1" {
		t.Fatalf("Class = %q, want TELA-DOC-1", sc.Class)
	}
	if len(sc.Tags) != 2 || sc.Tags[0] != "all" || sc.Tags[1] != "tela" {
		t.Fatalf("Tags = %v, want [all tela]", sc.Tags)
	}
	if sc.Version != "1" {
		t.Fatalf("Version = %q, want 1 (uint64 docVersion through varString)", sc.Version)
	}
	if sc.DURL != "algorithm-of-faith.shard" {
		t.Fatalf("DURL = %q, want hex-decoded algorithm-of-faith.shard", sc.DURL)
	}
	if !sc.DocShard {
		t.Fatal("DocShard = false, want true (.shard suffix after hex decode)")
	}
	if sc.DocType != "TELA-HTML-1" {
		t.Fatalf("DocType = %q, want TELA-HTML-1", sc.DocType)
	}
	if sc.Name != "index.html" {
		t.Fatalf("Name = %q, want index.html", sc.Name)
	}

	// INDEX class applies telaVersion + mods instead of docVersion/docType.
	indexVars := []*structures.SCIDVariable{
		{Key: "telaVersion", Value: "1.1.0"},
		{Key: "mods", Value: "VSOO, TXDWA"},
		{Key: "docVersion", Value: uint64(9)}, // must be ignored on INDEX
	}
	idx := ClassifySCVarsWithClass("scid-index", "TELA-INDEX-1", indexVars)
	if idx.Version != "1.1.0" {
		t.Fatalf("INDEX Version = %q, want 1.1.0", idx.Version)
	}
	if len(idx.Mods) != 2 || idx.Mods[0] != "VSOO" || idx.Mods[1] != "TXDWA" {
		t.Fatalf("INDEX Mods = %v, want [VSOO TXDWA]", idx.Mods)
	}
	if idx.DocType != "" {
		t.Fatalf("INDEX DocType = %q, want empty", idx.DocType)
	}

	// Empty class falls back to the code-less classifier (UNKNOWN).
	fb := ClassifySCVarsWithClass("scid-doc", "", vars)
	if fb.Class != "UNKNOWN" {
		t.Fatalf("fallback Class = %q, want UNKNOWN", fb.Class)
	}
	if fb.Version != "" {
		t.Fatalf("fallback Version = %q, want empty (class gates version keys)", fb.Version)
	}
}

func TestArgStringReadsTypedDVMArguments(t *testing.T) {
	args := rpc.Arguments{
		{Name: "entrypoint", DataType: rpc.DataString, Value: "Initialize"},
		{Name: "SC_ACTION", DataType: rpc.DataUint64, Value: uint64(1)},
	}
	if got := argString(args, "entrypoint", rpc.DataString); got != "Initialize" {
		t.Fatalf("entrypoint = %q, want Initialize", got)
	}
	if got := argString(args, "SC_ACTION", rpc.DataUint64); got != "1" {
		t.Fatalf("SC_ACTION = %q, want 1", got)
	}
	if got := argString(args, "missing", rpc.DataString); got != "" {
		t.Fatalf("missing = %q, want empty", got)
	}
}
