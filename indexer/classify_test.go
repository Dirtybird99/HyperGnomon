package indexer

import "testing"

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
