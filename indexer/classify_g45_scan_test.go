package indexer

import (
	"testing"
)

// These gates prove the H9 scanner tier is behavior-neutral: for EVERY input,
// extractG45MetadataString (scanner -> struct -> map) must produce the same
// (Name, Desc, IconURL) as extractG45MetadataFallback (struct -> map), the
// unchanged oracle. The only bug the scanner can introduce is a false ok=true,
// so the fuzz target is the load-bearing check; the table pins the deviations
// we specifically reasoned about, and the "fires" tests stop the differential
// from being vacuously satisfied by a scanner that never accepts anything.

// g45DiffCheck runs both paths from an identical starting SCClass and fails if
// the resulting triple diverges. Runs against both a fresh class and a
// header-prefilled class (the empty-guard interaction).
func g45DiffCheck(t *testing.T, meta string) {
	t.Helper()
	pres := []SCClass{
		{},
		{Name: "HDR", Desc: "HDRD", IconURL: "HDRI"},
		{Name: "HDR"},
		{Desc: "HDRD"},
		{IconURL: "HDRI"},
	}
	for _, pre := range pres {
		got := pre
		extractG45MetadataString(&got, meta)
		want := pre
		extractG45MetadataFallback(&want, meta)
		if got.Name != want.Name || got.Desc != want.Desc || got.IconURL != want.IconURL {
			t.Fatalf("scanner/fallback divergence for meta=%q pre=%+v:\n  scanner:  Name=%q Desc=%q Icon=%q\n  fallback: Name=%q Desc=%q Icon=%q",
				meta, pre, got.Name, got.Desc, got.IconURL, want.Name, want.Desc, want.IconURL)
		}
	}
}

// g45Adversarial is the shared corpus of tricky inputs (also used as fuzz seeds).
var g45Adversarial = []string{
	// plain / real shapes (scanner SHOULD fire)
	`{"name":"Duck #1","description":"a duck","icon":"ipfs://x"}`,
	`{"attributes":{"Background":"White","Body":"Body 1"},"id":1801,"image":"ipfs://abc/1801.png","name":"Dero Duck #1801"}`,
	`{"name":"only name"}`,
	`{}`,
	`   {  "name"  :  "x"  ,  "description" : "y"  }   `,
	"{\n\t\"name\": \"tabbed\"\r\n}",
	// escapes in VALUES (scanner must bail -> fallback decodes)
	`{"name":"café \"q\""}`,
	`{"name":"line\nbreak"}`,
	`{"name":"tab\tsep"}`,
	`{"name":"slash\/y"}`,
	`{"name":"unicodeé"}`,
	`{"name":"😀 emoji"}`,
	`{"name":"back\\slash"}`,
	// escapes in KEYS (scanner must bail on any escaped top-level key; the
	// fallback decodes n etc. to the real key and still matches the field)
	"{\"\\u006eame\":\"x\"}",                  // key "name" spelled via n
	"{\"na\\u006de\":\"escaped-key\"}",        // key "name" with mid escape
	"{\"desc\\u0072iption\":\"d\"}",           // key "description" escaped
	"{\"esc\\tkey\":\"v\",\"name\":\"real\"}", // non-target escaped key
	// duplicate keys — before/after, string/non-string (last wins in json)
	`{"name":"first","name":"last"}`,
	`{"name":123,"name":"last"}`,
	`{"name":"first","name":456}`,
	`{"name":"a","description":"b","name":"c"}`,
	// nested "name" must NOT be picked up
	`{"a":{"name":"nested"},"id":1}`,
	`{"attributes":[{"name":"attr"}],"name":"top"}`,
	`{"outer":{"inner":{"name":"deep"}}}`,
	// non-string target values (numbers, bool, null, array, object)
	`{"name":123}`,
	`{"name":1.5}`,
	`{"name":12345678901234}`,
	`{"name":1e3}`,
	`{"name":-0.5e+2}`,
	`{"name":true}`,
	`{"name":false}`,
	`{"name":null}`,
	`{"name":null,"description":null,"icon":null}`,
	`{"name":[1,2,3]}`,
	`{"name":{"a":1}}`,
	`{"name":"x","description":[1,2]}`,
	// strings containing braces/brackets/quotes/colons/commas
	`{"name":"a{b}c[d]e"}`,
	`{"name":"has:colon,and comma"}`,
	`{"description":"nested {\"json\":true} text"}`,
	// numbers in odd-but-valid and invalid forms (validity affects whole object)
	`{"id":1E10,"name":"sci"}`,
	`{"id":-0,"name":"negzero"}`,
	`{"id":01,"name":"leadingzero-invalid"}`,
	`{"id":1.,"name":"trailingdot-invalid"}`,
	`{"id":.5,"name":"leadingdot-invalid"}`,
	`{"id":1e,"name":"badexp-invalid"}`,
	`{"id":+1,"name":"plus-invalid"}`,
	// invalid JSON (truncated, trailing garbage, bare values, empty)
	`{bad json`,
	`{"name":"x"`,
	`{"name":"x"}g`,
	`{"name":"x"} {"name":"y"}`,
	`{"name":"x",}`,
	`{,"name":"x"}`,
	`{"name":}`,
	`{"name""x"}`,
	`{"name":"x":}`,
	`123`,
	`"bare string"`,
	`[1,2,3]`,
	`null`,
	`true`,
	``,
	`   `,
	// empty-string values (empty-guard no-op interplay)
	`{"name":"","description":"d"}`,
	`{"name":"","description":"","icon":""}`,
	// control char and raw bytes in a target string (invalid / replaced)
	"{\"name\":\"a\x01b\"}",
	"{\"name\":\"a\tb\"}",
	"{\"name\":\"raw\xffbyte\"}",
	"{\"name\":\"caf\xc3\xa9\"}",
	"{\"description\":\"emoji\xf0\x9f\x98\x80end\"}",
	// keys with trailing/leading spaces (json does not trim inside quotes)
	`{" name ":"x","name":"real"}`,
	`{"Name":"capital-not-target","name":"real"}`,
	// case-insensitive field matching (encoding/json folds struct field names):
	// scanner must bail so the fallback applies the fold-matched value.
	`{"nAme":"000"}`,
	`{"NAME":"upper"}`,
	`{"Name":"cap"}`,
	`{"DESCRIPTION":"d"}`,
	// review-regression seeds: fold-key override attempt (original semantics:
	// exact "name" wins, "Name" ignored), whole-blob strictness (an
	// out-of-float64-range number anywhere sets NOTHING), and backslash-u
	// escape decoding incl. a surrogate pair and a lone surrogate (the
	// fallback is stdlib encoding/json, the authoritative oracle).
	`{"name":"Duck","Name":"Evil"}`,
	`{"supply":1e999,"name":"x"}`,
	`{"name":"x","weight":-1e999}`,
	`{"name":"\ud83d\ude00"}`,
	`{"description":"\ud800 lone"}`,
	`{"Icon":"i"}`,
	`{"naMe":"x","name":"exact"}`,
	`{"name":"exact","NAME":"folded-after"}`,
	"{\"de\xc5\xbfcription\":\"long-s-fold\"}", // "deſcription" folds to description
	"{\"na\xc4\xb1me\":\"dotless-i\"}",         // "naıme" (dotless i) — not a fold of name
	// large-ish attribute block
	`{"attributes":{"a":"1","b":"2","c":"3","d":"4","e":"5","f":"6","g":"7","h":"8"},"id":9,"image":"ipfs://z","name":"Big #9"}`,
}

func TestG45ScanDifferentialTable(t *testing.T) {
	for _, meta := range g45Adversarial {
		g45DiffCheck(t, meta)
	}
	// Nesting depth cases (built to avoid huge literals).
	deep := func(levels int) string {
		s := ""
		for k := 0; k < levels; k++ {
			s += `{"a":`
		}
		s += `{"name":"deep"}`
		for k := 0; k < levels; k++ {
			s += `}`
		}
		return s
	}
	g45DiffCheck(t, deep(5))
	g45DiffCheck(t, deep(100))
	g45DiffCheck(t, deep(1200)) // beyond g45MaxDepth: scanner bails, fallback still validates
	// A huge blob with a real top-level name.
	big := `{"name":"Huge","description":"D","attributes":{`
	for k := 0; k < 500; k++ {
		big += `"k` + string(rune('0'+k%10)) + `":"v",`
	}
	big += `"last":"v"},"id":7}`
	g45DiffCheck(t, big)
}

// TestG45ScanDifferentialCorpus runs the differential over every real metadata
// blob in the committed corpus (the shape production actually sees).
func TestG45ScanDifferentialCorpus(t *testing.T) {
	all := corpusAll(t)
	blobs := 0
	for i := range all {
		for _, v := range all[i].Vars {
			if k, ok := v.Key.(string); ok && k == "metadata" {
				if s, ok := v.Value.(string); ok {
					blobs++
					g45DiffCheck(t, s)
				}
			}
		}
	}
	if blobs == 0 {
		t.Fatal("no metadata blobs found in corpus; differential vacuous")
	}
	t.Logf("differential over %d corpus metadata blobs", blobs)
}

// TestG45ScanFires guards against a regression that silently disables the fast
// path (which would leave the differential vacuously green). The scanner MUST
// accept the common simple-string shapes, and MUST decline the ones we route to
// the fallback on purpose.
func TestG45ScanFires(t *testing.T) {
	fires := []struct {
		meta             string
		name, desc, icon string
	}{
		{`{"name":"n"}`, "n", "", ""},
		{`{"name":"n","description":"d","icon":"i"}`, "n", "d", "i"},
		{`{"attributes":{"x":"y"},"id":1,"image":"ipfs://z","name":"Real #1"}`, "Real #1", "", ""},
		{`{}`, "", "", ""},
		{`{"name":"a{b}","description":"c,d"}`, "a{b}", "c,d", ""},
		{`{"name":"first","name":"last"}`, "last", "", ""}, // dup string: still fires, last wins
	}
	for _, f := range fires {
		n, d, i, ok := g45ScanMeta([]byte(f.meta))
		if !ok {
			t.Errorf("scanner should FIRE for %q but returned ok=false", f.meta)
			continue
		}
		if n != f.name || d != f.desc || i != f.icon {
			t.Errorf("scanner %q = (%q,%q,%q), want (%q,%q,%q)", f.meta, n, d, i, f.name, f.desc, f.icon)
		}
	}

	declines := []string{
		`{"name":123}`,            // non-string
		`{"name":"esc\n"}`,        // escaped value
		"{\"\\u006eame\":\"x\"}",  // escaped key
		`{"name":"x"}g`,           // trailing garbage
		`{"id":01,"name":"x"}`,    // invalid nested number
		`{"a":{"name":"nested"}`,  // truncated
		`{"nAme":"x"}`,            // case-fold variant of a target key
		`{"DESCRIPTION":"d"}`,     // upper-case fold variant
		`123`,                     // non-object
		"{\"name\":\"raw\xffb\"}", // invalid utf8 target
	}
	for _, m := range declines {
		if _, _, _, ok := g45ScanMeta([]byte(m)); ok {
			t.Errorf("scanner should DECLINE %q but returned ok=true", m)
		}
	}
}

// TestG45ScanCorpusFireRate confirms the scanner takes the fast path on a
// substantial majority of real blobs — otherwise H9 would not move the metric
// even while the differential passes.
func TestG45ScanCorpusFireRate(t *testing.T) {
	all := corpusAll(t)
	var total, fired int
	for i := range all {
		for _, v := range all[i].Vars {
			if k, ok := v.Key.(string); ok && k == "metadata" {
				if s, ok := v.Value.(string); ok {
					total++
					if _, _, _, ok := g45ScanMeta(readOnlyBytes(s)); ok {
						fired++
					}
				}
			}
		}
	}
	if total == 0 {
		t.Skip("no metadata blobs in corpus")
	}
	t.Logf("scanner fired on %d/%d (%.1f%%) corpus metadata blobs", fired, total, 100*float64(fired)/float64(total))
	if fired*2 < total { // require >50%
		t.Fatalf("scanner fired on only %d/%d corpus blobs; fast path not engaging", fired, total)
	}
}

// FuzzG45ScanDifferential is the load-bearing check for a false ok=true: it
// feeds arbitrary bytes to both paths and asserts the triple is identical from
// both a fresh and a prefilled SCClass. Seeded with the adversarial table.
func FuzzG45ScanDifferential(f *testing.F) {
	for _, s := range g45Adversarial {
		f.Add(s)
	}
	f.Add(`{"name":"x","description":"y","icon":"z"}`)
	f.Fuzz(func(t *testing.T, meta string) {
		for _, pre := range []SCClass{{}, {Name: "HDR"}, {Name: "HDR", Desc: "D", IconURL: "I"}} {
			got := pre
			extractG45MetadataString(&got, meta)
			want := pre
			extractG45MetadataFallback(&want, meta)
			if got.Name != want.Name || got.Desc != want.Desc || got.IconURL != want.IconURL {
				t.Fatalf("divergence meta=%q pre=%+v: scanner=(%q,%q,%q) fallback=(%q,%q,%q)",
					meta, pre, got.Name, got.Desc, got.IconURL, want.Name, want.Desc, want.IconURL)
			}
		}
	})
}
