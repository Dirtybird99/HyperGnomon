package indexer

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// G45 media extraction — external-oracle gate.
//
// WHY THIS EXISTS SEPARATELY FROM THE GOLDEN GATE: TestClassifyCorpusGolden
// pins whatever the classifier currently emits, and FuzzG45ScanDifferential
// only asserts that the fast scanner and the map-decode fallback AGREE. Both
// are blind to a consistent error — if the scanner and the fallback oracle
// miss the same key, the fuzz gate passes and `-update` bakes the miss in as
// truth. The golden then records the bug instead of catching it.
//
// The counts below are therefore NOT derived from this package's output. They
// come from an independent scan of the raw testdata JSON (decode `metadata`,
// count top-level keys) performed outside Go. They are the external ruler; if
// the classifier disagrees, the classifier is wrong.
//
// Derivation, over testdata/ with the ""-placeholder entry excluded:
//
//	nfts.json.gz        45,514 entries; 1 has no `metadata` var, 12 fail to
//	                    decode as a JSON object → 13 yield nothing.
//	collections.json.gz     74 entries; 3 no `metadata`, 5 undecodable.
//
// Every media value present in the corpus is a non-empty string (there are no
// empty-string media values to disambiguate), except `images`, which is always
// a JSON object.
const (
	// NFTs carry `image`; no NFT blob in the corpus has `backdropImage`.
	wantNFTImage = 45399
	// Collections carry `backdropImage`; Image holds it via the
	// image → backdropImage precedence in extractG45MetadataString.
	wantCollectionImage = 64

	wantNFTAltImage        = 239
	wantCollectionAltImage = 2 // `alt-backdropImage`
	wantNFTAudio           = 295
	wantNFTVideo           = 148
	// `images` is the sole non-string media value: {"Sculpture": "ipfs://…"}.
	// Captured verbatim by g45ScanImagesRaw rather than decoded.
	wantNFTImagesJSON = 23
)

// mediaCounts tallies non-empty media fields across a corpus half.
type mediaCounts struct {
	Image, AltImage, Audio, Video, ImagesJSON int
}

func countMedia(tb testing.TB, entries []corpusEntry) mediaCounts {
	tb.Helper()
	var c mediaCounts
	for i := range entries {
		e := &entries[i]
		sc := ClassifySCVars(e.SCID, e.Code, e.Vars)
		if sc.Image != "" {
			c.Image++
		}
		if sc.AltImage != "" {
			c.AltImage++
		}
		if sc.Audio != "" {
			c.Audio++
		}
		if sc.Video != "" {
			c.Video++
		}
		if sc.ImagesJSON != "" {
			c.ImagesJSON++
		}
	}
	return c
}

// TestG45MediaCorpusCounts is the gate that proves the media extraction
// actually fires on real mainnet data — the one assertion in this package that
// a self-consistent classifier bug cannot satisfy.
func TestG45MediaCorpusCounts(t *testing.T) {
	cols, nfts := mustCorpus(t)

	gotNFT := countMedia(t, nfts)
	wantNFT := mediaCounts{
		Image:      wantNFTImage,
		AltImage:   wantNFTAltImage,
		Audio:      wantNFTAudio,
		Video:      wantNFTVideo,
		ImagesJSON: wantNFTImagesJSON,
	}
	if gotNFT != wantNFT {
		t.Errorf("NFT media counts over %d entries:\n  got:  %+v\n  want: %+v\n"+
			"(want values come from an independent scan of testdata/nfts.json.gz, "+
			"not from this package — a mismatch means the classifier is wrong, "+
			"not that the constants are stale)", len(nfts), gotNFT, wantNFT)
	}

	gotCol := countMedia(t, cols)
	wantCol := mediaCounts{
		Image:    wantCollectionImage,
		AltImage: wantCollectionAltImage,
	}
	if gotCol != wantCol {
		t.Errorf("collection media counts over %d entries:\n  got:  %+v\n  want: %+v",
			len(cols), gotCol, wantCol)
	}
}

// TestG45ImagesRawMatchesMapDecode is the real gate on ImagesJSON.
//
// ImagesJSON is filled by g45ScanImagesRaw from BOTH extraction paths, so the
// scanner/fallback differential agrees on it trivially and proves nothing. What
// must actually hold is that the raw text the scan lifts out parses to exactly
// the value encoding/json's map decode produces for the same key — that is what
// makes a hand-rolled locator safe to substitute for a decode.
//
// Runs over every adversarial input and every real corpus blob.
func TestG45ImagesRawMatchesMapDecode(t *testing.T) {
	check := func(t *testing.T, blob string) {
		t.Helper()
		var meta map[string]interface{}
		if err := json.Unmarshal([]byte(blob), &meta); err != nil {
			// Invalid JSON: the map decode sets nothing, so the scan must not
			// claim a value either. (It is only ever called on valid input in
			// production; this asserts the degradation is safe regardless.)
			return
		}
		want, wantOK := meta["images"]
		raw, gotOK := g45ScanImagesRaw(blob)

		if !wantOK {
			if gotOK {
				t.Errorf("scan found images=%q but map decode has no such key: %q", raw, blob)
			}
			return
		}
		if !gotOK {
			// The one documented miss: an escaped spelling of the key decodes
			// to "images" for encoding/json but is declined by the scan.
			if !strings.Contains(blob, `\u`) && !strings.Contains(blob, `\U`) {
				t.Errorf("map decode has images but scan missed it: %q", blob)
			}
			return
		}
		var got interface{}
		if err := json.Unmarshal([]byte(raw), &got); err != nil {
			t.Errorf("scan returned %q for %q, which is not valid JSON: %v", raw, blob, err)
			return
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("images value mismatch for %q:\n  scan(%q) -> %#v\n  map decode  -> %#v", blob, raw, got, want)
		}
	}

	for _, blob := range g45Adversarial {
		check(t, blob)
	}
	extra := []string{
		`{"images":{"Sculpture":"ipfs://x"}}`,
		`{"images":{"b":"2","a":"1"},"name":"n"}`,
		`{"name":"n","images":{"nested":{"deep":[1,2,{"k":"v"}]}}}`,
		`{"images":{},"id":1}`,
		`{"images":[1,2,3]}`,
		`{"images":"a string"}`,
		`{"images":null}`,
		`{"images":123}`,
		`{"images":true}`,
		`{"a":{"images":{"nested":"not top level"}}}`,
		`{"images":{"a":"1"},"images":{"b":"2"}}`, // last wins
		`{"attributes":{"x":"images"},"name":"decoy"}`,
		`{ "images" : { "spaced" : "ipfs://x" } }`,
		`{"images":{"esc\"key":"v"}}`,
		`{"IMAGES":{"a":"1"}}`, // exact-match only, like the map decode
	}
	for _, blob := range extra {
		check(t, blob)
	}

	all := corpusAll(t)
	blobs, found := 0, 0
	for i := range all {
		for _, v := range all[i].Vars {
			if k, ok := v.Key.(string); ok && k == "metadata" {
				if s, ok := v.Value.(string); ok {
					blobs++
					if _, ok := g45ScanImagesRaw(s); ok {
						found++
					}
					check(t, s)
				}
			}
		}
	}
	if blobs == 0 {
		t.Fatal("no corpus blobs; gate vacuous")
	}
	if found != wantNFTImagesJSON {
		t.Errorf("scan found images on %d corpus blobs, want %d", found, wantNFTImagesJSON)
	}
}

// TestG45MediaPrecedence pins the image → backdropImage and
// alt-image → alt-backdropImage fallbacks, and that a media key never
// overwrites an already-set value. Table-driven against synthetic blobs
// because the corpus has no entry carrying both members of either pair.
func TestG45MediaPrecedence(t *testing.T) {
	tests := []struct {
		name                          string
		meta                          string
		image, altImage, audio, video string
		imagesJSON                    string
	}{
		{
			name:  "image wins over backdropImage",
			meta:  `{"image":"ipfs://a","backdropImage":"ipfs://b"}`,
			image: "ipfs://a",
		},
		{
			name:  "backdropImage fills when image absent",
			meta:  `{"backdropImage":"ipfs://b"}`,
			image: "ipfs://b",
		},
		{
			name:     "alt-image wins over alt-backdropImage",
			meta:     `{"alt-image":"https://a","alt-backdropImage":"https://b"}`,
			altImage: "https://a",
		},
		{
			name:     "alt-backdropImage fills when alt-image absent",
			meta:     `{"alt-backdropImage":"https://b"}`,
			altImage: "https://b",
		},
		{
			name:  "audio and video are independent",
			meta:  `{"audio":"ipfs://s.mp3","video":"ipfs://v.mp4"}`,
			audio: "ipfs://s.mp3",
			video: "ipfs://v.mp4",
		},
		{
			// Captured by g45ScanImagesRaw as a zero-copy substring, so key
			// order is whatever the minter wrote — NOT sorted, and never
			// varString's "map[Sculpture:ipfs://x]" fmt rendering.
			name:       "images object is captured verbatim, not re-encoded",
			meta:       `{"images":{"b":"ipfs://2","a":"ipfs://1"}}`,
			imagesJSON: `{"b":"ipfs://2","a":"ipfs://1"}`,
		},
		{
			name:  "media keys absent leaves every field empty",
			meta:  `{"name":"x","attributes":{"Body":"1"}}`,
			image: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sc SCClass
			extractG45MetadataString(&sc, tt.meta)
			if sc.Image != tt.image {
				t.Errorf("Image = %q, want %q", sc.Image, tt.image)
			}
			if sc.AltImage != tt.altImage {
				t.Errorf("AltImage = %q, want %q", sc.AltImage, tt.altImage)
			}
			if sc.Audio != tt.audio {
				t.Errorf("Audio = %q, want %q", sc.Audio, tt.audio)
			}
			if sc.Video != tt.video {
				t.Errorf("Video = %q, want %q", sc.Video, tt.video)
			}
			if sc.ImagesJSON != tt.imagesJSON {
				t.Errorf("ImagesJSON = %q, want %q", sc.ImagesJSON, tt.imagesJSON)
			}
			if sc.ImagesJSON != "" && !json.Valid([]byte(sc.ImagesJSON)) {
				t.Errorf("ImagesJSON = %q is not valid JSON", sc.ImagesJSON)
			}
		})
	}
}
