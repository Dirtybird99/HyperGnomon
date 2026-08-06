package api

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// Asset-API surface for G45 media. These are the two user-visible pieces of
// the change — the new assetEntry JSON fields, and G45-C joining the catalog —
// and both are invisible to the indexer/ and structures/ gates that cover the
// extraction and the wire format.

func TestAssetEntryCarriesMediaFields(t *testing.T) {
	meta := &structures.ClassMeta{
		Class: "G45-NFT", Tags: []string{"all", "g45"},
		Name: "Dero Duck #1801",
		// IconURL stays empty on purpose: G45 metadata never sets `icon`, so
		// a client keying off icon_url alone renders nothing. That is the
		// whole reason the media fields exist.
		Image:         "ipfs://bafy/low/1801.png",
		AltImage:      "https://dl/78.webp?dl=1",
		Audio:         "ipfs://Qm/282.mp3",
		Video:         "ipfs://Qm/T5.mp4",
		ImagesJSON:    `{"Sculpture":"ipfs://Qm/hobo.jpg"}`,
		InstallHeight: 100, LastHeight: 200,
	}
	entry := assetEntryFromMeta("scid1", "owner1", meta)

	if entry.Image != meta.Image || entry.AltImage != meta.AltImage ||
		entry.Audio != meta.Audio || entry.Video != meta.Video ||
		entry.ImagesJSON != meta.ImagesJSON {
		t.Fatalf("media fields not projected onto assetEntry: %+v", entry)
	}

	raw, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for key, want := range map[string]string{
		"image":     meta.Image,
		"alt_image": meta.AltImage,
		"audio":     meta.Audio,
		"video":     meta.Video,
		"images":    meta.ImagesJSON,
	} {
		if got[key] != want {
			t.Errorf("JSON field %q = %v, want %q", key, got[key], want)
		}
	}

	// ImagesJSON is a raw JSON string carried as a JSON string value, so it
	// must survive the round trip intact rather than being spliced in as an
	// object (which would break any client decoding into a string field).
	if !json.Valid([]byte(entry.ImagesJSON)) {
		t.Errorf("ImagesJSON is not valid JSON: %q", entry.ImagesJSON)
	}
}

// TestAssetEntryOmitsEmptyMedia pins the omitempty tags: an asset with no
// media must serialize to the pre-change response shape, so existing
// consumers see no new keys.
func TestAssetEntryOmitsEmptyMedia(t *testing.T) {
	entry := assetEntryFromMeta("scid1", "owner1", &structures.ClassMeta{
		Class: "TELA-INDEX-1", Tags: []string{"all", "tela"}, Name: "app",
	})
	raw, err := json.Marshal(entry)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for _, key := range []string{`"image"`, `"alt_image"`, `"audio"`, `"video"`, `"images"`} {
		if strings.Contains(string(raw), key) {
			t.Errorf("empty media key %s should be omitted, got %s", key, raw)
		}
	}
}

// TestG45CollectionsAreAssets covers the catalog change. G45-C records carry
// tags ["all","g45"], not "asset", so isAssetMeta's tag branch never reaches
// them — isAssetClass is the only thing that can let a collection (and its
// backdropImage) through.
func TestG45CollectionsAreAssets(t *testing.T) {
	if !isAssetClass("G45-C") {
		t.Error("G45-C must be an asset class or collections are unreachable")
	}
	meta := &structures.ClassMeta{Class: "G45-C", Tags: []string{"all", "g45"}}
	if !isAssetMeta(meta) {
		t.Error("a G45-C ClassMeta must satisfy isAssetMeta")
	}

	classes, ok := assetClassesForParam("g45-c")
	if !ok || len(classes) != 1 || classes[0] != "G45-C" {
		t.Errorf(`assetClassesForParam("g45-c") = %v, %v; want ["G45-C"], true`, classes, ok)
	}

	all, ok := assetClassesForParam("")
	if !ok {
		t.Fatal("empty class param must select the full catalog")
	}
	var found bool
	for _, c := range all {
		if c == "G45-C" {
			found = true
		}
	}
	if !found {
		t.Errorf("default catalog %v must include G45-C", all)
	}
}
