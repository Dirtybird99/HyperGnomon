package structures

import (
	"bytes"
	"testing"
)

// ClassMeta media-tail wire compatibility.
//
// The media fields were appended to the v1 typed layout rather than given a new
// tag byte. That is only safe if BOTH directions hold across a mixed-version
// deployment (an operator upgrades the binary against an existing DB, or rolls
// back), so each direction gets an explicit gate here.

func mediaMeta() *ClassMeta {
	return &ClassMeta{
		Class: "G45-NFT", Tags: []string{"all", "g45"},
		Name: "Dero Duck #1801", Desc: "d", IconURL: "", DURL: "", Version: "",
		Image:         "ipfs://bafy/low/1801.png",
		AltImage:      "https://dl/78.webp?dl=1",
		Audio:         "ipfs://Qm/282.mp3",
		Video:         "ipfs://Qm/T5.mp4",
		ImagesJSON:    `{"Sculpture":"ipfs://Qm/hobo.jpg"}`,
		InstallHeight: 100, LastHeight: 200,
	}
}

func TestClassMetaMediaRoundTrip(t *testing.T) {
	want := mediaMeta()
	var got ClassMeta
	if err := got.UnmarshalTyped(want.MarshalTyped()); err != nil {
		t.Fatalf("UnmarshalTyped: %v", err)
	}
	if got.Image != want.Image || got.AltImage != want.AltImage ||
		got.Audio != want.Audio || got.Video != want.Video || got.ImagesJSON != want.ImagesJSON {
		t.Errorf("media round-trip mismatch:\n  got:  %+v\n  want: %+v", got, *want)
	}
	if got.Class != want.Class || got.Name != want.Name || got.InstallHeight != want.InstallHeight {
		t.Errorf("non-media fields disturbed by the tail: got %+v", got)
	}
}

// TestClassMetaMediaTailIsPurelyAppended is the forward-compat proof: a media
// record must be byte-identical to the same record without media, plus a
// suffix. A pre-media reader stops after Version and discards trailing bytes,
// so identical-prefix is exactly the condition under which it still decodes a
// media record correctly.
func TestClassMetaMediaTailIsPurelyAppended(t *testing.T) {
	withMedia := mediaMeta()
	bare := *withMedia
	bare.Image, bare.AltImage, bare.Audio, bare.Video, bare.ImagesJSON = "", "", "", "", ""

	full, prefix := withMedia.MarshalTyped(), bare.MarshalTyped()
	if len(full) <= len(prefix) {
		t.Fatalf("media record (%d bytes) should be longer than bare (%d)", len(full), len(prefix))
	}
	if !bytes.Equal(full[:len(prefix)], prefix) {
		t.Errorf("media tail is not a pure append:\n  bare:   %x\n  prefix: %x", prefix, full[:len(prefix)])
	}
}

// TestClassMetaNoMediaIsByteIdentical guards the hasMedia() gate. Records
// without media — every TELA, NFA, and nameservice record — must encode to
// exactly the bytes the pre-media encoder produced, so existing size
// benchmarks and stored records are untouched by this change.
func TestClassMetaNoMediaIsByteIdentical(t *testing.T) {
	m := &ClassMeta{
		Class: "TELA-INDEX-1", Tags: []string{"all", "tela"},
		Name: "app", Desc: "d", IconURL: "ipfs://i", DURL: "app.tela", Version: "1.1.0",
		InstallHeight: 7, LastHeight: 9,
	}
	got := m.MarshalTyped()

	// Hand-built expectation: header + tags + the six fixed strings, no tail.
	want := []byte{TagClassMetaV1}
	want = append(want, 0, 0, 0, 0, 0, 0, 0, 7)
	want = append(want, 0, 0, 0, 0, 0, 0, 0, 9)
	want = append(want, 2)
	for _, s := range []string{"all", "tela", "TELA-INDEX-1", "app", "d", "ipfs://i", "app.tela", "1.1.0"} {
		want = append(want, byte(len(s)))
		want = append(want, s...)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("no-media record grew or changed shape:\n  got:  %x\n  want: %x", got, want)
	}
}

// TestClassMetaPreMediaRecordDecodes is the backward-compat proof: a record
// written before the media tail existed must decode without error and leave
// every media field empty — not fail with ErrInvalidClassMeta.
func TestClassMetaPreMediaRecordDecodes(t *testing.T) {
	bare := mediaMeta()
	bare.Image, bare.AltImage, bare.Audio, bare.Video, bare.ImagesJSON = "", "", "", "", ""
	old := bare.MarshalTyped() // hasMedia() false → no tail, i.e. the old layout

	// Decode into a struct that ALREADY carries media: the absent-tail path
	// must clear it, or a pooled/reused ClassMeta leaks the previous record's
	// URLs onto an asset that has none.
	got := *mediaMeta()
	if err := got.UnmarshalTyped(old); err != nil {
		t.Fatalf("pre-media record must decode cleanly, got %v", err)
	}
	if got.Image != "" || got.AltImage != "" || got.Audio != "" || got.Video != "" || got.ImagesJSON != "" {
		t.Errorf("stale media survived decode of a pre-media record: %+v", got)
	}
	if got.Class != bare.Class || got.Name != bare.Name || got.LastHeight != bare.LastHeight {
		t.Errorf("fixed fields wrong after pre-media decode: %+v", got)
	}
}

// TestClassMetaMalformedTailIsIgnored pins that a partial or garbage tail is
// treated as absent rather than as a decode error. The v1 format documented
// trailing bytes as forward-compat slack; turning that into a hard failure
// would make a future field addition unreadable by this binary.
func TestClassMetaMalformedTailIsIgnored(t *testing.T) {
	bare := mediaMeta()
	bare.Image, bare.AltImage, bare.Audio, bare.Video, bare.ImagesJSON = "", "", "", "", ""
	base := bare.MarshalTyped()

	for name, tail := range map[string][]byte{
		"single stray byte":  {0x01},
		"truncated string":   {0x05, 'a', 'b'},
		"only three of five": {0x01, 'a', 0x01, 'b', 0x01, 'c'},
		"length past buffer": {0xff, 0xff, 0xff, 0x7f},
	} {
		t.Run(name, func(t *testing.T) {
			var got ClassMeta
			if err := got.UnmarshalTyped(append(append([]byte(nil), base...), tail...)); err != nil {
				t.Fatalf("malformed tail should be ignored, got error %v", err)
			}
			if got.Class != bare.Class || got.Name != bare.Name {
				t.Errorf("fixed fields corrupted by malformed tail: %+v", got)
			}
		})
	}
}
