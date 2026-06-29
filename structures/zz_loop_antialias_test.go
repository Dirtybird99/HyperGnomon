package structures

// Anti-aliasing gate for the optimize-loop (loop.run.yaml).
//
// The typed decoders on the hot path coalesce their length-prefixed string
// fields into ONE owned backing buffer and return unsafe.String views of it
// (helpers ownedCut / locLenString / locVarField). The whole point of that
// pattern is that the returned strings view memory the decoder OWNS, never the
// caller's input buffer b — because b is a bbolt-owned page that the View
// transaction frees once the read returns. A decoder that instead returned a
// string aliasing b would produce silent, GC-unsafe use-after-free corruption.
//
// These tests are the bound correctness gate that forbids that aliasing. Each
// encodes KNOWN literal inputs into a fresh buffer b, decodes, then scribbles
// 0xFF over every byte of b and asserts the decoded fields STILL equal the
// known literals. The comparison target is the literal (which lives in rodata,
// independent of b) — NOT a snapshot of the decoded string, which would alias b
// identically on a buggy decoder and corrupt in lockstep (a false green). With
// an independent target, an aliasing decoder turns the post-scribble bytes to
// 0xFF and the assertion fails; an owned-buffer decoder is unaffected.
//
// Proven to discriminate: temporarily rewriting a decoder to return
// unsafe.String(&b[start], n) makes the matching test go RED.

import "testing"

// scribble overwrites every byte of b with 0xFF. 0xFF is outside the ASCII
// range of all test inputs, so any string still aliasing b becomes detectably
// corrupt.
func scribble(b []byte) {
	for i := range b {
		b[i] = 0xFF
	}
}

func TestLoop_NoAlias_SCTXParseTurbo(t *testing.T) {
	const (
		wantTxid   = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
		wantScid   = "a05395bb0cf77adc850928b0db00eb5ca7a9ccbafd9a38d021c8d299ad5ce1a4"
		wantEntry  = "InputStr"
		wantSender = "dero1qyjjxxaabbccddeeff0011223344556677889900aabbccddee00112233445566"
	)
	in := SCTXParse{
		Txid: wantTxid, Scid: wantScid, Entrypoint: wantEntry,
		Method: MethodInvokeSC, Sender: wantSender, Fees: 12345, Height: 6927400,
	}
	b := in.MarshalTurboTyped()

	var got SCTXParse
	if err := got.UnmarshalTurboTyped(b); err != nil {
		t.Fatalf("UnmarshalTurboTyped: %v", err)
	}
	// Sanity: decode is correct before we touch b (passes on any impl).
	if got.Txid != wantTxid || got.Scid != wantScid ||
		got.Entrypoint != wantEntry || got.Sender != wantSender {
		t.Fatalf("decode drift before scribble: %+v", got)
	}

	scribble(b)

	// The decoded strings must be unaffected by corrupting the source buffer.
	if got.Txid != wantTxid {
		t.Errorf("Txid aliases source buffer b: got %q want %q", got.Txid, wantTxid)
	}
	if got.Scid != wantScid {
		t.Errorf("Scid aliases source buffer b: got %q want %q", got.Scid, wantScid)
	}
	if got.Entrypoint != wantEntry {
		t.Errorf("Entrypoint aliases source buffer b: got %q want %q", got.Entrypoint, wantEntry)
	}
	if got.Sender != wantSender {
		t.Errorf("Sender aliases source buffer b: got %q want %q", got.Sender, wantSender)
	}
}

func TestLoop_NoAlias_ClassMeta(t *testing.T) {
	want := ClassMeta{
		Class:         "TELA-INDEX-1",
		Tags:          []string{"all", "tela", "index"},
		Name:          "MyTELA Application",
		Desc:          "A decentralized guide to everything",
		IconURL:       "https://example.com/icon.png",
		DURL:          "app.tela",
		Version:       "1.0.0",
		InstallHeight: 6927000,
		LastHeight:    6927500,
	}
	b := want.MarshalTyped()

	var got ClassMeta
	if err := got.UnmarshalTyped(b); err != nil {
		t.Fatalf("UnmarshalTyped: %v", err)
	}
	if got.Class != want.Class || got.Name != want.Name || got.Desc != want.Desc ||
		got.IconURL != want.IconURL || got.DURL != want.DURL || got.Version != want.Version ||
		len(got.Tags) != len(want.Tags) {
		t.Fatalf("decode drift before scribble: %+v", got)
	}

	scribble(b)

	for i := range want.Tags {
		if got.Tags[i] != want.Tags[i] {
			t.Errorf("Tags[%d] aliases source buffer b: got %q want %q", i, got.Tags[i], want.Tags[i])
		}
	}
	if got.Class != want.Class {
		t.Errorf("Class aliases source buffer b: got %q want %q", got.Class, want.Class)
	}
	if got.Name != want.Name {
		t.Errorf("Name aliases source buffer b: got %q want %q", got.Name, want.Name)
	}
	if got.Desc != want.Desc {
		t.Errorf("Desc aliases source buffer b: got %q want %q", got.Desc, want.Desc)
	}
	if got.IconURL != want.IconURL {
		t.Errorf("IconURL aliases source buffer b: got %q want %q", got.IconURL, want.IconURL)
	}
	if got.DURL != want.DURL {
		t.Errorf("DURL aliases source buffer b: got %q want %q", got.DURL, want.DURL)
	}
	if got.Version != want.Version {
		t.Errorf("Version aliases source buffer b: got %q want %q", got.Version, want.Version)
	}
}

func TestLoop_NoAlias_SCIDVariables(t *testing.T) {
	in := []*SCIDVariable{
		{Key: "nameHdr", Value: "TELA-INDEX-1 Test App"},
		{Key: "descrHdr", Value: "A longer description field used by TELA INDEX apps"},
		{Key: "iconURLHdr", Value: "https://example.com/icon.png"},
		{Key: "balance", Value: uint64(1000000)},
		{Key: "owner", Value: "dero1qyjjxxaabbccddeeff0011223344"},
	}
	b := MarshalSCIDVariablesTypedAppend(nil, in)

	out, err := UnmarshalSCIDVariablesTyped(b)
	if err != nil {
		t.Fatalf("UnmarshalSCIDVariablesTyped: %v", err)
	}
	if len(out) != len(in) {
		t.Fatalf("count: got %d want %d", len(out), len(in))
	}
	for i := range in {
		if out[i].Key != in[i].Key || out[i].Value != in[i].Value {
			t.Fatalf("decode drift before scribble at [%d]: got (%v,%v)", i, out[i].Key, out[i].Value)
		}
	}

	scribble(b)

	for i := range in {
		if out[i].Key != in[i].Key {
			t.Errorf("[%d] Key aliases source buffer b: got %v want %v", i, out[i].Key, in[i].Key)
		}
		if out[i].Value != in[i].Value {
			t.Errorf("[%d] Value aliases source buffer b: got %v want %v", i, out[i].Value, in[i].Value)
		}
	}
}
