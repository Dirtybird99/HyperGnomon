package structures

import "testing"

// Anti-aliasing gate (added by the optimize-loop before the string-coalescing
// iterations). The typed decoders must return strings backed by memory they
// OWN, never by the caller's input buffer — a bbolt View frees that buffer the
// moment the read closure returns, so an aliasing decoder is a use-after-free.
//
// Each test decodes, snapshots the decoded strings, scribbles 0xFF over the
// entire source buffer, and requires the snapshots to be unchanged. A decoder
// that aliased its input fails here. These pass on the copy-based baseline and
// must keep passing after coalescing.

func TestLoop_NoAlias_SCTXParseTurbo(t *testing.T) {
	b := append([]byte(nil), benchSCTXParseTurbo.MarshalTurboTyped()...)
	var r SCTXParse
	if err := r.UnmarshalTurboTyped(b); err != nil {
		t.Fatal(err)
	}
	txid, scid, ep, sender := r.Txid, r.Scid, r.Entrypoint, r.Sender
	for i := range b {
		b[i] = 0xFF
	}
	if r.Txid != txid || r.Scid != scid || r.Entrypoint != ep || r.Sender != sender {
		t.Fatal("UnmarshalTurboTyped strings alias the source buffer (use-after-free)")
	}
}

func TestLoop_NoAlias_ClassMeta(t *testing.T) {
	b := append([]byte(nil), benchClass.MarshalTyped()...)
	var m ClassMeta
	if err := m.UnmarshalTyped(b); err != nil {
		t.Fatal(err)
	}
	class, name, desc, icon := m.Class, m.Name, m.Desc, m.IconURL
	tags := append([]string(nil), m.Tags...)
	for i := range b {
		b[i] = 0xFF
	}
	if m.Class != class || m.Name != name || m.Desc != desc || m.IconURL != icon {
		t.Fatal("UnmarshalTyped(ClassMeta) strings alias the source buffer (use-after-free)")
	}
	for i := range tags {
		if m.Tags[i] != tags[i] {
			t.Fatalf("ClassMeta tag %d aliases the source buffer (use-after-free)", i)
		}
	}
}

func TestLoop_NoAlias_InstallRecord(t *testing.T) {
	b := append([]byte(nil), benchInstall.MarshalTyped()...)
	var r InstallRecord
	if err := r.UnmarshalTyped(b); err != nil {
		t.Fatal(err)
	}
	owner, ep := r.Owner, r.Entrypoint
	for i := range b {
		b[i] = 0xFF
	}
	if r.Owner != owner || r.Entrypoint != ep {
		t.Fatal("UnmarshalTyped(InstallRecord) strings alias the source buffer (use-after-free)")
	}
}

func TestLoop_NoAlias_SCIDVariables(t *testing.T) {
	b := append([]byte(nil), MarshalSCIDVariablesTypedAppend(nil, benchVars)...)
	out, err := UnmarshalSCIDVariablesTyped(b)
	if err != nil {
		t.Fatal(err)
	}
	snap := make([]SCIDVariable, len(out))
	for i, v := range out {
		snap[i] = *v
	}
	for i := range b {
		b[i] = 0xFF
	}
	for i, v := range out {
		if v.Key != snap[i].Key || v.Value != snap[i].Value {
			t.Fatalf("SCIDVariable %d aliases the source buffer (use-after-free)", i)
		}
	}
}
