package indexer

import (
	"testing"

	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

// Regression tests for the invoke-path class refresh (review finding
// 2026-07-07): handleInvokeSC used to call ClassifySCVars(scid, "", vars) —
// with no code every rule misses, so the refresh overwrote the stored
// install-time Class with "UNKNOWN", reset Tags to ["all"], and dropped the
// class-gated fields (Version etc.) on EVERY invoke. Since invoking is how
// TELA apps update, this degraded every live TELA app's ClassMeta. The fix
// seeds the refresh with the stored class via ClassifySCVarsWithClass.

func strVar(k, v string) *structures.SCIDVariable {
	return &structures.SCIDVariable{Key: k, Value: v}
}

// TestInvokeRefreshPreservesStoredClass pins the full pipeline: install-time
// AddClass -> flush -> invoke refresh -> flush -> stored meta keeps its class.
func TestInvokeRefreshPreservesStoredClass(t *testing.T) {
	store, err := storage.NewBboltStore(t.TempDir(), "")
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer store.Close()
	idx := &Indexer{Store: store}

	const scid = "aa01000000000000000000000000000000000000000000000000000000000001"

	// Install-time classification (code available): TELA-INDEX-1 at height 100.
	install := storage.NewWriteBatch()
	install.AddClass(scid, &structures.ClassMeta{
		Class: "TELA-INDEX-1", Tags: []string{"all", "tela"},
		Name: "MyApp", DURL: "myapp.tela", Version: "1.0.0",
		InstallHeight: 100, LastHeight: 100,
	})
	if err := store.FlushBatch(install); err != nil {
		t.Fatalf("install flush: %v", err)
	}

	// Invoke at height 200: fresh var snapshot bumps telaVersion (the normal
	// TELA update flow). No code is available on the invoke path.
	invokeVars := []*structures.SCIDVariable{
		strVar("var_header_name", "MyApp"),
		strVar("dURL", "myapp.tela"),
		strVar("telaVersion", "1.1.0"),
	}
	refresh := storage.NewWriteBatch()
	idx.refreshClassMetaOnInvoke(scid, 200, invokeVars, refresh)
	if err := store.FlushBatch(refresh); err != nil {
		t.Fatalf("refresh flush: %v", err)
	}

	got, err := store.GetSCIDClass(scid)
	if err != nil || got == nil {
		t.Fatalf("GetSCIDClass: meta=%v err=%v", got, err)
	}
	if got.Class != "TELA-INDEX-1" {
		t.Fatalf("invoke refresh degraded Class: got %q, want TELA-INDEX-1", got.Class)
	}
	if len(got.Tags) != 2 || got.Tags[0] != "all" || got.Tags[1] != "tela" {
		t.Fatalf("invoke refresh degraded Tags: got %v, want [all tela]", got.Tags)
	}
	if got.Version != "1.1.0" {
		t.Fatalf("refresh did not pick up bumped telaVersion: got %q, want 1.1.0", got.Version)
	}
	if got.Name != "MyApp" || got.DURL != "myapp.tela" {
		t.Fatalf("refresh lost header fields: Name=%q DURL=%q", got.Name, got.DURL)
	}
	if got.InstallHeight != 100 {
		t.Fatalf("InstallHeight not preserved: got %d, want 100", got.InstallHeight)
	}
	if got.LastHeight != 200 {
		t.Fatalf("LastHeight not advanced: got %d, want 200", got.LastHeight)
	}
}

// TestInvokeRefreshG45KeepsClassAndMetadata covers the G45 shape: the class
// survives and the metadata blob still fills Name on refresh.
func TestInvokeRefreshG45KeepsClassAndMetadata(t *testing.T) {
	store, err := storage.NewBboltStore(t.TempDir(), "")
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer store.Close()
	idx := &Indexer{Store: store}

	const scid = "bb02000000000000000000000000000000000000000000000000000000000002"
	install := storage.NewWriteBatch()
	install.AddClass(scid, &structures.ClassMeta{
		Class: "G45-NFT", Tags: []string{"all", "g45"},
		Name: "Duck #1", InstallHeight: 50, LastHeight: 50,
	})
	if err := store.FlushBatch(install); err != nil {
		t.Fatalf("install flush: %v", err)
	}

	refresh := storage.NewWriteBatch()
	idx.refreshClassMetaOnInvoke(scid, 60, []*structures.SCIDVariable{
		strVar("metadata", `{"name":"Duck #1","attributes":{"Body":"B1"}}`),
	}, refresh)
	if err := store.FlushBatch(refresh); err != nil {
		t.Fatalf("refresh flush: %v", err)
	}

	got, err := store.GetSCIDClass(scid)
	if err != nil || got == nil {
		t.Fatalf("GetSCIDClass: meta=%v err=%v", got, err)
	}
	if got.Class != "G45-NFT" {
		t.Fatalf("invoke refresh degraded Class: got %q, want G45-NFT", got.Class)
	}
	if got.Name != "Duck #1" {
		t.Fatalf("refresh lost metadata name: got %q", got.Name)
	}
}

// TestInvokeRefreshNoPriorMeta pins the fallback: an invoke seen with no
// stored meta (install not indexed) still records a code-less UNKNOWN entry
// with the invoke height as InstallHeight — the pre-fix behavior for this
// case, unchanged.
func TestInvokeRefreshNoPriorMeta(t *testing.T) {
	store, err := storage.NewBboltStore(t.TempDir(), "")
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer store.Close()
	idx := &Indexer{Store: store}

	const scid = "cc03000000000000000000000000000000000000000000000000000000000003"
	refresh := storage.NewWriteBatch()
	idx.refreshClassMetaOnInvoke(scid, 300, []*structures.SCIDVariable{
		strVar("var_header_name", "Mystery"),
	}, refresh)
	if err := store.FlushBatch(refresh); err != nil {
		t.Fatalf("refresh flush: %v", err)
	}

	got, err := store.GetSCIDClass(scid)
	if err != nil || got == nil {
		t.Fatalf("GetSCIDClass: meta=%v err=%v", got, err)
	}
	if got.Class != "UNKNOWN" {
		t.Fatalf("no-prior-meta refresh: got Class %q, want UNKNOWN", got.Class)
	}
	if got.Name != "Mystery" {
		t.Fatalf("no-prior-meta refresh lost header name: got %q", got.Name)
	}
	if got.InstallHeight != 300 {
		t.Fatalf("no-prior-meta InstallHeight: got %d, want 300", got.InstallHeight)
	}
}
