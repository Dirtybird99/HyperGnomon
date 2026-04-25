package storage

import (
	"strings"
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

func TestSCCode_RoundTrip(t *testing.T) {
	store := openTestStore(t)

	scid := fakeSCID()
	code := "Function Initialize() Uint64\n\t10 STORE(\"initialized\", 1)\n\t20 RETURN 0\nEnd Function\n"
	if err := store.PutSCCode(scid, &structures.SCCodeEntry{Code: code, InstallHeight: 42}); err != nil {
		t.Fatalf("PutSCCode: %v", err)
	}

	got, err := store.GetSCCode(scid)
	if err != nil {
		t.Fatalf("GetSCCode: %v", err)
	}
	if got == nil {
		t.Fatal("GetSCCode returned nil on round-trip")
	}
	if got.Code != code {
		t.Fatalf("code mismatch:\n got %q\nwant %q", got.Code, code)
	}
	if got.InstallHeight != 42 {
		t.Fatalf("install height: got %d, want 42", got.InstallHeight)
	}
}

func TestSCCode_Absent(t *testing.T) {
	store := openTestStore(t)
	got, err := store.GetSCCode(fakeSCID())
	if err != nil {
		t.Fatalf("GetSCCode on absent: %v", err)
	}
	if got != nil {
		t.Fatalf("expected (nil, nil) for absent scid, got %+v", got)
	}
}

func TestSCCode_EmptyScid(t *testing.T) {
	store := openTestStore(t)
	got, err := store.GetSCCode("")
	if err != nil || got != nil {
		t.Fatalf("empty scid: got (%+v, %v), want (nil, nil)", got, err)
	}
	// PutSCCode("") should be a silent no-op.
	if err := store.PutSCCode("", &structures.SCCodeEntry{Code: "ignored"}); err != nil {
		t.Fatalf("PutSCCode empty scid returned %v, want nil (silent skip)", err)
	}
}

func TestSCCode_WriteBatch(t *testing.T) {
	store := openTestStore(t)

	batch := NewWriteBatch()
	scidA := fakeSCID()
	scidB := fakeSCID()
	batch.AddSCCode(scidA, 100, "FUNCTION A() END FUNCTION")
	batch.AddSCCode(scidB, 200, strings.Repeat("X", 4096)) // larger contract
	batch.AddSCCode("", 0, "skipped")                      // guard: empty scid
	batch.AddSCCode(scidA, 300, "")                        // guard: empty code
	batch.LastHeight = 300

	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}
	PutWriteBatch(batch)

	a, _ := store.GetSCCode(scidA)
	if a == nil || a.Code != "FUNCTION A() END FUNCTION" || a.InstallHeight != 100 {
		t.Fatalf("scidA round-trip failed: %+v", a)
	}

	b, _ := store.GetSCCode(scidB)
	if b == nil || len(b.Code) != 4096 || b.InstallHeight != 200 {
		t.Fatalf("scidB round-trip failed: len=%d heightSet=%t", func() int {
			if b != nil {
				return len(b.Code)
			}
			return -1
		}(), b != nil && b.InstallHeight == 200)
	}
}

func TestResetIndex_ClearsSCCode(t *testing.T) {
	store := openTestStore(t)

	scid := fakeSCID()
	if err := store.PutSCCode(scid, &structures.SCCodeEntry{Code: "FN() END FN"}); err != nil {
		t.Fatalf("PutSCCode: %v", err)
	}
	if got, _ := store.GetSCCode(scid); got == nil {
		t.Fatal("pre-reset: GetSCCode returned nil")
	}

	if err := store.ResetIndex(); err != nil {
		t.Fatalf("ResetIndex: %v", err)
	}

	got, err := store.GetSCCode(scid)
	if err != nil {
		t.Fatalf("post-reset GetSCCode: %v", err)
	}
	if got != nil {
		t.Fatalf("sccode bucket not cleared by ResetIndex: %+v", got)
	}

	// Bucket should still exist — subsequent Put must work.
	if err := store.PutSCCode(scid, &structures.SCCodeEntry{Code: "FN2() END FN"}); err != nil {
		t.Fatalf("PutSCCode post-reset: %v", err)
	}
}
