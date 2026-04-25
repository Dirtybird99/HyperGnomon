package api

import (
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// readTELASigFields walks a vars slice and extracts fileCheckC /
// fileCheckS / owner. Cost is dominated by decodeHexIfPrintableASCII
// on the three matched keys. The two shapes cover (a) the common
// TELA-DOC-1 contract case where sig fields are present and (b) a
// vars slice where no sig fields exist — the scanner still walks
// every entry but matches nothing.

func buildBenchVars(withSig bool, extraKeys int) []*structures.SCIDVariable {
	vars := make([]*structures.SCIDVariable, 0, extraKeys+3)
	for i := 0; i < extraKeys; i++ {
		vars = append(vars, &structures.SCIDVariable{
			Key:   "other_key",
			Value: "48656c6c6f20576f726c64", // "Hello World" in hex
		})
	}
	if withSig {
		vars = append(vars,
			&structures.SCIDVariable{Key: "fileCheckC", Value: "0123456789abcdef0123456789abcdef"},
			&structures.SCIDVariable{Key: "fileCheckS", Value: "fedcba9876543210fedcba9876543210"},
			&structures.SCIDVariable{Key: "owner", Value: "64657230313233343536373839"}, // "dero0123456789" hex
		)
	}
	return vars
}

// BenchmarkReadTELASigFields_Signed is the hot path: a TELA-DOC-1
// contract that carries a signature. Three matches + three
// hex-decode calls.
func BenchmarkReadTELASigFields_Signed(b *testing.B) {
	vars := buildBenchVars(true, 12) // 12 other keys + 3 sig keys ≈ realistic DOC shape
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := readTELASigFields(vars)
		if !f.Present {
			b.Fatal("expected Present=true")
		}
	}
}

// BenchmarkReadTELASigFields_Unsigned exercises the scan-with-no-
// matches path. Cost is the switch statement walking to default on
// every var. Useful to confirm the early-return cost matches the
// signed case within a small constant.
func BenchmarkReadTELASigFields_Unsigned(b *testing.B) {
	vars := buildBenchVars(false, 15)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := readTELASigFields(vars)
		if f.Present {
			b.Fatal("expected Present=false")
		}
	}
}
