package structures

import (
	"testing"

	"github.com/vmihailenco/msgpack/v5"
)

// Pre-C6.3 measurement: how expensive is msgpack on InstallRecord and
// ClassMeta? These are written less often per batch than AddrSCIDEntry
// (installs: 0-5/batch, classes: 0-10/batch vs addr_scids: 50-2000), so
// the per-struct cost must clearly dominate *some* bench before we pay
// for the length-prefix complexity of typed encoding for strings.

var benchInstall = InstallRecord{
	Owner:      "dero1qyjjxxaabbccddeeff0011223344556677889900aabbccddee00112233445566",
	Entrypoint: "Initialize",
	Fees:       12345,
}

var benchClass = ClassMeta{
	Class:         "TELA-INDEX-1",
	Tags:          []string{"all", "tela"},
	Name:          "MyTELA",
	Desc:          "A decentralized guide",
	IconURL:       "https://example.com/icon.png",
	InstallHeight: 6927000,
	LastHeight:    6927500,
}

func BenchmarkInstallRecord_Marshal_Msgpack(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_, err := msgpack.Marshal(&benchInstall)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInstallRecord_Unmarshal_Msgpack(b *testing.B) {
	blob, _ := msgpack.Marshal(&benchInstall)
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		var r InstallRecord
		if err := msgpack.Unmarshal(blob, &r); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkInstallRecord_Marshal_Typed(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		benchSinkBytes = benchInstall.MarshalTyped()
	}
}

func BenchmarkInstallRecord_MarshalTypedAppend(b *testing.B) {
	buf := make([]byte, 0, 160)
	b.ReportAllocs()
	for b.Loop() {
		benchSinkBytes = benchInstall.MarshalTypedAppend(buf[:0])
	}
}

func BenchmarkInstallRecord_Unmarshal_Typed(b *testing.B) {
	blob := benchInstall.MarshalTyped()
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		var r InstallRecord
		if err := r.UnmarshalTyped(blob); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClassMeta_Marshal_Msgpack(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_, err := msgpack.Marshal(&benchClass)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClassMeta_Unmarshal_Msgpack(b *testing.B) {
	blob, _ := msgpack.Marshal(&benchClass)
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		var r ClassMeta
		if err := msgpack.Unmarshal(blob, &r); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkClassMeta_Marshal_Typed is the production path after the typed
// v1 switch. Head-to-head comparison against the msgpack baseline above.
// Target: strictly fewer ns/op and fewer allocs.
func BenchmarkClassMeta_Marshal_Typed(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		benchSinkBytes = benchClass.MarshalTyped()
	}
}

func BenchmarkClassMeta_Unmarshal_Typed(b *testing.B) {
	blob := benchClass.MarshalTyped()
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		var r ClassMeta
		if err := r.UnmarshalTyped(blob); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkClassMeta_MarshalTypedAppend — hot-path variant that reuses
// caller-owned scratch. Expected to be roughly marshal-cost minus the
// slice-header allocation in MarshalTyped. Shows the appendable API's
// value for batched contexts that can pool a backing array.
func BenchmarkClassMeta_MarshalTypedAppend(b *testing.B) {
	// Big enough that no iteration re-grows the underlying array.
	buf := make([]byte, 0, 512)
	b.ReportAllocs()
	for b.Loop() {
		benchSinkBytes = benchClass.MarshalTypedAppend(buf[:0])
	}
}

// C8 pre-measurement: what's the msgpack cost on a realistic SCTXParse?
// Two variants: turbo (ScArgs+Payloads nil — typical hot path) and
// non-turbo (args populated). Turbo is what most of the indexer writes.
var benchSCTXParseTurbo = SCTXParse{
	Txid:       "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
	Scid:       "a05395bb0cf77adc850928b0db00eb5ca7a9ccbafd9a38d021c8d299ad5ce1a4",
	Entrypoint: "InputStr",
	Method:     MethodInvokeSC,
	Sender:     "dero1qyjjxxaabbccddeeff0011223344556677889900aabbccddee00112233445566",
	Fees:       12345,
	Height:     6927400,
	// ScArgs, Payloads left nil — matches turbo mode
}

func BenchmarkSCTXParse_Turbo_Marshal_Msgpack(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_, err := msgpack.Marshal(&benchSCTXParseTurbo)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSCTXParse_Turbo_Unmarshal_Msgpack(b *testing.B) {
	blob, _ := msgpack.Marshal(&benchSCTXParseTurbo)
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		var r SCTXParse
		if err := msgpack.Unmarshal(blob, &r); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSCTXParse_Turbo_Marshal_Typed(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		benchSinkBytes = benchSCTXParseTurbo.MarshalTurboTyped()
	}
}

func BenchmarkSCTXParse_Turbo_MarshalTypedAppend(b *testing.B) {
	buf := make([]byte, 0, 320)
	b.ReportAllocs()
	for b.Loop() {
		benchSinkBytes = benchSCTXParseTurbo.MarshalTurboTypedAppend(buf[:0])
	}
}

func BenchmarkSCTXParse_Turbo_Unmarshal_Typed(b *testing.B) {
	blob := benchSCTXParseTurbo.MarshalTurboTyped()
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		var r SCTXParse
		if err := r.UnmarshalTurboTyped(blob); err != nil {
			b.Fatal(err)
		}
	}
}
