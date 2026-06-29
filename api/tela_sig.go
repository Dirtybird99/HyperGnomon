package api

import (
	"strings"

	"github.com/hypergnomon/hypergnomon/structures"
)

// TELA signature infrastructure.
//
// TELA-DOC-1 contracts store a DERO Schnorr signature over the body in two
// hex-encoded variables: `fileCheckC` (the C component) and `fileCheckS`
// (the S component). The signer is the wallet address stored in
// `LOAD("owner")`. See civilware/tela/parse.go::ParseSignature for the
// canonical parser.
//
// v1.0 scope: we parse the signature fields and surface their presence
// via the `X-TELA-Verify` response header, but we do NOT yet run the
// bn256 Schnorr math. Operators who want cryptographic guarantees should
// wait for v1.0.1 when verification lands. This is an honest middle
// ground — we don't lie about having verified, and we don't block
// content delivery on crypto we can't fully test yet.
//
// Header value semantics:
//
//   disabled           — operator did not enable verification
//   unsigned           — DOC has no fileCheckC or fileCheckS
//   signed-unverified  — DOC has signature but we didn't verify (v1.0)
//   passed             — (v1.2+) cryptographic verification succeeded
//   failed             — (v1.2+) cryptographic verification failed

const (
	telaVerifyDisabled         = "disabled"
	telaVerifyUnsigned         = "unsigned"
	telaVerifySignedUnverified = "signed-unverified"
	telaVerifyPassed           = "passed"
	telaVerifyFailed           = "failed"
)

// telaSigFields carries the three STORE values that make a TELA signature:
// C and S are the Schnorr components; owner is the wallet address that
// signed. All three are post-hex-decode (DVM STORE strings come back
// hex-encoded from derod's RPC layer).
type telaSigFields struct {
	CHex    string
	SHex    string
	Owner   string
	Present bool // at least CHex and SHex are non-empty
}

// readTELASigFields extracts the three signature fields from a DOC's
// variables. Returns a zeroed struct with Present=false if any mandatory
// field is missing — caller surfaces as `unsigned` in that case.
func readTELASigFields(vars []*structures.SCIDVariable) telaSigFields {
	out := telaSigFields{}
	for _, v := range vars {
		k, ok := v.Key.(string)
		if !ok {
			continue
		}
		val := ""
		if s, ok := v.Value.(string); ok {
			val = s
		}
		switch k {
		case "fileCheckC":
			out.CHex = strings.TrimSpace(decodeHexIfPrintableASCII(val))
		case "fileCheckS":
			out.SHex = strings.TrimSpace(decodeHexIfPrintableASCII(val))
		case "owner":
			out.Owner = strings.TrimSpace(decodeHexIfPrintableASCII(val))
		}
	}
	out.Present = out.CHex != "" && out.SHex != ""
	return out
}

// decodeHexIfPrintableASCII is the api-package copy of the indexer's
// hex-decode heuristic. Kept here to avoid a cross-package dependency
// solely for tela signatures. Behavioral contract: if s is valid hex
// that decodes to printable ASCII, return decoded; else return s
// unchanged. DERO addresses + signature hex values survive either branch.
func decodeHexIfPrintableASCII(s string) string {
	if len(s) < 2 || len(s)%2 != 0 {
		return s
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return s
		}
	}
	decoded := make([]byte, len(s)/2)
	for i := 0; i < len(decoded); i++ {
		decoded[i] = nibbleByte(s[i*2])<<4 | nibbleByte(s[i*2+1])
	}
	// Accept only if every byte is printable ASCII (space..tilde) or
	// newline/tab. Binary hex (actual Schnorr C/S values) fails this
	// check and passes through as the original hex string — which is
	// what downstream verification wants anyway.
	for _, b := range decoded {
		if b == '\t' || b == '\n' || b == '\r' {
			continue
		}
		if b < 0x20 || b > 0x7e {
			return s
		}
	}
	return string(decoded)
}

func nibbleByte(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	}
	return 0
}

// verifyTELASignature is the pluggable verifier. v1.0 returns
// (matched=false, err=nil) meaning "not implemented, caller surfaces as
// signed-unverified". v1.2+ will replace this with the actual bn256
// Schnorr math described in civilware/tela/parse.go::ParseSignature.
//
// Kept as a package variable so tests (and a future drop-in verifier
// package) can swap the implementation without touching call sites.
var verifyTELASignature = func(body []byte, sig telaSigFields) (matched bool, err error) {
	return false, nil
}
