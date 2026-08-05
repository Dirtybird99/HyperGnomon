package api

import (
	"net/http"
	"net/url"
	"testing"
)

// reqWithRawQuery builds the minimal request queryParam and r.URL.Query() both
// read: only URL.RawQuery matters to either path.
func reqWithRawQuery(q string) *http.Request {
	return &http.Request{URL: &url.URL{RawQuery: q}}
}

// The real call sites only ever look up these literal keys. Fuzzing the lookup
// key itself would surface "divergences" that can't occur in production (the
// helper deliberately does not unescape the lookup key), so we hold it fixed.
var queryParamKeys = []string{"scid", "height", "address", "class"}

// TestQueryParam_MatchesURLValues documents the equivalence on hand-picked
// queries that exercise each tolerance clause: first-wins, bare keys, empty
// values, '+' and encoded escapes, a ';'-poisoned pair, and an encoded key.
func TestQueryParam_MatchesURLValues(t *testing.T) {
	cases := []string{
		"",
		"scid=abc",
		"scid=abc&height=100",
		"scid=first&scid=second",     // first-wins
		"scid",                       // bare key, no '='
		"scid=",                      // present but empty
		"height=1%30",                // encoded '0' -> "10"
		"address=a+b",                // '+' -> space
		"class=TELA-INDEX-1&scid=x",  // multi-pair
		"scid=a%2Gb",                 // undecodable escape in value
		"a=1;scid=2",                 // ';'-containing segment
		"sc%69d=hit",                 // percent-encoded key -> "scid"
		"nomatch=1&other=2",          // none of our keys present
		"height=1&height=2&height=3", // repeated -> first
		"scid=%E2%9C%93",             // multibyte UTF-8 value
	}
	for _, q := range cases {
		r := reqWithRawQuery(q)
		vals := r.URL.Query()
		for _, k := range queryParamKeys {
			want := vals.Get(k)
			got := queryParam(r, k)
			if got != want {
				t.Errorf("queryParam(%q, %q) = %q, url.Values.Get = %q", q, k, got, want)
			}
		}
	}
}

// FuzzQueryParam asserts queryParam stays byte-for-byte equal to url.Values.Get
// across arbitrary raw query strings, for each realistic lookup key.
func FuzzQueryParam(f *testing.F) {
	for _, q := range []string{
		"scid=abc&height=100", "scid=a&scid=b", "scid", "scid=",
		"a=1;scid=2", "address=a+b", "height=1%30", "scid=a%2Gb",
		"sc%69d=hit", "class=TELA-INDEX-1", "%=x", "&&&", "=noKey",
	} {
		f.Add(q)
	}
	f.Fuzz(func(t *testing.T, q string) {
		r := reqWithRawQuery(q)
		vals := r.URL.Query() // ignores ParseQuery's error exactly as net/http does
		for _, k := range queryParamKeys {
			want := vals.Get(k)
			got := queryParam(r, k)
			if got != want {
				t.Fatalf("divergence: raw=%q key=%q queryParam=%q url.Values.Get=%q", q, k, got, want)
			}
		}
	})
}
