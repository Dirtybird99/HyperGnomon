package indexer

import "testing"

// These tests pin the exact behavior of extractG45MetadataString so the
// json-fast-path rewrite (RawMessage struct + exact map fallback) stays
// byte-identical to the original map[string]interface{} + fmt.Sprintf("%v")
// implementation. The golden corpus only ever exercises the string/absent
// fast path (name always a string, description rarely present, icon never,
// null never), so the composite/null/error cases below are the ONLY guard on
// the fallback path and the varString (H2) swap. Every expected value here was
// captured by running these tests against the UNMODIFIED code first.

func TestExtractG45MetadataPin(t *testing.T) {
	cases := []struct {
		name                         string
		pre                          SCClass // fields possibly pre-filled by headers
		meta                         string
		wantName, wantDesc, wantIcon string
	}{
		{
			name:     "plain strings all three",
			meta:     `{"name":"Duck #1","description":"a duck","icon":"ipfs://x"}`,
			wantName: "Duck #1", wantDesc: "a duck", wantIcon: "ipfs://x",
		},
		{
			name:     "real-shape name only with attributes/id/image",
			meta:     `{"attributes":{"Background":"White","Body":"Body 1"},"id":1801,"image":"ipfs://abc/1801.png","name":"Dero Duck #1801"}`,
			wantName: "Dero Duck #1801",
		},
		{
			name:     "numeric name integer",
			meta:     `{"name":123}`,
			wantName: "123",
		},
		{
			name:     "numeric name float",
			meta:     `{"name":1.5}`,
			wantName: "1.5",
		},
		{
			name:     "numeric name large",
			meta:     `{"name":12345678901234}`,
			wantName: "1.2345678901234e+13",
		},
		{
			name:     "bool name true",
			meta:     `{"name":true}`,
			wantName: "true",
		},
		{
			name:     "bool name false",
			meta:     `{"name":false}`,
			wantName: "false",
		},
		{
			name:     "null name",
			meta:     `{"name":null}`,
			wantName: "<nil>",
		},
		{
			name:     "null description and icon",
			meta:     `{"name":"n","description":null,"icon":null}`,
			wantName: "n", wantDesc: "<nil>", wantIcon: "<nil>",
		},
		{
			name:     "array name",
			meta:     `{"name":[1,2,3]}`,
			wantName: "[1 2 3]",
		},
		{
			name:     "object name",
			meta:     `{"name":{"a":1}}`,
			wantName: "map[a:1]",
		},
		{
			name:     "mixed composite name string desc",
			meta:     `{"name":"x","description":[1,2]}`,
			wantName: "x", wantDesc: "[1 2]",
		},
		{
			name: "missing keys leave everything empty",
			meta: `{"foo":"bar","attributes":{"z":"y"}}`,
		},
		{
			name: "empty-string name behaves like absent (empty-guard)",
			meta: `{"name":"","description":"d"}`,
			// name stays "" (empty is a no-op either way), desc filled.
			wantDesc: "d",
		},
		{
			name:     "escaped unicode and quotes",
			meta:     `{"name":"café \"q\""}`,
			wantName: "café \"q\"",
		},
		{
			name: "invalid json sets nothing",
			meta: `{bad json`,
		},
		{
			name: "non-object json sets nothing",
			meta: `123`,
		},
		{
			name: "only attributes, no target keys",
			meta: `{"attributes":{"A":"1","B":"2","C":"3","D":"4"}}`,
		},
		{
			name:     "huge attributes with name",
			meta:     `{"attributes":{"a":"1","b":"2","c":"3","d":"4","e":"5","f":"6","g":"7","h":"8"},"id":9,"image":"ipfs://z","name":"Big #9"}`,
			wantName: "Big #9",
		},
		{
			name:     "header wins over metadata name, metadata fills desc",
			pre:      SCClass{Name: "HDR"},
			meta:     `{"name":"meta","description":"d"}`,
			wantName: "HDR", wantDesc: "d",
		},
		{
			name:     "header wins over null name",
			pre:      SCClass{Name: "HDR"},
			meta:     `{"name":null,"description":"d"}`,
			wantName: "HDR", wantDesc: "d",
		},
		// Review-regression pins (2026-07-07): the RawMessage struct fast
		// path was removed because encoding/json struct decoding matched
		// keys case-insensitively and skipped unconvertible unknown fields,
		// silently diverging from the original exact-key, whole-blob-strict
		// map decode. These pin the restored original semantics.
		{
			name: "fold-variant key is ignored (exact-key matching)",
			meta: `{"Name":"x"}`,
		},
		{
			name:     "exact key wins over fold variant",
			meta:     `{"name":"Duck","Name":"Evil"}`,
			wantName: "Duck",
		},
		{
			name: "out-of-range number anywhere sets nothing (whole-blob strictness)",
			meta: `{"supply":1e999,"name":"x"}`,
		},
		{
			name:     "surrogate-pair escape decodes via stdlib",
			meta:     `{"name":"\ud83d\ude00"}`,
			wantName: "😀",
		},
		{
			name:     "lone surrogate escape becomes U+FFFD via stdlib",
			meta:     `{"description":"\ud800 lone"}`,
			wantDesc: "� lone",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sc := tc.pre
			extractG45MetadataString(&sc, tc.meta)
			if sc.Name != tc.wantName {
				t.Errorf("Name = %q, want %q", sc.Name, tc.wantName)
			}
			if sc.Desc != tc.wantDesc {
				t.Errorf("Desc = %q, want %q", sc.Desc, tc.wantDesc)
			}
			if sc.IconURL != tc.wantIcon {
				t.Errorf("IconURL = %q, want %q", sc.IconURL, tc.wantIcon)
			}
		})
	}
}
