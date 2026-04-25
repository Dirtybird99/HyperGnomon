package indexer

import "testing"

func TestNormalizePostScanVarsMode(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty defaults lazy", in: "", want: PostScanVarsLazy},
		{name: "lazy", in: "lazy", want: PostScanVarsLazy},
		{name: "all", in: "all", want: PostScanVarsAll},
		{name: "case and spaces", in: " ALL ", want: PostScanVarsAll},
		{name: "invalid defaults lazy", in: "everything", want: PostScanVarsLazy},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := normalizePostScanVarsMode(tt.in); got != tt.want {
				t.Fatalf("normalizePostScanVarsMode(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
