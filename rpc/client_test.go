package rpc

import "testing"

func TestResolveDialURL(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"203.0.113.10:10102", "ws://203.0.113.10:10102/ws"},
		{"ws://203.0.113.10:10102", "ws://203.0.113.10:10102/ws"},
		{"wss://node.example:11012", "wss://node.example:11012/ws"},
		{"http://node.example:10102", "ws://node.example:10102/ws"},
		{"https://node.example:11012", "wss://node.example:11012/ws"},
		{"ws://host:1/ws", "ws://host:1/ws"},
		{"https://host:1/ws", "wss://host:1/ws"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got := resolveDialURL(tc.in)
			if got != tc.want {
				t.Fatalf("resolveDialURL(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
