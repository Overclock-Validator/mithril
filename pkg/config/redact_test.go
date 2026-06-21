package config

import (
	"strings"
	"testing"
)

func TestRedactEndpointForDisplay(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		want     string
	}{
		{
			name:     "api key query",
			endpoint: "https://rpc.example.invalid/?api-key=test-key-00000000-0000-4000-8000-000000000000",
			want:     "https://rpc.example.invalid/?api-key=REDACTED",
		},
		{
			name:     "preserve non secret query",
			endpoint: "https://example.invalid/rpc?network=mainnet",
			want:     "https://example.invalid/rpc?network=mainnet",
		},
		{
			name:     "redact userinfo",
			endpoint: "https://user:pass@example.invalid/rpc",
			want:     "https://REDACTED@example.invalid/rpc",
		},
		{
			name:     "fallback malformed query",
			endpoint: "://bad?token=secret&network=mainnet",
			want:     "://bad?token=REDACTED&network=mainnet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := RedactEndpointForDisplay(tt.endpoint); got != tt.want {
				t.Fatalf("RedactEndpointForDisplay() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRedactSecretsInText(t *testing.T) {
	got := RedactSecretsInText("Reference slot from https://rpc.invalid/?api-key=test-key-00000000&network=mainnet")
	if got != "Reference slot from https://rpc.invalid/?api-key=REDACTED&network=mainnet" {
		t.Fatalf("RedactSecretsInText() = %q", got)
	}

	got = RedactSecretsInText("Reference slot from https://rpc.invalid/?key=test-key-00000000&network=mainnet")
	if got != "Reference slot from https://rpc.invalid/?key=REDACTED&network=mainnet" {
		t.Fatalf("RedactSecretsInText() bare key = %q", got)
	}

	got = RedactSecretsInText("RPC failed at https://user:pass@rpc.invalid/path")
	if got != "RPC failed at https://REDACTED@rpc.invalid/path" {
		t.Fatalf("RedactSecretsInText() userinfo = %q", got)
	}

	plain := "2026-06-01 ERROR [lightbringer::repair::peer_manager] no repair peers available"
	if got := RedactSecretsInText(plain); got != plain {
		t.Fatalf("RedactSecretsInText() changed plain log line: %q", got)
	}
}

// redacts path-embedded keys and passwd/bearer params; keeps short legit segments
func TestRedact_PathAndExtraParams(t *testing.T) {
	cases := []struct{ in, mustNot, must string }{
		{"https://solana-mainnet.g.alchemy.com/v2/aB3xK9mN2pQ7rS5tU8vW1yZ4cD6eF0gH", "aB3xK9mN2pQ7rS5tU8vW1yZ4cD6eF0gH", "REDACTED"},
		{"https://free.rpcpool.com/9f8e7d6c5b4a3210fedcba9876543210/", "9f8e7d6c5b4a3210fedcba9876543210", "REDACTED"},
		{"https://x.io/?passwd=hunter2", "hunter2", "REDACTED"},
		{"https://x.io/?bearer=abc123def456", "abc123def456", "REDACTED"},
		{"https://prov.io/ALLUPPERCASESECRETTOKEN/", "ALLUPPERCASESECRETTOKEN", "REDACTED"},
		{"https://api.mainnet-beta.solana.com", "", "solana.com"}, // legit, unchanged
		{"https://x.io/v2/rpc", "", "/v2/rpc"},                    // legit short segments unchanged
	}
	for _, c := range cases {
		got := RedactEndpointForDisplay(c.in)
		if c.mustNot != "" && strings.Contains(got, c.mustNot) {
			t.Errorf("secret leaked: %q -> %q (still contains %q)", c.in, got, c.mustNot)
		}
		if c.must != "" && !strings.Contains(got, c.must) {
			t.Errorf("expected %q in output for %q, got %q", c.must, c.in, got)
		}
	}
}
