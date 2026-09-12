package mcp

import (
	"errors"
	"net"
	"net/url"
	"strings"
	"testing"
)

func TestIsBlockedIPTransitionAddrs(t *testing.T) {
	blocked := []string{
		"64:ff9b::a9fe:a9fe", // NAT64 of 169.254.169.254 (metadata)
		"64:ff9b::a00:1",     // NAT64 of 10.0.0.1
		"64:ff9b:1::808:808", // RFC 8215 local-use translation prefix
		"2002:a9fe:a9fe::",   // 6to4 of 169.254.169.254
		"2002:a00:1::",       // 6to4 of 10.0.0.1
	}
	for _, s := range blocked {
		ip := net.ParseIP(s)
		if ip == nil {
			t.Fatalf("bad test IP %q", s)
		}
		if !isBlockedIP(ip) {
			t.Errorf("isBlockedIP(%s) = false, want true (transition-address bypass)", s)
		}
	}
	// NAT64/6to4 of a public address is allowed. Transition-encoded loopback is
	// not: only a literal local address receives the loopback exception.
	for _, s := range []string{
		"64:ff9b::808:808", // 8.8.8.8
		"2002:808:808::",   // 8.8.8.8
	} {
		ip := net.ParseIP(s)
		if ip == nil {
			t.Fatalf("bad test IP %q", s)
		}
		if isBlockedIP(ip) {
			t.Errorf("isBlockedIP(%s) = true, want false", s)
		}
	}
	if ip := net.ParseIP("64:ff9b::7f00:1"); ip == nil || !isBlockedIP(ip) {
		t.Error("transition-encoded loopback must be blocked")
	}
}

func TestValidateURL(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr bool
	}{
		{"empty string", "", true},
		{"file scheme", "file:///etc/passwd", true},
		{"ftp scheme", "ftp://host/x", true},
		{"metadata ip", "http://169.254.169.254/latest/meta-data", true},
		{"metadata hostname", "http://metadata.google.internal/x", true},
		{"embedded credentials", "http://user:pass@127.0.0.1:9090/metrics", true},
		{"private 10", "http://10.0.0.5:9090/metrics", true},
		{"private 172.16", "http://172.16.0.1:9090/metrics", true},
		{"private 192.168", "http://192.168.1.1:9090/metrics", true},
		{"cgnat literal", "http://100.64.0.1:9090/metrics", true},
		{"broadcast", "http://255.255.255.255/x", true},
		{"ipv4 unspecified", "http://0.0.0.0:9090/x", true},
		{"ipv6 unspecified", "http://[::]:9090/x", true},
		{"ipv6 unique-local", "http://[fc00::1]:9090/x", true},
		{"ipv6 link-local", "http://[fe80::1]:9090/x", true},
		{"ipv4-mapped private", "http://[::ffff:10.0.0.1]:9090/x", true},
		{"NAT64 metadata", "http://[64:ff9b::a9fe:a9fe]:9090/metrics", true},
		{"http localhost allowed", "http://127.0.0.1:9090/metrics", false},
		{"ipv6 loopback allowed", "http://[::1]:9090/metrics", false},
		{"https public allowed", "https://api.mainnet-beta.solana.com", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := validateURL(tt.url)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateURL(%q) err = %v, wantErr = %v", tt.url, err, tt.wantErr)
			}
		})
	}
}

func TestIsBlockedIP(t *testing.T) {
	blocked := []string{
		"10.0.0.1", "172.16.5.5", "192.168.0.1", "169.254.169.254",
		"100.64.0.1", "100.127.255.255", "0.0.0.0", "255.255.255.255",
		"fc00::1", "fe80::1", "::", "::ffff:192.168.1.1",
		"0.1.2.3", "192.0.2.1", "198.18.0.1", "198.51.100.1",
		"203.0.113.1", "224.0.0.1", "240.0.0.1", "2001:db8::1",
		"ff02::1", "fec0::1", "::2", "::c0a8:101",
	}
	allowed := []string{"127.0.0.1", "::1", "8.8.8.8", "1.1.1.1", "100.63.0.1", "100.128.0.1", "2606:4700:4700::1111"}
	for _, value := range blocked {
		if ip := net.ParseIP(value); ip == nil || !isBlockedIP(ip) {
			t.Errorf("isBlockedIP(%s) = false, want true", value)
		}
	}
	for _, value := range allowed {
		if ip := net.ParseIP(value); ip == nil || isBlockedIP(ip) {
			t.Errorf("isBlockedIP(%s) = true, want false", value)
		}
	}
}

func TestValidateURLErrorDoesNotLeakSecret(t *testing.T) {
	// A space in the host makes url.Parse return an error containing the raw URL.
	_, err := validateURL("http://exa mple.com/x?api-key=SUPERSECRET123")
	if err == nil {
		t.Fatal("malformed URL should error")
	}
	if strings.Contains(err.Error(), "SUPERSECRET123") {
		t.Errorf("error message leaks the query secret: %v", err)
	}
}

func TestCanonicalOrigin(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"https default port and DNS normalization", "https://RPC.Example.COM./secret?api-key=x", "https://rpc.example.com:443"},
		{"scheme normalization", "HTTPS://rpc.example.com/path", "https://rpc.example.com:443"},
		{"http default port", "http://rpc.example.com/path", "http://rpc.example.com:80"},
		{"explicit port", "https://rpc.example.com:8443/path", "https://rpc.example.com:8443"},
		{"IPv6 normalization", "http://[2001:0db8::1]:8080/x", "http://[2001:db8::1]:8080"},
		{"IPv4 mapped normalization", "http://[::ffff:127.0.0.1]/x", "http://127.0.0.1:80"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := canonicalOrigin(tc.raw)
			if err != nil {
				t.Fatalf("canonicalOrigin(%q): %v", tc.raw, err)
			}
			if got != tc.want {
				t.Fatalf("canonicalOrigin(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}

	for _, raw := range []string{
		"ftp://rpc.example.com/x",
		"http:///missing-host",
		"https://user:secret@rpc.example.com/x",
		"https://rpc.example.com:0/x",
		"https://rpc.example.com:65536/x",
	} {
		if got, err := canonicalOrigin(raw); err == nil {
			t.Errorf("canonicalOrigin(%q) = %q, want error", raw, got)
		}
	}
}

func TestSanitizeEndpointForDisplayRemovesPathSecrets(t *testing.T) {
	raw := "https://user:password@rpc.example.com:8899/PATHSECRET?api-key=QUERYSECRET#frag"
	got := sanitizeEndpointForDisplay(raw)
	if got != "https://rpc.example.com:8899" {
		t.Fatalf("sanitizeEndpointForDisplay = %q", got)
	}
	for _, secret := range []string{"user", "password", "PATHSECRET", "QUERYSECRET"} {
		if strings.Contains(got, secret) {
			t.Errorf("sanitized endpoint leaks %q: %s", secret, got)
		}
	}
	compatDisplay := sanitizeURLForDisplay(raw)
	if compatDisplay != "https://rpc.example.com:8899/" {
		t.Fatalf("sanitizeURLForDisplay = %q", compatDisplay)
	}

	httpErr := &url.Error{
		Op:  "POST",
		URL: "https://rpc.example.com/PATHSECRET?api-key=QUERYSECRET",
		Err: errors.New("connection refused"),
	}
	sanitized := sanitizeHTTPError(httpErr).Error()
	if !strings.Contains(sanitized, "https://rpc.example.com:443") {
		t.Fatalf("sanitized HTTP error omitted safe origin: %s", sanitized)
	}
	if strings.Contains(sanitized, "PATHSECRET") || strings.Contains(sanitized, "QUERYSECRET") {
		t.Fatalf("sanitized HTTP error leaks endpoint secret: %s", sanitized)
	}
}

func TestSanitizeHTTPErrorRedactsAndBoundsNestedText(t *testing.T) {
	inner := errors.New("Authorization: Bearer INNER_SECRET https://rpc.example/INNER_PATH?token=INNER_QUERY\n" + strings.Repeat("x", maxHTTPErrorBytes*2))
	httpErr := &url.Error{
		Op:  "POST",
		URL: "https://rpc.example/OUTER_PATH?api-key=OUTER_QUERY",
		Err: inner,
	}
	gotErr := sanitizeHTTPError(httpErr)
	got := gotErr.Error()
	for _, secret := range []string{"INNER_SECRET", "INNER_PATH", "INNER_QUERY", "OUTER_PATH", "OUTER_QUERY"} {
		if strings.Contains(got, secret) {
			t.Fatalf("sanitized HTTP error leaks %q: %q", secret, got)
		}
	}
	if len(got) > maxHTTPErrorBytes || strings.ContainsAny(got, "\r\n") {
		t.Fatalf("sanitized HTTP error is not bounded single-line text: len=%d text=%q", len(got), got)
	}
	if !errors.Is(gotErr, inner) {
		t.Fatal("sanitized HTTP error lost its wrapped cause")
	}

	fallback := errors.New("token=FALLBACK_SECRET " + strings.Repeat("y", maxHTTPErrorBytes*2))
	gotErr = sanitizeHTTPError(fallback)
	if strings.Contains(gotErr.Error(), "FALLBACK_SECRET") || len(gotErr.Error()) > maxHTTPErrorBytes || !errors.Is(gotErr, fallback) {
		t.Fatalf("fallback HTTP error was not safely wrapped: %q", gotErr)
	}
}
