package mcp

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestReadCappedBody(t *testing.T) {
	mk := func(body string, contentLength int64) *http.Response {
		return &http.Response{Body: io.NopCloser(strings.NewReader(body)), ContentLength: contentLength}
	}
	if body, err := readCappedBody(mk("hello", 5), 100); err != nil || string(body) != "hello" {
		t.Errorf("under cap = %q, %v", body, err)
	}
	if _, err := readCappedBody(mk("hello", 1000), 100); err == nil {
		t.Error("over-cap Content-Length must be rejected")
	}
	if _, err := readCappedBody(mk(strings.Repeat("x", 200), -1), 100); err == nil {
		t.Error("body exceeding cap must be rejected when Content-Length is absent")
	}
}

type fixedResolver struct {
	addresses []net.IPAddr
	err       error
}

func (r fixedResolver) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	return r.addresses, r.err
}

func TestResolvePinnedIPsFailsClosed(t *testing.T) {
	u, _ := url.Parse("https://example.test/path")
	if _, err := resolvePinnedIPs(context.Background(), fixedResolver{err: errors.New("dns down")}, u); err == nil {
		t.Fatal("DNS failure must be rejected")
	}
	if _, err := resolvePinnedIPs(context.Background(), fixedResolver{}, u); err == nil {
		t.Fatal("empty DNS result must be rejected")
	}
	if _, err := resolvePinnedIPs(context.Background(), fixedResolver{addresses: []net.IPAddr{
		{IP: net.ParseIP("8.8.8.8")},
		{IP: net.ParseIP("169.254.169.254")},
	}}, u); err == nil {
		t.Fatal("one blocked address must reject the whole mixed answer")
	}
}

func TestResolvePinnedIPsLimitsLoopbackToLocalhost(t *testing.T) {
	tests := []struct {
		name      string
		rawURL    string
		addresses []string
		wantErr   bool
	}{
		{"localhost", "http://localhost:8899", []string{"127.0.0.1", "::1"}, false},
		{"localhost case and trailing dot", "http://LOCALHOST.:8899", []string{"::1"}, false},
		{"localhost public answer", "https://localhost", []string{"8.8.8.8"}, true},
		{"public name rebound to loopback", "https://example.test", []string{"127.0.0.1"}, true},
		{"mixed public and loopback answer", "https://example.test", []string{"8.8.8.8", "::1"}, true},
		{"public rotation", "https://example.test", []string{"8.8.8.8", "1.1.1.1"}, false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			u, err := url.Parse(test.rawURL)
			if err != nil {
				t.Fatal(err)
			}
			addresses := make([]net.IPAddr, len(test.addresses))
			for i, address := range test.addresses {
				addresses[i].IP = net.ParseIP(address)
			}
			got, err := resolvePinnedIPs(context.Background(), fixedResolver{addresses: addresses}, u)
			if test.wantErr {
				if err == nil {
					t.Fatalf("resolvePinnedIPs() = %v, want error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolvePinnedIPs() error = %v", err)
			}
			if len(got) != len(addresses) {
				t.Fatalf("resolved addresses = %v, want %v", got, addresses)
			}
		})
	}
}

func TestResolvePinnedIPsAllowsLiteralLoopbackWithoutDNS(t *testing.T) {
	u, _ := url.Parse("http://127.0.0.1:8899")
	got, err := resolvePinnedIPs(context.Background(), fixedResolver{err: errors.New("must not resolve")}, u)
	if err != nil {
		t.Fatalf("resolvePinnedIPs() error = %v", err)
	}
	if len(got) != 1 || !got[0].Equal(net.ParseIP("127.0.0.1")) {
		t.Fatalf("resolved addresses = %v, want literal loopback", got)
	}
}

func TestPinnedDialIgnoresReboundHostname(t *testing.T) {
	var target string
	dial := pinnedDialContext([]net.IP{net.ParseIP("127.0.0.1")}, func(_ context.Context, _, address string) (net.Conn, error) {
		target = address
		return nil, errors.New("stop after capture")
	})
	_, _ = dial(context.Background(), "tcp", "attacker-controlled.example:8443")
	if target != "127.0.0.1:8443" {
		t.Fatalf("dial target = %q, want pinned IP", target)
	}
}

func TestPinnedRequestRequiresTLSOffLoopback(t *testing.T) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://8.8.8.8/metrics", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := doPinnedRequest(context.Background(), req, time.Second); err == nil || !strings.Contains(err.Error(), "only for loopback") {
		t.Fatalf("public cleartext HTTP error = %v", err)
	}
}

func TestPinnedRequestDoesNotFollowRedirect(t *testing.T) {
	var followed atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		followed.Store(true)
	}))
	defer target.Close()

	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer source.Close()

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, source.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := doPinnedRequest(t.Context(), req, time.Second)
	if err != nil {
		t.Fatalf("doPinnedRequest() error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusFound)
	}
	if followed.Load() {
		t.Fatal("redirect target was contacted")
	}
}
