package dashboard

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// This proxy deliberately forwards every route: the public reader itself must
// enforce its boundary even when the proxy has no route allowlist.
func TestPublicBoundaryThroughUnfilteredHTTPSProxy(t *testing.T) {
	path, body := fixtureFeed(t)
	s := testSnapshot(time.Now().UTC())
	s.RunID = "first-run"
	identity := "old-identity"
	s.IdentityKey = &identity
	setFixture(t, body, s)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	upstream, _ := url.Parse("http://" + listener.Addr().String())
	proxy := httptest.NewUnstartedServer(httputil.NewSingleHostReverseProxy(upstream))
	origin := "https://" + proxy.Listener.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveProfile(ctx, path, listener, origin, PublicOptions{}) }()
	proxy.StartTLS()
	t.Cleanup(func() {
		cancel()
		proxy.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("public reader did not shut down")
		}
	})
	client := proxy.Client()
	client.Timeout = 3 * time.Second
	request := func(path, host, requestOrigin string) *http.Response {
		t.Helper()
		r, err := http.NewRequest(http.MethodGet, origin+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if host != "" {
			r.Host = host
		}
		if requestOrigin != "" {
			r.Header.Set("Origin", requestOrigin)
		}
		r.Header.Set("X-Forwarded-Host", proxy.Listener.Addr().String())
		r.Header.Set("X-Forwarded-Proto", "https")
		r.Header.Set("Forwarded", `host="`+proxy.Listener.Addr().String()+`";proto=https`)
		resp, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	for _, tc := range []struct {
		path, host, origin string
		status             int
	}{
		{"/api/v1/status", "", "", 200},
		{"/api/v1/status", "attacker.invalid", "", 403},
		{"/api/v1/status", "", "https://attacker.invalid", 403},
		{"/snapshot", "", "", 404},
		{"/admin", "", "", 404},
		{"/debug/pprof/", "", "", 404},
	} {
		resp := request(tc.path, tc.host, tc.origin)
		if resp.StatusCode != tc.status {
			t.Errorf("%s host=%q origin=%q: got %d want %d", tc.path, tc.host, tc.origin, resp.StatusCode, tc.status)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	dialer := websocket.Dialer{HandshakeTimeout: 3 * time.Second, TLSClientConfig: client.Transport.(*http.Transport).TLSClientConfig}
	address := "wss://" + proxy.Listener.Addr().String() + "/websocket"
	var clients []*websocket.Conn
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()
	for i := 0; i < 16; i++ {
		conn, _, err := dialer.Dial(address, http.Header{"Origin": {origin}})
		if err != nil {
			t.Fatalf("viewer %d: %v", i, err)
		}
		clients = append(clients, conn)
	}
	conn, resp, err := dialer.Dial(address, http.Header{"Origin": {origin}})
	if conn != nil {
		conn.Close()
	}
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
	if err == nil || resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("17th viewer: response=%v error=%v", resp, err)
	}
	clients[1].Close()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, resp, err = dialer.Dial(address, http.Header{"Origin": {origin}})
		if err == nil {
			clients = append(clients, conn)
			break
		}
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("viewer slot not released after disconnect: %v", err)
	}
	// Ordinary samples with the same node binding must update the existing
	// connection without disconnecting it.
	awaitFirstRun := func(sampledAt time.Time) {
		t.Helper()
		until := time.Now().Add(6 * time.Second)
		for time.Now().Before(until) {
			m := readMessage(t, clients[0])
			if string(m["topic"]) != `"mithril"` || string(m["key"]) != `"public"` {
				t.Fatalf("private frame on public route: %v", m)
			}
			var value PublicSnapshot
			if err := json.Unmarshal(m["value"], &value); err != nil {
				t.Fatal(err)
			}
			if value.SourceState == "fresh" && value.SampledAt != nil && value.SampledAt.Equal(sampledAt) {
				if value.Node == nil || value.Node.RunID != "first-run" || value.Node.Identity == nil || *value.Node.Identity != identity {
					t.Fatal("same-run update lost its node binding")
				}
				return
			}
		}
		t.Fatal("same-run update was not observed on the existing viewer")
	}
	awaitFirstRun(s.SampledAt)
	s.SampledAt = time.Now().UTC()
	setFixture(t, body, s)
	awaitFirstRun(s.SampledAt)
	// A changed source can close the old stream before publishing its
	// replacement. Reconnect and check the entire new public object.
	s.RunID = "replacement-run"
	s.IdentityKey = nil
	s.SampledAt = time.Now().UTC()
	setFixture(t, body, s)
	until := time.Now().Add(6 * time.Second)
	viewer := clients[0]
	for time.Now().Before(until) {
		m, open := readTransitionMessage(t, viewer, until)
		if !open {
			_ = viewer.Close()
			for time.Now().Before(until) {
				conn, resp, err = dialer.Dial(address, http.Header{"Origin": {origin}})
				if err == nil {
					viewer = conn
					clients = append(clients, conn)
					break
				}
				if resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
					t.Fatalf("reconnecting viewer: response=%v error=%v", resp, err)
				}
				if resp.Body != nil {
					resp.Body.Close()
				}
				time.Sleep(20 * time.Millisecond)
			}
			if err != nil {
				t.Fatalf("viewer could not reconnect: %v", err)
			}
			continue
		}
		if string(m["topic"]) != `"mithril"` || string(m["key"]) != `"public"` {
			t.Fatalf("private frame on public route: %v", m)
		}
		var value PublicSnapshot
		if err := json.Unmarshal(m["value"], &value); err != nil {
			t.Fatal(err)
		}
		if value.Node != nil && value.Node.RunID == "replacement-run" {
			if value.Node.Identity != nil || strings.Contains(string(m["value"]), identity) {
				t.Fatal("new run retained old identity")
			}
			return
		}
	}
	t.Fatal("replacement run was not published to a public viewer")
}
