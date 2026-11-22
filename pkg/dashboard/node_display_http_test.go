package dashboard

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard/cluster"
)

func nodeDisplayHTTPServer(t *testing.T, socket string, options PublicOptions) func(method, target, host, origin, body string) (int, http.Header, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveProfile(ctx, socket, listener, "https://stats.example", options) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(3 * time.Second):
			t.Error("node-display public server did not join")
		}
	})
	client := &http.Client{Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	t.Cleanup(client.CloseIdleConnections)
	return func(method, target, host, origin, body string) (int, http.Header, string) {
		t.Helper()
		r, err := http.NewRequest(method, "http://"+listener.Addr().String()+target, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		r.Host = host
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		r.Header.Set("X-Forwarded-Host", "stats.example")
		response, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		b, readErr := io.ReadAll(io.LimitReader(response.Body, nodeDisplayBytes+1))
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil || len(b) > nodeDisplayBytes {
			t.Fatalf("bounded response: bytes=%d read=%v close=%v", len(b), readErr, closeErr)
		}
		if response.Header.Get("Cache-Control") != "no-store" || response.Header.Get("X-Content-Type-Options") != "nosniff" || response.Header.Get("Access-Control-Allow-Origin") != "" {
			t.Fatal("node-display response bypassed public header policy")
		}
		return response.StatusCode, response.Header, string(b)
	}
}

func TestNodeDisplayHTTPRealFeedProfiles(t *testing.T) {
	for _, role := range []string{"validator", "verifying"} {
		t.Run(role, func(t *testing.T) {
			at := time.Now().UTC()
			fixture, view := nodeDisplayFixture(t, at)
			// Only the node identity and completed epoch are needed. In particular,
			// verifying mode has no voting, pipeline or validator-only groups.
			s := testSnapshot(at)
			s.Mode, s.IdentityKey, s.GenesisHash = role, fixture.IdentityKey, fixture.GenesisHash
			s.Replay = &Replay{SampledAt: at, Completed: fixture.Replay.Completed}
			path := socketPath(t)
			feed, err := ListenFeed(context.Background(), path, func() Snapshot { return s })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(feed.Close)
			cache := filepath.Join(t.TempDir(), "cluster.json")
			if err := cluster.WriteFile(cache, clusterTestSnapshot(at, 3)); err != nil {
				t.Fatal(err)
			}
			request := nodeDisplayHTTPServer(t, path, PublicOptions{ClusterFile: cache})
			want, err := encodeNodeDisplay(selectNodeDisplay(&s, "fresh", view, at))
			if err != nil || nodeDisplayTestValue(t, want.body).State != "fresh" {
				t.Fatalf("invalid expected fixture: %v", err)
			}
			deadline := time.Now().Add(4 * time.Second)
			for {
				code, header, body := request("GET", nodeDisplayPublicPath, "stats.example", "https://stats.example", "")
				if code != http.StatusOK {
					t.Fatalf("display: %d %s", code, body)
				}
				if body == want.body {
					remaining, err := strconv.Atoi(header.Get(replayDetailRemainingHeader))
					if err != nil || remaining <= 0 || remaining > 5000 || header.Get("Content-Type") != "application/json" {
						t.Fatalf("invalid original lease/content type: %v", header)
					}
					t.Logf("actual Feed %s node-display fixture: %s", role, body)
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("real Feed/reader did not publish exact directory selection: %s", body)
				}
				time.Sleep(250 * time.Millisecond)
			}
			// A failed actual Unix collection must remove the label on the next
			// existing collector tick, not retain a still-fresh external name.
			feed.Close()
			deadline = time.Now().Add(3 * time.Second)
			for {
				code, header, body := request("GET", nodeDisplayPublicPath, "stats.example", "", "")
				if code != http.StatusOK {
					t.Fatalf("source-loss response: %d %s", code, body)
				}
				if body == nodeDisplayUnavailableBody {
					if header.Get(replayDetailRemainingHeader) != "" {
						t.Fatal("failed collection retained a live lease")
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("failed collection retained old label: %s", body)
				}
				time.Sleep(250 * time.Millisecond)
			}
		})
	}
}

func TestNodeDisplayHTTPOptInAndGrammar(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(strconv.FormatBool(enabled), func(t *testing.T) {
			options := PublicOptions{}
			if enabled {
				options.ClusterFile = filepath.Join(t.TempDir(), "missing.json")
			}
			request := nodeDisplayHTTPServer(t, socketPath(t), options)
			code, _, body := request("GET", "/api/v1/features", "stats.example", "", "")
			var features map[string]json.RawMessage
			if code != http.StatusOK || json.Unmarshal([]byte(body), &features) != nil {
				t.Fatalf("features: %d %s", code, body)
			}
			if features["node_display"] != nil {
				t.Fatalf("legacy features included node-display: %s", body)
			}
			code, _, body = request("GET", "/api/v1/features?"+advancedProfileQuery, "stats.example", "", "")
			features = nil
			if code != http.StatusOK || json.Unmarshal([]byte(body), &features) != nil {
				t.Fatalf("advanced features: %d %s", code, body)
			}
			if enabled && string(features["node_display"]) != "true" || !enabled && features["node_display"] != nil {
				t.Fatalf("advanced node-display opt-in: %s", body)
			}
			code, header, body := request("GET", nodeDisplayPublicPath, "stats.example", "", "")
			if !enabled {
				if code != http.StatusNotFound {
					t.Fatalf("default route exposed: %d %s", code, body)
				}
				return
			}
			if code != http.StatusOK || body != nodeDisplayUnavailableBody || header.Get(replayDetailRemainingHeader) != "" {
				t.Fatalf("missing source was not fixed unavailable: %d %s", code, body)
			}
			for _, tc := range []struct {
				name, method, target, host, origin, body string
				status                                   int
			}{
				{"host", "GET", nodeDisplayPublicPath, "other.example", "", "", 403},
				{"opaque origin", "GET", nodeDisplayPublicPath, "stats.example", "null", "", 403},
				{"wrong origin", "GET", nodeDisplayPublicPath, "stats.example", "http://stats.example", "", 403},
				{"method", "POST", nodeDisplayPublicPath, "stats.example", "", "", 400},
				{"head", "HEAD", nodeDisplayPublicPath, "stats.example", "", "", 400},
				{"query", "GET", nodeDisplayPublicPath + "?x=1", "stats.example", "", "", 400},
				{"empty query", "GET", nodeDisplayPublicPath + "?", "stats.example", "", "", 400},
				{"body", "GET", nodeDisplayPublicPath, "stats.example", "", "body", 400},
				{"escaped name", "GET", "/api/v1/%6eode-display", "stats.example", "", "", 400},
				{"double slash", "GET", "/api/v1//node-display", "stats.example", "", "", 400},
				{"dot path", "GET", "/api/v1/unused/../node-display", "stats.example", "", "", 400},
				{"trailing slash", "GET", nodeDisplayPublicPath + "/", "stats.example", "", "", 400},
			} {
				t.Run(tc.name, func(t *testing.T) {
					code, header, body := request(tc.method, tc.target, tc.host, tc.origin, tc.body)
					if code != tc.status || header.Get(replayDetailRemainingHeader) != "" || header.Get("Location") != "" {
						t.Fatalf("closed request grammar: %d %v %s", code, header, body)
					}
				})
			}
		})
	}
}

func TestNodeDisplayCollectorCachedReadsAndReplacement(t *testing.T) {
	at := time.Now().UTC()
	fixture, _ := nodeDisplayFixture(t, at)
	s := testSnapshot(at)
	s.IdentityKey, s.GenesisHash = fixture.IdentityKey, fixture.GenesisHash
	s.Replay = &Replay{SampledAt: at, Completed: fixture.Replay.Completed}
	path := socketPath(t)
	var samples atomic.Uint64
	feed, err := ListenFeed(context.Background(), path, func() Snapshot { samples.Add(1); return s })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	cache := filepath.Join(t.TempDir(), "cluster.json")
	if err := cluster.WriteFile(cache, clusterTestSnapshot(at, 3)); err != nil {
		t.Fatal(err)
	}
	external := testClusterReader(nil)
	external.poll(cache, time.Now())
	c := newCollector(path)
	c.public, c.external, c.display = true, external, &nodeDisplayOwner{}
	t.Cleanup(c.client.CloseIdleConnections)
	transport := c.client.Transport.(*http.Transport)
	dial := transport.DialContext
	var dials atomic.Uint64
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		dials.Add(1)
		return dial(ctx, network, address)
	}
	c.poll(context.Background(), time.Now().UTC())
	frame := c.display.selectFrame(time.Now())
	if frame.body == "" || nodeDisplayTestValue(t, frame.body).State != "fresh" {
		t.Fatal("collector did not prepare real Unix/file selection")
	}
	c.display.admit = func(frame nodeDisplayFrame) (time.Duration, bool) { return c.admitNodeDisplay(c.display, frame) }
	handler := c.display.handler(func(*http.Request) bool { return true })
	request := func() *httptest.ResponseRecorder {
		t.Helper()
		w := httptest.NewRecorder()
		handler(w, httptest.NewRequest("GET", "http://local"+nodeDisplayPublicPath, nil))
		return w
	}
	feed.Close()
	if err := os.Remove(cache); err != nil {
		t.Fatal(err)
	}
	beforeDials, beforeSamples := dials.Load(), samples.Load()
	beforeView, beforeStatus := external.latest.Load(), c.latest.Load()
	previousRemaining := int64(5001)
	for range 8 {
		w := request()
		remaining, err := strconv.ParseInt(w.Header().Get(replayDetailRemainingHeader), 10, 64)
		if w.Code != http.StatusOK || w.Body.String() != frame.body || err != nil || remaining <= 0 || remaining > previousRemaining {
			t.Fatalf("cached reply changed/renewed: %d %v %s", w.Code, w.Header(), w.Body.String())
		}
		previousRemaining = remaining
	}
	if dials.Load() != beforeDials || samples.Load() != beforeSamples || external.latest.Load() != beforeView || c.latest.Load() != beforeStatus || c.display.selectFrame(time.Now()) != frame {
		t.Fatal("viewer performed source work or updated a cached publication")
	}
	c.poll(context.Background(), time.Now().UTC())
	if w := request(); w.Body.String() != nodeDisplayUnavailableBody || w.Header().Get(replayDetailRemainingHeader) != "" {
		t.Fatal("failed collection retained label or lease")
	}
	next := s
	next.RunID = "replacement"
	identity := clusterTestSnapshot(at, 3).Validators[1].Identity
	next.IdentityKey = &identity
	replacement, err := ListenFeed(context.Background(), path, func() Snapshot { return next })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(replacement.Close)
	c.poll(context.Background(), time.Now().UTC())
	w := request()
	v := nodeDisplayTestValue(t, w.Body.String())
	if w.Code != http.StatusOK || v.RunID != next.RunID || v.Identity == nil || *v.Identity != identity || v.Name == nil || *v.Name != "Node 001" || c.display.admitFrame(frame, time.Now()) != nodeDisplayUnavailableBody {
		t.Fatalf("replacement reused old binding/label: %s", w.Body.String())
	}
}

func TestNodeDisplayHTTPOriginalExpiryConflictAndRate(t *testing.T) {
	for _, state := range []string{"fresh", "conflict", "expired"} {
		t.Run(state, func(t *testing.T) {
			raw := time.Now()
			if state == "expired" {
				raw = raw.Add(-StaleAfter - time.Second)
			}
			s, view := nodeDisplayFixture(t, raw.UTC())
			if state == "conflict" {
				view.rows[1].Identity, view.rows[1].Name = *s.IdentityKey, "Conflicting label"
			}
			var owner nodeDisplayOwner
			frame := publishNodeDisplayTest(t, &owner, &s, view, raw.UTC(), raw)
			owner.admit = func(frame nodeDisplayFrame) (time.Duration, bool) { return time.Until(frame.deadline), true }
			handler := owner.handler(func(*http.Request) bool { return true })
			w := httptest.NewRecorder()
			handler(w, httptest.NewRequest("GET", "http://local"+nodeDisplayPublicPath, nil))
			if w.Code != http.StatusOK || w.Header().Get("Content-Type") != "application/json" {
				t.Fatalf("valid state status/content type: %d %v", w.Code, w.Header())
			}
			if state == "expired" {
				if w.Body.String() != nodeDisplayUnavailableBody || w.Header().Get(replayDetailRemainingHeader) != "" {
					t.Fatal("original lease expired but old label/context remained")
				}
			} else if v := nodeDisplayTestValue(t, w.Body.String()); w.Body.String() != frame.body || v.State != state || w.Header().Get(replayDetailRemainingHeader) == "" || state == "conflict" && v.Name != nil {
				t.Fatalf("prepared state altered by handler: %s", w.Body.String())
			}
		})
	}
	var owner nodeDisplayOwner
	handler := owner.handler(func(*http.Request) bool { return true })
	started, accepted, refused := time.Now(), 0, 0
	for range 64 {
		w := httptest.NewRecorder()
		handler(w, httptest.NewRequest("GET", "http://local"+nodeDisplayPublicPath, nil))
		switch w.Code {
		case http.StatusOK:
			accepted++
		case http.StatusTooManyRequests:
			refused++
		default:
			t.Fatalf("unexpected rate response: %d", w.Code)
		}
	}
	if accepted < 32 || accepted > 32+int(time.Since(started)/(time.Second/16)) || refused == 0 {
		t.Fatalf("16/s burst32 route admission: accepted=%d refused=%d", accepted, refused)
	}
}
