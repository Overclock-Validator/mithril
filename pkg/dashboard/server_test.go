package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func fixtureFeed(t *testing.T) (string, *atomic.Pointer[[]byte]) {
	t.Helper()
	path := socketPath(t)
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	body := new(atomic.Pointer[[]byte])
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/snapshot" || r.Method != "GET" {
			t.Errorf("unexpected collector request: %s %s", r.Method, r.URL)
		}
		b := body.Load()
		if b == nil {
			http.Error(w, "offline", 503)
			return
		}
		w.Write(*b)
	})}
	done := make(chan struct{})
	go func() { defer close(done); server.Serve(listener) }()
	t.Cleanup(func() { server.Close(); listener.Close(); <-done })
	return path, body
}

func setFixture(t *testing.T, body *atomic.Pointer[[]byte], value any) {
	t.Helper()
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	body.Store(&b)
}

func TestCollectorFreshnessRunReplacementAndProjection(t *testing.T) {
	path, body := fixtureFeed(t)
	c := newCollector(path)
	defer c.client.CloseIdleConnections()
	now := time.Now().UTC()
	poll := func(want string) {
		t.Helper()
		c.poll(context.Background(), now)
		if got := statusFields(t, c.latest.Load())["source_state"]; got != want {
			t.Fatalf("source state: got %v, want %s", got, want)
		}
	}
	poll("unavailable")
	s := fullTestSnapshot(now)
	key, genesis := "public-identity", "public-genesis"
	s.IdentityKey, s.GenesisHash = &key, &genesis
	setFixture(t, body, s)
	poll("fresh")
	// Forward only known fields, even if a future node adds private fields.
	b, _ := json.Marshal(s)
	b = append(b[:len(b)-1], []byte(`,"private_key":"secret-test-value","config_path":"/private/test"}`)...)
	body.Store(&b)
	poll("fresh")
	for _, frame := range c.latest.Load().messages {
		if strings.Contains(string(frame), "secret-test-value") || strings.Contains(string(frame), "/private/test") {
			t.Fatal("unrecognized fields forwarded")
		}
	}
	body.Store(nil)
	poll("stale")
	if string(decodedFrames(t, c.latest.Load())["summary.identity_key"]) != `"public-identity"` {
		t.Fatal("last sample was lost")
	}
	older := s
	older.SampledAt = now.Add(-time.Second)
	older.IdentityKey = nil
	setFixture(t, body, older)
	poll("stale")
	if c.last.IdentityKey == nil {
		t.Fatal("older same-run sample replaced newer data")
	}
	s.RunID = "next-run"
	s.Runtime, s.Resources, s.Replay, s.Consensus, s.RPC, s.Recovery = nil, nil, nil, nil, nil, nil
	s.IdentityKey, s.GenesisHash, s.ReplayFrontier = nil, nil, nil
	s.Mode, s.Phase = "validator", "starting"
	setFixture(t, body, s)
	poll("fresh")
	values := decodedFrames(t, c.latest.Load())
	status := statusFields(t, c.latest.Load())
	if string(values["summary.identity_key"]) != "null" || status["genesis_hash"] != nil || status["replay_frontier"] != nil || status["run_id"] != "next-run" {
		t.Fatalf("old run values survived: %v", status)
	}
	for _, group := range []string{"runtime", "resources", "replay", "consensus", "rpc", "recovery"} {
		if string(values["mithril."+group]) != "null" {
			t.Fatalf("old run retained %s", group)
		}
	}
	now = now.Add(StaleAfter + time.Nanosecond)
	poll("stale")
}

func TestCollectorRejectsMalformedOversizedAndFutureSamples(t *testing.T) {
	path, body := fixtureFeed(t)
	now := time.Now().UTC()
	future := testSnapshot(now.Add(2 * time.Second))
	wrongVersion := testSnapshot(now)
	wrongVersion.SchemaVersion++
	futureJSON, _ := json.Marshal(future)
	versionJSON, _ := json.Marshal(wrongVersion)
	for _, b := range [][]byte{[]byte("{"), []byte(`{} {}`), []byte(strings.Repeat("x", MaxPrivateSnapshotBytes+1)), futureJSON, versionJSON} {
		body.Store(&b)
		c := newCollector(path)
		c.poll(context.Background(), now)
		c.client.CloseIdleConnections()
		if c.last != nil || statusFields(t, c.latest.Load())["source_state"] != "unavailable" {
			t.Fatal("invalid sample accepted")
		}
	}
}

func TestCollectorPrivateSizeBoundary(t *testing.T) {
	path, body := fixtureFeed(t)
	now := time.Now().UTC()
	value, err := json.Marshal(testSnapshot(now))
	if err != nil || len(value) >= MaxPrivateSnapshotBytes {
		t.Fatalf("invalid fixture: %d bytes, %v", len(value), err)
	}
	value = append(value, strings.Repeat(" ", MaxPrivateSnapshotBytes-len(value))...)
	for _, tc := range []struct {
		name string
		data []byte
		want string
	}{
		{name: "at limit", data: value, want: "fresh"},
		{name: "over limit", data: append(append([]byte(nil), value...), ' '), want: "unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body.Store(&tc.data)
			c := newCollector(path)
			defer c.client.CloseIdleConnections()
			c.poll(context.Background(), now)
			if got := statusFields(t, c.latest.Load())["source_state"]; got != tc.want {
				t.Fatalf("state %v, want %s", got, tc.want)
			}
		})
	}
}

func TestLoopbackHostAndOriginBoundary(t *testing.T) {
	for _, test := range []struct {
		host, origin string
		allowed      bool
	}{
		{"127.0.0.1:8080", "", true}, {"localhost:8080", "http://localhost:8080", true},
		{"[::1]:8080", "http://[::1]:8080", true}, {"127.0.0.1:8080", "https://127.0.0.1:8080", true},
		{"attacker.example:8080", "http://attacker.example:8080", false},
		{"127.0.0.1:8081", "", false}, {"127.0.0.1", "", false},
		{"127.0.0.1:8080", "https://attacker.example", false}, {"127.0.0.1:8080", "null", false},
		{"127.0.0.1:8080", "http://user@127.0.0.1:8080", false},
		{"127.0.0.1:8080", "http://127.0.0.1:8080/path", false},
	} {
		r := &http.Request{Host: test.host, Header: make(http.Header)}
		if test.origin != "" {
			r.Header.Set("Origin", test.origin)
		}
		if got := allowedRequest(r, "8080"); got != test.allowed {
			t.Errorf("Host %q Origin %q: got %v, want %v", test.host, test.origin, got, test.allowed)
		}
	}
	// A proxy's default upstream Host is the listener itself; any proxy header
	// marks the request as relayed, and the private profile refuses it.
	for _, name := range proxyHeaders {
		r := &http.Request{Host: "127.0.0.1:8080", Header: make(http.Header)}
		r.Header.Set(name, "203.0.113.2")
		if allowedRequest(r, "8080") {
			t.Errorf("proxied request with %s accepted", name)
		}
	}
	r := &http.Request{Host: "127.0.0.1:8080", Header: http.Header{"Origin": {"http://127.0.0.1:8080", "http://127.0.0.1:8080"}}}
	if allowedRequest(r, "8080") {
		t.Fatal("multiple origins accepted")
	}
	for _, address := range []string{"0.0.0.0:0", "[::]:0", ":0", "localhost:0", "203.0.113.2:0"} {
		if Serve(context.Background(), "/unused/node.sock", address) == nil {
			t.Fatalf("non-loopback address accepted: %s", address)
		}
	}
	if Serve(context.Background(), "relative.sock", "127.0.0.1:0") == nil {
		t.Fatal("relative node path accepted")
	}
}

func startViewerServer(t *testing.T, socket string) (string, func()) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, socket, listener) }()
	stop := func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("viewer server failed to stop")
		}
	}
	return "ws://" + listener.Addr().String() + "/websocket", stop
}

func connectViewer(t *testing.T, address string) *websocket.Conn {
	t.Helper()
	dialer := websocket.Dialer{HandshakeTimeout: time.Second}
	conn, _, err := dialer.Dial(address, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func readMessage(t *testing.T, conn *websocket.Conn) map[string]json.RawMessage {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(6 * time.Second))
	_, b, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// A source transition may close the stream before its replacement is ready.
// Retry abnormal closure only; callers still verify the replacement data.
func readTransitionMessage(t *testing.T, conn *websocket.Conn, deadline time.Time) (map[string]json.RawMessage, bool) {
	t.Helper()
	if err := conn.SetReadDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	_, b, err := conn.ReadMessage()
	if websocket.IsCloseError(err, websocket.CloseAbnormalClosure) {
		return nil, false
	}
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	return m, true
}

func awaitStatus(t *testing.T, conn *websocket.Conn, state, run string) {
	t.Helper()
	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		m := readMessage(t, conn)
		if string(m["topic"]) != `"mithril"` || string(m["key"]) != `"status"` {
			continue
		}
		var status map[string]any
		if err := json.Unmarshal(m["value"], &status); err != nil {
			t.Fatal(err)
		}
		if status["source_state"] == state && (run == "" || status["run_id"] == run) {
			return
		}
	}
	t.Fatalf("status %s for run %q not observed", state, run)
}

func TestLiveFeedViewerDisconnectAndNodeRestart(t *testing.T) {
	path := socketPath(t)
	address, stop := startViewerServer(t, path)
	defer stop()
	conn := connectViewer(t, address)
	awaitStatus(t, conn, "unavailable", "")
	started := time.Now().UTC().Add(-time.Hour)
	feed, err := ListenFeed(context.Background(), path, func() Snapshot {
		s := testSnapshot(time.Now().UTC())
		s.StartedAt = started
		return s
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	awaitStatus(t, conn, "fresh", "test-run")
	feed.Close()
	// A source transition can close the old stream before its replacement.
	// Reconnect on closure, but still require the expected run and state.
	awaitTransition := func(state, run string) {
		t.Helper()
		deadline := time.Now().Add(6 * time.Second)
		for time.Now().Before(deadline) {
			m, open := readTransitionMessage(t, conn, deadline)
			if !open {
				_ = conn.Close()
				time.Sleep(20 * time.Millisecond)
				conn = connectViewer(t, address)
				continue
			}
			if string(m["topic"]) != `"mithril"` || string(m["key"]) != `"status"` {
				continue
			}
			var status map[string]any
			if err := json.Unmarshal(m["value"], &status); err != nil {
				t.Fatal(err)
			}
			if status["source_state"] == state && status["run_id"] == run {
				return
			}
		}
		t.Fatalf("status %s for run %q not observed after transition", state, run)
	}
	awaitTransition("stale", "test-run")
	restartedAt := time.Now().UTC()
	feed, err = ListenFeed(context.Background(), path, func() Snapshot {
		s := testSnapshot(time.Now().UTC())
		s.RunID = "restarted-run"
		s.StartedAt = restartedAt
		return s
	})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close()
	awaitTransition("fresh", "restarted-run")
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"topic":"summary","key":"ping","id":18446744073709551615}`)); err != nil {
		t.Fatal(err)
	}
	for {
		m := readMessage(t, conn)
		if string(m["key"]) == `"ping"` {
			if string(m["id"]) != "18446744073709551615" || string(m["value"]) != "null" {
				t.Fatalf("bad ping reply: %v", m)
			}
			break
		}
	}
}

func TestViewerLimitsAndNoAdministrativeRoutes(t *testing.T) {
	address, stop := startViewerServer(t, socketPath(t))
	defer stop()
	for range 16 {
		connectViewer(t, address)
	}
	dialer := websocket.Dialer{HandshakeTimeout: time.Second}
	conn, response, err := dialer.Dial(address, nil)
	if conn != nil {
		conn.Close()
	}
	if err == nil || response == nil || response.StatusCode != 503 {
		t.Fatalf("viewer cap: %v %v", response, err)
	}
	u, _ := url.Parse(address)
	client := &http.Client{Timeout: time.Second}
	for _, path := range []string{"/admin", "/setIdentity", "/metrics", "/debug/pprof/", "/snapshot"} {
		response, err := client.Get("http://" + u.Host + path)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if response.StatusCode != 404 {
			t.Fatalf("unexpected route %s: %d", path, response.StatusCode)
		}
	}
}

func TestViewerRejectsBadRequests(t *testing.T) {
	for name, payload := range map[string]string{
		"method":            `{"topic":"admin","key":"setIdentity","id":1}`,
		"unknown field":     `{"topic":"summary","key":"ping","id":1,"path":"/etc/passwd"}`,
		"trailing document": `{"topic":"summary","key":"ping","id":1} {}`,
		"oversized":         strings.Repeat("x", 1025),
		"missing id":        `{"topic":"summary","key":"ping"}`,
	} {
		t.Run(name, func(t *testing.T) {
			address, stop := startViewerServer(t, socketPath(t))
			defer stop()
			conn := connectViewer(t, address)
			if err := conn.WriteMessage(websocket.TextMessage, []byte(payload)); err != nil {
				t.Fatal(err)
			}
			conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			for {
				_, _, err := conn.ReadMessage()
				if err == nil {
					continue
				}
				if e, ok := err.(net.Error); ok && e.Timeout() {
					t.Fatal("invalid viewer was not disconnected")
				}
				break
			}
		})
	}
}

func TestViewerRateLimitsDataAndControlFrames(t *testing.T) {
	for _, kind := range []int{websocket.TextMessage, websocket.PongMessage} {
		t.Run(map[int]string{websocket.TextMessage: "data", websocket.PongMessage: "control"}[kind], func(t *testing.T) {
			address, stop := startViewerServer(t, socketPath(t))
			defer stop()
			conn := connectViewer(t, address)
			payload := []byte(`{"topic":"summary","key":"ping","id":1}`)
			// Send the whole burst together so later client writes do not race
			// the server's rejection and make TCP close observation flaky.
			frame := append([]byte{byte(0x80 | kind), byte(0x80 | len(payload)), 0, 0, 0, 0}, payload...)
			if err := conn.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			if _, err := conn.UnderlyingConn().Write(bytes.Repeat(frame, 10)); err != nil {
				if e, ok := err.(net.Error); ok && e.Timeout() {
					t.Fatal("flood write timed out")
				}
			}
			conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			for {
				_, _, err := conn.ReadMessage()
				if err == nil {
					continue
				}
				if e, ok := err.(net.Error); ok && e.Timeout() {
					t.Fatal("flooding viewer was not disconnected")
				}
				break
			}
		})
	}
}

type pipeListener struct {
	connections chan net.Conn
	done        chan struct{}
	once        sync.Once
}

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.connections:
		return conn, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error   { l.once.Do(func() { close(l.done) }); return nil }
func (l *pipeListener) Addr() net.Addr { return &net.UnixAddr{Name: "test-pipe", Net: "unix"} }

func TestSlowViewerWriteHasDeadline(t *testing.T) {
	// net.Pipe has no TCP buffering: once the client finishes the handshake,
	// its refusal to read blocks the actual WebSocket writer deterministically.
	c := &collector{}
	messages, err := frames(nil, "unavailable", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	c.latest.Store(&frameSet{messages: messages})
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	listener := &pipeListener{connections: make(chan net.Conn, 1), done: make(chan struct{})}
	listener.connections <- serverConn
	finished := make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		stream(context.Background(), conn, c)
	}))
	server.Listener.Close()
	server.Listener = listener
	server.Start()
	defer server.Close()
	dialer := websocket.Dialer{HandshakeTimeout: time.Second, NetDialContext: func(context.Context, string, string) (net.Conn, error) {
		return clientConn, nil
	}}
	conn, _, err := dialer.Dial("ws://localhost/websocket", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("slow viewer held the writer beyond its deadline")
	}
}
