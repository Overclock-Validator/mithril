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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestPublicOriginValidation(t *testing.T) {
	for _, origin := range []string{"", "http://example.com", "https://", "https://user@example.com", "https://example.com/", "https://example.com/path", "https://example.com?", "https://example.com#", "https://example.com/#x", "https://example.com:", "https://example.com:0", "https://example.com:65536", "https://example.com:abc"} {
		if err := ServePublic(context.Background(), "/unused.sock", "127.0.0.1:0", origin); err == nil {
			t.Fatalf("accepted origin %q", origin)
		}
	}
	if err := ServePublic(context.Background(), "/unused.sock", "0.0.0.0:0", "https://example.com"); err == nil {
		t.Fatal("public profile accepted non-loopback listener")
	}
}

func TestPublicViewersShareCollector(t *testing.T) {
	path := socketPath(t)
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	var polls atomic.Int64
	snapshot, err := json.Marshal(testSnapshot(time.Now().UTC()))
	if err != nil {
		t.Fatal(err)
	}
	feed := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		polls.Add(1)
		_, _ = w.Write(snapshot)
	})}
	go feed.Serve(listener)
	defer feed.Close()
	viewer, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveProfile(ctx, path, viewer, "https://stats.example", PublicOptions{}) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for polls.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if polls.Load() == 0 {
		t.Fatal("collector did not poll")
	}
	before, started := polls.Load(), time.Now()
	var group sync.WaitGroup
	for i := 0; i < 16; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			r, err := http.NewRequest("GET", "http://"+viewer.Addr().String()+"/api/v1/status", nil)
			if err != nil {
				t.Error(err)
				return
			}
			r.Host = "stats.example"
			response, err := (&http.Client{Timeout: 3 * time.Second}).Do(r)
			if err != nil {
				t.Error(err)
				return
			}
			if response.StatusCode != http.StatusOK {
				t.Errorf("status: %d", response.StatusCode)
			}
			io.Copy(io.Discard, response.Body)
			response.Body.Close()
		}()
	}
	group.Wait()
	if delta := polls.Load() - before; delta > int64(time.Since(started)/SampleInterval)+2 {
		t.Fatalf("viewer requests triggered source polls: %d extra", delta)
	}
}

func TestPublicHTTPAndWebSocket(t *testing.T) {
	for _, resources := range []bool{false, true} {
		name := "default"
		if resources {
			name = "resources"
		}
		t.Run(name, func(t *testing.T) { testPublicHTTPAndWebSocket(t, resources) })
	}
}

func testPublicHTTPAndWebSocket(t *testing.T, resources bool) {
	path, body := fixtureFeed(t)
	s := fullTestSnapshot(time.Now().UTC())
	s.Resources = resourceFixture(s.SampledAt).Resources
	s.RPCConfigured, s.RPC = true, rpcFixture(s.SampledAt)
	setFixture(t, body, s)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- serveProfile(ctx, path, listener, "https://stats.example", PublicOptions{Resources: resources})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(5 * time.Second):
			t.Error("public server failed to stop")
		}
	})
	base := "http://" + listener.Addr().String()
	client := &http.Client{Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	request := func(method, path, host, origin, data string) *http.Response {
		t.Helper()
		r, err := http.NewRequest(method, base+path, strings.NewReader(data))
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
		return response
	}
	for _, tc := range []struct {
		method, path, host, origin, body string
		status                           int
	}{
		{"GET", "/api/v1/features", "stats.example", "", "", 200},
		{"GET", "/api/v1/cluster", "stats.example", "", "", 404},
		{"GET", "/api/v1/cluster-map", "stats.example", "", "", 404},
		{"GET", "/api/v1/cluster-versions", "stats.example", "", "", 404},
		{"GET", "/api/v1/cluster-performance-history", "stats.example", "", "", 404},
		{"GET", "/api/v1/node-display", "stats.example", "", "", 404},
		{"GET", "/api/v1/details/network", "stats.example", "", "", 404},
		{"GET", "/api/v1/status", "stats.example", "", "", 200},
		{"GET", "/api/v1/status", "stats.example", "https://stats.example", "", 200},
		{"GET", "/api/v1/status", "other.example", "", "", 403},
		{"GET", "/api/v1/status", "stats.example", "null", "", 403},
		{"GET", "/api/v1/status", "stats.example", "http://stats.example", "", 403},
		{"GET", "/api/v1/status?x=1", "stats.example", "", "", 400},
		{"GET", "/api/v1/status?", "stats.example", "", "", 400},
		{"GET", "/api/v1/status", "stats.example", "", "body", 400},
		{"POST", "/api/v1/status", "stats.example", "", "", 400},
		{"GET", "/", "stats.example", "", "", 404},
		{"GET", "/index.html", "stats.example", "", "", 404},
		{"GET", "/dashboard.css", "stats.example", "", "", 404},
		{"GET", "/dashboard.mjs", "stats.example", "", "", 404},
		{"GET", "/model.mjs", "stats.example", "", "", 404},
		{"GET", "/map.mjs", "stats.example", "", "", 404},
		{"GET", "/snapshot", "stats.example", "", "", 404},
		{"GET", "/debug/pprof/", "stats.example", "", "", 404},
	} {
		response := request(tc.method, tc.path, tc.host, tc.origin, tc.body)
		if response.StatusCode != tc.status {
			t.Errorf("%s %s: got %d, want %d", tc.method, tc.path, response.StatusCode, tc.status)
		}
		if response.Header.Get("Cache-Control") != "no-store" || response.Header.Get("X-Content-Type-Options") != "nosniff" || response.Header.Get("Access-Control-Allow-Origin") != "" {
			t.Error("incorrect public response headers")
		}
		io.Copy(io.Discard, response.Body)
		response.Body.Close()
	}
	featuresResponse := request("GET", "/api/v1/features", "stats.example", "", "")
	var features struct {
		Cluster        bool `json:"cluster"`
		NodeDisplay    bool `json:"node_display"`
		NetworkDetails bool `json:"network_details"`
	}
	err = json.NewDecoder(featuresResponse.Body).Decode(&features)
	featuresResponse.Body.Close()
	if err != nil || features.Cluster || features.NodeDisplay || features.NetworkDetails {
		t.Fatalf("deferred features advertised: %+v, %v", features, err)
	}
	dialer := websocket.Dialer{HandshakeTimeout: time.Second, NetDialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	}}
	conn, _, err := dialer.Dial("ws://stats.example/websocket", http.Header{"Origin": {"https://stats.example"}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	m := readMessage(t, conn)
	if string(m["topic"]) != `"mithril"` || string(m["key"]) != `"public"` || !json.Valid(m["value"]) {
		t.Fatalf("unexpected public frame: %s", m)
	}
	_, response, err := dialer.Dial("ws://stats.example/websocket", http.Header{"Origin": {"https://evil.example"}})
	if err == nil || response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("cross-origin websocket accepted: %v %v", response, err)
	}
	_, response, err = dialer.Dial("ws://stats.example/websocket", nil)
	if err == nil || response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("originless websocket accepted: %v %v", response, err)
	}
	_, response, err = dialer.Dial("ws://stats.example/websocket", http.Header{"Origin": {"https://stats.example", "https://stats.example"}})
	if err == nil || response == nil || response.StatusCode != http.StatusForbidden {
		t.Fatalf("multiple websocket origins accepted: %v %v", response, err)
	}
	awaitState := func(want string) {
		t.Helper()
		deadline := time.Now().Add(4 * time.Second)
		for time.Now().Before(deadline) {
			response := request("GET", "/api/v1/status", "stats.example", "", "")
			var value struct {
				SourceState string `json:"source_state"`
			}
			err := json.NewDecoder(response.Body).Decode(&value)
			response.Body.Close()
			if err == nil && value.SourceState == want {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("public state %q not observed", want)
	}
	awaitState("fresh")
	checkResources := func(p PublicSnapshot) {
		t.Helper()
		if p.RPC == nil || p.RPC.Activity == nil || p.RPC.Activity.Completed != 3 || p.RPC.Activity.Errors != 1 || p.RPC.Activity.Active != 3 {
			t.Fatalf("RPC activity changed in transport: %+v", p.RPC)
		}
		if (p.Resources != nil) != resources {
			t.Fatalf("incorrect resource opt-in: %+v", p.Resources)
		}
		if resources && (p.Resources.State != "fresh" || p.Resources.RSSBytes == nil || *p.Resources.RSSBytes != *s.Resources.RSSBytes) {
			t.Fatalf("resource transport changed the source: %+v", p.Resources)
		}
	}
	r := request("GET", "/api/v1/status", "stats.example", "", "")
	var p PublicSnapshot
	err = json.NewDecoder(r.Body).Decode(&p)
	r.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	checkResources(p)
	for {
		m = readMessage(t, conn)
		if err := json.Unmarshal(m["value"], &p); err != nil {
			t.Fatal(err)
		}
		if p.SourceState == "fresh" {
			checkResources(p)
			break
		}
	}
	body.Store(nil)
	awaitState("stale")
}

func TestPublicHTTPSProxy(t *testing.T) {
	path, body := fixtureFeed(t)
	s := testSnapshot(time.Now().UTC())
	setFixture(t, body, s)
	viewer, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	upstream, _ := url.Parse("http://" + viewer.Addr().String())
	reverse := httputil.NewSingleHostReverseProxy(upstream)
	proxy := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/status" && r.URL.Path != "/websocket" {
			http.NotFound(w, r)
			return
		}
		reverse.ServeHTTP(w, r)
	}))
	origin := "https://" + proxy.Listener.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveProfile(ctx, path, viewer, origin, PublicOptions{}) }()
	proxy.StartTLS()
	defer func() {
		proxy.Close()
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	client := proxy.Client()
	client.Timeout = 2 * time.Second
	getState := func() PublicSnapshot {
		t.Helper()
		resp, err := client.Get(origin + "/api/v1/status")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK || resp.Header.Get("Cache-Control") != "no-store" {
			t.Fatalf("proxy status: %d, %v", resp.StatusCode, resp.Header)
		}
		var result PublicSnapshot
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	await := func(state, run string) {
		t.Helper()
		deadline := time.Now().Add(4 * time.Second)
		for time.Now().Before(deadline) {
			v := getState()
			if v.SourceState == state && (run == "" || v.Node != nil && v.Node.RunID == run) {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("proxy did not observe %s for run %s", state, run)
	}
	await("fresh", s.RunID)
	for _, route := range []string{"/snapshot", "/debug/pprof/", "/admin"} {
		resp, err := client.Get(origin + route)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("private route %s: %d", route, resp.StatusCode)
		}
	}
	dialer := websocket.Dialer{HandshakeTimeout: time.Second, TLSClientConfig: client.Transport.(*http.Transport).TLSClientConfig}
	ws := "wss://" + proxy.Listener.Addr().String() + "/websocket"
	conn, _, err := dialer.Dial(ws, http.Header{"Origin": {origin}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	m := readMessage(t, conn)
	if string(m["topic"]) != `"mithril"` || string(m["key"]) != `"public"` {
		t.Fatalf("bad proxy frame: %v", m)
	}
	_, resp, err := dialer.Dial(ws, nil)
	if err == nil || resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("originless proxy websocket: %v %v", resp, err)
	}
	body.Store(nil)
	await("stale", s.RunID)
	s.RunID = "new-run"
	s.SampledAt = time.Now().UTC()
	setFixture(t, body, s)
	await("fresh", s.RunID)
}

func TestPublicSlowViewerWriteDeadline(t *testing.T) {
	c := &collector{public: true}
	initial, err := c.encode(nil, "unavailable", time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	c.latest.Store(initial)
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()
	listener := &pipeListener{connections: make(chan net.Conn, 1), done: make(chan struct{})}
	listener.connections <- serverConn
	finished := make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(finished)
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
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
	dialer := websocket.Dialer{HandshakeTimeout: time.Second, NetDialContext: func(context.Context, string, string) (net.Conn, error) { return clientConn, nil }}
	conn, _, err := dialer.Dial("ws://localhost/websocket", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("slow public viewer held writer beyond deadline")
	}
}
