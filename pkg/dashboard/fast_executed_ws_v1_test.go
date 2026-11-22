package dashboard

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func fastV1Fixture(t *testing.T) (*collector, *fastExecutedPublication) {
	t.Helper()
	now := time.Now()
	value, gate := fastExecutedFixture(now)
	c := newCollector("/unused")
	c.detailGate.Store(&gate)
	frame := &frameSet{freshUntil: now.Add(StaleAfter), clockOrigin: c.clockOrigin, clockUncertain: &c.clockUncertain,
		lease: newStatusFrameLease(now.Add(StaleAfter))}
	frame.fence.nodeEpoch, frame.fence.families[statusReplay] = 1, 1
	c.publishStatusFrame(frame)
	c.fast = newFastExecutedReader(&http.Client{Transport: &http.Transport{}}, &c.detailGate)
	t.Cleanup(c.fast.client.CloseIdleConnections)
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	p := &fastExecutedPublication{value: value, body: body, stamp: gate.stamp,
		freshUntil: now.Add(StaleAfter), invalidated: make(chan struct{})}
	c.fast.latest.Store(p)
	return c, p
}

func fastV1Server(t *testing.T, c *collector) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err == nil {
			streamFastExecutedV1(ctx, conn, c)
		}
	}))
	t.Cleanup(func() { cancel(); server.Close() })
	return "ws" + strings.TrimPrefix(server.URL, "http")
}

func fastV1Connect(t *testing.T, address string) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial(address, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func fastV1Read(t *testing.T, conn *websocket.Conn) map[string]json.RawMessage {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, body, err := conn.ReadMessage()
	var value map[string]json.RawMessage
	if err != nil || json.Unmarshal(body, &value) != nil {
		t.Fatalf("invalid v1 frame: %s (%v)", body, err)
	}
	return value
}

func fastV1Challenge(t *testing.T, conn *websocket.Conn, id string) (time.Time, map[string]json.RawMessage) {
	t.Helper()
	started := time.Now()
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"challenge","id":"`+id+`"}`)); err != nil {
		t.Fatal(err)
	}
	ack := fastV1Read(t, conn)
	if len(ack) != 2 || string(ack["type"]) != `"anchor"` || string(ack["challenge"]) != `"`+id+`"` {
		t.Fatalf("ACK mismatch or ordinary status leaked: %s", ack)
	}
	frame := fastV1Read(t, conn)
	if len(frame) != 6 || string(frame["type"]) != `"frame"` || string(frame["topic"]) != `"mithril"` ||
		string(frame["key"]) != `"recent_executed_v1"` || string(frame["challenge"]) != `"`+id+`"` {
		t.Fatalf("v1 envelope mismatch: %s", frame)
	}
	return started, frame
}

func TestFastExecutedV1AnchorDeliveryAndExpiry(t *testing.T) {
	receipt := time.Now()
	until := receipt.Add(1500 * time.Millisecond)
	ttl := fastExecutedV1TTL(receipt, until)
	if ttl != 1488 || fastExecutedV1TTL(receipt, receipt) != 0 || fastExecutedV1TTL(receipt, receipt.Add(9*time.Millisecond)) != 0 ||
		fastExecutedV1TTL(receipt, receipt.Add(time.Hour)) != 4985 {
		t.Fatalf("TTL cap/precision margin changed: %d", ttl)
	}

	c, p := fastV1Fixture(t)
	conn := fastV1Connect(t, fastV1Server(t, c))
	firstSent, first := fastV1Challenge(t, conn, strings.Repeat("1", 32))
	var n1 int64
	var value FastExecuted
	if json.Unmarshal(first["expires_after_challenge_ms"], &n1) != nil || n1 < 1 || n1 > 5000 ||
		json.Unmarshal(first["value"], &value) != nil || value.Sequence != p.value.Sequence {
		t.Fatalf("invalid cached v1 payload: %s", first)
	}
	// A refreshed anchor retransmits the same publication without renewing its
	// immutable source cap. This also lets a newly ready parent retry admission.
	time.Sleep(25 * time.Millisecond)
	secondSent, second := fastV1Challenge(t, conn, strings.Repeat("2", 32))
	var n2 int64
	_ = json.Unmarshal(second["expires_after_challenge_ms"], &n2)
	if n2 >= n1 || secondSent.Add(time.Duration(n2)*time.Millisecond).After(p.freshUntil) ||
		secondSent.Add(time.Duration(n2)*time.Millisecond).After(firstSent.Add(time.Duration(n1+2)*time.Millisecond)) {
		t.Fatalf("refreshed anchor extended cached source: first=%d second=%d", n1, n2)
	}
	c.fast.replace(nil)
	cleared := fastV1Read(t, conn)
	if string(cleared["value"]) != "null" || string(cleared["expires_after_challenge_ms"]) != "0" || string(cleared["challenge"]) != `"`+strings.Repeat("2", 32)+`"` {
		t.Fatalf("invalidation did not clear active anchor: %s", cleared)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("idle anchor survived its two-second horizon")
	} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("idle anchor was not closed by server")
	}
	// Reconnect owns a new challenge and cannot inherit the prior connection.
	fresh := *p
	fresh.invalidated = make(chan struct{})
	c.fast.replace(&fresh)
	reconnected := fastV1Connect(t, fastV1Server(t, c))
	_, frame := fastV1Challenge(t, reconnected, strings.Repeat("3", 32))
	if string(frame["value"]) == "null" || string(frame["challenge"]) == string(second["challenge"]) {
		t.Fatal("reconnect reused the prior anchor or lost a fresh publication")
	}
}

func TestFastExecutedV1ClosedChallengeAndRate(t *testing.T) {
	valid := `{"type":"challenge","id":"` + strings.Repeat("a", 32) + `"}`
	for _, body := range []string{valid + " ", strings.Replace(valid, "challenge", "other", 1),
		strings.Replace(valid, `"id":`, `"extra":1,"id":`, 1), strings.Replace(valid, "aaaa", "AAAA", 1),
		`{"id":"` + strings.Repeat("a", 32) + `","type":"challenge"}`, strings.Repeat("x", 129)} {
		if _, ok := parseFastExecutedChallenge([]byte(body), time.Now()); ok {
			t.Fatalf("open challenge grammar accepted %q", body)
		}
		c, _ := fastV1Fixture(t)
		conn := fastV1Connect(t, fastV1Server(t, c))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(body))
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		if _, _, err := conn.ReadMessage(); err == nil {
			t.Fatalf("malformed challenge accepted on wire: %q", body)
		}
	}
	c, _ := fastV1Fixture(t)
	conn := fastV1Connect(t, fastV1Server(t, c))
	fastV1Challenge(t, conn, strings.Repeat("1", 32))
	fastV1Challenge(t, conn, strings.Repeat("2", 32))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"challenge","id":"`+strings.Repeat("3", 32)+`"}`))
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("third immediate challenge escaped 2/s burst2")
	}
	conn = fastV1Connect(t, fastV1Server(t, c))
	fastV1Challenge(t, conn, strings.Repeat("4", 32))
	_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"challenge","id":"`+strings.Repeat("4", 32)+`"}`))
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("repeated active challenge was accepted")
	}
}

type fastV1BlockedConn struct {
	net.Conn
	writes  int
	entered chan struct{}
	before  func()
}

func (c *fastV1BlockedConn) Write(body []byte) (int, error) {
	c.writes++
	if c.writes == 2 { // Let the HTTP upgrade finish, then block the payload.
		if c.before != nil {
			c.before()
		}
		close(c.entered)
	}
	return c.Conn.Write(body)
}

func TestFastExecutedV1BlockedInvalidation(t *testing.T) {
	for _, cause := range []string{"source", "status", "rewind", "lease", "registration_gap"} {
		t.Run(cause, func(t *testing.T) {
			if cause == "registration_gap" {
				previous := runtime.GOMAXPROCS(1)
				defer runtime.GOMAXPROCS(previous)
			}
			c, p := fastV1Fixture(t)
			serverConn, clientConn := net.Pipe()
			defer clientConn.Close()
			conn := &fastV1BlockedConn{Conn: serverConn, entered: make(chan struct{})}
			if cause == "registration_gap" {
				// The sole running goroutine enters the real data Write before
				// its newly spawned watcher can register. Status replacement
				// closes the old token; subscribing only to the new one misses it.
				conn.before = func() { c.publishStatusFrame(nil) }
			}
			listener := &pipeListener{connections: make(chan net.Conn, 1), done: make(chan struct{})}
			listener.connections <- conn
			result := make(chan bool, 1)
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ws, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					result <- false
					return
				}
				defer ws.Close()
				_, _, ok := writeFastExecutedV1(context.Background(), ws, c, fastExecutedChallenge{id: strings.Repeat("1", 32), receivedAt: time.Now()}, p)
				result <- ok
			}))
			server.Listener.Close()
			server.Listener = listener
			server.Start()
			defer server.Close()
			dialer := websocket.Dialer{HandshakeTimeout: time.Second, NetDialContext: func(context.Context, string, string) (net.Conn, error) { return clientConn, nil }}
			ws, _, err := dialer.Dial("ws://localhost/websocket", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer ws.Close()
			select {
			case <-conn.entered:
			case <-time.After(time.Second):
				t.Fatal("payload did not block")
			}
			switch cause {
			case "source":
				c.fast.replace(nil)
			case "status":
				c.publishStatusFrame(nil)
			case "rewind":
				gate := *c.detailGate.Load()
				gate.Rewinds++
				gate.stamp.familyEpoch++
				c.detailGate.Store(&gate)
				c.publishStatusFrame(c.latest.Load())
			case "lease":
				deadline := time.Now().Add(40 * time.Millisecond)
				c.latest.Load().lease.tighten(deadline, deadline.Round(0))
			}
			select {
			case ok := <-result:
				if ok {
					t.Fatal("invalidated blocked payload succeeded")
				}
			case <-time.After(300 * time.Millisecond):
				t.Fatal("invalidation did not interrupt blocked writer promptly")
			}
			_ = ws.SetReadDeadline(time.Now().Add(time.Second))
			if _, _, err := ws.ReadMessage(); err == nil {
				t.Fatal("invalidated buffered payload reached viewer")
			}
		})
	}
}

func TestFastExecutedV1PublicRoute(t *testing.T) {
	core := replayDetailLiveCore(0)
	genesis := "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"
	core.Binding.GenesisHash = &genesis
	now := time.Now().UTC()
	value := FastExecuted{SchemaVersion: 1, Family: "recent_executed", Scope: "locally_executed", State: "fresh", Sequence: 1,
		SampledAt: now, Binding: FastExecutedBinding{RunID: core.Binding.RunID, GenesisHash: &genesis,
			NodeStartedAt: core.Binding.NodeStartedAt, Attempt: core.Binding.Attempt, Rewinds: core.Binding.Rewinds,
			AttemptStartedAt: core.Binding.AttemptStartedAt},
		Records: []ExecutedSlot{{At: now, Slot: 3, Bankhash: "11111111111111111111111111111111"}}}
	path := socketPath(t)
	feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot {
		s := replayDetailLiveStatus(core)
		s.Replay.SampledAt = time.Now().UTC()
		return s
	}, DetailSamplers{Replay: func(at time.Time) (ReplayDetailCore, error) {
		copy := core
		copy.CapturedAt = at.UTC()
		return copy, nil
	}, FastExecuted: func() (FastExecuted, bool) { return value, true }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	address, cancel, done := replayDetailPublicServer(t, path, PublicOptions{Resources: true, ReplayDetails: true, FastExecuted: true})
	t.Cleanup(func() { replayDetailStopPublic(t, cancel, done) })
	dialer := websocket.Dialer{HandshakeTimeout: time.Second, NetDialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", strings.TrimPrefix(address, "http://"))
	}}
	for _, query := range []string{fastExecutedV1Query + "&extra=1", "recent_executed=v1&include=advanced_status", strings.Replace(fastExecutedV1Query, "=v1", "=v3", 1)} {
		conn, response, err := dialer.Dial("ws://stats.example/websocket?"+query, http.Header{"Origin": {"https://stats.example"}})
		if conn != nil {
			conn.Close()
		}
		if err == nil || response == nil || response.StatusCode != http.StatusBadRequest {
			t.Fatalf("unsupported query accepted: %s (%v)", query, err)
		}
		response.Body.Close()
	}
	conn, _, err := dialer.Dial("ws://stats.example/websocket?"+fastExecutedV1Query, http.Header{"Origin": {"https://stats.example"}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, frame := fastV1Challenge(t, conn, strings.Repeat("7", 32))
	for string(frame["value"]) == "null" {
		frame = fastV1Read(t, conn)
	}
	var got FastExecuted
	if json.Unmarshal(frame["value"], &got) != nil || got.Sequence != value.Sequence || got.Records[0].Slot != 3 {
		t.Fatalf("cached feed body changed through public route: %s", frame)
	}
	v2, _, err := dialer.Dial("ws://stats.example/websocket?"+fastExecutedV2Query, http.Header{"Origin": {"https://stats.example"}})
	if err != nil {
		t.Fatal(err)
	}
	defer v2.Close()
	id := strings.Repeat("8", 32)
	if err := v2.WriteMessage(websocket.TextMessage, []byte(`{"type":"challenge","id":"`+id+`"}`)); err != nil {
		t.Fatal(err)
	}
	ack := fastV1Read(t, v2)
	if string(ack["challenge"]) != `"`+id+`"` {
		t.Fatal("v2 anchor mismatch")
	}
	frameV2 := fastV1Read(t, v2)
	var responseTTL, sourceTTL int64
	if len(frameV2) != 7 || string(frameV2["key"]) != `"recent_executed_v2"` || json.Unmarshal(frameV2["expires_after_challenge_ms"], &responseTTL) != nil || json.Unmarshal(frameV2["source_expires_after_challenge_ms"], &sourceTTL) != nil || responseTTL < 1 || sourceTTL < responseTTL {
		t.Fatalf("invalid v2 frame: %s", frameV2)
	}
	code, _, _ := replayDetailPublicRequest(t, address, "/api/v1/status?"+fastExecutedV1Query, "stats.example", "https://stats.example")
	if code != http.StatusBadRequest {
		t.Fatalf("v1 query expanded ordinary HTTP grammar: %d", code)
	}
}

func TestFastExecutedV1BufferedArrivalCannotRenew(t *testing.T) {
	c, p := fastV1Fixture(t)
	p.freshUntil = time.Now().Add(150 * time.Millisecond)
	conn := fastV1Connect(t, fastV1Server(t, c))
	sent := time.Now()
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"challenge","id":"`+strings.Repeat("8", 32)+`"}`)); err != nil {
		t.Fatal(err)
	}
	// TCP may buffer a valid write beyond its original lease. The echoed anchor
	// retains send time, so receipt of these bytes cannot grant fresh lifetime.
	time.Sleep(200 * time.Millisecond)
	ack := fastV1Read(t, conn)
	frame := fastV1Read(t, conn)
	var n int64
	if string(ack["type"]) != `"anchor"` || json.Unmarshal(frame["expires_after_challenge_ms"], &n) != nil || n < 1 || n >= 150 || string(frame["value"]) == "null" {
		t.Fatalf("expected originally admitted buffered payload: %s %s", ack, frame)
	}
	deadline := sent.Add(time.Duration(n) * time.Millisecond)
	if deadline.After(p.freshUntil) || deadline.After(time.Now()) {
		t.Fatalf("buffered delivery renewed expiry: ttl=%d deadline=%v cap=%v", n, deadline, p.freshUntil)
	}
}

// fastV1QueuedReadConn signals only after one complete, unfragmented masked
// challenge has been read and the session reader asks for its next message.
// That next read follows the real parser's pending-channel send.
type fastV1QueuedReadConn struct {
	net.Conn
	wantRead  atomic.Int32
	readBytes int // sole reader
	queued    chan struct{}
}

func (c *fastV1QueuedReadConn) Read(p []byte) (int, error) {
	if want := c.wantRead.Load(); want > 0 && c.readBytes >= int(want) {
		c.wantRead.Store(0)
		close(c.queued)
	}
	n, err := c.Conn.Read(p)
	if c.wantRead.Load() > 0 {
		c.readBytes += n
	}
	return n, err
}

func TestFastExecutedV1QueuedChallengeBeforePayload(t *testing.T) {
	c, p := fastV1Fixture(t)
	originalUntil := time.Now().Add(time.Second)
	p.freshUntil = originalUntil
	serverPipe, clientPipe := net.Pipe()
	wire := &fastV1QueuedReadConn{Conn: serverPipe, queued: make(chan struct{})}
	// Existing writer barrier: HTTP upgrade is write 1, initial ACK is write 2.
	// net.Pipe holds that ACK until the client reads it; no session hook is used.
	blocked := &fastV1BlockedConn{Conn: wire, entered: make(chan struct{})}
	listener := &pipeListener{connections: make(chan net.Conn, 1), done: make(chan struct{})}
	listener.connections <- blocked
	ctx, cancel := context.WithCancel(context.Background())
	done, served := make(chan struct{}), make(chan struct{})
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err == nil {
			streamFastExecutedV1(ctx, conn, c)
		}
	})}
	go func() { defer close(served); _ = server.Serve(listener) }()
	t.Cleanup(func() {
		cancel()
		clientPipe.Close()
		blocked.Close()
		server.Close()
		for _, finished := range []<-chan struct{}{done, served} {
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Error("session cleanup did not finish")
			}
		}
	})
	dialer := websocket.Dialer{HandshakeTimeout: time.Second, NetDialContext: func(context.Context, string, string) (net.Conn, error) { return clientPipe, nil }}
	ws, _, err := dialer.Dial("ws://localhost/websocket", nil)
	if err != nil {
		t.Fatal(err)
	}
	first, second := strings.Repeat("1", 32), strings.Repeat("2", 32)
	_ = ws.SetWriteDeadline(time.Now().Add(time.Second))
	if err := ws.WriteMessage(websocket.TextMessage, []byte(`{"type":"challenge","id":"`+first+`"}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("initial ACK did not reach writer barrier")
	}
	body := []byte(`{"type":"challenge","id":"` + second + `"}`)
	if len(body) >= 126 {
		t.Fatal("challenge must use the six-byte masked frame header")
	}
	wire.wantRead.Store(int32(len(body) + 6))
	sent := time.Now()
	if err := ws.WriteMessage(websocket.TextMessage, body); err != nil {
		t.Fatal(err)
	}
	select {
	case <-wire.queued:
	case <-time.After(time.Second):
		t.Fatal("second real challenge did not reach pending queue")
	}
	// Reading ACK 1 releases the actual sole writer. It calls send(true)
	// directly, so an old-anchor payload here cannot be blamed on select order.
	ack := fastV1Read(t, ws)
	if string(ack["type"]) != `"anchor"` || string(ack["challenge"]) != `"`+first+`"` {
		t.Fatal("initial ACK mismatch")
	}
	ack = fastV1Read(t, ws)
	if string(ack["type"]) != `"anchor"` || string(ack["challenge"]) != `"`+second+`"` {
		t.Fatalf("queued challenge lost to an additional old-anchor payload: %s", ack)
	}
	data := fastV1Read(t, ws)
	var ttl int64
	var value FastExecuted
	if string(data["type"]) != `"frame"` || string(data["challenge"]) != `"`+second+`"` ||
		json.Unmarshal(data["expires_after_challenge_ms"], &ttl) != nil || ttl < 1 ||
		json.Unmarshal(data["value"], &value) != nil || value.Sequence != p.value.Sequence ||
		sent.Add(time.Duration(ttl)*time.Millisecond).After(originalUntil) || !p.freshUntil.Equal(originalUntil) {
		t.Fatal("queued challenge changed payload identity or original source expiry")
	}
}

func TestFastExecutedV2EnvelopeSeparatesCeilings(t *testing.T) {
	c := fastExecutedChallenge{id: strings.Repeat("a", 32), receivedAt: time.Now()}
	legacy := fastExecutedV1Envelope(c, 1000, []byte(`{"sequence":"1"}`))
	var old map[string]json.RawMessage
	if json.Unmarshal(legacy, &old) != nil || len(old) != 6 || old["source_expires_after_challenge_ms"] != nil {
		t.Fatal("legacy envelope changed")
	}
	next := fastExecutedV2Envelope(c, 1000, 2000, []byte(`{"sequence":"1"}`))
	var fields map[string]json.RawMessage
	if json.Unmarshal(next, &fields) != nil || len(fields) != 7 || string(fields["key"]) != `"recent_executed_v2"` || string(fields["expires_after_challenge_ms"]) != "1000" || string(fields["source_expires_after_challenge_ms"]) != "2000" {
		t.Fatalf("bad v2 envelope: %s", next)
	}
}

func TestFastExecutedV2WriterSeparatesStatusAndSource(t *testing.T) {
	c, _ := fastV1Fixture(t)
	c.latest.Load().freshUntil = time.Now().Add(2 * time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err == nil {
			streamFastExecutedV2(ctx, conn, c, true)
		}
	}))
	defer func() { cancel(); server.Close() }()
	conn := fastV1Connect(t, "ws"+strings.TrimPrefix(server.URL, "http"))
	id := strings.Repeat("9", 32)
	if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"challenge","id":"`+id+`"}`)); err != nil {
		t.Fatal(err)
	}
	ack := fastV1Read(t, conn)
	if string(ack["challenge"]) != `"`+id+`"` {
		t.Fatal("anchor mismatch")
	}
	frame := fastV1Read(t, conn)
	var status, source int64
	if json.Unmarshal(frame["expires_after_challenge_ms"], &status) != nil || json.Unmarshal(frame["source_expires_after_challenge_ms"], &source) != nil || status < 1 || source < status+500 {
		t.Fatalf("writer did not separate deadlines: %s", frame)
	}
	c.fast.replace(nil)
	cleared := fastV1Read(t, conn)
	if len(cleared) != 7 || string(cleared["key"]) != `"recent_executed_v2"` || string(cleared["value"]) != "null" || string(cleared["expires_after_challenge_ms"]) != "0" || string(cleared["source_expires_after_challenge_ms"]) != "0" {
		t.Fatalf("v2 withdrawal changed: %s", cleared)
	}
}
