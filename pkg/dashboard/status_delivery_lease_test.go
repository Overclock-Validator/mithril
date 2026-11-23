package dashboard

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestCollectorStatusDeliveryDeadline(t *testing.T) {
	for _, headers := range [][]string{nil, {"2000"}} {
		now := time.Now()
		value := statusLeaseFixtureValue{sample: testSnapshot(now.UTC()), headers: headers}
		c, current := statusLeaseFixture(t, value)
		c.poll(context.Background(), now)
		first := c.latest.Load()
		if first.freshUntil.IsZero() || !first.freshUntil.After(time.Now()) {
			t.Fatal("fresh status has no live deadline")
		}
		if headers != nil && first.freshUntil.After(c.statusLeaseEnd) {
			t.Fatal("public deadline exceeds private lease")
		}
		// Even a backwards wall reference and a larger advertised source lease
		// cannot renew the same immutable sample, including for old sources.
		if headers != nil {
			value.headers = []string{"5000"}
			current.Store(&value)
		}
		c.poll(context.Background(), now.Add(-500*time.Millisecond))
		if later := c.latest.Load().freshUntil; later.After(first.freshUntil) {
			t.Fatal("repeated sample extended the original delivery deadline")
		}
		c.poll(context.Background(), now.Add(StaleAfter+time.Second))
		if later := c.latest.Load().freshUntil; !later.IsZero() {
			t.Fatal("stale status retained a fresh deadline")
		}
	}
}

func TestStatusDeliveryClockFailsClosed(t *testing.T) {
	now := time.Now()
	var uncertain atomic.Bool
	f := &frameSet{freshUntil: now.Add(time.Second), clockOrigin: now.UTC(), clockUncertain: &uncertain}
	if _, ok := f.remainingStatus(0, now); ok || !uncertain.Load() {
		t.Fatal("missing monotonic clock did not fail closed")
	}
	f.clockOrigin = now
	if _, ok := f.remainingStatus(0, now); ok {
		t.Fatal("detected clock discontinuity was not retained")
	}
	uncertain.Store(false)
	if _, ok := f.remainingStatus(0, now.Add(2*time.Second)); ok {
		t.Fatal("expired deadline was accepted")
	}
	f.safetyUnavailable = true
	if remaining, ok := f.remainingStatus(3, now.Add(2*time.Second)); !ok || remaining != 0 {
		t.Fatal("unavailable safety profile inherited the default profile's lease")
	}
}

func TestStatusWriteDeadlineRespectsLease(t *testing.T) {
	now := time.Now()
	var uncertain atomic.Bool
	f := &frameSet{freshUntil: now.Add(300 * time.Millisecond), clockOrigin: now, clockUncertain: &uncertain}
	deadline, ok := f.statusWriteDeadline(0, now)
	if !ok || !deadline.Equal(f.freshUntil) {
		t.Fatal("write deadline exceeded the remaining status lease")
	}
	f.freshUntil = now.Add(3 * time.Second)
	deadline, ok = f.statusWriteDeadline(0, now)
	if !ok || !deadline.Equal(now.Add(2*time.Second)) {
		t.Fatal("ordinary write timeout changed")
	}
	f.freshUntil = now.Add(-time.Second)
	if _, ok := f.statusWriteDeadline(0, now); ok {
		t.Fatal("expired status allowed a write")
	}
}

type delayedStatusConn struct {
	net.Conn
	writes  int
	entered chan struct{}
}

func (c *delayedStatusConn) Write(b []byte) (int, error) {
	c.writes++
	if c.writes == 2 { // The first write is the WebSocket handshake.
		close(c.entered)
		time.Sleep(500 * time.Millisecond)
	}
	return c.Conn.Write(b)
}

type delayedStatusResponse struct {
	http.ResponseWriter
	entered chan struct{}
}

func (w delayedStatusResponse) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, buffered, err := w.ResponseWriter.(http.Hijacker).Hijack()
	if err != nil {
		return nil, nil, err
	}
	return &delayedStatusConn{Conn: conn, entered: w.entered}, buffered, nil
}

func TestStatusWebSocketBlockedWriteExpires(t *testing.T) {
	c := newCollector("/unused")
	c.public = true
	now := time.Now()
	s := testSnapshot(now.UTC())
	f, err := c.encode(&s, "fresh", now)
	if err != nil {
		t.Fatal(err)
	}
	f.clockOrigin, f.clockUncertain = c.clockOrigin, &c.clockUncertain
	entered := make(chan struct{})
	start := make(chan struct{})
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(delayedStatusResponse{ResponseWriter: w, entered: entered}, r, nil)
		if err == nil {
			select {
			case <-start:
				stream(ctx, conn, c)
			case <-ctx.Done():
				conn.Close()
			}
		}
	}))
	t.Cleanup(server.Close)
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	f.freshUntil = time.Now().Add(300 * time.Millisecond)
	c.latest.Store(f)
	close(start)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("fixture did not start the blocked data write")
	}
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("blocked status write succeeded after its lease expired")
	}
}

type blockedReplyConn struct {
	net.Conn
	block     *atomic.Bool
	entered   chan struct{}
	delay     time.Duration
	deadlines chan time.Time
}

func (c *blockedReplyConn) SetWriteDeadline(deadline time.Time) error {
	if c.block.Load() && c.deadlines != nil {
		select {
		case c.deadlines <- deadline:
		default:
		}
	}
	return c.Conn.SetWriteDeadline(deadline)
}

func (c *blockedReplyConn) Write(b []byte) (int, error) {
	if c.block.CompareAndSwap(true, false) {
		close(c.entered)
		time.Sleep(c.delay)
	}
	return c.Conn.Write(b)
}

type blockedReplyResponse struct {
	http.ResponseWriter
	block     *atomic.Bool
	entered   chan struct{}
	delay     time.Duration
	deadlines chan time.Time
}

func (w blockedReplyResponse) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, buffered, err := w.ResponseWriter.(http.Hijacker).Hijack()
	if err != nil {
		return nil, nil, err
	}
	return &blockedReplyConn{Conn: conn, block: w.block, entered: w.entered, delay: w.delay, deadlines: w.deadlines}, buffered, nil
}

func TestStatusHTTPRemainingLease(t *testing.T) {
	value := statusLeaseFixtureValue{sample: testSnapshot(time.Now().UTC()), headers: []string{"1500"}}
	_, current, path := statusLeaseFixturePath(t, value)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	value.sample = testSnapshot(time.Now().UTC())
	current.Store(&value)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveProfile(ctx, path, listener, "https://stats.example", PublicOptions{}) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	client := &http.Client{Timeout: time.Second}
	request := func() (*http.Response, string) {
		t.Helper()
		r, err := http.NewRequest(http.MethodGet, "http://"+listener.Addr().String()+"/api/v1/status", nil)
		if err != nil {
			t.Fatal(err)
		}
		r.Host = "stats.example"
		res, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		var body struct {
			SourceState string `json:"source_state"`
		}
		if res.StatusCode == http.StatusOK {
			if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
		}
		return res, body.SourceState
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		res, state := request()
		if res.StatusCode == http.StatusOK && state == "fresh" {
			remaining, present, valid := statusLease(res.Header)
			if !present || !valid || remaining <= 0 || remaining > 1500*time.Millisecond {
				t.Fatalf("incorrect public lease: %v, %v, %v", remaining, present, valid)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("fresh status not published")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(1600 * time.Millisecond)
	res, state := request()
	if res.StatusCode == http.StatusOK && state == "fresh" {
		t.Fatal("expired status was served as fresh")
	}
	if res.Header.Get(statusRemainingHeader) != "" {
		t.Fatal("stale or unavailable status carried a fresh lease")
	}
}

func TestStatusWebSocketDeliveryExpiry(t *testing.T) {
	for _, public := range []bool{false, true} {
		name := "private"
		if public {
			name = "public"
		}
		t.Run(name, func(t *testing.T) {
			c := newCollector("/unused")
			c.public = public
			now := time.Now()
			s := testSnapshot(now.UTC())
			f, err := c.encode(&s, "fresh", now)
			if err != nil {
				t.Fatal(err)
			}
			f.clockOrigin, f.clockUncertain = c.clockOrigin, &c.clockUncertain
			start := make(chan struct{})
			upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := upgrader.Upgrade(w, r, nil)
				if err == nil {
					select {
					case <-start:
						stream(ctx, conn, c)
					case <-ctx.Done():
						conn.Close()
					}
				}
			}))
			t.Cleanup(server.Close)
			url := "ws" + strings.TrimPrefix(server.URL, "http")
			conn, _, err := websocket.DefaultDialer.Dial(url, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			f.freshUntil = time.Now().Add(300 * time.Millisecond)
			c.latest.Store(f)
			close(start)
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			if _, _, err := conn.ReadMessage(); err != nil {
				t.Fatalf("fresh frame missing: %v", err)
			}
			for {
				_, _, err := conn.ReadMessage()
				if err != nil {
					if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
						t.Fatal("connection remained open after expiry")
					}
					break
				}
			}
			late, _, err := websocket.DefaultDialer.Dial(url, nil)
			if err != nil {
				return
			}
			defer late.Close()
			_ = late.SetReadDeadline(time.Now().Add(time.Second))
			if _, _, err := late.ReadMessage(); err == nil {
				t.Fatal("new connection received an expired frame")
			}
		})
	}
}
