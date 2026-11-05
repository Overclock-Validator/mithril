package dashboard

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestStatusCapIndependentClockBounds(t *testing.T) {
	now := time.Now()
	lease := newStatusFrameLease(now.Add(4 * time.Second))
	old := lease.value.Load()
	lease.tighten(now.Add(3*time.Second), now.Round(0).Add(2*time.Second))
	select {
	case <-old.changed:
	default:
		t.Fatal("shortening did not signal existing frame users")
	}
	value := lease.value.Load()
	lease.tighten(now.Add(5*time.Second), now.Round(0).Add(5*time.Second))
	if lease.value.Load() != value {
		t.Fatal("later bounds extended or needlessly changed a cap")
	}
	frame := &frameSet{freshUntil: now.Add(4 * time.Second), lease: lease, clockOrigin: now, clockUncertain: &atomic.Bool{}}
	remaining, ok := frame.remainingStatus(0, now)
	if !ok || remaining != 2*time.Second {
		t.Fatalf("earlier wall bound ignored: %v", remaining)
	}
	lease.tighten(now.Add(time.Second), now.Round(0).Add(5*time.Second))
	remaining, ok = frame.remainingStatus(0, now)
	if !ok || remaining != time.Second {
		t.Fatalf("earlier monotonic bound ignored: %v", remaining)
	}
}

func TestStatusCapBoundedRetirement(t *testing.T) {
	now := time.Now()
	c, _ := statusLeaseFixture(t, statusLeaseFixtureValue{sample: testSnapshot(now.UTC()), headers: []string{"5000"}})
	c.poll(context.Background(), now)
	old := c.latest.Load()
	for range 16 {
		c.poll(context.Background(), time.Now().UTC())
	}
	if len(c.statusLeases) != 16 {
		t.Fatalf("lease set exceeded its bound: %d", len(c.statusLeases))
	}
	if _, ok := old.remainingStatus(0, time.Now()); ok {
		t.Fatal("evicted old frame retained a live deadline")
	}
	if _, ok := c.latest.Load().remainingStatus(0, time.Now()); !ok {
		t.Fatal("retirement invalidated the newest fresh frame")
	}
}

func TestStatusCapRepeatedSampleKeepsWallBound(t *testing.T) {
	now := time.Now()
	c, _ := statusLeaseFixture(t, statusLeaseFixtureValue{sample: testSnapshot(now.UTC()), headers: []string{"5000"}})
	c.poll(context.Background(), now)
	if c.statusSampleEnd.IsZero() {
		t.Fatal("first sample did not establish a bound")
	}
	// Simulate a wall-only expiry while the monotonic component is still live.
	c.statusSampleWall = time.Now().Round(0).Add(-time.Millisecond)
	c.poll(context.Background(), time.Now().UTC())
	if frame := c.latest.Load(); frame == nil || !frame.freshUntil.IsZero() || c.statusSampleWall.After(time.Now().Round(0)) {
		t.Fatal("repeated sample renewed its earlier wall deadline")
	}
}

func TestStatusCapBlockedWebSocket(t *testing.T) {
	for _, profile := range []int{0, 3} {
		t.Run(strconv.Itoa(profile), func(t *testing.T) {
			now := time.Now()
			c := newCollector("/unused")
			c.public = true
			snapshot := testSnapshot(now.UTC())
			frame, err := c.encode(&snapshot, "fresh", now)
			if err != nil {
				t.Fatal(err)
			}
			frame.freshUntil, frame.clockOrigin, frame.clockUncertain = now.Add(5*time.Second), c.clockOrigin, &c.clockUncertain
			frame.lease = newStatusFrameLease(frame.freshUntil)
			c.latest.Store(frame)
			entered := make(chan struct{})
			handlerDone := make(chan struct{})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(handlerDone)
				conn, err := upgrader.Upgrade(delayedStatusResponse{ResponseWriter: w, entered: entered}, r, nil)
				if err == nil {
					streamProfile(ctx, conn, c, profile)
				}
			}))
			defer server.Close()
			conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("status write did not block")
			}
			deadline := time.Now().Add(150 * time.Millisecond)
			frame.lease.tighten(deadline, deadline.Round(0))
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			if _, _, err := conn.ReadMessage(); err == nil {
				t.Fatal("blocked frame succeeded after its status lease shortened")
			} else if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
				t.Fatal("client timed out without observing a server-side close")
			}
			select {
			case <-handlerDone:
			case <-time.After(2 * time.Second):
				t.Fatal("blocked stream handler did not exit")
			}
		})
	}
}

func TestStatusCapUnavailableProfiles(t *testing.T) {
	now := time.Now()
	frame := &frameSet{freshUntil: now.Add(time.Second), safetyUnavailable: true, lease: newStatusFrameLease(now.Add(time.Second)),
		clockOrigin: now, clockUncertain: &atomic.Bool{}}
	frame.lease.tighten(now.Add(-time.Second), now.Round(0).Add(-time.Second))
	if _, ok := frame.remainingStatus(0, now); ok {
		t.Fatal("expired default profile remained fresh")
	}
	if remaining, ok := frame.remainingStatus(3, now); !ok || remaining != 0 {
		t.Fatal("unavailable v3 profile inherited a fresh lease")
	}
	legacy := &frameSet{}
	if remaining, ok := legacy.remainingStatus(0, now); !ok || remaining != 0 {
		t.Fatal("legacy unavailable frame behavior changed")
	}
}

func TestStatusCapHealthyWebSocketUpdate(t *testing.T) {
	for _, profile := range []int{0, 3} {
		t.Run(strconv.Itoa(profile), func(t *testing.T) {
			now := time.Now()
			c := newCollector("/unused")
			c.public = true
			snapshot := testSnapshot(now.UTC())
			first, err := c.encode(&snapshot, "fresh", now)
			if err != nil {
				t.Fatal(err)
			}
			first.freshUntil, first.clockOrigin, first.clockUncertain = now.Add(4*time.Second), c.clockOrigin, &c.clockUncertain
			first.lease = newStatusFrameLease(first.freshUntil)
			c.publishStatusFrame(first)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			handlerDone := make(chan struct{})
			upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(handlerDone)
				conn, err := upgrader.Upgrade(w, r, nil)
				if err == nil {
					streamProfile(ctx, conn, c, profile)
				}
			}))
			defer server.Close()
			conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = conn.Close()
				cancel()
				select {
				case <-handlerDone:
				case <-time.After(2 * time.Second):
					t.Error("healthy stream did not exit")
				}
			}()
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			if _, _, err := conn.ReadMessage(); err != nil {
				t.Fatal("initial status was not delivered", err)
			}
			snapshot.SampledAt = snapshot.SampledAt.Add(100 * time.Millisecond)
			next, err := c.encode(&snapshot, "fresh", time.Now())
			if err != nil {
				t.Fatal(err)
			}
			next.freshUntil, next.clockOrigin, next.clockUncertain = time.Now().Add(4*time.Second), c.clockOrigin, &c.clockUncertain
			next.lease = newStatusFrameLease(next.freshUntil)
			shorter := time.Now().Add(1500 * time.Millisecond)
			first.lease.tighten(shorter, shorter.Round(0))
			priorUpdate := c.statusUpdate.Load()
			c.publishStatusFrame(next)
			select {
			case <-priorUpdate.changed:
			default:
				t.Fatal("new status did not notify existing streams")
			}
			_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
			if _, _, err := conn.ReadMessage(); err != nil {
				t.Fatal("healthy new sample disconnected instead of updating", err)
			}
		})
	}
}

func TestStatusCapTransientSourceFailureReconnects(t *testing.T) {
	now := time.Now()
	snapshot := testSnapshot(now.UTC())
	var source atomic.Pointer[Snapshot]
	source.Store(&snapshot)
	var failed atomic.Bool
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, _ *http.Request) {
		if failed.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set(statusRemainingHeader, "5000")
		_ = json.NewEncoder(w).Encode(source.Load())
	})
	c := newCollector(path)
	defer c.client.CloseIdleConnections()
	c.public = true
	c.poll(context.Background(), now)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan struct{}, 2)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { finished <- struct{}{} }()
		conn, err := upgrader.Upgrade(w, r, nil)
		if err == nil {
			streamProfile(ctx, conn, c, 0)
		}
	}))
	defer server.Close()
	dial := func() *websocket.Conn {
		t.Helper()
		conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, _, err := conn.ReadMessage(); err != nil {
			conn.Close()
			t.Fatal("fresh status not delivered", err)
		}
		return conn
	}
	first := dial()
	defer first.Close()
	oldLease := c.latest.Load().lease.value.Load()
	done := make(chan struct{})
	failed.Store(true)
	go func() { defer close(done); c.poll(context.Background(), time.Now().UTC()) }()
	defer func() {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("failed status poll did not exit")
		}
	}()
	select {
	case <-oldLease.changed:
	case <-time.After(3 * time.Second):
		t.Fatal("failed source did not invalidate the old status")
	}
	_ = first.SetReadDeadline(time.Now().Add(2 * time.Second))
	resolved := false
	// A failure can either close the stream or replace it with a stale status.
	// Frames written before invalidation may already be in the socket buffer.
	for range 16 {
		_, body, err := first.ReadMessage()
		if err != nil {
			if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
				t.Fatal("stream neither closed nor reported the failed source")
			}
			resolved = true
			break
		}
		var frame struct {
			Value struct {
				SourceState string `json:"source_state"`
			} `json:"value"`
		}
		if json.Unmarshal(body, &frame) == nil && frame.Value.SourceState != "" && frame.Value.SourceState != "fresh" {
			resolved = true
			break
		}
	}
	if !resolved {
		t.Fatal("failed stream retained fresh status through buffered frames")
	}
	_ = first.Close()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("failed stream did not exit after client close")
	}
	<-done
	failed.Store(false)
	recovered := snapshot
	recovered.SampledAt = time.Now().UTC()
	if !recovered.SampledAt.After(snapshot.SampledAt) {
		recovered.SampledAt = snapshot.SampledAt.Add(time.Nanosecond)
	}
	source.Store(&recovered)
	c.poll(context.Background(), time.Now().UTC())
	second := dial()
	_ = second.Close()
	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("reconnected stream did not exit")
	}
}
