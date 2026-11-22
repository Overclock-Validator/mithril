package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func replayDetailLiveCore(count int) ReplayDetailCore {
	c := replayDetailTestCore(count)
	at := time.Now().UTC().Add(-time.Hour)
	c.Binding.NodeStartedAt, c.Binding.AttemptStartedAt, c.Binding.ResetAt = at, at, at
	c.CapturedAt, c.UpdatedAt = at, at
	for i := range c.Records {
		c.Records[i].CompletedAt, c.Records[i].ObservedAt = at, at
	}
	return c
}

func replayDetailLiveStatus(c ReplayDetailCore) Snapshot {
	s := testSnapshot(time.Now().UTC())
	s.RunID, s.StartedAt, s.GenesisHash = c.Binding.RunID, c.Binding.NodeStartedAt, c.Binding.GenesisHash
	s.Replay = &Replay{Attempt: c.Binding.Attempt, Rewinds: c.Binding.Rewinds, StartedAt: c.Binding.AttemptStartedAt, Phase: "replaying"}
	return s
}

func replayDetailHTTP(t *testing.T, client *http.Client, method, url, body string) (int, string, http.Header) {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxSnapshotBytes+1))
	if err != nil || len(data) > MaxSnapshotBytes {
		t.Fatalf("read bounded detail response: bytes=%d error=%v", len(data), err)
	}
	return resp.StatusCode, string(data), resp.Header
}

func replayDetailWaitManifest(t *testing.T, client *http.Client, url string) ReplayDetailManifest {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		status, body, headers := replayDetailHTTP(t, client, "GET", url, "")
		if status == 200 {
			var m ReplayDetailManifest
			if err := json.Unmarshal([]byte(body), &m); err != nil {
				t.Fatal(err)
			}
			remaining, err := strconv.Atoi(headers.Get(replayDetailRemainingHeader))
			if err != nil || remaining < 13000 || remaining > 30000 || headers.Get("Cache-Control") != "no-store" || headers.Get("Content-Type") != "application/json" {
				t.Fatal("manifest headers invalid")
			}
			return m
		}
		if time.Now().After(deadline) {
			t.Fatalf("manifest unavailable: %d %s", status, body)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestReplayDetailFeedRealUnixPagesAndDefault(t *testing.T) {
	for _, count := range []int{0, 256} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			core := replayDetailLiveCore(count)
			var calls atomic.Int64
			path := socketPath(t)
			feed, err := ListenFeedWithReplayDetails(context.Background(), path, "", func() Snapshot { return replayDetailLiveStatus(core) }, func(capturedAt time.Time) (ReplayDetailCore, error) {
				calls.Add(1)
				copy := core
				copy.CapturedAt = capturedAt.UTC()
				return copy, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(feed.Close)
			collector := newCollector(path)
			t.Cleanup(collector.client.CloseIdleConnections)
			m := replayDetailWaitManifest(t, collector.client, "http://node/details/replay")
			if m.Count != count || m.PageCount != (count+1)/2 {
				t.Fatal("known empty or full history changed")
			}
			rows := 0
			for page := 0; page < m.PageCount; page++ {
				status, body, headers := replayDetailHTTP(t, collector.client, "GET", "http://node/details/replay?page="+strconv.Itoa(page)+"&generation="+m.Generation, "")
				var p ReplayDetailPage
				if status != 200 || json.Unmarshal([]byte(body), &p) != nil || p.Page != page || p.Generation != m.Generation || !sameReplayDetailBinding(p.Binding, m.Binding) || headers.Get(replayDetailRemainingHeader) != "" {
					t.Fatal("invalid page response")
				}
				rows += len(p.Records)
			}
			for range 30 {
				replayDetailHTTP(t, collector.client, "GET", "http://node/details/replay", "")
			}
			if rows != count || calls.Load() != 1 {
				t.Fatal("viewers triggered captures or lost rows")
			}
			if feed.server.ReadTimeout != 2*time.Second || feed.server.WriteTimeout != 2*time.Second || feed.server.MaxHeaderBytes != 4096 {
				t.Fatal("existing server limits changed")
			}
		})
	}
	for _, useWrapper := range []bool{false, true} {
		path := socketPath(t)
		sample := func() Snapshot { return testSnapshot(time.Now().UTC()) }
		var feed *Feed
		var err error
		if useWrapper {
			feed, err = ListenFeedWithReplayDetails(context.Background(), path, "", sample, nil)
		} else {
			feed, err = ListenFeed(context.Background(), path, sample)
		}
		if err != nil {
			t.Fatal(err)
		}
		collector := newCollector(path)
		status, _, _ := replayDetailHTTP(t, collector.client, "GET", "http://node/details/replay", "")
		collector.client.CloseIdleConnections()
		feed.Close()
		if status != 404 || feed.details != nil {
			t.Fatal("default feed enabled details")
		}
	}
}

func TestReplayDetailFeedStatusContinuesAndCloseJoins(t *testing.T) {
	core := replayDetailLiveCore(1)
	entered, release := make(chan time.Time, 1), make(chan struct{})
	var releaseOnce sync.Once
	var statusCalls atomic.Int64
	path := socketPath(t)
	feed, err := ListenFeedWithReplayDetails(context.Background(), path, "", func() Snapshot {
		statusCalls.Add(1)
		return replayDetailLiveStatus(core)
	}, func(capturedAt time.Time) (ReplayDetailCore, error) {
		entered <- capturedAt
		<-release
		copy := core
		copy.CapturedAt = capturedAt.UTC()
		return copy, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }); feed.Close() })
	select {
	case capturedAt := <-entered:
		if capturedAt == capturedAt.UTC() {
			t.Fatal("sampler received a stripped capture clock")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("detail worker did not start")
	}
	collector := newCollector(path)
	defer collector.client.CloseIdleConnections()
	status, beforeBody, _ := replayDetailHTTP(t, collector.client, "GET", "http://node/snapshot", "")
	var before Snapshot
	if status != 200 || json.Unmarshal([]byte(beforeBody), &before) != nil {
		t.Fatal("ordinary status unavailable during detail capture")
	}
	deadline := time.Now().Add(3 * time.Second)
	advanced := false
	for time.Now().Before(deadline) {
		status, body, _ := replayDetailHTTP(t, collector.client, "GET", "http://node/snapshot", "")
		var after Snapshot
		if status == 200 && json.Unmarshal([]byte(body), &after) == nil && after.SampledAt.After(before.SampledAt) {
			advanced = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if statusCalls.Load() < 2 || !advanced {
		t.Fatal("slow details blocked ordinary status")
	}
	closed := make(chan struct{})
	go func() { feed.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("close did not join in-flight sampler")
	case <-time.After(20 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("close did not finish after sampler returned")
	}
	select {
	case <-feed.details.done:
	default:
		t.Fatal("detail worker remained running")
	}
	if feed.details.cache.items[0] != nil {
		t.Fatal("cancelled in-flight callback published a generation")
	}
}

func TestReplayDetailSourcePreflightAvoidsExtraCaptures(t *testing.T) {
	core := replayDetailLiveCore(0)
	calls := 0
	s := &replayDetailSource{sample: func(at time.Time) (ReplayDetailCore, error) {
		calls++
		copy := core
		copy.CapturedAt = at.UTC()
		return copy, nil
	}}
	s.poll(context.Background())
	status := replayDetailLiveStatus(core)
	status.Replay = nil
	s.observeStatus(status, time.Now())
	s.poll(context.Background())
	if calls != 0 {
		t.Fatal("uninitialized status allowed owner capture")
	}
	s.observeStatus(replayDetailLiveStatus(core), time.Now())
	for range 3 {
		s.poll(context.Background())
	}
	if calls != 3 || s.cache.items[2] == nil {
		t.Fatal("three distinct captures did not fill holder")
	}
	s.poll(context.Background())
	if calls != 3 {
		t.Fatal("full holder called owner before deferring")
	}
}

func TestReplayDetailSourceFailureAndContextFence(t *testing.T) {
	core := replayDetailLiveCore(3)
	var mode atomic.Int64
	var calls atomic.Int64
	s := &replayDetailSource{}
	s.sample = func(at time.Time) (ReplayDetailCore, error) {
		calls.Add(1)
		copy := core
		copy.CapturedAt = at.UTC()
		switch mode.Load() {
		case 1:
			return ReplayDetailCore{}, errors.New("/private/secret-input")
		case 2:
			changed := replayDetailLiveStatus(core)
			changed.Replay.Attempt++
			s.observeStatus(changed, time.Now())
		case 3:
			copy.Records = nil
		}
		return copy, nil
	}
	s.observeStatus(replayDetailLiveStatus(core), time.Now())
	s.poll(context.Background())
	server := httptest.NewServer(http.HandlerFunc(s.serveHTTP))
	defer server.Close()
	m := replayDetailWaitManifest(t, server.Client(), server.URL+"/details/replay")
	query := "/details/replay?generation=" + m.Generation + "&page=0"
	mode.Store(1)
	s.poll(context.Background())
	status, body, _ := replayDetailHTTP(t, server.Client(), "GET", server.URL+"/details/replay", "")
	if status != 503 || !strings.Contains(body, `"reason":"source_unavailable"`) || strings.Contains(body, "secret") {
		t.Fatal("capture error was not bounded and unavailable")
	}
	status, _, _ = replayDetailHTTP(t, server.Client(), "GET", server.URL+query, "")
	if status != 200 {
		t.Fatal("new capture failure revoked an advertised page")
	}
	mode.Store(2)
	s.poll(context.Background())
	status, body, _ = replayDetailHTTP(t, server.Client(), "GET", server.URL+"/details/replay", "")
	if status != 503 || !strings.Contains(body, `"reason":"binding_changed"`) {
		t.Fatal("reset-crossing capture was installed")
	}
	status, _, _ = replayDetailHTTP(t, server.Client(), "GET", server.URL+query, "")
	if status != 409 {
		t.Fatal("old page remained accessible after context change")
	}
	before := calls.Load()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.poll(ctx)
	if calls.Load() != before {
		t.Fatal("cancelled poll called source")
	}
	s.observeStatus(replayDetailLiveStatus(core), time.Now())
	mode.Store(3)
	s.poll(context.Background())
	status, body, _ = replayDetailHTTP(t, server.Client(), "GET", server.URL+"/details/replay", "")
	if status != 503 || !strings.Contains(body, `"reason":"source_invalid"`) {
		t.Fatal("invalid source did not fail closed")
	}
}

func TestReplayDetailFeedStrictRequestsAndValidStatusGate(t *testing.T) {
	core := replayDetailLiveCore(0)
	var statusMode atomic.Int64
	var calls atomic.Int64
	path := socketPath(t)
	feed, err := ListenFeedWithReplayDetails(context.Background(), path, "", func() Snapshot {
		switch statusMode.Load() {
		case 0:
			return Snapshot{}
		case 1:
			s := replayDetailLiveStatus(core)
			s.Version = strings.Repeat("x", MaxPrivateSnapshotBytes)
			return s
		default:
			return replayDetailLiveStatus(core)
		}
	}, func(at time.Time) (ReplayDetailCore, error) {
		calls.Add(1)
		copy := core
		copy.CapturedAt = at.UTC()
		return copy, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer feed.Close()
	collector := newCollector(path)
	defer collector.client.CloseIdleConnections()
	for _, test := range []struct{ method, path, body string }{
		{"POST", "/details/replay", ""}, {"GET", "/details/replay?", ""},
		{"GET", "/details/replay", "{}"}, {"GET", "/details/replay?path=/etc/passwd", ""},
		{"GET", "/details/replay?page=00&generation=" + strings.Repeat("a", 64), ""},
		{"GET", "/%64etails/replay", ""},
	} {
		status, body, _ := replayDetailHTTP(t, collector.client, test.method, "http://node"+test.path, test.body)
		if status != 400 || !strings.Contains(body, `"reason":"invalid_request"`) {
			t.Fatalf("invalid request not rejected: %s %s status=%d", test.method, test.path, status)
		}
	}
	if feed.details.gate.Load() != nil || calls.Load() != 0 {
		t.Fatal("invalid status initialized detail gate")
	}
	statusMode.Store(1)
	// Wait for one ordinary status tick; oversized status must not initialize it.
	time.Sleep(SampleInterval + 50*time.Millisecond)
	if feed.details.gate.Load() != nil || calls.Load() != 0 {
		t.Fatal("oversized status initialized detail gate")
	}
	_, err = ListenFeedWithReplayDetails(context.Background(), socketPath(t), "mithril-dashboard-group-does-not-exist", func() Snapshot { return replayDetailLiveStatus(core) }, func(time.Time) (ReplayDetailCore, error) {
		t.Fatal("permission refusal invoked detail sampler")
		return ReplayDetailCore{}, nil
	})
	if err == nil {
		t.Fatal("detail wrapper bypassed existing group checks")
	}
}

func TestReplayDetailSourceCurrentGatePrecedesCaptureFailure(t *testing.T) {
	for _, test := range []struct {
		name   string
		offset time.Duration
		change func(*replayDetailGate)
		reason string
	}{
		{"fresh", 0, func(*replayDetailGate) {}, "source_unavailable"},
		{"stale", -StaleAfter - time.Second, func(*replayDetailGate) {}, "status_stale"},
		{"future", time.Hour, func(*replayDetailGate) {}, "source_time_invalid"},
		{"unavailable", 0, func(g *replayDetailGate) { g.State, g.Reason = "unavailable", "waiting_source" }, "waiting_source"},
	} {
		t.Run(test.name, func(t *testing.T) {
			core, now := replayDetailLiveCore(1), time.Now().Add(test.offset)
			core.CapturedAt = now.UTC()
			var source replayDetailSource
			source.observeStatus(replayDetailLiveStatus(core), now)
			gate := *source.gate.Load()
			prepared, err := prepareReplayDetail(core, now)
			if err != nil {
				t.Fatal(err)
			}
			if err := source.cache.publish(prepared, gate, now); err != nil {
				t.Fatal(err)
			}
			source.fail(gate, "source_unavailable")
			test.change(&gate)
			source.gate.Store(&gate)
			recorder := httptest.NewRecorder()
			source.serveHTTP(recorder, httptest.NewRequest("GET", "/details/replay", nil))
			if recorder.Code != 503 || !strings.Contains(recorder.Body.String(), `"reason":"`+test.reason+`"`) {
				t.Fatalf("current gate lost to earlier capture failure: %d %s", recorder.Code, recorder.Body.String())
			}
			if recorder.Header().Get(replayDetailRemainingHeader) != "" {
				t.Fatal("unavailable response advertised a collection lease")
			}
		})
	}
}
