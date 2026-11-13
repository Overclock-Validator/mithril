package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestSelectedWindowFeedLifecycle(t *testing.T) {
	core := replayDetailLiveCore(0)
	window := selectedWindowFixture(9)
	window.Binding = core.Binding
	window.ObservedAt = time.Now().UTC()
	var fail atomic.Bool
	feed, err := ListenFeedWithDetails(context.Background(), socketPath(t), "", func() Snapshot {
		return replayDetailLiveStatus(core)
	}, DetailSamplers{Replay: func(at time.Time) (ReplayDetailCore, error) {
		c := core
		c.CapturedAt = at.UTC()
		return c, nil
	}, SelectedWindow: func(at time.Time) (SelectedWindow, error) {
		if fail.Load() {
			return SelectedWindow{}, errors.New("waiting_source")
		}
		w := window
		w.CapturedAt = at.UTC()
		return w, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	client := newCollector(feed.listener.Addr().String()).client
	t.Cleanup(client.CloseIdleConnections)
	var manifest selectedWindowManifest
	deadline := time.Now().Add(3 * time.Second)
	for {
		status, body, headers := replayDetailHTTP(t, client, "GET", "http://node/details/selected", "")
		if status == http.StatusOK {
			if err := json.Unmarshal([]byte(body), &manifest); err != nil || headers.Get(replayDetailRemainingHeader) == "" {
				t.Fatal("missing selected manifest or lease", err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("selected manifest did not become ready: %d %s", status, body)
		}
		time.Sleep(10 * time.Millisecond)
	}
	pages := make([][]byte, manifest.PageCount)
	for i := range pages {
		query := fmt.Sprintf("http://node/details/selected?generation=%s&page=%d", manifest.Digest, i)
		status, body, _ := replayDetailHTTP(t, client, "GET", query, "")
		if status != http.StatusOK {
			t.Fatalf("page %d: %d %s", i, status, body)
		}
		pages[i] = []byte(body)
	}
	status, body, _ := replayDetailHTTP(t, client, "GET", "http://node/details/selected", "")
	if status != http.StatusOK {
		t.Fatal("manifest unexpectedly unavailable")
	}
	got, err := decodeSelectedWindow([]byte(body), pages)
	if err != nil || len(got.Records) != 9 || got.Records[0].Slot != window.Records[0].Slot {
		t.Fatal("private feed lost selected data", err)
	}
	for _, path := range []string{"/details/selected?", "/details/selected?page=00&generation=" + manifest.Digest,
		"/details/selected?generation=" + manifest.Digest + "&page=32", "/details/selected?path=/etc/passwd"} {
		status, _, _ := replayDetailHTTP(t, client, "GET", "http://node"+path, "")
		if status == http.StatusOK {
			t.Fatalf("invalid path was accepted: %s", path)
		}
	}
	for _, path := range []string{"/details/%73elected", "/details/selected/../selected", "/details/selected/extra"} {
		recorder := httptest.NewRecorder()
		feed.server.Handler.ServeHTTP(recorder, httptest.NewRequest("GET", path, nil))
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("noncanonical selected path was redirected or accepted: %s: %d", path, recorder.Code)
		}
	}
	fail.Store(true)
	feed.details.selected.poll(context.Background())
	status, _, _ = replayDetailHTTP(t, client, "GET", "http://node/details/selected", "")
	if status != http.StatusServiceUnavailable {
		t.Fatal("failed capture did not make current manifest unavailable")
	}
	query := fmt.Sprintf("http://node/details/selected?generation=%s&page=0", manifest.Digest)
	status, _, _ = replayDetailHTTP(t, client, "GET", query, "")
	if status != http.StatusOK {
		t.Fatal("already advertised page disappeared after a failed capture")
	}
}

func TestSelectedWindowRejectsABAStatusDuringCapture(t *testing.T) {
	core := replayDetailLiveCore(0)
	status := replayDetailLiveStatus(core)
	window := selectedWindowFixture(1)
	window.Binding, window.ObservedAt = core.Binding, time.Now().UTC()
	started, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	parent := &replayDetailSource{}
	s := &selectedWindowSource{gate: &parent.gate, gateMu: &parent.gateMu, revision: 1,
		sample: func(at time.Time) (SelectedWindow, error) {
			if first.Swap(false) {
				close(started)
				<-release
			}
			w := window
			w.CapturedAt = at.UTC()
			return w, nil
		}}
	parent.selected = s
	parent.observeStatus(status, time.Now())
	s.poll(context.Background())
	initial := httptest.NewRecorder()
	s.serveHTTP(initial, httptest.NewRequest("GET", "/details/selected", nil))
	if initial.Code != http.StatusOK {
		t.Fatal("initial capture unavailable")
	}
	first.Store(true)
	done := make(chan struct{})
	go func() { defer close(done); s.poll(context.Background()) }()
	<-started
	other := status
	other.RunID = "replacement"
	parent.observeStatus(other, time.Now())
	parent.observeStatus(status, time.Now())
	close(release)
	<-done
	if s.revision != 3 {
		t.Fatalf("status revision did not capture both replacements: %d", s.revision)
	}
	response := httptest.NewRecorder()
	s.serveHTTP(response, httptest.NewRequest("GET", "/details/selected", nil))
	if response.Code == http.StatusOK {
		t.Fatal("A -> B -> A published the old capture")
	}
	s.poll(context.Background())
	response = httptest.NewRecorder()
	s.serveHTTP(response, httptest.NewRequest("GET", "/details/selected", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("fresh capture did not recover: %d %s", response.Code, response.Body.String())
	}
	stopping := status
	stopping.Phase = "stopping"
	parent.observeStatus(stopping, time.Now())
	response = httptest.NewRecorder()
	s.serveHTTP(response, httptest.NewRequest("GET", "/details/selected", nil))
	if response.Code == http.StatusOK {
		t.Fatal("stopping node still advertised a fresh selected window")
	}
}

func TestSelectedWindowDisabledWithoutReplayWorker(t *testing.T) {
	path := socketPath(t)
	if _, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return Snapshot{} }, DetailSamplers{
		SelectedWindow: func(time.Time) (SelectedWindow, error) { return SelectedWindow{}, nil },
	}); err == nil {
		t.Fatal("selected source started without a replay worker")
	}
	core := replayDetailLiveCore(0)
	feed, err := ListenFeedWithReplayDetails(context.Background(), path, "", func() Snapshot { return replayDetailLiveStatus(core) },
		func(at time.Time) (ReplayDetailCore, error) { c := core; c.CapturedAt = at.UTC(); return c, nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(feed.Close)
	client := newCollector(path).client
	t.Cleanup(client.CloseIdleConnections)
	code, _, _ := replayDetailHTTP(t, client, "GET", "http://node/details/selected", "")
	if code != http.StatusNotFound {
		t.Fatalf("disabled selected source returned %d", code)
	}
}

func TestSelectedWindowFeedCloseJoinsSampler(t *testing.T) {
	core := replayDetailLiveCore(0)
	status := replayDetailLiveStatus(core)
	started, release := make(chan struct{}), make(chan struct{})
	feed, err := ListenFeedWithDetails(context.Background(), socketPath(t), "", func() Snapshot { return status }, DetailSamplers{
		Replay: func(at time.Time) (ReplayDetailCore, error) { c := core; c.CapturedAt = at.UTC(); return c, nil },
		SelectedWindow: func(time.Time) (SelectedWindow, error) {
			close(started)
			<-release
			return SelectedWindow{}, errors.New("waiting_source")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		feed.Close()
	})
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("selected sampler did not start")
	}
	done := make(chan struct{})
	go func() { feed.Close(); close(done) }()
	select {
	case <-done:
		t.Fatal("feed closed before its sampler joined")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("feed did not close after sampler returned")
	}
}

func TestSelectedWindowCaptureHighWater(t *testing.T) {
	w := selectedWindowFixture(1)
	last := &selectedWindowHighWater{binding: w.Binding, at: w.CapturedAt, digest: "same"}
	if selectedWindowRegressed(last, w.Binding, w.CapturedAt, "same") ||
		selectedWindowRegressed(last, w.Binding, w.CapturedAt.Add(time.Nanosecond), "new") ||
		!selectedWindowRegressed(last, w.Binding, w.CapturedAt.Add(-time.Nanosecond), "same") ||
		!selectedWindowRegressed(last, w.Binding, w.CapturedAt, "changed") {
		t.Fatal("capture high-water did not distinguish identical, newer and changed observations")
	}
	replacement := w.Binding
	replacement.RunID = "replacement"
	if selectedWindowRegressed(last, replacement, w.CapturedAt.Add(-time.Hour), "new") {
		t.Fatal("new context inherited a previous run's timestamp")
	}
}
