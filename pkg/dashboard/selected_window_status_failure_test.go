package dashboard

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

type selectedEpochTransport func(*http.Request) (*http.Response, error)

func (f selectedEpochTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSelectedWindowFailureIsBoundToStatusStamp(t *testing.T) {
	setup := func() (*collector, ReplayDetailCore) {
		core := replayDetailLiveCore(0)
		status := replayDetailLiveStatus(core)
		c := &collector{last: &status}
		c.observeReplayDetailStatus("fresh", "", time.Now(), 1)
		return c, core
	}
	t.Run("late failure from old epoch", func(t *testing.T) {
		c, _ := setup()
		before := c.detailGate.Load()
		started, release := make(chan struct{}), make(chan struct{})
		client := &http.Client{Transport: selectedEpochTransport(func(*http.Request) (*http.Response, error) {
			close(started)
			<-release
			return nil, errors.New("synthetic old epoch failure")
		})}
		r := &selectedWindowReader{client: client, gate: &c.detailGate}
		done := make(chan struct{})
		go func() { defer close(done); r.collect(context.Background()) }()
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("old-epoch fetch did not start")
		}
		c.observeReplayDetailStatus("fresh", "", time.Now(), 2)
		after := c.detailGate.Load()
		if after.revision != before.revision || after.stamp == before.stamp {
			t.Fatal("test did not reach the same-revision, new-stamp case")
		}
		close(release)
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("old-epoch fetch did not return")
		}
		r.mu.Lock()
		failure := r.failure
		r.mu.Unlock()
		if failure != nil {
			t.Fatal("old-epoch failure was attached to the new epoch")
		}
	})
	t.Run("current failure is retained", func(t *testing.T) {
		c, _ := setup()
		client := &http.Client{Transport: selectedEpochTransport(func(*http.Request) (*http.Response, error) {
			return nil, errors.New("synthetic current failure")
		})}
		r := &selectedWindowReader{client: client, gate: &c.detailGate}
		r.collect(context.Background())
		if r.failure == nil || r.failure.stamp != c.detailGate.Load().stamp || r.failure.reason != "source_unavailable" {
			t.Fatal("current-epoch source failure was lost")
		}
	})
	t.Run("earlier failure does not hide revalidated data", func(t *testing.T) {
		c, core := setup()
		before := c.detailGate.Load()
		c.observeReplayDetailStatus("fresh", "", time.Now(), 2)
		gate := c.detailGate.Load()
		if gate.revision != before.revision || gate.stamp == before.stamp {
			t.Fatal("test did not reach the same-revision, new-stamp case")
		}
		now := time.Now()
		window := selectedWindowFixture(1)
		window.Binding, window.CapturedAt, window.ObservedAt = core.Binding, now.UTC(), now.UTC()
		prepared, err := prepareSelectedWindow(window)
		if err != nil {
			t.Fatal(err)
		}
		publication := &replayDetailPrepared{binding: window.Binding, generation: prepared.digest,
			capturedAt: now, pages: prepared.pages, bytes: prepared.bytes}
		for i := range publication.manifests {
			publication.manifests[i] = prepared.manifest
		}
		r := &selectedWindowReader{gate: &c.detailGate, revision: gate.revision,
			failure: &selectedWindowFailure{revision: before.revision, stamp: before.stamp, reason: "source_unavailable"},
			limit:   rate.NewLimiter(16, 32), active: make(chan struct{}, 4)}
		if err := r.cache.publish(publication, *gate, now); err != nil {
			t.Fatal(err)
		}
		response := httptest.NewRecorder()
		r.serveHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/details/selected", nil), allowReplayStatusForTest)
		if response.Code != http.StatusOK {
			t.Fatalf("old-epoch failure hid a revalidated family: status=%d", response.Code)
		}
	})
}
