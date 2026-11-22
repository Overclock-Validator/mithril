package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func heldScheduleSourceFixture(t *testing.T) (*heldScheduleSource, *PreparedHeldScheduleData, *HeldScheduleData) {
	t.Helper()
	d := heldScheduleDataFixture(t)
	p, err := PrepareHeldScheduleData(d)
	require.NoError(t, err)
	var gate atomic.Pointer[replayDetailGate]
	g := replayGate(rootedHistoryLiveStatus(rootedHistoryDetailFixture()), time.Now())
	gate.Store(&g)
	s := &heldScheduleSource{gate: &gate, gateMu: &sync.Mutex{}, active: make(chan struct{}, 2)}
	s.sample = func(now time.Time) (HeldScheduleCapture, error) {
		return HeldScheduleCapture{Data: p, CapturedAt: now.UTC(), FreshUntil: now.Add(HeldScheduleFreshFor),
			ReferenceSlot: 3, ReferenceBankhash: d.GeometryBankhash, ReferenceCompletedAt: d.GeometryBankCompletedAt}, nil
	}
	return s, p, &d
}

func heldScheduleResponse(s *heldScheduleSource, method, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	s.serveHTTP(w, httptest.NewRequest(method, path, nil))
	return w
}

func TestHeldScheduleSourceCachedRoutesAndReplacement(t *testing.T) {
	s, p, d := heldScheduleSourceFixture(t)
	s.poll(context.Background())
	first := s.latest.Load()
	require.NotNil(t, first)
	require.Equal(t, uint64(1), first.manifest.CaptureSequence)
	manifest := heldScheduleResponse(s, "GET", "/details/schedule")
	require.Equal(t, http.StatusOK, manifest.Code)
	require.Equal(t, first.body, manifest.Body.String())
	data := heldScheduleResponse(s, "GET", "/details/schedule/data/"+p.digest)
	require.Equal(t, http.StatusOK, data.Code)
	require.Equal(t, p.body, data.Body.String())
	require.Equal(t, `"`+p.digest+`"`, data.Header().Get("ETag"))
	require.NotEmpty(t, data.Header().Get(replayDetailRemainingHeader))
	for _, path := range []string{"/details/schedule?", "/details/%73chedule", "/details/schedule/data/", "/details/schedule/data/" + strings.Repeat("A", 64), "/details/schedule/data/" + p.digest + "?x=1"} {
		require.Equal(t, http.StatusBadRequest, heldScheduleResponse(s, "GET", path).Code, path)
	}
	require.Equal(t, http.StatusBadRequest, heldScheduleResponse(s, "POST", "/details/schedule").Code)
	require.Same(t, first, s.latest.Load(), "HTTP must not resample or replace a capture")
	// Same-generation altered data must not bypass the high-water fence.
	d.Current.Leaders[0] = (solana.PublicKey{9}).String()
	changed, err := PrepareHeldScheduleData(*d)
	require.NoError(t, err)
	var sourceErr error
	s.sample = func(now time.Time) (HeldScheduleCapture, error) {
		return HeldScheduleCapture{Data: changed, CapturedAt: now.UTC(), FreshUntil: now.Add(HeldScheduleFreshFor),
			ReferenceSlot: 3, ReferenceBankhash: d.GeometryBankhash, ReferenceCompletedAt: d.GeometryBankCompletedAt}, sourceErr
	}
	s.poll(context.Background())
	require.Equal(t, "source_invalid", s.failure.Load().reason)
	require.Equal(t, http.StatusServiceUnavailable, heldScheduleResponse(s, "GET", "/details/schedule/data/"+p.digest).Code)
	d.Binding.SourceGeneration++
	changed, err = PrepareHeldScheduleData(*d)
	require.NoError(t, err)
	s.poll(context.Background())
	require.Nil(t, s.failure.Load())
	require.Equal(t, uint64(2), s.latest.Load().manifest.CaptureSequence)
	require.NotEqual(t, first.manifest.Digest, s.latest.Load().manifest.Digest)
	require.Equal(t, http.StatusServiceUnavailable, heldScheduleResponse(s, "GET", "/details/schedule/data/"+p.digest).Code, "retired digest must not stay available")
	require.Equal(t, changed.body, heldScheduleResponse(s, "GET", "/details/schedule/data/"+changed.digest).Body.String())
	second := changed
	sourceErr = errors.New("source_unavailable")
	s.poll(context.Background())
	require.Equal(t, "source_unavailable", s.failure.Load().reason)
	sourceErr = nil
	changed = p
	s.poll(context.Background())
	require.Equal(t, "binding_changed", s.failure.Load().reason, "an earlier generation cannot return after an error")
	changed = second
	s.poll(context.Background())
	require.Nil(t, s.failure.Load())
	require.Equal(t, uint64(3), s.latest.Load().manifest.CaptureSequence)
	s.active <- struct{}{}
	s.active <- struct{}{}
	busy := heldScheduleResponse(s, "GET", "/details/schedule/data/"+changed.digest)
	require.Equal(t, http.StatusTooManyRequests, busy.Code)
	require.Empty(t, busy.Header().Get(replayDetailRemainingHeader))
	<-s.active
	<-s.active
	expired := *s.latest.Load()
	expired.freshUntil = time.Now().Add(-time.Millisecond)
	s.latest.Store(&expired)
	stale := heldScheduleResponse(s, "GET", "/details/schedule/data/"+changed.digest)
	require.Equal(t, http.StatusServiceUnavailable, stale.Code)
	require.Empty(t, stale.Header().Get(replayDetailRemainingHeader))
	s.sequence = math.MaxUint64
	s.poll(context.Background())
	require.Equal(t, "source_unavailable", s.failure.Load().reason)
	require.Equal(t, uint64(math.MaxUint64), s.sequence)
}

func TestHeldScheduleFeedCloseJoinsInFlightScheduleCapture(t *testing.T) {
	d := heldScheduleDataFixture(t)
	p, err := PrepareHeldScheduleData(d)
	require.NoError(t, err)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	feed, err := ListenFeedWithDetails(context.Background(), socketPath(t), "", func() Snapshot {
		return rootedHistoryLiveStatus(rootedHistoryDetailFixture())
	}, DetailSamplers{
		Replay: func(now time.Time) (ReplayDetailCore, error) {
			c := replayDetailTestCore(0)
			c.Binding = d.Binding
			c.CapturedAt, c.UpdatedAt = now.UTC(), now.UTC()
			return c, nil
		},
		HeldSchedules: func(now time.Time) (HeldScheduleCapture, error) {
			once.Do(func() { close(entered) })
			<-release
			return HeldScheduleCapture{Data: p, CapturedAt: now.UTC(), FreshUntil: now.Add(HeldScheduleFreshFor),
				ReferenceSlot: 3, ReferenceBankhash: d.GeometryBankhash, ReferenceCompletedAt: d.GeometryBankCompletedAt}, nil
		},
	})
	require.NoError(t, err)
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		feed.Close()
		t.Fatal("shared worker did not capture schedules")
	}
	closed := make(chan struct{})
	go func() { feed.Close(); close(closed) }()
	<-feed.served
	select {
	case <-closed:
		close(release)
		t.Fatal("Feed.Close returned before the schedule callback joined")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Feed.Close did not join the released callback")
	}
	require.Nil(t, feed.details.held.latest.Load(), "cancelled capture must not publish")
}

func TestHeldScheduleSourceCancellationAndContextFence(t *testing.T) {
	s, _, _ := heldScheduleSourceFixture(t)
	original := s.sample
	s.sample = func(now time.Time) (HeldScheduleCapture, error) {
		g := *s.gate.Load()
		g.Attempt++
		s.gate.Store(&g)
		return original(now)
	}
	s.poll(context.Background())
	require.Nil(t, s.latest.Load())
	require.Equal(t, "binding_changed", s.failure.Load().reason)
	s, _, _ = heldScheduleSourceFixture(t)
	original = s.sample
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	s.sample = func(now time.Time) (HeldScheduleCapture, error) { close(entered); <-release; return original(now) }
	ctx, cancel := context.WithCancel(context.Background())
	go func() { defer close(done); s.poll(ctx) }()
	<-entered
	cancel()
	close(release)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancelled callback did not join")
	}
	require.Nil(t, s.latest.Load())
	require.Zero(t, s.sequence)
}

func TestHeldScheduleSourceRealUnixFeedAndOptIn(t *testing.T) {
	d := heldScheduleDataFixture(t)
	p, err := PrepareHeldScheduleData(d)
	require.NoError(t, err)
	var samples atomic.Int64
	samplers := DetailSamplers{HeldSchedules: func(now time.Time) (HeldScheduleCapture, error) {
		samples.Add(1)
		return HeldScheduleCapture{Data: p, CapturedAt: now.UTC(), FreshUntil: now.Add(HeldScheduleFreshFor),
			ReferenceSlot: 3, ReferenceBankhash: d.GeometryBankhash, ReferenceCompletedAt: d.GeometryBankCompletedAt}, nil
	}}
	path := socketPath(t)
	_, err = ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return rootedHistoryLiveStatus(rootedHistoryDetailFixture()) }, samplers)
	require.Error(t, err, "a schedule opt-in must not silently create another worker")
	samplers.Replay = func(now time.Time) (ReplayDetailCore, error) {
		c := replayDetailTestCore(0)
		c.Binding = d.Binding
		c.CapturedAt, c.UpdatedAt = now.UTC(), now.UTC()
		return c, nil
	}
	feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return rootedHistoryLiveStatus(rootedHistoryDetailFixture()) }, samplers)
	require.NoError(t, err)
	t.Cleanup(feed.Close)
	collector := newCollector(path)
	t.Cleanup(collector.client.CloseIdleConnections)
	deadline := time.Now().Add(3 * time.Second)
	for {
		response, err := collector.client.Get("http://node/details/schedule")
		require.NoError(t, err)
		body, err := io.ReadAll(io.LimitReader(response.Body, MaxSnapshotBytes+1))
		require.NoError(t, response.Body.Close())
		require.NoError(t, err)
		if response.StatusCode == http.StatusOK {
			var manifest HeldScheduleManifest
			require.NoError(t, json.Unmarshal(body, &manifest))
			require.Equal(t, p.digest, manifest.Digest)
			break
		}
		require.True(t, time.Now().Before(deadline), string(body))
		time.Sleep(10 * time.Millisecond)
	}
	before := samples.Load()
	response, err := collector.client.Get("http://node/details/schedule/data/" + p.digest)
	require.NoError(t, err)
	body, err := io.ReadAll(io.LimitReader(response.Body, MaxHeldScheduleDataBytes+1))
	require.NoError(t, response.Body.Close())
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, response.StatusCode)
	require.Equal(t, p.body, string(body))
	require.Equal(t, before, samples.Load(), "dataset viewers must not call the sampler")
	feed.Close()
	select {
	case <-feed.details.done:
	default:
		t.Fatal("existing joined worker still running")
	}
}
