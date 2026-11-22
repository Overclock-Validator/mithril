package dashboard

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestRootedHistoryReaderRealUnixAccuracyAndLease(t *testing.T) {
	d := rootedHistoryDetailFixture()
	d.Epoch = &RootedHistoryEpoch{Number: 0, FirstSlot: 0, Slots: 32}
	var requests atomic.Int64
	var omitHeader atomic.Bool
	var remaining atomic.Int64
	remaining.Store(30000)
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, req *http.Request) {
		requests.Add(1)
		require.Equal(t, "/details/slots", req.URL.Path)
		if !omitHeader.Load() {
			w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(remaining.Load(), 10))
		}
		body, err := json.Marshal(d)
		require.NoError(t, err)
		_, _ = w.Write(body)
	})
	var gate atomic.Pointer[replayDetailGate]
	g := replayGate(rootedHistoryLiveStatus(d), time.Now())
	g.stamp = statusStamp{nodeEpoch: 1, familyEpoch: 1}
	gate.Store(&g)
	owner, err := newReplayDetailReader(newCollector(path).client, &gate)
	require.NoError(t, err)
	t.Cleanup(owner.client.CloseIdleConnections)
	r := &rootedHistoryReader{client: owner.client, gate: &gate, limit: rate.NewLimiter(4, 8)}
	r.collect(context.Background())
	require.Nil(t, r.failure.Load())
	first := r.latest.Load()
	require.NotNil(t, first)
	w := httptest.NewRecorder()
	r.serveHTTP(w, httptest.NewRequest("GET", "/api/v1/details/slots", nil), allowReplayStatusForTest)
	require.Equal(t, http.StatusOK, w.Code)
	var public PublicRootedHistoryDetail
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &public))
	require.Equal(t, d.Bitmap, public.Bitmap)
	require.Equal(t, d.ObservedAt, public.ObservedAt)
	require.Equal(t, d.CapturedAt, public.CapturedAt)
	require.Equal(t, d.Epoch, public.Epoch)
	require.True(t, sameReplayDetailBinding(d.Binding, public.Binding))
	require.LessOrEqual(t, w.Body.Len(), MaxRootedHistoryBytes)
	// A new parent-status epoch cannot reuse this capture until it has been
	// revalidated, and that revalidation cannot extend its original lease.
	g.stamp = statusStamp{nodeEpoch: 2, familyEpoch: 2}
	gate.Store(&g)
	w = httptest.NewRecorder()
	r.serveHTTP(w, httptest.NewRequest("GET", "/api/v1/details/slots", nil), allowReplayStatusForTest)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	r.collect(context.Background())
	require.Nil(t, r.failure.Load())
	require.Equal(t, g.stamp, r.latest.Load().stamp)
	require.False(t, r.latest.Load().freshUntil.After(first.freshUntil))
	for range 5 {
		r.serveHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/api/v1/details/slots", nil), allowReplayStatusForTest)
	}
	require.Equal(t, int64(2), requests.Load(), "viewers must not call the node")
	r.collect(context.Background())
	require.Nil(t, r.failure.Load())
	require.Equal(t, first.freshUntil, r.latest.Load().freshUntil, "re-reading identical bytes must not renew their lease")
	require.Equal(t, first.body, r.latest.Load().body, "a repeated capture must retain its original collection date")
	remaining.Store(1000)
	r.collect(context.Background())
	require.Nil(t, r.failure.Load())
	require.Equal(t, first.body, r.latest.Load().body)
	require.True(t, r.latest.Load().freshUntil.Before(first.freshUntil), "a shorter same-capture lease must be honored")
	omitHeader.Store(true)
	r.collect(context.Background())
	require.Equal(t, "source_invalid", r.failure.Load().reason)
	r.limit = rate.NewLimiter(4, 8)
	w = httptest.NewRecorder()
	r.serveHTTP(w, httptest.NewRequest("GET", "/api/v1/details/slots", nil), allowReplayStatusForTest)
	require.Equal(t, http.StatusServiceUnavailable, w.Code, "failed capture must not appear fresh")
	require.Empty(t, w.Header().Get(replayDetailRemainingHeader))
	// A later status attempt cannot expose the former attempt's cached bitmap.
	r.failure.Store(nil)
	g.Attempt++
	gate.Store(&g)
	w = httptest.NewRecorder()
	r.serveHTTP(w, httptest.NewRequest("GET", "/api/v1/details/slots", nil), allowReplayStatusForTest)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.Contains(t, w.Body.String(), "binding_changed")
}

func TestRootedHistoryReaderRequestAndWireBounds(t *testing.T) {
	d := rootedHistoryDetailFixture()
	for _, tc := range []struct {
		name   string
		status int
		header string
		body   string
		want   string
	}{
		{"unsupported", 404, "", "", "unsupported_source"},
		{"redirect", 302, "", "", "source_invalid"},
		{"oversize", 200, "30000", string(make([]byte, MaxRootedHistoryBytes+1)), "response_limit"},
		{"error-body", 503, "", `{"schema_version":1,"family":"rooted_history","state":"unavailable","reason":"waiting_source"}`, "waiting_source"},
		{"error-unknown", 503, "", `{"schema_version":1,"family":"rooted_history","state":"unavailable","reason":"secret"}`, "source_invalid"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := replayDetailReaderServer(t, func(w http.ResponseWriter, req *http.Request) {
				if tc.header != "" {
					w.Header().Set(replayDetailRemainingHeader, tc.header)
				}
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			})
			var gate atomic.Pointer[replayDetailGate]
			g := replayGate(rootedHistoryLiveStatus(d), time.Now())
			g.stamp = statusStamp{nodeEpoch: 1, familyEpoch: 1}
			gate.Store(&g)
			owner, err := newReplayDetailReader(newCollector(path).client, &gate)
			require.NoError(t, err)
			t.Cleanup(owner.client.CloseIdleConnections)
			r := &rootedHistoryReader{client: owner.client, gate: &gate, limit: rate.NewLimiter(4, 8)}
			r.collect(context.Background())
			require.Equal(t, tc.want, r.failure.Load().reason)
		})
	}
	r := &rootedHistoryReader{gate: &atomic.Pointer[replayDetailGate]{}, limit: rate.NewLimiter(0, 1)}
	for i, target := range []string{"/api/v1/details/slots?", "/api/v1/details/%73lots", "/api/v1/details/slots", "/api/v1/details/slots"} {
		w := httptest.NewRecorder()
		r.serveHTTP(w, httptest.NewRequest("GET", target, nil), allowReplayStatusForTest)
		want := http.StatusBadRequest
		if i == 2 {
			want = http.StatusServiceUnavailable
		}
		if i == 3 {
			want = http.StatusTooManyRequests
		}
		require.Equal(t, want, w.Code)
	}
}

func TestRootedHistoryReaderRejectsFutureCopyDate(t *testing.T) {
	d := rootedHistoryDetailFixture()
	d.CapturedAt = time.Now().Add(time.Hour).UTC()
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set(replayDetailRemainingHeader, "30000")
		body, err := json.Marshal(d)
		require.NoError(t, err)
		_, _ = w.Write(body)
	})
	var gate atomic.Pointer[replayDetailGate]
	g := replayGate(rootedHistoryLiveStatus(d), time.Now())
	g.stamp = statusStamp{nodeEpoch: 1, familyEpoch: 1}
	gate.Store(&g)
	owner, err := newReplayDetailReader(newCollector(path).client, &gate)
	require.NoError(t, err)
	t.Cleanup(owner.client.CloseIdleConnections)
	r := &rootedHistoryReader{client: owner.client, gate: &gate, limit: rate.NewLimiter(4, 8)}
	r.collect(context.Background())
	require.Equal(t, "source_time_invalid", r.failure.Load().reason)
	require.Nil(t, r.latest.Load())
}

func TestRootedHistoryPublicActualFeedAndShutdown(t *testing.T) {
	d := rootedHistoryDetailFixture()
	d.Epoch = &RootedHistoryEpoch{Number: 0, FirstSlot: 0, Slots: 32}
	var captures atomic.Int64
	path := socketPath(t)
	feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot { return rootedHistoryLiveStatus(d) }, DetailSamplers{
		RootedHistory: func(now time.Time) (RootedHistoryCapture, error) {
			captures.Add(1)
			copy := d
			copy.CapturedAt = now.UTC()
			return RootedHistoryCapture{Detail: copy, FreshUntil: now.Add(RootedHistoryFreshFor)}, nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(feed.Close)
	address, cancel, done := replayDetailPublicServer(t, path, PublicOptions{Resources: true, ReplayDetails: true})
	t.Cleanup(func() { replayDetailStopPublic(t, cancel, done) })
	client := &http.Client{Timeout: time.Second}
	get := func(host, origin string) (int, []byte, http.Header) {
		request, err := http.NewRequest("GET", address+"/api/v1/details/slots", nil)
		require.NoError(t, err)
		request.Host = host
		if origin != "" {
			request.Header.Set("Origin", origin)
		}
		response, err := client.Do(request)
		require.NoError(t, err)
		body, err := io.ReadAll(io.LimitReader(response.Body, MaxRootedHistoryBytes+1))
		require.NoError(t, response.Body.Close())
		require.NoError(t, err)
		require.LessOrEqual(t, len(body), MaxRootedHistoryBytes)
		return response.StatusCode, body, response.Header
	}
	status, _, _ := get("wrong.example", "")
	require.Equal(t, http.StatusForbidden, status)
	status, _, _ = get("stats.example", "https://wrong.example")
	require.Equal(t, http.StatusForbidden, status)
	deadline := time.Now().Add(3 * time.Second)
	for {
		status, body, header := get("stats.example", "https://stats.example")
		if status == http.StatusOK {
			var public PublicRootedHistoryDetail
			require.NoError(t, json.Unmarshal(body, &public))
			require.Equal(t, d.Bitmap, public.Bitmap)
			require.Equal(t, d.Epoch, public.Epoch)
			require.Equal(t, d.ObservedAt, public.ObservedAt)
			require.True(t, sameReplayDetailBinding(d.Binding, public.Binding))
			require.NotEmpty(t, header.Get(replayDetailRemainingHeader))
			break
		}
		require.True(t, time.Now().Before(deadline), string(body))
		time.Sleep(50 * time.Millisecond)
	}
	for range 4 {
		status, _, _ := get("stats.example", "")
		require.Equal(t, http.StatusOK, status)
	}
	require.Equal(t, int64(1), captures.Load(), "public viewers must not resample the node")
}

func TestRootedHistoryPublicCancellationJoinsRequest(t *testing.T) {
	d := rootedHistoryDetailFixture()
	entered, released := make(chan struct{}), make(chan struct{})
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/snapshot":
			_ = json.NewEncoder(w).Encode(rootedHistoryLiveStatus(d))
		case "/details/slots":
			close(entered)
			<-request.Context().Done()
			close(released)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	_, cancel, done := replayDetailPublicServer(t, path, PublicOptions{Resources: true, ReplayDetails: true})
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("rooted history request did not start")
	}
	replayDetailStopPublic(t, cancel, done)
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("rooted history request outlived reader shutdown")
	}
}

func TestRootedHistoryReaderMaximumRetainedStringBound(t *testing.T) {
	d := rootedHistoryDetailFixture()
	d.Binding.RunID = strings.Repeat("\x00", 128)
	d.Binding.Attempt, d.Binding.Rewinds, d.Binding.SourceGeneration = math.MaxUint64, math.MaxUint64, math.MaxUint64
	d.BankSlot, d.NextSlot = math.MaxUint64-1, math.MaxUint64
	d.OldestSlot = d.NextSlot - RootedHistoryCapacity
	// All bits set also exercises the current-bank check at the uint64 limit.
	d.Bitmap = strings.Repeat("/", 174762) + "8="
	body, err := json.Marshal(d)
	require.NoError(t, err)
	require.NoError(t, validateRootedHistoryDetail(d))
	path := replayDetailReaderServer(t, func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set(replayDetailRemainingHeader, "30000")
		_, _ = w.Write(body)
	})
	var gate atomic.Pointer[replayDetailGate]
	g := replayGate(rootedHistoryLiveStatus(d), time.Now())
	g.stamp = statusStamp{nodeEpoch: 1, familyEpoch: 1}
	gate.Store(&g)
	owner, err := newReplayDetailReader(newCollector(path).client, &gate)
	require.NoError(t, err)
	t.Cleanup(owner.client.CloseIdleConnections)
	r := &rootedHistoryReader{client: owner.client, gate: &gate, limit: rate.NewLimiter(4, 8)}
	r.collect(context.Background())
	require.Nil(t, r.failure.Load())
	p := r.latest.Load()
	require.NotNil(t, p)
	require.LessOrEqual(t, len(p.body), MaxRootedHistoryBytes)
	require.LessOrEqual(t, len(p.body)+len(p.detail.Bitmap)+len(p.detail.Binding.RunID)+len(p.detail.Bankhash), 2*MaxRootedHistoryBytes)
	require.Same(t, p, r.lastGood, "cache must not retain a second encoded variant")
}
