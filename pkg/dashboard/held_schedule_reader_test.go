package dashboard

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestDetailFailureReaders(t *testing.T) {
	for _, tc := range []struct {
		family string
		decode func([]byte) string
	}{
		{"held_schedule", decodeHeldScheduleFailure},
		{"rooted_history", decodeRootedHistoryFailure},
		{"local_peers", decodeLocalPeerFailure},
		{"network", decodeNetworkDetailFailure},
	} {
		t.Run(tc.family, func(t *testing.T) {
			body := `{"schema_version":1,"family":"` + tc.family + `","state":"unavailable","reason":"waiting_source"}`
			if got := tc.decode([]byte(body)); got != "waiting_source" {
				t.Fatalf("valid reason: %q", got)
			}
			for _, bad := range []string{" " + body, strings.Replace(body, tc.family, "other", 1), strings.Replace(body, "waiting_source", "invalid_request", 1), strings.Replace(body, `"state":`, `"extra":1,"state":`, 1), strings.Replace(body, `"state":`, `"state":"unavailable","state":`, 1), body + strings.Repeat(" ", 512)} {
				if got := tc.decode([]byte(bad)); got != "source_invalid" {
					t.Fatalf("invalid response: %q", got)
				}
			}
		})
	}
	rateLimited := `{"schema_version":1,"family":"held_schedule","state":"unavailable","reason":"rate_limited"}`
	if got := decodeHeldScheduleFailure([]byte(rateLimited)); got != "source_unavailable" {
		t.Fatalf("rate-limited response: %q", got)
	}
	if got := decodeHeldScheduleFailure([]byte(" " + rateLimited)); got != "source_invalid" {
		t.Fatalf("noncanonical rate-limited response: %q", got)
	}
}

type heldReaderFixture struct {
	mu                             sync.Mutex
	manifest                       HeldScheduleManifest
	data                           string
	manifestStatus, dataStatus     int
	manifestHeader, dataHeader     []string
	manifestDelay, dataDelay       time.Duration
	manifestRaw                    string
	etag                           string
	manifestRequests, dataRequests atomic.Int64
}

func newHeldReaderFixture(t *testing.T) (*heldReaderFixture, *PreparedHeldScheduleData) {
	t.Helper()
	return newHeldReaderDataFixture(t, heldScheduleDataFixture(t))
}

func newHeldReaderDataFixture(t *testing.T, d HeldScheduleData) (*heldReaderFixture, *PreparedHeldScheduleData) {
	t.Helper()
	p, err := PrepareHeldScheduleData(d)
	require.NoError(t, err)
	now := time.Now()
	m, _, err := prepareHeldScheduleManifest(HeldScheduleCapture{Data: p, CapturedAt: now.UTC(), FreshUntil: now.Add(HeldScheduleFreshFor),
		ReferenceSlot: 3, ReferenceBankhash: d.GeometryBankhash, ReferenceCompletedAt: d.GeometryBankCompletedAt}, 1, now)
	require.NoError(t, err)
	return &heldReaderFixture{manifest: m, data: p.body, manifestStatus: 200, dataStatus: 200,
		manifestHeader: []string{"30000"}, dataHeader: []string{"30000"}, etag: `"` + p.digest + `"`}, p
}

func TestHeldScheduleReaderReplacementAndInterruptedFetch(t *testing.T) {
	f, old := newHeldReaderFixture(t)
	r, _ := heldReaderForServer(t, f.serve)
	r.collect(context.Background())
	require.Nil(t, r.failure.Load())
	d := heldScheduleDataFixture(t)
	d.Binding.SourceGeneration++
	d.Next, d.NextReason = nil, "waiting_source"
	next, replacement := newHeldReaderDataFixture(t, d)
	next.manifest.CaptureSequence = 2
	f.mu.Lock()
	f.manifest, f.data, f.etag = next.manifest, next.data, next.etag
	f.dataStatus, f.dataHeader = 503, nil
	f.data = `{"schema_version":1,"family":"held_schedule","state":"unavailable","reason":"binding_changed"}`
	f.mu.Unlock()
	r.collect(context.Background())
	require.Equal(t, "binding_changed", r.failure.Load().reason)
	require.Equal(t, old.digest, r.latest.Load().data.digest, "an incomplete replacement must not publish")
	require.Equal(t, http.StatusServiceUnavailable, heldPublicResponse(r, "/api/v1/details/schedule").Code)
	f.mu.Lock()
	f.data, f.dataStatus, f.dataHeader = next.data, 200, []string{"30000"}
	f.mu.Unlock()
	r.collect(context.Background())
	require.Nil(t, r.failure.Load())
	require.Equal(t, replacement.body, r.latest.Load().data.body)
	require.Equal(t, http.StatusServiceUnavailable, heldPublicResponse(r, "/api/v1/details/schedule/data/"+old.digest).Code)
	require.Equal(t, replacement.body, heldPublicResponse(r, "/api/v1/details/schedule/data/"+replacement.digest).Body.String())
	require.Equal(t, int64(3), f.dataRequests.Load())
}

func TestHeldScheduleReaderMaximumDatasetRealUnixTransfer(t *testing.T) {
	d := heldScheduleMaximumDataFixture(t)
	d.Current.StakeSet, d.Next.StakeSet = heldStakeSetFixture(2000), heldStakeSetFixture(2000)
	date := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	d.Current.LocalReplay = &HeldEpochReplayTimes{StartedAt: &date, EndedAt: &date}
	d.Next.LocalReplay = &HeldEpochReplayTimes{StartedAt: &date, EndedAt: &date}
	d.Current.TargetSlotDuration = &HeldEpochTargetDuration{Nanoseconds: ^uint64(0), Provenance: "snapshot_seed_and_bank_features"}
	d.Next.TargetSlotDuration = &HeldEpochTargetDuration{Nanoseconds: ^uint64(0), Provenance: "snapshot_seed_and_bank_features"}
	f, p := newHeldReaderDataFixture(t, d)
	r, _ := heldReaderForServer(t, f.serve)
	g := *r.gate.Load()
	g.RunID = d.Binding.RunID
	r.gate.Store(&g)
	r.collect(context.Background())
	require.Nil(t, r.failure.Load())
	require.Equal(t, p.digest, r.latest.Load().data.digest)
	response := heldPublicResponse(r, "/api/v1/details/schedule/data/"+p.digest)
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, p.body, response.Body.String())
	require.LessOrEqual(t, response.Body.Len(), MaxHeldScheduleDataBytes)
	first := r.latest.Load()
	r.collect(context.Background())
	require.Same(t, first.data, r.latest.Load().data)
	require.Equal(t, int64(1), f.dataRequests.Load())
	t.Logf("maximum private/public dataset transferred without truncation: %d bytes", len(p.body))
}

func TestHeldSchedulePublicActualFeedOptInAndOriginGates(t *testing.T) {
	d := heldScheduleDataFixture(t)
	d.Current.StakeSet = heldStakeSetFixture(2000)
	d.Next.StakeSetReason = "selection_unverified"
	p, err := PrepareHeldScheduleData(d)
	require.NoError(t, err)
	var samples atomic.Int64
	path := socketPath(t)
	feed, err := ListenFeedWithDetails(context.Background(), path, "", func() Snapshot {
		return rootedHistoryLiveStatus(rootedHistoryDetailFixture())
	}, DetailSamplers{
		Replay: func(now time.Time) (ReplayDetailCore, error) {
			c := replayDetailTestCore(0)
			c.Binding, c.CapturedAt, c.UpdatedAt = d.Binding, now.UTC(), now.UTC()
			return c, nil
		},
		HeldSchedules: func(now time.Time) (HeldScheduleCapture, error) {
			samples.Add(1)
			return HeldScheduleCapture{Data: p, CapturedAt: now.UTC(), FreshUntil: now.Add(HeldScheduleFreshFor),
				ReferenceSlot: 3, ReferenceBankhash: d.GeometryBankhash, ReferenceCompletedAt: d.GeometryBankCompletedAt}, nil
		},
	})
	require.NoError(t, err)
	t.Cleanup(feed.Close)
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "default-off", true: "enabled"}[enabled], func(t *testing.T) {
			address, cancel, done := replayDetailPublicServer(t, path, PublicOptions{Resources: true, ReplayDetails: enabled})
			t.Cleanup(func() { replayDetailStopPublic(t, cancel, done) })
			status, body, _ := replayDetailPublicRequest(t, address, "/api/v1/features?include=advanced_status", "stats.example", "")
			require.Equal(t, http.StatusOK, status)
			var features map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(body, &features))
			require.Equal(t, map[bool]string{false: "", true: "true"}[enabled], string(features["held_schedule_details"]))
			if !enabled {
				for _, target := range []string{"/api/v1/details/schedule", "/api/v1/details/schedule/data/" + p.digest} {
					status, _, _ = replayDetailPublicRequest(t, address, target, "stats.example", "")
					require.Equal(t, http.StatusNotFound, status)
				}
				return
			}
			deadline := time.Now().Add(3 * time.Second)
			for {
				status, body, _ = replayDetailPublicRequest(t, address, "/api/v1/details/schedule", "stats.example", "")
				if status == http.StatusOK {
					break
				}
				require.True(t, time.Now().Before(deadline), "%d %s", status, body)
				time.Sleep(300 * time.Millisecond)
			}
			var manifest PublicHeldScheduleManifest
			require.NoError(t, json.Unmarshal(body, &manifest))
			require.Equal(t, p.digest, manifest.Digest)
			require.Equal(t, d.ObservedAt, manifest.ObservedAt)
			for _, target := range []string{"/api/v1/details/schedule", "/api/v1/details/schedule/data/" + p.digest} {
				for _, gate := range []struct{ host, origin string }{{"attacker.example", ""}, {"stats.example", "https://attacker.example"}} {
					status, _, _ := replayDetailPublicRequest(t, address, target, gate.host, gate.origin)
					require.Equal(t, http.StatusForbidden, status)
				}
			}
			request, err := http.NewRequest(http.MethodGet, address+"/api/v1/details/schedule/data/"+p.digest, nil)
			require.NoError(t, err)
			request.Host = "stats.example"
			request.Header.Set("Origin", "https://stats.example")
			response, err := (&http.Client{Timeout: 2 * time.Second}).Do(request)
			require.NoError(t, err)
			data, err := io.ReadAll(io.LimitReader(response.Body, MaxHeldScheduleDataBytes+1))
			require.NoError(t, response.Body.Close())
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, response.StatusCode)
			require.Equal(t, p.body, string(data))
			require.Equal(t, `"`+p.digest+`"`, response.Header.Get("ETag"))
			require.Equal(t, "no-store", response.Header.Get("Cache-Control"))
			require.Equal(t, int64(1), samples.Load(), "viewers cannot trigger the node sampler")
		})
	}
}

func (f *heldReaderFixture) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	var body []byte
	var status int
	var headers []string
	var delay time.Duration
	switch {
	case r.URL.Path == "/details/schedule":
		f.manifestRequests.Add(1)
		body, _ = json.Marshal(f.manifest)
		if f.manifestRaw != "" {
			body = []byte(f.manifestRaw)
		}
		status, headers, delay = f.manifestStatus, append([]string(nil), f.manifestHeader...), f.manifestDelay
	case strings.HasPrefix(r.URL.Path, "/details/schedule/data/"):
		f.dataRequests.Add(1)
		body, status, headers, delay = []byte(f.data), f.dataStatus, append([]string(nil), f.dataHeader...), f.dataDelay
		w.Header().Set("ETag", f.etag)
	default:
		f.mu.Unlock()
		http.NotFound(w, r)
		return
	}
	f.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	for _, header := range headers {
		w.Header().Add(replayDetailRemainingHeader, header)
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func heldReaderForServer(t *testing.T, handler http.HandlerFunc) (*heldScheduleReader, *replayDetailReader) {
	t.Helper()
	path := replayDetailReaderServer(t, handler)
	var gate atomic.Pointer[replayDetailGate]
	g := replayGate(rootedHistoryLiveStatus(rootedHistoryDetailFixture()), time.Now())
	g.stamp = statusStamp{nodeEpoch: 1, familyEpoch: 1}
	gate.Store(&g)
	owner, err := newReplayDetailReader(newCollector(path).client, &gate)
	require.NoError(t, err)
	t.Cleanup(owner.client.CloseIdleConnections)
	return &heldScheduleReader{client: owner.client, gate: &gate, manifestLimit: rate.NewLimiter(4, 8), dataLimit: rate.NewLimiter(0.2, 2), active: make(chan struct{}, 2)}, owner
}

func heldPublicResponse(r *heldScheduleReader, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r.serveHTTP(w, httptest.NewRequest("GET", path, nil), allowReplayStatusForTest)
	return w
}

func TestHeldScheduleReaderAccuracyReuseAndOriginalLease(t *testing.T) {
	f, p := newHeldReaderFixture(t)
	r, _ := heldReaderForServer(t, f.serve)
	r.collect(context.Background())
	require.Nil(t, r.failure.Load())
	first := r.latest.Load()
	require.NotNil(t, first)
	require.Equal(t, int64(1), f.dataRequests.Load())
	manifest := heldPublicResponse(r, "/api/v1/details/schedule")
	require.Equal(t, http.StatusOK, manifest.Code)
	var public PublicHeldScheduleManifest
	require.NoError(t, json.Unmarshal(manifest.Body.Bytes(), &public))
	require.Equal(t, p.digest, public.Digest)
	require.Equal(t, p.observedAt, public.ObservedAt)
	require.Equal(t, p.current, public.Current)
	require.True(t, sameReplayDetailBinding(p.binding, public.Binding))
	data := heldPublicResponse(r, "/api/v1/details/schedule/data/"+p.digest)
	require.Equal(t, http.StatusOK, data.Code)
	require.Equal(t, p.body, data.Body.String())
	require.Equal(t, int64(1), f.manifestRequests.Load(), "viewers cannot collect")
	gate := *r.gate.Load()
	gate.stamp = statusStamp{nodeEpoch: 2, familyEpoch: 2}
	r.gate.Store(&gate)
	require.Equal(t, http.StatusServiceUnavailable, heldPublicResponse(r, "/api/v1/details/schedule").Code)
	r.collect(context.Background())
	require.Nil(t, r.failure.Load())
	require.Equal(t, gate.stamp, r.latest.Load().stamp)
	require.False(t, r.latest.Load().freshUntil.After(first.freshUntil))
	require.Same(t, first.data, r.latest.Load().data)
	r.collect(context.Background())
	require.Nil(t, r.failure.Load())
	require.Equal(t, first.body, r.latest.Load().body)
	require.Equal(t, first.freshUntil, r.latest.Load().freshUntil)
	require.Same(t, first.data, r.latest.Load().data)
	f.mu.Lock()
	f.manifestHeader = []string{"500"}
	f.mu.Unlock()
	r.collect(context.Background())
	require.Nil(t, r.failure.Load())
	short := r.latest.Load()
	require.True(t, short.freshUntil.Before(first.freshUntil))
	f.mu.Lock()
	f.manifestStatus = 503
	f.manifestHeader = nil
	f.manifestRaw = `{"schema_version":1,"family":"held_schedule","state":"unavailable","reason":"source_unavailable"}`
	f.mu.Unlock()
	r.collect(context.Background())
	require.Equal(t, "source_unavailable", r.failure.Load().reason)
	f.mu.Lock()
	f.manifestStatus = 200
	f.manifestHeader = []string{"30000"}
	f.manifestRaw = ""
	f.mu.Unlock()
	r.collect(context.Background())
	require.Nil(t, r.failure.Load())
	require.Equal(t, short.freshUntil, r.latest.Load().freshUntil, "recovery cannot renew the same capture")
	expired := *r.latest.Load()
	expired.freshUntil = time.Now().Add(-time.Millisecond)
	r.latest.Store(&expired)
	r.collect(context.Background())
	require.Equal(t, "source_time_invalid", r.failure.Load().reason)
	// A new sequence may reuse immutable bytes under its own validated lease.
	f.mu.Lock()
	f.manifest.CaptureSequence++
	f.manifest.CapturedAt = time.Now().UTC()
	f.mu.Unlock()
	r.collect(context.Background())
	require.Nil(t, r.failure.Load())
	require.Same(t, first.data, r.latest.Load().data)
	require.True(t, r.latest.Load().freshUntil.After(short.freshUntil))
	require.NotEqual(t, first.body, r.latest.Load().body)
	require.Equal(t, int64(1), f.dataRequests.Load(), "a new capture with unchanged data must not refetch")
}

func TestHeldScheduleReaderRejectsInvalidMetadataDataAndBudgets(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*heldReaderFixture)
		want   string
		noData bool
	}{
		{"unsupported", func(f *heldReaderFixture) {
			f.manifestStatus = 404
			f.manifestHeader = nil
			f.manifestRaw = "not found"
		}, "unsupported_source", true},
		{"redirect", func(f *heldReaderFixture) { f.manifestStatus = 302; f.manifestHeader = nil; f.manifestRaw = "redirect" }, "source_invalid", true},
		{"oversize failure", func(f *heldReaderFixture) {
			f.manifestStatus = 503
			f.manifestHeader = nil
			f.manifestRaw = strings.Repeat("x", 513)
		}, "response_limit", true},
		{"missing lease", func(f *heldReaderFixture) { f.manifestHeader = nil }, "source_invalid", true},
		{"duplicate lease", func(f *heldReaderFixture) { f.manifestHeader = []string{"30000", "30000"} }, "source_invalid", true},
		{"noncanonical lease", func(f *heldReaderFixture) { f.manifestHeader = []string{"030000"} }, "source_invalid", true},
		{"invalid digest", func(f *heldReaderFixture) { f.manifest.Digest = strings.Repeat("A", 64) }, "source_invalid", true},
		{"zero sequence", func(f *heldReaderFixture) { f.manifest.CaptureSequence = 0 }, "source_invalid", true},
		{"oversize data", func(f *heldReaderFixture) { f.manifest.Bytes = MaxHeldScheduleDataBytes + 1 }, "source_invalid", true},
		{"invalid coverage", func(f *heldReaderFixture) { f.manifest.Current.KnownSlots = 9 }, "source_invalid", true},
		{"invalid reference", func(f *heldReaderFixture) { f.manifest.ReferenceSlot = 8 }, "source_invalid", true},
		{"future capture", func(f *heldReaderFixture) { f.manifest.CapturedAt = time.Now().Add(2 * time.Second).UTC() }, "source_time_invalid", true},
		{"hash mismatch", func(f *heldReaderFixture) {
			f.data = strings.Replace(f.data, `"geometry_bank_slot":"3"`, `"geometry_bank_slot":"2"`, 1)
		}, "source_invalid", false},
		{"wrong etag", func(f *heldReaderFixture) { f.etag = `"wrong"` }, "source_invalid", false},
		{"data unavailable", func(f *heldReaderFixture) {
			f.dataStatus = 503
			f.dataHeader = nil
			f.data = `{"schema_version":1,"family":"held_schedule","state":"unavailable","reason":"binding_changed"}`
		}, "binding_changed", false},
		{"manifest request start budget", func(f *heldReaderFixture) { f.manifestHeader = []string{"1"}; f.manifestDelay = 10 * time.Millisecond }, "source_time_invalid", true},
		{"short data deadline", func(f *heldReaderFixture) { f.dataHeader = []string{"1"}; f.dataDelay = 10 * time.Millisecond }, "source_time_invalid", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _ := newHeldReaderFixture(t)
			tc.change(f)
			r, _ := heldReaderForServer(t, f.serve)
			r.collect(context.Background())
			require.NotNil(t, r.failure.Load())
			require.Equal(t, tc.want, r.failure.Load().reason)
			require.Nil(t, r.latest.Load())
			if tc.noData {
				require.Zero(t, f.dataRequests.Load())
			}
		})
	}
}

func TestHeldScheduleReaderCacheHitStillValidatesAllMetadata(t *testing.T) {
	for _, change := range []func(*HeldScheduleManifest){
		func(m *HeldScheduleManifest) { m.Current.KnownSlots-- },
		func(m *HeldScheduleManifest) { m.ObservedAt = m.ObservedAt.Add(-time.Second) },
		func(m *HeldScheduleManifest) { m.Bytes++ },
		func(m *HeldScheduleManifest) { m.Next = nil; m.NextReason = "waiting_source" },
		func(m *HeldScheduleManifest) { m.Binding.SourceGeneration++ },
	} {
		f, _ := newHeldReaderFixture(t)
		r, _ := heldReaderForServer(t, f.serve)
		r.collect(context.Background())
		require.Nil(t, r.failure.Load())
		first := r.latest.Load()
		f.mu.Lock()
		f.manifest.CaptureSequence++
		change(&f.manifest)
		f.mu.Unlock()
		r.collect(context.Background())
		require.NotNil(t, r.failure.Load())
		require.Equal(t, "source_invalid", r.failure.Load().reason)
		require.Same(t, first, r.latest.Load())
		require.Equal(t, int64(1), f.dataRequests.Load())
	}
}

func TestHeldScheduleReaderSequenceAndContextFencesSurviveFailure(t *testing.T) {
	f, _ := newHeldReaderFixture(t)
	r, _ := heldReaderForServer(t, f.serve)
	f.manifest.CaptureSequence = 5
	r.collect(context.Background())
	require.Nil(t, r.failure.Load())
	first := r.latest.Load()
	f.mu.Lock()
	f.manifestHeader = nil
	f.mu.Unlock()
	r.collect(context.Background())
	require.NotNil(t, r.failure.Load())
	f.mu.Lock()
	f.manifestHeader = []string{"30000"}
	f.manifest.CaptureSequence = 4
	f.mu.Unlock()
	r.collect(context.Background())
	require.Equal(t, "binding_changed", r.failure.Load().reason)
	require.Same(t, first, r.latest.Load())
	f.mu.Lock()
	f.manifest.CaptureSequence = 5
	f.mu.Unlock()
	// Change valid metadata without changing its sequence.
	f.mu.Lock()
	f.manifest.ReferenceSlot = 4
	f.mu.Unlock()
	r.collect(context.Background())
	require.Equal(t, "source_invalid", r.failure.Load().reason)
	g := *r.gate.Load()
	g.Attempt++
	r.gate.Store(&g)
	r.collect(context.Background())
	require.Equal(t, "binding_changed", r.failure.Load().reason)
	require.Same(t, first, r.latest.Load())
}

func TestHeldScheduleReaderJoinsCancellationDuringDatasetFetch(t *testing.T) {
	f, _ := newHeldReaderFixture(t)
	entered := make(chan struct{})
	var once sync.Once
	r, owner := heldReaderForServer(t, func(w http.ResponseWriter, req *http.Request) {
		if strings.HasPrefix(req.URL.Path, "/details/schedule/data/") {
			once.Do(func() { close(entered) })
			<-req.Context().Done()
			return
		}
		f.serve(w, req)
	})
	owner.held = r
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go owner.run(ctx)
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("joined worker did not fetch dataset")
	}
	cancel()
	select {
	case <-owner.done:
	case <-time.After(3 * time.Second):
		t.Fatal("joined reader did not stop")
	}
	require.Nil(t, r.latest.Load())
	require.Nil(t, r.failure.Load())
}

func TestHeldScheduleReaderGenerationFenceAndNewContext(t *testing.T) {
	d := heldScheduleDataFixture(t)
	d.Binding.SourceGeneration = 2
	f, _ := newHeldReaderDataFixture(t, d)
	r, _ := heldReaderForServer(t, f.serve)
	r.collect(context.Background())
	require.Nil(t, r.failure.Load())
	accepted := r.latest.Load()
	f.mu.Lock()
	f.manifestHeader = nil
	f.mu.Unlock()
	r.collect(context.Background())
	d.Binding.SourceGeneration = 1
	lower, _ := newHeldReaderDataFixture(t, d)
	f.mu.Lock()
	f.manifest, f.data, f.etag = lower.manifest, lower.data, lower.etag
	f.manifest.CaptureSequence = 10
	f.manifestHeader = []string{"30000"}
	f.mu.Unlock()
	r.collect(context.Background())
	require.Equal(t, "binding_changed", r.failure.Load().reason, "a newer sequence cannot revive an earlier data generation")
	require.Same(t, accepted, r.latest.Load())

	var changeContext atomic.Bool
	var changing *heldScheduleReader
	changing, _ = heldReaderForServer(t, func(w http.ResponseWriter, req *http.Request) {
		if strings.HasPrefix(req.URL.Path, "/details/schedule/data/") && changeContext.CompareAndSwap(true, false) {
			gate := *changing.gate.Load()
			gate.Attempt++
			changing.gate.Store(&gate)
		}
		f.serve(w, req)
	})
	changeContext.Store(true)
	changing.collect(context.Background())
	require.Nil(t, changing.latest.Load(), "a context changed during data fetch cannot publish")
	require.Equal(t, "binding_changed", changing.failure.Load().reason)
	d.Binding.Attempt++
	next, _ := newHeldReaderDataFixture(t, d)
	f.mu.Lock()
	f.manifest, f.data, f.etag = next.manifest, next.data, next.etag
	f.mu.Unlock()
	changing.collect(context.Background())
	require.Nil(t, changing.failure.Load())
	require.Equal(t, uint64(1), changing.latest.Load().manifest.CaptureSequence, "a fully validated new context can start a new sequence")
	require.Equal(t, uint64(1), changing.latest.Load().manifest.Binding.SourceGeneration)
}

func TestHeldScheduleReaderPublicRequestAndConcurrencyLimits(t *testing.T) {
	f, p := newHeldReaderFixture(t)
	r, _ := heldReaderForServer(t, f.serve)
	r.collect(context.Background())
	require.Nil(t, r.failure.Load())
	for _, target := range []string{"/api/v1/details/schedule?", "/api/v1/details/%73chedule", "/api/v1/details/schedule/",
		"/api/v1/details/schedule/data/", "/api/v1/details/schedule/data/" + strings.Repeat("A", 64),
		"/api/v1/details/schedule/data/" + p.digest + "?x=1"} {
		response := heldPublicResponse(r, target)
		require.Equal(t, http.StatusBadRequest, response.Code, target)
		require.Empty(t, response.Header().Get(replayDetailRemainingHeader))
	}
	r.active <- struct{}{}
	r.active <- struct{}{}
	busy := heldPublicResponse(r, "/api/v1/details/schedule/data/"+p.digest)
	require.Equal(t, http.StatusTooManyRequests, busy.Code)
	require.Empty(t, busy.Header().Get(replayDetailRemainingHeader))
	<-r.active
	<-r.active
	r.manifestLimit = rate.NewLimiter(0, 1)
	require.Equal(t, http.StatusOK, heldPublicResponse(r, "/api/v1/details/schedule").Code)
	require.Equal(t, http.StatusTooManyRequests, heldPublicResponse(r, "/api/v1/details/schedule").Code)
	require.Equal(t, int64(1), f.manifestRequests.Load(), "limited viewers must not fetch the source")
}
