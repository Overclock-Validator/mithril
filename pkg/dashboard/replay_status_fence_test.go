package dashboard

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// Unit tests without a collector still exercise each reader's final gate check.
func allowReplayStatusForTest(stamp statusStamp, readGate func() (statusStamp, bool)) (time.Duration, bool) {
	current, eligible := readGate()
	return 10 * time.Second, eligible && stamp.valid() && stamp == current
}

func TestRootedHistoryFinalAdmission(t *testing.T) {
	setup := func() (*collector, *rootedHistoryReader) {
		now := time.Now()
		detail := rootedHistoryDetailFixture()
		c := &collector{clockOrigin: now}
		gate := replayGate(rootedHistoryLiveStatus(detail), now)
		gate.stamp = statusStamp{nodeEpoch: 1, familyEpoch: 1}
		c.detailGate.Store(&gate)
		frame := &frameSet{freshUntil: now.Add(2 * time.Second), clockOrigin: now, clockUncertain: &c.clockUncertain}
		frame.fence.nodeEpoch, frame.fence.families[statusReplay] = 1, 1
		c.latest.Store(frame)
		r := &rootedHistoryReader{gate: &c.detailGate, limit: rate.NewLimiter(100, 100)}
		r.latest.Store(&publicRootedHistory{detail: detail, body: `{"state":"fresh"}`, freshUntil: now.Add(20 * time.Second), stamp: gate.stamp})
		return c, r
	}
	request := func(r *rootedHistoryReader, admit func(statusStamp, func() (statusStamp, bool)) (time.Duration, bool)) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r.serveHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/details/slots", nil), admit)
		return w
	}
	admitFor := func(c *collector) func(statusStamp, func() (statusStamp, bool)) (time.Duration, bool) {
		return func(stamp statusStamp, gate func() (statusStamp, bool)) (time.Duration, bool) {
			return c.admitLiveDetail(statusReplay, stamp, gate, time.Now())
		}
	}
	t.Run("independent leases", func(t *testing.T) {
		c, r := setup()
		w := request(r, admitFor(c))
		statusMS, statusErr := strconv.ParseInt(w.Header().Get(statusRemainingHeader), 10, 64)
		detailMS, detailErr := strconv.ParseInt(w.Header().Get(replayDetailRemainingHeader), 10, 64)
		if w.Code != http.StatusOK || statusErr != nil || detailErr != nil || statusMS <= 0 || detailMS <= statusMS {
			t.Fatal("status and detail leases were not independently retained")
		}
		if request(r, nil).Code != http.StatusServiceUnavailable {
			t.Fatal("missing admission exposed a cached capture")
		}
	})
	for name, change := range map[string]func(*collector, *rootedHistoryReader){
		"gate changed": func(c *collector, r *rootedHistoryReader) {
			gate := *r.gate.Load()
			gate.stamp = statusStamp{nodeEpoch: 2, familyEpoch: 2}
			c.detailGate.Store(&gate)
		},
		"capture replaced": func(_ *collector, r *rootedHistoryReader) {
			copy := *r.latest.Load()
			r.latest.Store(&copy)
		},
		"capture failed": func(_ *collector, r *rootedHistoryReader) {
			r.failure.Store(&replayDetailCaptureFailure{gate: *r.gate.Load(), reason: "source_unavailable"})
		},
		"failure then replacement": func(_ *collector, r *rootedHistoryReader) {
			r.failure.Store(&replayDetailCaptureFailure{gate: *r.gate.Load(), reason: "source_unavailable"})
			replacement := *r.latest.Load()
			r.latest.Store(&replacement)
			r.failure.Store(nil)
		},
		"status expired": func(c *collector, _ *rootedHistoryReader) {
			frame := *c.latest.Load()
			frame.freshUntil = time.Now().Add(-time.Second)
			c.latest.Store(&frame)
		},
		"detail expired": func(_ *collector, r *rootedHistoryReader) {
			// This runs synchronously after selection, with no other test goroutine.
			r.latest.Load().freshUntil = time.Now().Add(-time.Second)
		},
	} {
		t.Run(name, func(t *testing.T) {
			c, r := setup()
			admit := func(stamp statusStamp, gate func() (statusStamp, bool)) (time.Duration, bool) {
				change(c, r)
				return c.admitLiveDetail(statusReplay, stamp, gate, time.Now())
			}
			w := request(r, admit)
			if w.Code != http.StatusServiceUnavailable || w.Header().Get(statusRemainingHeader) != "" || w.Header().Get(replayDetailRemainingHeader) != "" {
				t.Fatal("changed or expired status/detail was admitted")
			}
		})
	}
}

func TestHeldScheduleFinalAdmission(t *testing.T) {
	setup := func(t *testing.T) (*collector, *heldScheduleReader) {
		t.Helper()
		now := time.Now()
		fixture, data := newHeldReaderFixture(t)
		c := &collector{clockOrigin: now}
		gate := replayGate(rootedHistoryLiveStatus(rootedHistoryDetailFixture()), now)
		gate.stamp = statusStamp{nodeEpoch: 1, familyEpoch: 1}
		c.detailGate.Store(&gate)
		frame := &frameSet{freshUntil: now.Add(2 * time.Second), clockOrigin: now, clockUncertain: &c.clockUncertain}
		frame.fence.nodeEpoch, frame.fence.families[statusReplay] = 1, 1
		c.latest.Store(frame)
		r := &heldScheduleReader{gate: &c.detailGate, manifestLimit: rate.NewLimiter(100, 100), dataLimit: rate.NewLimiter(100, 100), active: make(chan struct{}, 2)}
		r.latest.Store(&publicHeldSchedule{manifest: fixture.manifest, body: `{"state":"fresh"}`, data: data, freshUntil: now.Add(20 * time.Second), stamp: gate.stamp})
		return c, r
	}
	admitFor := func(c *collector) func(statusStamp, func() (statusStamp, bool)) (time.Duration, bool) {
		return func(stamp statusStamp, gate func() (statusStamp, bool)) (time.Duration, bool) {
			return c.admitLiveDetail(statusReplay, stamp, gate, time.Now())
		}
	}
	request := func(r *heldScheduleReader, path string, admit func(statusStamp, func() (statusStamp, bool)) (time.Duration, bool)) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r.serveHTTP(w, httptest.NewRequest(http.MethodGet, path, nil), admit)
		return w
	}
	t.Run("manifest and data keep independent leases", func(t *testing.T) {
		c, r := setup(t)
		for _, path := range []string{"/api/v1/details/schedule", "/api/v1/details/schedule/data/" + r.latest.Load().data.digest} {
			w := request(r, path, admitFor(c))
			statusMS, statusErr := strconv.ParseInt(w.Header().Get(statusRemainingHeader), 10, 64)
			detailMS, detailErr := strconv.ParseInt(w.Header().Get(replayDetailRemainingHeader), 10, 64)
			if w.Code != http.StatusOK || statusErr != nil || detailErr != nil || statusMS <= 0 || detailMS <= statusMS {
				t.Fatal("status and schedule leases were not independently retained")
			}
		}
		if request(r, "/api/v1/details/schedule", nil).Code != http.StatusServiceUnavailable {
			t.Fatal("missing admission exposed a cached schedule")
		}
	})
	for name, change := range map[string]func(*collector, *heldScheduleReader){
		"gate changed": func(c *collector, r *heldScheduleReader) {
			gate := *r.gate.Load()
			gate.stamp = statusStamp{nodeEpoch: 2, familyEpoch: 2}
			c.detailGate.Store(&gate)
		},
		"capture replaced": func(_ *collector, r *heldScheduleReader) {
			copy := *r.latest.Load()
			r.latest.Store(&copy)
		},
		"capture failed": func(_ *collector, r *heldScheduleReader) {
			r.failure.Store(&replayDetailCaptureFailure{gate: *r.gate.Load(), reason: "source_unavailable"})
		},
		"failure then replacement": func(_ *collector, r *heldScheduleReader) {
			r.failure.Store(&replayDetailCaptureFailure{gate: *r.gate.Load(), reason: "source_unavailable"})
			replacement := *r.latest.Load()
			r.latest.Store(&replacement)
			r.failure.Store(nil)
		},
		"status expired": func(c *collector, _ *heldScheduleReader) {
			frame := *c.latest.Load()
			frame.freshUntil = time.Now().Add(-time.Second)
			c.latest.Store(&frame)
		},
		"detail expired": func(_ *collector, r *heldScheduleReader) {
			r.latest.Load().freshUntil = time.Now().Add(-time.Second)
		},
	} {
		t.Run(name, func(t *testing.T) {
			c, r := setup(t)
			admit := func(stamp statusStamp, gate func() (statusStamp, bool)) (time.Duration, bool) {
				change(c, r)
				return c.admitLiveDetail(statusReplay, stamp, gate, time.Now())
			}
			w := request(r, "/api/v1/details/schedule/data/"+r.latest.Load().data.digest, admit)
			if w.Code != http.StatusServiceUnavailable || w.Header().Get(statusRemainingHeader) != "" || w.Header().Get(replayDetailRemainingHeader) != "" {
				t.Fatal("changed or expired status/schedule was admitted")
			}
		})
	}
}

func TestReplayDetailEpochRevalidationKeepsOriginalLeases(t *testing.T) {
	now := time.Now()
	core := replayDetailReaderWindow(2, 1, now)
	r := replayDetailReaderFixture(t, core, now, "/unused")
	replayDetailReaderAccept(t, r, core, now, 30*time.Second)
	original := r.items[0]
	query := "generation=" + original.generation + "&page=0"
	if r.lookup(query, false, time.Now()).status != http.StatusOK {
		t.Fatal("original page unavailable")
	}
	gate := *r.gate.Load()
	gate.ObservedAt = time.Now()
	r.gate.Store(&gate)
	if r.lookup(query, false, time.Now()).status != http.StatusOK {
		t.Fatal("ordinary status refresh lost an advertised page")
	}
	gate.stamp = statusStamp{nodeEpoch: 2, familyEpoch: 2}
	r.gate.Store(&gate)
	if r.lookup(query, false, time.Now()).status == http.StatusOK {
		t.Fatal("old capture was visible under an unvalidated status epoch")
	}
	started := time.Now()
	candidate, reason := r.prepare(core, original.generation, gate, started, started.Add(20*time.Second), time.Now())
	if reason != "" {
		t.Fatal(reason)
	}
	if reason = r.commit(candidate, time.Now()); reason != "" {
		t.Fatal(reason)
	}
	current := r.items[0]
	if current.stamp != gate.stamp || current.holdUntil != original.holdUntil || current.publishedAt != original.publishedAt ||
		current.sourceFreshUntil.After(original.sourceFreshUntil) || r.lookup(query, false, time.Now()).status != http.StatusOK {
		t.Fatal("validated capture lost or extended its original leases")
	}
	started = time.Now()
	shorter := started.Add(15 * time.Second)
	shortCandidate, reason := r.prepare(core, original.generation, gate, started, shorter, time.Now())
	if reason != "" || r.commit(shortCandidate, time.Now()) != "" || r.items[0].sourceFreshUntil.After(shorter) ||
		r.items[0].holdUntil != original.holdUntil || r.lookup(query, false, time.Now()).status != http.StatusOK {
		t.Fatal("shorter source lease was ignored or revoked the advertised page")
	}
	gate.stamp = statusStamp{nodeEpoch: 3, familyEpoch: 3}
	r.gate.Store(&gate)
	if reason := r.commit(candidate, time.Now()); reason != "binding_changed" {
		t.Fatal("in-flight capture crossed a replacement status epoch")
	}
}

func TestReplayDetailFinalAdmission(t *testing.T) {
	setup := func(t *testing.T) (*collector, *replayDetailReader) {
		t.Helper()
		now := time.Now()
		core := replayDetailReaderWindow(2, 1, now)
		r := replayDetailReaderFixture(t, core, now, "/unused")
		replayDetailReaderAccept(t, r, core, now, 30*time.Second)
		c := &collector{clockOrigin: now}
		c.detailGate.Store(r.gate.Load())
		r.gate = &c.detailGate
		frame := &frameSet{freshUntil: now.Add(2 * time.Second), clockOrigin: now, clockUncertain: &c.clockUncertain}
		frame.fence.nodeEpoch, frame.fence.families[statusReplay] = 1, 1
		c.latest.Store(frame)
		return c, r
	}
	admitFor := func(c *collector) func(statusStamp, func() (statusStamp, bool)) (time.Duration, bool) {
		return func(stamp statusStamp, gate func() (statusStamp, bool)) (time.Duration, bool) {
			return c.admitLiveDetail(statusReplay, stamp, gate, time.Now())
		}
	}
	t.Run("manifest and page use separate leases", func(t *testing.T) {
		c, r := setup(t)
		paths := []string{"/api/v1/details/replay", "/api/v1/details/replay?generation=" + r.items[0].generation + "&page=0"}
		for i, path := range paths {
			w := httptest.NewRecorder()
			r.serveHTTP(w, httptest.NewRequest(http.MethodGet, path, nil), admitFor(c))
			statusMS, statusErr := strconv.ParseInt(w.Header().Get(statusRemainingHeader), 10, 64)
			if w.Code != http.StatusOK || statusErr != nil || statusMS <= 0 {
				t.Fatal("fresh status lease missing")
			}
			if i == 0 {
				detailMS, err := strconv.ParseInt(w.Header().Get(replayDetailRemainingHeader), 10, 64)
				if err != nil || detailMS <= statusMS {
					t.Fatal("replay detail lease was shortened to the status lease")
				}
			} else if w.Header().Get(replayDetailRemainingHeader) != "" {
				t.Fatal("retained page invented a separate detail lease")
			}
		}
		w := httptest.NewRecorder()
		r.serveHTTP(w, httptest.NewRequest(http.MethodGet, paths[0], nil), nil)
		if w.Code != http.StatusServiceUnavailable {
			t.Fatal("missing final admission exposed replay data")
		}
	})
	t.Run("changed status aborts a bundle", func(t *testing.T) {
		c, r := setup(t)
		path := "/api/v1/details/replay?generation=" + r.items[0].generation + "&bundle=1"
		sink := &replayBundleSink{header: make(http.Header), onFirstWrite: func() {
			gate := *r.gate.Load()
			gate.stamp = statusStamp{nodeEpoch: 2, familyEpoch: 2}
			c.detailGate.Store(&gate)
		}}
		r.serveHTTP(sink, httptest.NewRequest(http.MethodGet, path, nil), admitFor(c))
		length, err := strconv.Atoi(sink.header.Get("Content-Length"))
		if sink.status != http.StatusOK || err != nil || sink.bytes == 0 || sink.bytes >= length {
			t.Fatal("bundle did not stop after its status binding changed")
		}
	})
	t.Run("retained page survives a detail fetch failure", func(t *testing.T) {
		c, r := setup(t)
		admit := func(stamp statusStamp, gate func() (statusStamp, bool)) (time.Duration, bool) {
			r.mu.Lock()
			r.failure = &replayDetailCaptureFailure{gate: *r.gate.Load(), reason: "source_unavailable"}
			r.mu.Unlock()
			return c.admitLiveDetail(statusReplay, stamp, gate, time.Now())
		}
		w := httptest.NewRecorder()
		path := "/api/v1/details/replay?generation=" + r.items[0].generation + "&page=0"
		r.serveHTTP(w, httptest.NewRequest(http.MethodGet, path, nil), admit)
		if w.Code != http.StatusOK {
			t.Fatal("a later detail failure revoked an advertised page")
		}
	})
	t.Run("retained page survives a shorter same-generation refresh", func(t *testing.T) {
		c, r := setup(t)
		admit := func(stamp statusStamp, gate func() (statusStamp, bool)) (time.Duration, bool) {
			r.mu.Lock()
			updated := *r.items[0]
			updated.sourceFreshUntil = updated.sourceFreshUntil.Add(-time.Second)
			r.items[0] = &updated
			r.mu.Unlock()
			return c.admitLiveDetail(statusReplay, stamp, gate, time.Now())
		}
		w := httptest.NewRecorder()
		path := "/api/v1/details/replay?generation=" + r.items[0].generation + "&page=0"
		r.serveHTTP(w, httptest.NewRequest(http.MethodGet, path, nil), admit)
		if w.Code != http.StatusOK {
			t.Fatal("a shorter same-generation refresh revoked an advertised page")
		}
	})
}

func TestSelectedWindowFinalAdmissionAndPageRetention(t *testing.T) {
	now := time.Now()
	core := replayDetailLiveCore(0)
	window := selectedWindowFixture(1)
	window.Binding, window.CapturedAt, window.ObservedAt = core.Binding, now.UTC(), now.UTC()
	prepared, err := prepareSelectedWindow(window)
	if err != nil {
		t.Fatal(err)
	}
	publication := &replayDetailPrepared{binding: window.Binding, generation: prepared.digest, capturedAt: now, pages: prepared.pages, bytes: prepared.bytes}
	for i := range publication.manifests {
		publication.manifests[i] = prepared.manifest
	}
	c := &collector{clockOrigin: now}
	gate := replayGate(replayDetailLiveStatus(core), now)
	gate.revision, gate.stamp = 1, statusStamp{nodeEpoch: 1, familyEpoch: 1}
	c.detailGate.Store(&gate)
	frame := &frameSet{freshUntil: now.Add(2 * time.Second), clockOrigin: now, clockUncertain: &c.clockUncertain}
	frame.fence.nodeEpoch, frame.fence.families[statusReplay] = 1, 1
	c.latest.Store(frame)
	r := &selectedWindowReader{gate: &c.detailGate, revision: 1, limit: rate.NewLimiter(100, 100), active: make(chan struct{}, 4)}
	if err := r.cache.publish(publication, gate, now); err != nil {
		t.Fatal(err)
	}
	admit := func(stamp statusStamp, gate func() (statusStamp, bool)) (time.Duration, bool) {
		return c.admitLiveDetail(statusReplay, stamp, gate, time.Now())
	}
	request := func(path string, admission func(statusStamp, func() (statusStamp, bool)) (time.Duration, bool)) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r.serveHTTP(w, httptest.NewRequest(http.MethodGet, path, nil), admission)
		return w
	}
	page := "/api/v1/details/selected?generation=" + prepared.digest + "&page=0"
	for _, path := range []string{"/api/v1/details/selected", page} {
		w := request(path, admit)
		if w.Code != http.StatusOK || w.Header().Get(statusRemainingHeader) == "" {
			t.Fatal("fresh selected data did not carry its status lease")
		}
	}
	if request(page, nil).Code == http.StatusOK {
		t.Fatal("missing admission exposed an advertised page")
	}
	gate.ObservedAt = time.Now()
	c.detailGate.Store(&gate)
	if request(page, admit).Code != http.StatusOK {
		t.Fatal("ordinary status refresh revoked an advertised page")
	}
	refresh := func(stamp statusStamp, readGate func() (statusStamp, bool)) (time.Duration, bool) {
		r.cache.mu.Lock()
		updated := *r.cache.items[0]
		prepared := *updated.prepared
		prepared.capturedAt = prepared.capturedAt.Add(-time.Second)
		updated.prepared = &prepared
		r.cache.items[0] = &updated
		r.cache.mu.Unlock()
		return c.admitLiveDetail(statusReplay, stamp, readGate, time.Now())
	}
	if request(page, refresh).Code != http.StatusOK {
		t.Fatal("a shorter same-generation refresh revoked an advertised selected page")
	}
	original := r.cache.items[0]
	gate.stamp = statusStamp{nodeEpoch: 2, familyEpoch: 2}
	c.detailGate.Store(&gate)
	if request(page, admit).Code == http.StatusOK {
		t.Fatal("old page was exposed under an unvalidated status epoch")
	}
	if err := r.cache.publish(publication, gate, time.Now()); err != nil {
		t.Fatal(err)
	}
	current := r.cache.items[0]
	frameB := *frame
	frameB.fence.nodeEpoch, frameB.fence.families[statusReplay] = 2, 2
	c.latest.Store(&frameB)
	if current.stamp != gate.stamp || current.holdUntil != original.holdUntil ||
		current.prepared.capturedAt.After(original.prepared.capturedAt) || request(page, admit).Code != http.StatusOK {
		t.Fatal("revalidation changed the original page lease or failed to restore the page")
	}
	changed := func(stamp statusStamp, readGate func() (statusStamp, bool)) (time.Duration, bool) {
		gate.stamp = statusStamp{nodeEpoch: 3, familyEpoch: 3}
		c.detailGate.Store(&gate)
		return c.admitLiveDetail(statusReplay, stamp, readGate, time.Now())
	}
	if w := request(page, changed); w.Code == http.StatusOK || w.Header().Get(statusRemainingHeader) != "" {
		t.Fatal("status changed after selection but the page was served")
	}
}
