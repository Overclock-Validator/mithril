package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"golang.org/x/time/rate"
)

func TestActivityHistoryCurrentReaderGates(t *testing.T) {
	now := time.Now()
	core := replayDetailLiveCore(0)
	genesis := "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"
	core.Binding.GenesisHash = &genesis
	parent := &replayDetailSource{}
	selected := &selectedWindowSource{gate: &parent.gate, gateMu: &parent.gateMu, revision: 1}
	parent.selected = selected
	parent.observeStatus(replayDetailLiveStatus(core), now)
	source := &activityHistorySource{selected: selected, sample: func(_ context.Context, at time.Time) (ActivityHistory, error) {
		return ActivityHistory{SchemaVersion: 1, Binding: core.Binding, CapturedAt: at.UTC(), ObservedAt: now.UTC(),
			Rows: []ActivityHistoryInterval{{AnchorSlot: 10, LastSlot: 20, StartTime: now.Unix() - 11,
				EndTime: now.Unix() - 1, Blocks: 10, Transactions: 100, NonVote: 80, Failed: 5, FailedNonVote: 4}}}, nil
	}}
	source.poll(context.Background())
	var gate atomic.Pointer[replayDetailGate]
	g := replayGate(replayDetailLiveStatus(core), now)
	g.revision, g.stamp = 1, statusStamp{nodeEpoch: 1, familyEpoch: 1}
	gate.Store(&g)
	failed, replace := false, false
	client := &http.Client{Transport: selectedEpochTransport(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/details/activity-history" {
			t.Fatalf("unexpected private request: %s", request.URL.Path)
		}
		if failed {
			return nil, errors.New("synthetic fetch failure")
		}
		w := httptest.NewRecorder()
		source.serveHTTP(w, request)
		if replace {
			next := g
			next.revision += 2 // Same public binding after an intervening context must not revive.
			gate.Store(&next)
		}
		return w.Result(), nil
	})}
	s := &selectedWindowReader{client: client, gate: &gate, limit: rate.NewLimiter(16, 32), active: make(chan struct{}, 4)}
	r := &activityHistoryReader{selected: s, client: s.client, done: make(chan struct{})}
	r.collect(context.Background())
	request := func(path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r.serveHTTP(w, httptest.NewRequest(http.MethodGet, path, nil), allowReplayStatusForTest)
		return w
	}
	w := request("/api/v1/details/activity-history")
	if w.Code != http.StatusOK || w.Header().Get(statusRemainingHeader) == "" || w.Header().Get(replayDetailRemainingHeader) == "" {
		t.Fatalf("current history unavailable: %d %s", w.Code, w.Body.String())
	}
	h, err := decodeActivityHistory(w.Body.Bytes())
	if err != nil || h.Rows[0].Transactions != 100 || h.Rows[0].FailedNonVote != 4 || !sameReplayDetailBinding(h.Binding, core.Binding) {
		t.Fatalf("source counts/binding changed: %+v, %v", h, err)
	}
	firstUntil := r.latest.freshUntil
	r.collect(context.Background())
	if r.latest == nil || r.latest.freshUntil.After(firstUntil) {
		t.Fatal("duplicate private capture renewed its original reader lease")
	}
	if request("/api/v1/details/activity-history?x=1").Code != http.StatusBadRequest {
		t.Fatal("history accepted a query")
	}
	denied := httptest.NewRecorder()
	r.serveHTTP(denied, httptest.NewRequest(http.MethodGet, "/api/v1/details/activity-history", nil),
		func(statusStamp, func() (statusStamp, bool)) (time.Duration, bool) { return 0, false })
	if denied.Code != http.StatusServiceUnavailable {
		t.Fatal("history bypassed final parent-status admission")
	}
	for range cap(s.active) {
		s.active <- struct{}{}
	}
	if request("/api/v1/details/activity-history").Code != http.StatusTooManyRequests {
		t.Fatal("history bypassed the shared selected active-response cap")
	}
	for range cap(s.active) {
		<-s.active
	}
	failed = true
	r.collect(context.Background())
	if request("/api/v1/details/activity-history").Code != http.StatusServiceUnavailable {
		t.Fatal("fetch failure retained live history")
	}
	failed, replace = false, true
	r.collect(context.Background())
	if request("/api/v1/details/activity-history").Code != http.StatusServiceUnavailable {
		t.Fatal("in-flight A to B to A replacement revived history")
	}
	replace = false
	g = *gate.Load()
	gate.Store(&g)
	r.collect(context.Background())
	if r.latest == nil {
		t.Fatal("unchanged current source did not recover")
	}
	r.last.freshUntil = time.Now().Add(-time.Millisecond)
	r.collect(context.Background())
	if request("/api/v1/details/activity-history").Code != http.StatusServiceUnavailable {
		t.Fatal("repeated body revived an expired original lease")
	}
}

func TestActivityHistorySlowPrivateReadStopsWithContext(t *testing.T) {
	now := time.Now()
	core := replayDetailLiveCore(0)
	var gate atomic.Pointer[replayDetailGate]
	g := replayGate(replayDetailLiveStatus(core), now)
	g.revision, g.stamp = 1, statusStamp{nodeEpoch: 1, familyEpoch: 1}
	gate.Store(&g)
	entered := make(chan struct{})
	s := &selectedWindowReader{gate: &gate, client: &http.Client{Transport: selectedEpochTransport(func(request *http.Request) (*http.Response, error) {
		close(entered)
		<-request.Context().Done()
		return nil, request.Context().Err()
	})}}
	r := &activityHistoryReader{selected: s, client: s.client, done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); r.run(ctx) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("private read did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("slow private read outlived its canceled collection")
	}
	if r.latest != nil {
		t.Fatal("canceled read published activity")
	}
}

func TestActivityHistoryWorkerKeepsSiblingReader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		core := replayDetailLiveCore(0)
		var gate atomic.Pointer[replayDetailGate]
		g := replayGate(replayDetailLiveStatus(core), time.Now())
		g.revision, g.stamp = 1, statusStamp{nodeEpoch: 1, familyEpoch: 1}
		gate.Store(&g)
		var selectedReads atomic.Int32
		client := &http.Client{Transport: selectedEpochTransport(func(request *http.Request) (*http.Response, error) {
			if request.URL.Path == "/details/selected" {
				selectedReads.Add(1)
			}
			w := httptest.NewRecorder()
			w.WriteHeader(http.StatusServiceUnavailable)
			return w.Result(), nil
		})}
		selected := &selectedWindowReader{client: client, gate: &gate, last: &selectedWindowHighWater{}}
		sibling := &replayDetailReader{client: client, gate: &gate, done: make(chan struct{}),
			selected: selected, cursor: &replayDetailCursor{}}
		entered := make(chan struct{}, 1)
		activityClient := &http.Client{Transport: selectedEpochTransport(func(request *http.Request) (*http.Response, error) {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-request.Context().Done()
			return nil, request.Context().Err()
		})}
		activity := &activityHistoryReader{selected: selected, client: activityClient, done: make(chan struct{})}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		statusDone := make(chan struct{})
		go func() {
			defer close(statusDone)
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					next := g
					next.ObservedAt = time.Now()
					gate.Store(&next)
				}
			}
		}()
		go sibling.run(ctx)
		go activity.run(ctx)
		<-entered
		time.Sleep(replayDetailCaptureInterval + 500*time.Millisecond)
		if selectedReads.Load() < 2 {
			t.Fatal("blocked activity delayed the next sibling selected read")
		}
		cancel()
		for _, done := range []<-chan struct{}{sibling.done, activity.done, statusDone} {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("reader owner did not join cancellation")
			}
		}
	})
}

func TestActivityHistoryOnlyOnePromptRetry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var gate atomic.Pointer[replayDetailGate]
		g := replayGate(replayDetailLiveStatus(replayDetailLiveCore(0)), time.Now())
		g.revision, g.stamp = 1, statusStamp{nodeEpoch: 1, familyEpoch: 1}
		gate.Store(&g)
		var reads atomic.Int32
		client := &http.Client{Transport: selectedEpochTransport(func(*http.Request) (*http.Response, error) {
			reads.Add(1)
			return nil, errors.New("synthetic transient read failure")
		})}
		r := &activityHistoryReader{selected: &selectedWindowReader{gate: &gate}, client: client, done: make(chan struct{})}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go r.run(ctx)
		time.Sleep(4 * time.Second)
		if reads.Load() != 2 {
			t.Fatalf("prompt retries = %d reads, want initial plus one", reads.Load())
		}
		cancel()
		<-r.done
	})
}

func TestActivityHistoryPromptRecoveryKeepsOriginalLease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		now := time.Now()
		core := replayDetailLiveCore(0)
		genesis := "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"
		core.Binding.GenesisHash = &genesis
		h := ActivityHistory{SchemaVersion: 1, Binding: core.Binding, CapturedAt: now.UTC(), ObservedAt: now.UTC(),
			Rows: []ActivityHistoryInterval{{AnchorSlot: 10, LastSlot: 20, StartTime: now.Unix() - 11, EndTime: now.Unix() - 1, Blocks: 10, Transactions: 100, Failed: 5}}}
		h.setContext()
		body, err := json.Marshal(h)
		if err != nil {
			t.Fatal(err)
		}
		var gate atomic.Pointer[replayDetailGate]
		g := replayGate(replayDetailLiveStatus(core), now)
		g.revision, g.stamp = 1, statusStamp{nodeEpoch: 1, familyEpoch: 1}
		gate.Store(&g)
		var reads atomic.Int32
		client := &http.Client{Transport: selectedEpochTransport(func(*http.Request) (*http.Response, error) {
			if reads.Add(1) == 2 {
				return nil, errors.New("synthetic transient read failure")
			}
			w := httptest.NewRecorder()
			w.Header().Set(replayDetailRemainingHeader, "20000")
			_, _ = w.Write(body)
			return w.Result(), nil
		})}
		selected := &selectedWindowReader{gate: &gate}
		r := &activityHistoryReader{selected: selected, client: client, done: make(chan struct{})}
		current := func() *activityHistoryFamily { selected.mu.Lock(); defer selected.mu.Unlock(); return r.latest }
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go r.run(ctx)
		synctest.Wait()
		if current() == nil {
			t.Fatal("initial history was not published")
		}
		original := current().freshUntil
		beforeNextRead := activityHistoryCaptureInterval / 2
		time.Sleep(beforeNextRead)
		next := g
		next.ObservedAt = time.Now()
		gate.Store(&next)
		time.Sleep(activityHistoryCaptureInterval - beforeNextRead)
		synctest.Wait()
		if current() != nil {
			t.Fatal("failed read retained a publication")
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if reads.Load() != 3 || current() == nil {
			t.Fatal("one prompt retry did not recover")
		}
		if current().freshUntil.After(original) {
			t.Fatal("prompt duplicate retry renewed the original lease")
		}
		cancel()
		<-r.done
	})
}

func TestActivityHistoryCoreContextHighWater(t *testing.T) {
	for _, regression := range []string{"generation reversal", "same-generation reset mutation"} {
		t.Run(regression, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				core := replayDetailLiveCore(0)
				genesis := "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"
				core.Binding.GenesisHash = &genesis
				parent := &replayDetailSource{}
				selected := &selectedWindowSource{gate: &parent.gate, gateMu: &parent.gateMu, revision: 1}
				parent.selected = selected
				parent.observeStatus(replayDetailLiveStatus(core), time.Now())
				current := ActivityHistory{SchemaVersion: 1, Binding: core.Binding, ObservedAt: time.Now().UTC(),
					Rows: []ActivityHistoryInterval{{AnchorSlot: 10, LastSlot: 20, StartTime: time.Now().Unix() - 11,
						EndTime: time.Now().Unix() - 1, Blocks: 10, Transactions: 100, NonVote: 80, Failed: 5, FailedNonVote: 4}}}
				source := &activityHistorySource{selected: selected, sample: func(_ context.Context, at time.Time) (ActivityHistory, error) {
					h := current
					h.CapturedAt = at.UTC()
					return h, nil
				}}
				var gate atomic.Pointer[replayDetailGate]
				g := replayGate(replayDetailLiveStatus(core), time.Now())
				g.revision, g.stamp = 1, statusStamp{nodeEpoch: 1, familyEpoch: 1}
				gate.Store(&g)
				client := &http.Client{Transport: selectedEpochTransport(func(*http.Request) (*http.Response, error) {
					// Exercise reader admission independently of the source's own rejection.
					h := current
					h.CapturedAt = time.Now().UTC()
					h.setContext()
					body, err := json.Marshal(h)
					if err != nil {
						t.Fatal(err)
					}
					w := httptest.NewRecorder()
					w.Header().Set(replayDetailRemainingHeader, "30000")
					_, _ = w.Write(body)
					return w.Result(), nil
				})}
				r := &activityHistoryReader{selected: &selectedWindowReader{gate: &gate}, client: client}
				publish := func(want bool) {
					t.Helper()
					source.poll(context.Background())
					r.collect(context.Background())
					if (source.latest != nil) != want {
						t.Errorf("source publication = %v, want %v", source.latest != nil, want)
					}
					if (r.latest != nil) != want {
						t.Errorf("reader publication = %v, want %v", r.latest != nil, want)
					}
					if t.Failed() {
						t.FailNow()
					}
				}
				publish(true) // A.
				a := current.Binding
				time.Sleep(time.Millisecond)
				current.Binding.SourceGeneration++
				current.Binding.ResetAt = current.Binding.ResetAt.Add(time.Second)
				current.ObservedAt = time.Now().UTC()
				publish(true) // A valid newer source generation B is allowed.
				b := current.Binding
				time.Sleep(time.Millisecond)
				if regression == "generation reversal" {
					current.Binding = a
				} else {
					current.Binding.ResetAt = b.ResetAt.Add(time.Second)
				}
				current.ObservedAt = time.Now().UTC()
				publish(false) // Reject even with newer capture/observation timestamps.
				current.Binding = b
				time.Sleep(time.Millisecond)
				current.ObservedAt = time.Now().UTC()
				publish(true)
				until := time.Now().Add(time.Second)
				source.latest.freshUntil, r.last.freshUntil = until, until
				publish(true) // Identical capture cannot renew either original lease.
				if source.latest.freshUntil.After(until) || r.latest.freshUntil.After(until) {
					t.Fatal("duplicate capture renewed its original lease")
				}
				for _, change := range []string{"attempt", "rewind"} {
					time.Sleep(time.Millisecond)
					if change == "attempt" {
						current.Binding.Attempt++
						current.Binding.AttemptStartedAt = time.Now().UTC()
					} else {
						current.Binding.Rewinds++
					}
					current.Binding.SourceGeneration, current.Binding.ResetAt = 1, time.Now().UTC()
					current.ObservedAt = time.Now().UTC()
					core.Binding = current.Binding
					parent.observeStatus(replayDetailLiveStatus(core), time.Now())
					next := replayGate(replayDetailLiveStatus(core), time.Now())
					next.revision, next.stamp = g.revision+1, statusStamp{nodeEpoch: 1, familyEpoch: g.stamp.familyEpoch + 1}
					g = next
					gate.Store(&g)
					publish(true) // Fresh parent context permits a new attempt/rewind baseline.
				}
			})
		})
	}
}
