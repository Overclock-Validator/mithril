package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"testing/synctest"
	"time"
)

func TestActivityHistoryWorkerKeepsSiblingPublications(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		core := replayDetailLiveCore(0)
		now := time.Now()
		parent := &replayDetailSource{done: make(chan struct{}), sample: func(at time.Time) (ReplayDetailCore, error) {
			value := core
			value.CapturedAt, value.UpdatedAt = at.UTC(), at.UTC()
			return value, nil
		}}
		parent.selected = &selectedWindowSource{gate: &parent.gate, gateMu: &parent.gateMu, revision: 1,
			sample: func(at time.Time) (SelectedWindow, error) {
				value := selectedWindowFixture(0)
				value.Binding, value.CapturedAt, value.ObservedAt = core.Binding, at.UTC(), at.UTC()
				return value, nil
			}}
		entered := make(chan struct{}, 1)
		parent.activity = &activityHistorySource{selected: parent.selected, done: make(chan struct{}),
			sample: func(ctx context.Context, _ time.Time) (ActivityHistory, error) {
				select {
				case entered <- struct{}{}:
				default:
				}
				<-ctx.Done()
				return ActivityHistory{}, ctx.Err()
			}}
		parent.observeStatus(replayDetailLiveStatus(core), now)
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
					parent.observeStatus(replayDetailLiveStatus(core), time.Now())
				}
			}
		}()
		go parent.activity.run(ctx)
		go parent.run(ctx)
		<-entered
		select {
		case <-entered:
		case <-time.After(4500 * time.Millisecond):
			t.Fatal("activity sampler did not retry within its collection budget")
		}
		time.Sleep(time.Until(now.Add(replayDetailCaptureInterval + 500*time.Millisecond)))
		parent.cache.mu.RLock()
		advanced := false
		for _, item := range parent.cache.items {
			if item != nil && !item.prepared.capturedAt.Before(now.Add(replayDetailCaptureInterval)) {
				advanced = true
			}
		}
		parent.cache.mu.RUnlock()
		if !advanced {
			t.Fatal("blocked activity delayed the next sibling replay publication")
		}
		cancel()
		for _, done := range []<-chan struct{}{parent.done, parent.activity.done, statusDone} {
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("activity or sibling owner did not join cancellation")
			}
		}
		if parent.activity.latest != nil {
			t.Fatal("timed-out activity sampler published history")
		}
	})
}

func TestActivityHistoryValidation(t *testing.T) {
	at := time.Unix(2000, 0).UTC()
	genesis := "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"
	h := ActivityHistory{SchemaVersion: 1, Binding: ReplayDetailBinding{RunID: "history-test", NodeStartedAt: at,
		GenesisHash: &genesis, SourceGeneration: 1, Attempt: 1, AttemptStartedAt: at, ResetAt: at}, CapturedAt: at.Add(time.Minute), ObservedAt: at.Add(time.Minute),
		Rows: []ActivityHistoryInterval{{AnchorSlot: 20, LastSlot: 30, StartTime: 2020, EndTime: 2030, Blocks: 10, Transactions: 12, Failed: 2},
			{AnchorSlot: 10, LastSlot: 20, StartTime: 2010, EndTime: 2020, Blocks: 10, Transactions: 14, Failed: 3}}}
	h.setContext()
	body, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeActivityHistory(body); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeActivityHistory(append(body, ' ')); err == nil {
		t.Fatal("noncanonical payload accepted")
	}
	for name, mutate := range map[string]func(*ActivityHistory){
		"missing interval": func(h *ActivityHistory) { h.Rows[1].LastSlot-- },
		"missing time":     func(h *ActivityHistory) { h.Rows[1].EndTime-- },
		"invalid counts":   func(h *ActivityHistory) { h.Rows[0].Failed = 13 },
		"invalid non-vote": func(h *ActivityHistory) { h.Rows[0].FailedNonVote = 1 },
		"invalid binding":  func(h *ActivityHistory) { h.Binding.Attempt = 0 },
		"mismatched run":   func(h *ActivityHistory) { h.RunID += "x" },
		"mismatched tip":   func(h *ActivityHistory) { h.Frontier++ },
		"before rewind":    func(h *ActivityHistory) { h.Binding.ResetAt = h.ObservedAt.Add(time.Second) },
		"future interval":  func(h *ActivityHistory) { h.Rows[0].EndTime = 5000 },
	} {
		t.Run(name, func(t *testing.T) {
			copy := h
			copy.Rows = append([]ActivityHistoryInterval(nil), h.Rows...)
			mutate(&copy)
			if validActivityHistory(copy) {
				t.Fatal("invalid history accepted")
			}
		})
	}
}

func TestActivityHistorySourceFenceAndFailure(t *testing.T) {
	core := replayDetailLiveCore(0)
	genesis := "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"
	core.Binding.GenesisHash = &genesis
	parent := &replayDetailSource{}
	selected := &selectedWindowSource{gate: &parent.gate, gateMu: &parent.gateMu, revision: 1}
	parent.selected = selected
	parent.observeStatus(replayDetailLiveStatus(core), time.Now())
	fail := false
	s := &activityHistorySource{selected: selected, sample: func(_ context.Context, at time.Time) (ActivityHistory, error) {
		if fail {
			return ActivityHistory{}, errors.New("waiting_source")
		}
		return ActivityHistory{SchemaVersion: 1, Binding: core.Binding, CapturedAt: at.UTC(), ObservedAt: at.UTC(),
			Rows: []ActivityHistoryInterval{{AnchorSlot: 10, LastSlot: 12, StartTime: at.Unix() - 10, EndTime: at.Unix() - 1, Blocks: 2, Transactions: 3}}}, nil
	}}
	s.poll(context.Background())
	w := httptest.NewRecorder()
	s.serveHTTP(w, httptest.NewRequest(http.MethodGet, "/details/activity-history", nil))
	if w.Code != http.StatusOK || w.Header().Get(replayDetailRemainingHeader) == "" {
		t.Fatalf("capture unavailable: %d %s", w.Code, w.Body.String())
	}
	if _, err := decodeActivityHistory(w.Body.Bytes()); err != nil {
		t.Fatal(err)
	}
	fail = true
	s.poll(context.Background())
	w = httptest.NewRecorder()
	s.serveHTTP(w, httptest.NewRequest(http.MethodGet, "/details/activity-history", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("failed capture retained fresh history: %d", w.Code)
	}
	fail = false
	s.poll(context.Background())
	changed := replayDetailLiveStatus(core)
	changed.Replay.Rewinds++
	parent.observeStatus(changed, time.Now())
	w = httptest.NewRecorder()
	s.serveHTTP(w, httptest.NewRequest(http.MethodGet, "/details/activity-history", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("history survived replay rewind: %d", w.Code)
	}
}

func TestActivityHistoryKeepsHistoricalFoldTimestamp(t *testing.T) {
	core := replayDetailLiveCore(0)
	genesis := "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"
	core.Binding.GenesisHash = &genesis
	parent := &replayDetailSource{}
	selected := &selectedWindowSource{gate: &parent.gate, gateMu: &parent.gateMu, revision: 1}
	parent.selected = selected
	parent.observeStatus(replayDetailLiveStatus(core), time.Now())
	age := time.Minute
	s := &activityHistorySource{selected: selected, sample: func(_ context.Context, at time.Time) (ActivityHistory, error) {
		observed := at.Add(-age)
		return ActivityHistory{SchemaVersion: 1, Binding: core.Binding, CapturedAt: at.UTC(), ObservedAt: observed.UTC(),
			Rows: []ActivityHistoryInterval{{AnchorSlot: 10, LastSlot: 12, StartTime: observed.Unix() - 10, EndTime: observed.Unix() - 1, Blocks: 2, Transactions: 3}}}, nil
	}}
	s.poll(context.Background())
	w := httptest.NewRecorder()
	s.serveHTTP(w, httptest.NewRequest(http.MethodGet, "/details/activity-history", nil))
	remaining, err := strconv.ParseInt(w.Header().Get(replayDetailRemainingHeader), 10, 64)
	if w.Code != http.StatusOK || err != nil || remaining < 25000 || remaining > replayDetailFreshFor.Milliseconds() {
		t.Fatalf("current capture of historical folds unavailable: %d, %d, %v", w.Code, remaining, err)
	}
	h, err := decodeActivityHistory(w.Body.Bytes())
	if err != nil || h.CapturedAt.Sub(h.ObservedAt) != age {
		t.Fatal("historical fold timestamp changed")
	}
}

func TestActivityHistoryRejectsContextReplacement(t *testing.T) {
	core := replayDetailLiveCore(0)
	genesis := "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"
	core.Binding.GenesisHash = &genesis
	status := replayDetailLiveStatus(core)
	parent := &replayDetailSource{}
	selected := &selectedWindowSource{gate: &parent.gate, gateMu: &parent.gateMu, revision: 1}
	parent.selected = selected
	parent.observeStatus(status, time.Now())
	started, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	s := &activityHistorySource{selected: selected, sample: func(_ context.Context, at time.Time) (ActivityHistory, error) {
		close(started)
		<-release
		return ActivityHistory{SchemaVersion: 1, Binding: core.Binding, CapturedAt: at.UTC(), ObservedAt: at.UTC(),
			Rows: []ActivityHistoryInterval{{AnchorSlot: 10, LastSlot: 12, StartTime: at.Unix() - 10, EndTime: at.Unix() - 1, Blocks: 2, Transactions: 3}}}, nil
	}}
	go func() { defer close(done); s.poll(context.Background()) }()
	<-started
	changed := replayDetailLiveStatus(core)
	changed.Replay.Rewinds++
	parent.observeStatus(changed, time.Now())
	parent.observeStatus(status, time.Now())
	close(release)
	<-done
	w := httptest.NewRecorder()
	s.serveHTTP(w, httptest.NewRequest(http.MethodGet, "/details/activity-history", nil))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("replaced context published history: %d", w.Code)
	}
}
