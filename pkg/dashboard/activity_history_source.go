package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

// activityHistoryCaptureInterval sets the normal polling cadence for private
// capture and the reader. It does not change folds or bank-clock windows.
const activityHistoryCaptureInterval = time.Second

// ActivityHistorySampler returns a bounded in-memory observation and must honor cancellation.
type ActivityHistorySampler func(context.Context, time.Time) (ActivityHistory, error)

type activityHistoryPublication struct {
	body       []byte
	binding    ReplayDetailBinding
	revision   uint64
	capturedAt time.Time
	observedAt time.Time
	frontier   uint64
	freshUntil time.Time
}

// activityHistorySource owns one joined worker and shares only the selected context fence.
type activityHistorySource struct {
	sample   ActivityHistorySampler
	selected *selectedWindowSource
	latest   *activityHistoryPublication // guarded by selected.gateMu
	last     *activityHistoryPublication // collection worker only; survives withdrawal
	done     chan struct{}
}

func (s *activityHistorySource) run(ctx context.Context) {
	defer close(s.done)
	for ctx.Err() == nil {
		capture, cancel := context.WithTimeout(ctx, time.Second)
		s.poll(capture)
		cancel()
		timer := time.NewTimer(activityHistoryCaptureInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (s *activityHistorySource) poll(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	s.selected.gateMu.Lock()
	before, revision, exhausted := s.selected.gate.Load(), s.selected.revision, s.selected.exhausted
	s.selected.gateMu.Unlock()
	now := time.Now()
	if before == nil || exhausted || before.stopping || before.State != "fresh" || before.failure(now) != "" {
		return
	}
	clear := func() {
		s.selected.gateMu.Lock()
		defer s.selected.gateMu.Unlock()
		if s.selected.revision == revision {
			s.latest = nil
		}
	}
	h, err := s.sample(ctx, now)
	h.setContext()
	if err != nil || ctx.Err() != nil || !h.CapturedAt.Equal(now) || !before.matches(h.Binding) || !validActivityHistory(h) {
		clear()
		return
	}
	freshUntil := now.Add(replayDetailFreshFor)
	if !freshUntil.After(time.Now()) {
		clear()
		return
	}
	body, err := json.Marshal(h)
	if err != nil || len(body) > activityHistoryMaxBytes {
		clear()
		return
	}
	if previous := s.last; previous != nil && before.matches(previous.binding) {
		if activityHistoryRegressed(previous.binding, previous.capturedAt, previous.observedAt, previous.frontier, h) ||
			h.CapturedAt.Equal(previous.capturedAt) && !bytes.Equal(body, previous.body) {
			clear()
			return
		}
		if h.CapturedAt.Equal(previous.capturedAt) && previous.freshUntil.Before(freshUntil) {
			freshUntil = previous.freshUntil
		}
	}
	s.selected.gateMu.Lock()
	defer s.selected.gateMu.Unlock()
	after := s.selected.gate.Load()
	if ctx.Err() != nil || s.selected.exhausted || s.selected.revision != revision || after == nil ||
		!sameReplayDetailGateContext(*before, *after) || after.stopping || after.State != "fresh" ||
		after.failure(time.Now()) != "" || !freshUntil.After(time.Now()) {
		return
	}
	p := &activityHistoryPublication{body: body, binding: h.Binding, revision: revision,
		capturedAt: h.CapturedAt, observedAt: h.ObservedAt, frontier: h.Frontier, freshUntil: freshUntil}
	s.latest, s.last = p, p
}

func (s *activityHistorySource) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || r.URL.Path != "/details/activity-history" || r.URL.EscapedPath() != r.URL.Path ||
		r.URL.RawQuery != "" || r.URL.ForceQuery || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	s.selected.gateMu.Lock()
	g, p, now := s.selected.gate.Load(), s.latest, time.Now()
	if g == nil || p == nil || s.selected.exhausted || p.revision != s.selected.revision || g.stopping ||
		g.State != "fresh" || g.failure(now) != "" || !g.matches(p.binding) || !p.freshUntil.After(now) {
		s.selected.gateMu.Unlock()
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	remaining := p.freshUntil.Sub(now).Milliseconds()
	body := p.body
	s.selected.gateMu.Unlock()
	if remaining < 1 {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(remaining, 10))
	_, _ = w.Write(body)
}
