package dashboard

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// activityHistoryReader owns one joined worker and fixed Unix connection.
// It shares the selected response limiter/cap, not its collection cadence.
type activityHistoryReader struct {
	selected *selectedWindowReader
	client   *http.Client
	done     chan struct{}
	latest   *activityHistoryFamily // guarded by selected.mu
	last     *activityHistoryFamily // collection worker only; preserves duplicate leases
}

type activityHistoryFamily struct {
	body       string
	binding    ReplayDetailBinding
	capturedAt time.Time
	observedAt time.Time
	frontier   uint64
	freshUntil time.Time
	stamp      statusStamp
	revision   uint64
}

func newActivityHistoryReader(selected *selectedWindowReader) (*activityHistoryReader, error) {
	if selected == nil || selected.client == nil {
		return nil, errors.New("activity history requires the selected reader")
	}
	t, ok := selected.client.Transport.(*http.Transport)
	if !ok || t == nil || t.DialContext == nil {
		return nil, errors.New("activity history requires a fixed Unix transport")
	}
	return &activityHistoryReader{selected: selected, client: detailHTTPClient(t, 1, time.Second), done: make(chan struct{})}, nil
}

func (r *activityHistoryReader) run(ctx context.Context) {
	defer close(r.done)
	defer r.client.CloseIdleConnections()
	retried := false
	for ctx.Err() == nil {
		r.collect(ctx)
		delay := activityHistoryCaptureInterval
		r.selected.mu.Lock()
		unavailable := r.latest == nil
		r.selected.mu.Unlock()
		switch {
		case unavailable && !retried:
			delay, retried = time.Second, true
		case unavailable:
			delay, retried = replayDetailCaptureInterval, false
		default:
			retried = false
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func activityGateFresh(g *replayDetailGate, now time.Time) bool {
	return g != nil && !g.retired && !g.stopping && g.State == "fresh" && g.stamp.valid() && g.failure(now) == ""
}

func (r *activityHistoryReader) collect(ctx context.Context) {
	s := r.selected
	before := s.gate.Load()
	clear := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		after := s.gate.Load()
		if after == nil || before == nil || after.revision == before.revision && after.stamp == before.stamp {
			r.latest = nil
		}
	}
	if ctx.Err() != nil || !activityGateFresh(before, time.Now()) {
		clear()
		return
	}
	body, remaining, started, reason := fetchDetail(ctx, r.client, "activity-history", activityHistoryMaxBytes, nil,
		func(v string) (time.Duration, error) { return parseDetailRemaining(v, replayDetailFreshFor) },
		func([]byte) string { return "source_unavailable" })
	if reason != "" || ctx.Err() != nil {
		clear()
		return
	}
	h, err := decodeActivityHistory(body)
	until := started.Add(remaining)
	if err != nil || !before.matches(h.Binding) || !until.After(time.Now()) {
		clear()
		return
	}
	text := string(body)
	if previous := r.last; previous != nil && before.matches(previous.binding) {
		if activityHistoryRegressed(previous.binding, previous.capturedAt, previous.observedAt, previous.frontier, h) ||
			h.CapturedAt.Equal(previous.capturedAt) && text != previous.body {
			clear()
			return
		}
		if h.CapturedAt.Equal(previous.capturedAt) && previous.freshUntil.Before(until) {
			until = previous.freshUntil
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	after := s.gate.Load()
	if ctx.Err() != nil || !activityGateFresh(after, time.Now()) || after.revision != before.revision || after.stamp != before.stamp ||
		!sameReplayDetailGateContext(*before, *after) || !after.matches(h.Binding) || !until.After(time.Now()) {
		r.latest = nil
		return
	}
	p := &activityHistoryFamily{body: text, binding: h.Binding, capturedAt: h.CapturedAt, observedAt: h.ObservedAt,
		frontier: h.Frontier, freshUntil: until, stamp: after.stamp, revision: after.revision}
	r.latest, r.last = p, p
}

func (r *activityHistoryReader) serveHTTP(w http.ResponseWriter, request *http.Request,
	admit func(statusStamp, func() (statusStamp, bool)) (time.Duration, bool)) {
	if !validDetailGet(request, "/api/v1/details/activity-history") {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	s := r.selected
	if !s.limit.Allow() {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	}
	select {
	case s.active <- struct{}{}:
		defer func() { <-s.active }()
	default:
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	}
	s.mu.Lock()
	p := r.latest
	s.mu.Unlock()
	if p == nil || admit == nil {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	remaining, ok := admit(p.stamp, func() (statusStamp, bool) {
		g := s.gate.Load()
		if g == nil {
			return statusStamp{}, false
		}
		s.mu.Lock()
		present := r.latest == p
		s.mu.Unlock()
		return g.stamp, present && activityGateFresh(g, time.Now()) && g.revision == p.revision &&
			g.matches(p.binding) && p.freshUntil.After(time.Now())
	})
	detailRemaining := time.Until(p.freshUntil).Milliseconds()
	if !ok || remaining.Milliseconds() < 1 || detailRemaining < 1 {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	writeDetailResponse(w, http.StatusOK, p.body, remaining, detailRemaining)
}
