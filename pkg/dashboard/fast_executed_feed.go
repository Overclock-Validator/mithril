package dashboard

import (
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

type fastExecutedSample struct {
	value       FastExecuted
	body        []byte
	freshUntil  time.Time
	invalidated chan struct{}
}

// replace is called only while the sampling lock is held.
func (f *fastExecutedFeed) replace(next *fastExecutedSample) {
	old := f.latest.Swap(next)
	if old != nil && old != next {
		close(old.invalidated)
	}
}

type fastExecutedFeed struct {
	// ponytail: one sampling lock; coalesce requests if bounded sampling becomes expensive.
	mu     sync.Mutex
	sample func() (FastExecuted, bool)
	gate   *atomic.Pointer[replayDetailGate]
	latest atomic.Pointer[fastExecutedSample]
	limit  *rate.Limiter
	active chan struct{}
}

func fastMatchesGate(v FastExecuted, g *replayDetailGate, now time.Time) bool {
	b := v.Binding
	return g != nil && g.State == "fresh" && !g.retired && !g.stopping && g.failure(now) == "" &&
		g.HasGenesis && b.GenesisHash != nil && *b.GenesisHash == g.GenesisHash &&
		b.RunID == g.RunID && b.NodeStartedAt.Equal(g.NodeStartedAt) &&
		b.Attempt == g.Attempt && b.Rewinds == g.Rewinds && b.AttemptStartedAt.Equal(g.AttemptStartedAt)
}

// update samples on private requests and ordinary status refreshes. Repeated sequences never renew a lease.
func (f *fastExecutedFeed) update(now time.Time) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.sample()
	g := f.gate.Load()
	if current := time.Now(); current.After(now) {
		now = current
	}
	if !ok || !validFastExecuted(v) || !fastMatchesGate(v, g, now) || v.SampledAt.After(now.Add(time.Second)) {
		f.replace(nil)
		return
	}
	body, err := json.Marshal(v)
	if err != nil || len(body) > 16<<10 {
		f.replace(nil)
		return
	}
	old := f.latest.Load()
	if old != nil && sameFastBinding(old.value.Binding, v.Binding) &&
		old.value.Sequence >= v.Sequence {
		if old.value.Sequence > v.Sequence || string(old.body) != string(body) {
			f.replace(nil)
		}
		return
	}
	until := now.Add(StaleAfter)
	if v.State == "fresh" {
		age := now.Sub(v.Records[len(v.Records)-1].At)
		if age < 0 {
			age = 0
		}
		until = now.Add(StaleAfter - age)
	}
	if !until.After(now) {
		f.replace(nil)
		return
	}
	f.replace(&fastExecutedSample{value: v, body: body, freshUntil: until, invalidated: make(chan struct{})})
}

func (f *fastExecutedFeed) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet || r.URL.Path != "/details/recent-executed" ||
		r.URL.RawQuery != "" || r.URL.ForceQuery || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if !f.limit.Allow() {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	}
	select {
	case f.active <- struct{}{}:
		defer func() { <-f.active }()
	default:
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	}
	if r.Context().Err() != nil {
		return
	}
	f.update(time.Now())
	var original *fastExecutedSample
	var originalGate *replayDetailGate
	var budget time.Time
	for attempt := range 2 {
		s := f.latest.Load()
		gate := f.gate.Load()
		now := time.Now()
		if attempt > 0 && (s == nil || s == original || s.value.Sequence <= original.value.Sequence || gate == nil || originalGate == nil ||
			gate != originalGate || r.Context().Err() != nil || !budget.After(now) ||
			!sameFastBinding(original.value.Binding, s.value.Binding)) {
			panic(http.ErrAbortHandler)
		}
		if s == nil || !s.freshUntil.After(now) || !fastMatchesGate(s.value, gate, now) {
			if attempt > 0 {
				panic(http.ErrAbortHandler)
			}
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		detailUntil := s.freshUntil
		if attempt == 0 {
			budget = detailUntil
		} else if detailUntil.After(budget) {
			detailUntil = budget
		}
		original, originalGate = s, gate
		if writeFastResponse(w, r, s.body, time.Time{}, detailUntil, s.invalidated, nil, func() bool {
			now := time.Now()
			return f.latest.Load() == s && s.freshUntil.After(now) && fastMatchesGate(s.value, f.gate.Load(), now)
		}) {
			return
		}
	}
	panic(http.ErrAbortHandler)
}
