package dashboard

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

type fastExecutedPublication struct {
	value       FastExecuted
	body        []byte
	stamp       statusStamp
	freshUntil  time.Time
	invalidated chan struct{}
}

type fastExecutedChange struct{ changed chan struct{} }

type fastExecutedWatermark struct {
	binding  FastExecutedBinding
	sequence uint64
	digest   [sha256.Size]byte
	seenAt   time.Time
}

type fastExecutedReader struct {
	client     *http.Client
	gate       *atomic.Pointer[replayDetailGate]
	latest     atomic.Pointer[fastExecutedPublication]
	changed    atomic.Pointer[fastExecutedChange]
	done       chan struct{}
	limit      *rate.Limiter
	active     chan struct{}
	history    [64]fastExecutedWatermark
	historyLen int
	retryAfter time.Time
}

func newFastExecutedReader(client *http.Client, gate *atomic.Pointer[replayDetailGate]) *fastExecutedReader {
	transport := client.Transport.(*http.Transport)
	r := &fastExecutedReader{client: detailHTTPClient(transport, 1, time.Second), gate: gate, done: make(chan struct{}),
		limit: rate.NewLimiter(12, 4), active: make(chan struct{}, 4)}
	r.changed.Store(&fastExecutedChange{changed: make(chan struct{})})
	return r
}

func (r *fastExecutedReader) serveHTTP(w http.ResponseWriter, request *http.Request, c *collector) {
	if !validDetailGet(request, "/api/v1/details/recent-executed") {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	if !r.limit.Allow() {
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	}
	select {
	case r.active <- struct{}{}:
		defer func() { <-r.active }()
	default:
		http.Error(w, "rate limited", http.StatusTooManyRequests)
		return
	}
	var original *fastExecutedPublication
	var originalGate *replayDetailGate
	var budget time.Time
	for attempt := range 2 {
		p := r.latest.Load()
		gate := r.gate.Load()
		if attempt > 0 && (p == nil || p == original || p.value.Sequence <= original.value.Sequence || gate == nil || originalGate == nil ||
			p.stamp != original.stamp || gate.stamp != originalGate.stamp || gate.revision != originalGate.revision ||
			request.Context().Err() != nil || !budget.After(time.Now()) ||
			!sameFastBinding(original.value.Binding, p.value.Binding)) {
			panic(http.ErrAbortHandler)
		}
		if p == nil {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		admissionStarted := time.Now()
		readGate := func() (statusStamp, bool) {
			g := r.gate.Load()
			if g == nil {
				return statusStamp{}, false
			}
			now := time.Now()
			return g.stamp, r.latest.Load() == p && p.freshUntil.After(now) && fastMatchesGate(p.value, g, now)
		}
		statusRemaining, ok := c.admitLiveDetail(statusReplay, p.stamp, readGate, admissionStarted)
		if !ok || statusRemaining.Milliseconds() < 1 {
			if attempt > 0 {
				panic(http.ErrAbortHandler)
			}
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		var statusChanged <-chan struct{}
		if update := c.statusUpdate.Load(); update != nil {
			statusChanged = update.changed
		}
		statusUntil, detailUntil := admissionStarted.Add(statusRemaining), p.freshUntil
		if attempt == 0 {
			budget = detailUntil
			if statusUntil.Before(budget) {
				budget = statusUntil
			}
		} else if detailUntil.After(budget) {
			detailUntil = budget
		}
		original, originalGate = p, gate
		if writeFastResponse(w, request, p.body, statusUntil, detailUntil,
			p.invalidated, statusChanged, func() bool {
				_, ok := c.admitLiveDetail(statusReplay, p.stamp, readGate, time.Now())
				return ok
			}) {
			return
		}
	}
	panic(http.ErrAbortHandler)
}

// replace is called by the sole reader worker.
func (r *fastExecutedReader) replace(next *fastExecutedPublication) {
	old := r.latest.Swap(next)
	if old == next {
		return
	}
	if old != nil {
		close(old.invalidated)
	}
	previous := r.changed.Swap(&fastExecutedChange{changed: make(chan struct{})})
	close(previous.changed)
}

func (r *fastExecutedReader) run(ctx context.Context) {
	defer close(r.done)
	defer r.client.CloseIdleConnections()
	defer r.replace(nil)
	for ctx.Err() == nil {
		started := time.Now()
		r.collect(ctx)
		delay := 100 * time.Millisecond
		if current := r.latest.Load(); current != nil && current.value.State == "fresh" {
			delay = max(10*time.Millisecond, delay-time.Since(started))
		}
		if wait := time.Until(r.retryAfter); wait > delay {
			delay = wait
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

func (r *fastExecutedReader) collect(ctx context.Context) {
	before := r.gate.Load()
	if before == nil || !before.stamp.valid() || before.retired || before.stopping || before.State != "fresh" {
		r.replace(nil)
		return
	}
	body, remaining, started, reason := fetchDetailWithTransportReason(ctx, r.client, "recent-executed", 16<<10, nil,
		func(v string) (time.Duration, error) { return parseDetailRemaining(v, StaleAfter) },
		func([]byte) string { return "source_unavailable" })
	if reason != "" || ctx.Err() != nil {
		// A poll without an HTTP response cannot renew or revoke already
		// admitted data while its original lease and parent still hold.
		if reason == detailRequestTimeout && ctx.Err() == nil {
			current, old, now := r.gate.Load(), r.latest.Load(), time.Now()
			if current != nil && current.stamp == before.stamp && current.revision == before.revision &&
				old != nil && old.stamp == current.stamp && old.freshUntil.After(now) && fastMatchesGate(old.value, current, now) {
				return
			}
		}
		if reason == "unsupported_source" {
			r.retryAfter = time.Now().Add(5 * time.Second)
		}
		r.replace(nil)
		return
	}
	r.retryAfter = time.Time{}
	var v FastExecuted
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&v) != nil || decoder.Decode(new(any)) != io.EOF || !validFastExecuted(v) {
		r.replace(nil)
		return
	}
	now := time.Now()
	after := r.gate.Load()
	if after == nil || before.stamp != after.stamp || before.revision != after.revision ||
		after.retired || after.stopping || !fastMatchesGate(v, after, now) ||
		v.SampledAt.After(now.Add(time.Second)) || !started.Add(remaining).After(now) {
		r.replace(nil)
		return
	}
	until := started.Add(remaining)
	if v.State == "fresh" {
		age := now.Sub(v.Records[len(v.Records)-1].At)
		if age < 0 {
			age = 0
		}
		if sourceUntil := now.Add(StaleAfter - age); sourceUntil.Before(until) {
			until = sourceUntil
		}
	}
	if !until.After(now) {
		r.replace(nil)
		return
	}
	// Retain only recent bindings: an older fresh payload cannot pass the
	// source-time limit. If the bound is exhausted, fail closed.
	n := 0
	for i := 0; i < r.historyLen; i++ {
		if now.Sub(r.history[i].seenAt) <= StaleAfter+time.Second {
			r.history[n] = r.history[i]
			n++
		}
	}
	r.historyLen = n
	digest := sha256.Sum256(body)
	index := -1
	for i := 0; i < n; i++ {
		if sameFastBinding(r.history[i].binding, v.Binding) {
			index = i
			break
		}
	}
	old := r.latest.Load()
	if index >= 0 && r.history[index].sequence >= v.Sequence {
		if r.history[index].sequence > v.Sequence || r.history[index].digest != digest ||
			old == nil || !sameFastBinding(old.value.Binding, v.Binding) || !old.freshUntil.After(now) {
			r.replace(nil)
		}
		return
	}
	if index < 0 {
		if n == len(r.history) {
			r.replace(nil)
			return
		}
		index = n
		r.historyLen++
	}
	r.history[index] = fastExecutedWatermark{binding: v.Binding, sequence: v.Sequence, digest: digest, seenAt: now}
	r.replace(&fastExecutedPublication{value: v, body: body, stamp: after.stamp,
		freshUntil: until, invalidated: make(chan struct{})})
}
