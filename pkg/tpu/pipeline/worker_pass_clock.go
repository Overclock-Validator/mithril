package pipeline

import (
	"sync"
	"time"
)

// WorkerPassObservation is one goroutine's wall time from input through final
// handoff, including blocked output and descheduling, since pipeline start.
// It is not CPU time or a pinned-core utilization measurement.
type WorkerPassObservation struct {
	Index           int
	SampledAt       time.Time
	Active, Elapsed time.Duration
}

type workerPassClock struct {
	mu      sync.Mutex
	start   time.Time
	total   time.Duration
	invalid bool
}

func (w *workerPassClock) begin() {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := time.Now()
	if !w.start.IsZero() {
		w.invalid = true
	}
	w.start = now
}
func (w *workerPassClock) end() {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := time.Now()
	if w.start.IsZero() || now.Before(w.start) {
		w.invalid = true
		return
	}
	d := now.Sub(w.start)
	if d > time.Duration(1<<63-1)-w.total {
		w.invalid = true
	} else {
		w.total += d
	}
	w.start = time.Time{}
}
func (w *workerPassClock) observe(started time.Time, index int) (WorkerPassObservation, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(started)
	wall := now.Round(0).Sub(started.Round(0))
	if wall-elapsed > 250*time.Millisecond || elapsed-wall > 250*time.Millisecond {
		return WorkerPassObservation{}, false
	}
	active := w.total
	if !w.start.IsZero() {
		if now.Before(w.start) {
			return WorkerPassObservation{}, false
		}
		d := now.Sub(w.start)
		if d > time.Duration(1<<63-1)-active {
			return WorkerPassObservation{}, false
		}
		active += d
	}
	if w.invalid || started.IsZero() || elapsed <= 0 || active < 0 || active > elapsed {
		return WorkerPassObservation{}, false
	}
	return WorkerPassObservation{Index: index, SampledAt: now, Active: active, Elapsed: elapsed}, true
}
