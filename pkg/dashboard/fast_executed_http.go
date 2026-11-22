package dashboard

import (
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

// writeFastResponse aborts a blocked write when its source is invalidated.
// False permits a caller to reselect before commitment; its watcher has joined.
// Commit is reserved before WriteHeader; every later outcome cannot be retried.
func writeFastResponse(w http.ResponseWriter, r *http.Request, body []byte, statusUntil, detailUntil time.Time,
	invalidated, statusChanged <-chan struct{}, valid func() bool) bool {
	deadline := detailUntil
	if !statusUntil.IsZero() && statusUntil.Before(deadline) {
		deadline = statusUntil
	}
	if !deadline.After(time.Now()) || !valid() {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return true
	}
	controller := http.NewResponseController(w)
	if err := controller.SetWriteDeadline(deadline); err != nil {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return true
	}
	const (
		uncommitted uint32 = iota
		invalid
		committed
	)
	var phase atomic.Uint32
	done, joined := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(joined)
		timer := time.NewTimer(time.Until(deadline))
		defer timer.Stop()
		select {
		case <-done:
			return
		case <-r.Context().Done():
		case <-invalidated:
		case <-statusChanged:
		case <-timer.C:
		}
		if phase.CompareAndSwap(uncommitted, invalid) {
			return
		}
		if phase.Load() == committed {
			_ = controller.SetWriteDeadline(time.Now())
		}
	}()
	defer func() {
		close(done)
		<-joined
		if phase.Load() != committed {
			w.Header().Del("Content-Length")
			w.Header().Del(replayDetailRemainingHeader)
			w.Header().Del(statusRemainingHeader)
		}
	}()
	if !valid() {
		return false
	}
	detailMS := time.Until(detailUntil).Milliseconds()
	statusMS := int64(0)
	if !statusUntil.IsZero() {
		statusMS = time.Until(statusUntil).Milliseconds()
	}
	if detailMS < 1 || !statusUntil.IsZero() && statusMS < 1 {
		return false
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(detailMS, 10))
	if !statusUntil.IsZero() {
		w.Header().Set(statusRemainingHeader, strconv.FormatInt(statusMS, 10))
	}
	if !valid() || r.Context().Err() != nil || !deadline.After(time.Now()) || !phase.CompareAndSwap(uncommitted, committed) {
		return false
	}
	w.WriteHeader(http.StatusOK)
	if !valid() || !deadline.After(time.Now()) {
		return true
	}
	if _, err := w.Write(body); err != nil {
		return true
	}
	_ = controller.Flush()
	return true
}
