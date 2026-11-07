package dashboard

import (
	"context"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// SelectedWindowSampler reads a detached, bounded in-memory source. The replay
// detail worker owns its cadence and waits for it when the feed closes.
type SelectedWindowSampler func(time.Time) (SelectedWindow, error)

type selectedWindowSource struct {
	sample    SelectedWindowSampler
	cache     replayDetailCache
	gate      *atomic.Pointer[replayDetailGate]
	gateMu    *sync.Mutex
	revision  uint64                   // guarded by gateMu
	exhausted bool                     // guarded by gateMu
	failure   *selectedWindowFailure   // guarded by gateMu
	last      *selectedWindowHighWater // owned by the detail worker
}

type selectedWindowFailure struct {
	revision uint64
	stamp    statusStamp
	reason   string
}

type selectedWindowHighWater struct {
	binding ReplayDetailBinding
	at      time.Time
	digest  string
}

func selectedWindowRegressed(last *selectedWindowHighWater, binding ReplayDetailBinding, at time.Time, digest string) bool {
	return last != nil && sameReplayDetailBinding(last.binding, binding) &&
		(at.Before(last.at) || at.Equal(last.at) && digest != last.digest)
}

// advanceGateLocked detects A -> B -> A replacement while a capture runs.
func (s *selectedWindowSource) advanceGateLocked(old, next *replayDetailGate) {
	if old == nil || sameReplayDetailGateContext(*old, *next) && old.State == next.State && old.Reason == next.Reason && old.stopping == next.stopping {
		return
	}
	if s.revision == math.MaxUint64 {
		s.exhausted = true
	} else {
		s.revision++
	}
	// A prior matching binding must not revive after an A -> B -> A change.
	s.cache.mu.Lock()
	s.cache.items = [replayDetailGenerations]*replayDetailPublication{}
	s.cache.mu.Unlock()
	s.failure = nil
}

func (s *selectedWindowSource) poll(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	s.gateMu.Lock()
	before, revision, exhausted := s.gate.Load(), s.revision, s.exhausted
	s.gateMu.Unlock()
	if before == nil {
		return
	}
	fail := func(reason string) {
		if ctx.Err() == nil {
			s.gateMu.Lock()
			if !s.exhausted && s.revision == revision {
				s.failure = &selectedWindowFailure{revision: revision, reason: reason}
			}
			s.gateMu.Unlock()
		}
	}
	if exhausted {
		return
	}
	if before.stopping {
		fail("source_unavailable")
		return
	}
	now := time.Now()
	if reason := before.failure(now); reason != "" {
		fail(reason)
		return
	}
	if before.State != "fresh" {
		fail("status_stale")
		return
	}
	if !s.cache.canPublish(*before, now) {
		return
	}
	capturedAt := time.Now()
	w, err := s.sample(capturedAt)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		fail(heldScheduleReason(err.Error()))
		return
	}
	if !capturedAt.Equal(w.CapturedAt) {
		fail("source_time_invalid")
		return
	}
	if !before.matches(w.Binding) {
		fail("binding_changed")
		return
	}
	prepared, err := prepareSelectedWindow(w)
	if err != nil {
		fail(replayDetailCaptureReason(err))
		return
	}
	if selectedWindowRegressed(s.last, w.Binding, w.CapturedAt, prepared.digest) {
		fail("source_invalid")
		return
	}
	p := &replayDetailPrepared{binding: w.Binding, generation: prepared.digest, capturedAt: capturedAt,
		pages: prepared.pages, bytes: prepared.bytes}
	for i := range p.manifests {
		p.manifests[i] = prepared.manifest
	}
	s.gateMu.Lock()
	after := s.gate.Load()
	if ctx.Err() != nil {
		s.gateMu.Unlock()
		return
	}
	if s.exhausted || s.revision != revision || after == nil || !sameReplayDetailGateContext(*before, *after) || after.State != "fresh" || after.stopping {
		s.gateMu.Unlock()
		return
	}
	if err := s.cache.publish(p, *after, time.Now()); err != nil {
		s.failure = &selectedWindowFailure{revision: revision, reason: replayDetailCaptureReason(err)}
		s.gateMu.Unlock()
		return
	}
	s.failure = nil
	s.gateMu.Unlock()
	last := selectedWindowHighWater{binding: w.Binding, at: w.CapturedAt, digest: prepared.digest}
	if w.Binding.GenesisHash != nil {
		genesis := *w.Binding.GenesisHash
		last.binding.GenesisHash = &genesis
	}
	s.last = &last
}

func (s *selectedWindowSource) serveHTTP(w http.ResponseWriter, r *http.Request) {
	reply := replayDetailFailure("invalid_request")
	_, _, page, ok := parseReplayDetailQuery(r.URL.RawQuery, r.URL.ForceQuery)
	if r.Method == http.MethodGet && r.URL.Path == "/details/selected" && r.URL.EscapedPath() == "/details/selected" &&
		r.ContentLength == 0 && len(r.TransferEncoding) == 0 && ok {
		s.gateMu.Lock()
		gate := s.gate.Load()
		if gate == nil {
			reply = replayDetailFailure("waiting_source")
		} else if s.exhausted {
			reply = replayDetailFailure("source_unavailable")
		} else if gate.stopping {
			reply = replayDetailFailure("source_unavailable")
		} else if gate.State != "fresh" {
			reply = replayDetailFailure("status_stale")
		} else {
			now := time.Now()
			reply = s.cache.lookup(r.URL.RawQuery, r.URL.ForceQuery, *gate, now)
			if !page && gate.failure(now) == "" && s.failure != nil && s.failure.revision == s.revision {
				reply = replayDetailCaptureFailureReply(s.failure.reason)
			}
		}
		s.gateMu.Unlock()
	}
	if reply.status != http.StatusOK {
		reply.body = strings.Replace(reply.body, `"family":"replay"`, `"family":"selected_slots"`, 1)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if reply.status == http.StatusOK && reply.remainingMilliseconds > 0 {
		w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(reply.remainingMilliseconds, 10))
	}
	w.WriteHeader(reply.status)
	if _, err := io.WriteString(w, reply.body); err != nil {
		return
	}
}
