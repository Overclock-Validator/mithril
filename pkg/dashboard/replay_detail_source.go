package dashboard

import (
	"context"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// ReplayDetailSampler returns a detached core for the original capture instant.
// It must preserve capturedAt as core.CapturedAt.UTC() and use only bounded
// in-memory reads, with no I/O or waits for replay progress. The caller must
// fence its source copy against replay progress before returning. Feed.Close
// waits for an in-flight callback to finish.
type ReplayDetailSampler func(capturedAt time.Time) (ReplayDetailCore, error)

const replayDetailCaptureInterval = 10 * time.Second

type replayDetailCaptureFailure struct {
	gate   replayDetailGate
	reason string
}

// replayDetailSource has one background owner. Gate publication and cache
// commit share a short lock; sampling, encoding and HTTP responses never do.
type replayDetailSource struct {
	sample   ReplayDetailSampler
	cache    replayDetailCache
	gateMu   sync.Mutex
	gate     atomic.Pointer[replayDetailGate]
	failure  atomic.Pointer[replayDetailCaptureFailure]
	done     chan struct{}
	held     *heldScheduleSource
	selected *selectedWindowSource
}

func (s *replayDetailSource) observeStatus(snapshot Snapshot, observedAt time.Time) {
	g := replayGate(snapshot, observedAt)
	s.gateMu.Lock()
	if s.selected != nil {
		s.selected.advanceGateLocked(s.gate.Load(), &g)
	}
	s.gate.Store(&g)
	s.gateMu.Unlock()
}

func replayGate(snapshot Snapshot, observedAt time.Time) replayDetailGate {
	g := replayDetailGate{
		RunID: snapshot.RunID, NodeStartedAt: snapshot.StartedAt.UTC(),
		ObservedAt: observedAt, State: "fresh", stopping: snapshot.Phase == "stopping",
	}
	if snapshot.GenesisHash != nil {
		g.HasGenesis, g.GenesisHash = true, *snapshot.GenesisHash
	}
	if snapshot.Replay == nil || snapshot.Replay.Attempt == 0 || snapshot.Replay.StartedAt.IsZero() {
		g.State, g.Reason = "unavailable", "waiting_source"
	} else {
		g.Attempt, g.Rewinds, g.AttemptStartedAt = snapshot.Replay.Attempt, snapshot.Replay.Rewinds, snapshot.Replay.StartedAt.UTC()
	}
	return g
}

func (s *replayDetailSource) run(ctx context.Context) {
	defer close(s.done)
	for {
		if ctx.Err() != nil {
			return
		}
		s.poll(ctx)
		if s.held != nil {
			s.held.poll(ctx)
		}
		if s.selected != nil {
			s.selected.poll(ctx)
		}
		// Waiting after completion avoids catch-up ticks and a three-holder
		// boundary arriving before an earlier post-encoding hold has expired.
		timer := time.NewTimer(replayDetailCaptureInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// poll is called only by the detail worker. Tests may drive it directly without
// changing production cadence; concurrent calls are not part of its contract.
func (s *replayDetailSource) poll(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	before := s.gate.Load()
	if before == nil {
		return
	}
	now := time.Now()
	if reason := before.failure(now); reason != "" {
		s.fail(*before, reason)
		return
	}
	if !s.cache.canPublish(*before, now) {
		return
	}
	capturedAt := time.Now()
	core, err := s.sample(capturedAt)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		s.fail(*before, "source_unavailable")
		return
	}
	if !before.matches(core.Binding) {
		s.fail(*before, "binding_changed")
		return
	}
	prepared, err := prepareReplayDetail(core, capturedAt)
	if err != nil {
		s.fail(*before, replayDetailCaptureReason(err))
		return
	}
	if ctx.Err() != nil {
		return
	}
	s.gateMu.Lock()
	defer s.gateMu.Unlock()
	after := s.gate.Load()
	if after == nil || !sameReplayDetailGateContext(*before, *after) {
		if after != nil {
			s.fail(*after, "binding_changed")
		}
		return
	}
	if err := s.cache.publish(prepared, *after, time.Now()); err != nil {
		s.fail(*after, replayDetailCaptureReason(err))
		return
	}
	s.failure.Store(nil)
}

func (s *replayDetailSource) fail(gate replayDetailGate, reason string) {
	s.failure.Store(&replayDetailCaptureFailure{gate: gate, reason: reason})
}

func replayDetailCaptureReason(err error) string {
	// Only errors from the private encoder/cache reach this helper. Unknown
	// text is still discarded rather than copied into the HTTP response.
	switch err.Error() {
	case "source_time_invalid", "response_limit", "binding_changed", "generation_not_ready", "status_stale":
		return err.Error()
	default:
		return "source_invalid"
	}
}

func sameReplayDetailGateContext(a, b replayDetailGate) bool {
	return a.RunID == b.RunID && a.NodeStartedAt.Equal(b.NodeStartedAt) &&
		a.HasGenesis == b.HasGenesis && a.GenesisHash == b.GenesisHash &&
		a.Attempt == b.Attempt && a.Rewinds == b.Rewinds && a.AttemptStartedAt.Equal(b.AttemptStartedAt)
}

func sameReplayDetailStatusContext(a, b replayDetailGate) bool {
	return a.stamp.valid() && a.stamp == b.stamp && sameReplayDetailGateContext(a, b)
}

func (s *replayDetailSource) serveHTTP(w http.ResponseWriter, r *http.Request) {
	reply := replayDetailFailure("invalid_request")
	_, _, _, queryOK := parseReplayDetailQuery(r.URL.RawQuery, r.URL.ForceQuery)
	if r.Method == http.MethodGet && r.URL.Path == "/details/replay" && r.URL.EscapedPath() == "/details/replay" &&
		r.ContentLength == 0 && len(r.TransferEncoding) == 0 && queryOK {
		gate := s.gate.Load()
		if gate == nil {
			reply = replayDetailFailure("waiting_source")
		} else {
			now := time.Now()
			// Publication precedes failure clearing. Retain this failure-first
			// observation for the current manifest; advertised pages ignore it.
			failure := s.failure.Load()
			reply = s.cache.lookup(r.URL.RawQuery, r.URL.ForceQuery, *gate, now)
			// Failed captures stop new collections but do not revoke pages that
			// were already advertised. Current status/binding gates still apply.
			if r.URL.RawQuery == "" && !r.URL.ForceQuery && gate.failure(now) == "" {
				if failure != nil && sameReplayDetailGateContext(failure.gate, *gate) {
					reply = replayDetailCaptureFailureReply(failure.reason)
				}
			}
		}
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

func replayDetailCaptureFailureReply(reason string) replayDetailReply {
	if reason == "response_limit" {
		return replayDetailReply{status: http.StatusServiceUnavailable, body: `{"schema_version":1,"family":"replay","state":"unavailable","reason":"response_limit"}`}
	}
	r := replayDetailFailure(reason)
	// Capture errors describe an unavailable new manifest, not a page-query
	// conflict. The client may keep its last compatible, dated collection.
	r.status = http.StatusServiceUnavailable
	return r
}
