package dashboard

import (
	"context"
	"io"
	"math"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

const localPeerPath = "/details/peers"

// LocalPeerCapture keeps the source's original monotonic expiry alongside its
// immutable, endpoint-free body. The lease bounds age; it does not prove a
// Gossip Run remains active between samples. Viewer requests do not sample.
type LocalPeerCapture struct {
	Data       *PreparedLocalPeers
	FreshUntil time.Time
}

// LocalPeerSampler reads the cached owner on the background preparation cycle.
type LocalPeerSampler func(time.Time) (LocalPeerCapture, error)

type localPeerGate struct {
	binding    NodeNetworkBinding
	observedAt time.Time
	nodeEpoch  uint64
	revision   uint64
	reason     string
	exhausted  bool
}

func (g localPeerGate) failure(now time.Time) string {
	if g.exhausted {
		return "source_unavailable"
	}
	if g.observedAt.IsZero() || now.Before(g.observedAt) {
		return "source_time_invalid"
	}
	if now.Sub(g.observedAt) > StaleAfter {
		return "status_stale"
	}
	return g.reason
}

type localPeerPublication struct {
	body       string
	binding    NodeNetworkBinding
	nodeEpoch  uint64
	revision   uint64
	freshUntil time.Time
	reason     string
}

// localPeerSource is opt-in and owns one joined, low-rate preparation worker.
// It never performs node reads or JSON encoding in an HTTP request.
type localPeerSource struct {
	sample LocalPeerSampler
	gate   atomic.Pointer[localPeerGate]
	latest atomic.Pointer[localPeerPublication]
	active chan struct{}
	done   chan struct{}
}

// advanceLocalPeerGate runs on a serial status publisher. Its revision detects
// a binding that changes away and then back during preparation.
func advanceLocalPeerGate(before *localPeerGate, snapshot *Snapshot, reason string, now time.Time) localPeerGate {
	g := localPeerGate{observedAt: now, revision: 1, reason: reason}
	if snapshot == nil {
		g.reason = "source_unavailable"
	} else {
		base := networkGate(*snapshot, now)
		g.binding = base.nodeBinding()
		if snapshot.Phase == "stopping" {
			g.reason = "source_unavailable"
		}
	}
	if before != nil {
		g.revision, g.exhausted = before.revision, before.exhausted
		if !sameNodeNetworkBinding(before.binding, g.binding) || before.reason != g.reason {
			if g.revision == math.MaxUint64 {
				g.exhausted = true
			} else {
				g.revision++
			}
		}
	}
	return g
}

// advanceLocalPeerStatusGate preserves the source gate's revision semantics
// while also fencing a public reader across node-status observation gaps.
func advanceLocalPeerStatusGate(before *localPeerGate, snapshot *Snapshot, reason string, now time.Time, nodeEpoch uint64) localPeerGate {
	g := advanceLocalPeerGate(before, snapshot, reason, now)
	g.nodeEpoch = nodeEpoch
	if nodeEpoch == 0 || nodeEpoch == math.MaxUint64 || g.exhausted {
		g.exhausted, g.revision = true, math.MaxUint64
		return g
	}
	if before != nil && before.nodeEpoch != nodeEpoch && g.revision == before.revision {
		if g.revision >= math.MaxUint64-1 {
			g.exhausted, g.revision = true, math.MaxUint64
		} else {
			g.revision++
		}
	}
	if g.revision == math.MaxUint64 {
		g.exhausted = true
	}
	return g
}

func (s *localPeerSource) observeStatus(snapshot Snapshot, now time.Time) {
	g := advanceLocalPeerGate(s.gate.Load(), &snapshot, "", now)
	s.gate.Store(&g)
}

func (s *localPeerSource) run(ctx context.Context) {
	defer close(s.done)
	for ctx.Err() == nil {
		s.poll(ctx)
		timer := time.NewTimer(10 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (s *localPeerSource) poll(ctx context.Context) {
	before := s.gate.Load()
	if before == nil || ctx.Err() != nil {
		return
	}
	p := &localPeerPublication{binding: before.binding, revision: before.revision, reason: before.failure(time.Now())}
	if p.reason == "" {
		capture, err := s.sample(time.Now())
		now := time.Now()
		switch {
		case err != nil:
			p.reason = networkDetailReason(err.Error())
		case capture.Data == nil || !sameNodeNetworkBinding(before.binding, capture.Data.Binding()):
			p.reason = "binding_changed"
		case capture.FreshUntil.IsZero() || capture.FreshUntil == capture.FreshUntil.Round(0) ||
			!capture.FreshUntil.After(now) || capture.FreshUntil.After(now.Add(NetworkDetailFreshFor)):
			p.reason = "source_time_invalid"
		default:
			p.body, p.freshUntil = capture.Data.Body(), capture.FreshUntil
		}
	}
	after := s.gate.Load()
	if ctx.Err() == nil && after != nil && before.revision == after.revision &&
		sameNodeNetworkBinding(before.binding, after.binding) && after.failure(time.Now()) == "" {
		s.latest.Store(p)
	}
}

func (s *localPeerSource) serveHTTP(w http.ResponseWriter, r *http.Request) {
	status, reason, body, remaining := http.StatusServiceUnavailable, "waiting_source", "", int64(0)
	if r.Method != http.MethodGet || r.URL.Path != localPeerPath || r.URL.EscapedPath() != localPeerPath ||
		r.URL.RawQuery != "" || r.URL.ForceQuery || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
		status, reason = http.StatusBadRequest, "invalid_request"
	} else if gate := s.gate.Load(); gate != nil {
		now := time.Now()
		reason = gate.failure(now)
		if reason == "" {
			p := s.latest.Load()
			switch {
			case p == nil:
				reason = "waiting_source"
			case p.revision != gate.revision || !sameNodeNetworkBinding(p.binding, gate.binding):
				reason = "binding_changed"
			case p.reason != "":
				reason = p.reason
			case !p.freshUntil.After(now):
				reason = "status_stale"
			default:
				remaining = p.freshUntil.Sub(now).Milliseconds()
				if remaining < 1 {
					reason = "status_stale"
				} else {
					select {
					case s.active <- struct{}{}:
						defer func() { <-s.active }()
						status, body = http.StatusOK, p.body
					default:
						status, reason = http.StatusTooManyRequests, "rate_limited"
					}
				}
			}
		}
	}
	if body == "" {
		if reason != "rate_limited" {
			reason = networkDetailReason(reason)
		}
		body = `{"schema_version":1,"family":"local_peers","state":"unavailable","reason":"` + reason + `"}`
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if status == http.StatusOK {
		w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(remaining, 10))
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}
