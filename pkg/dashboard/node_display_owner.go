package dashboard

import (
	"errors"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard/cluster"
	"golang.org/x/time/rate"
)

// This is the encoded selectNodeDisplay(nil, ...) result. Expiry must not expose
// an old binding or label, and serving it requires no request-time encoding.
const nodeDisplayUnavailableBody = `{"schema_version":1,"scope":"linked_node_external_display","run_id":"","identity":null,"genesis_hash":null,"profile":"unknown","epoch":null,"state":"unavailable","reason":"waiting_source","name":null,"directory_source":null,"source_at":null,"captured_at":null,"icon_url":null,"icon_state":"not_supplied"}`

type nodeDisplayContext struct {
	runID, identity, genesis string
	startedAt                time.Time
	epoch                    uint64
}

type nodeDisplayProviderClock struct {
	epoch              uint64
	sourceAt, deadline time.Time
}

// nodeDisplayOwner has one collection caller and concurrent cached readers.
// The two provider slots mean Solstat and Titan under the fixed Alpenglow genesis;
// their source-date high-waters survive node changes and all unavailable states.
// No source DTO, directory pointer, clock, worker or fetch is owned here.
type nodeDisplayOwner struct {
	mu             sync.RWMutex
	admit          func(nodeDisplayFrame) (time.Duration, bool) // installed before the public server starts
	revision       uint64
	ready          bool
	observedAt     time.Time
	context        nodeDisplayContext
	statusSourceAt time.Time
	statusDeadline time.Time
	providers      [2]nodeDisplayProviderClock
	current        nodeDisplayFrame
}

// observeStatus runs before the collector publishes a replacement status.
// wallNow is that result's wall-clock reference; receivedAt is the original
// raw receipt clock. Every observation fences pending and selected work.
func (o *nodeDisplayOwner) observeStatus(s *Snapshot, state string, wallNow, receivedAt time.Time) uint64 {
	if o == nil {
		return 0
	}
	key, valid := nodeDisplayContextFor(s, state, wallNow)
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ready, o.current = false, nodeDisplayFrame{}
	if o.revision >= math.MaxUint64-1 {
		o.revision = math.MaxUint64
		return o.revision
	}
	o.revision++
	if !nodeDisplayRawClock(receivedAt) || receivedAt.Before(o.observedAt) {
		return o.revision
	}
	o.observedAt = receivedAt
	if !valid {
		return o.revision
	}
	sourceAt := s.SampledAt.UTC()
	deadline := nodeDisplayLease(receivedAt, wallNow, sourceAt, StaleAfter)
	switch {
	case o.statusSourceAt.IsZero() || sourceAt.After(o.statusSourceAt):
		key.runID, key.identity, key.genesis = strings.Clone(key.runID), strings.Clone(key.identity), strings.Clone(key.genesis)
		o.context, o.statusSourceAt, o.statusDeadline = key, sourceAt, deadline
	case sourceAt.Equal(o.statusSourceAt) && key == o.context:
		if deadline.Before(o.statusDeadline) {
			o.statusDeadline = deadline
		}
	default:
		// A new context cannot borrow an older observation's receipt clock.
		return o.revision
	}
	o.ready = receivedAt.Before(o.statusDeadline)
	return o.revision
}

// prepare performs selection and encoding on the collection caller. Its view
// must be the existing validated immutable cluster publication. Only the tiny
// resulting frame may escape; publish checks its original deadline after encode.
func (o *nodeDisplayOwner) prepare(s *Snapshot, view *clusterView, wallNow, observedAt time.Time, revision uint64) (nodeDisplayFrame, error) {
	if o == nil {
		return nodeDisplayFrame{}, errors.New("node display owner unavailable")
	}
	key, valid := nodeDisplayContextFor(s, "fresh", wallNow)
	epoch := strconv.FormatUint(key.epoch, 10)
	o.mu.Lock()
	if !valid || !o.ready || revision != o.revision || key != o.context || !s.SampledAt.Equal(o.statusSourceAt) ||
		!nodeDisplayRawClock(observedAt) || observedAt.Before(o.observedAt) || !observedAt.Before(o.statusDeadline) {
		o.mu.Unlock()
		return nodeDisplayFrame{}, errors.New("node display status changed")
	}
	deadline := o.statusDeadline
	var filtered clusterView
	var providers [5]cluster.ProviderStatus
	var leases [2]time.Time
	var eligible *clusterView
	profile := "alpenglow"
	if key.genesis == cluster.MainnetGenesisHash {
		profile = "mainnet"
	}
	if view != nil && len(view.metadata.Providers) <= len(providers) && len(view.rows) <= cluster.MaxRows &&
		view.metadata.State == "fresh" && view.metadata.Profile == profile &&
		view.metadata.GenesisHash != nil && *view.metadata.GenesisHash == key.genesis &&
		view.metadata.Epoch != nil && string(*view.metadata.Epoch) == epoch &&
		nodeDisplayDate(view.metadata.CapturedAt, wallNow) {
		filtered = *view
		copy(providers[:], view.metadata.Providers)
		filtered.metadata.Providers = providers[:len(view.metadata.Providers)]
		eligible = &filtered
		// Observe both eligible providers before name preference or conflicts.
		for i := range filtered.metadata.Providers {
			p := &filtered.metadata.Providers[i]
			index := -1
			switch p.ID {
			case "solstat":
				index = 0
			case "titan":
				index = 1
			}
			if index < 0 {
				continue
			}
			if p.State != "fresh" || p.Reason != "" || p.GenesisHash != cluster.GenesisHash || p.Epoch == nil ||
				*p.Epoch != *view.metadata.Epoch || !nodeDisplayDate(p.SourceAt, wallNow) || !nodeDisplayDate(&p.CapturedAt, wallNow) {
				p.State = "stale"
				continue
			}
			candidate := nodeDisplayLease(observedAt, wallNow, *p.SourceAt, cluster.MaxAge)
			for _, date := range []time.Time{p.CapturedAt, *view.metadata.CapturedAt} {
				if end := nodeDisplayLease(observedAt, wallNow, date, cluster.MaxAge); end.Before(candidate) {
					candidate = end
				}
			}
			clock := &o.providers[index]
			sourceAt := p.SourceAt.UTC()
			switch {
			case clock.sourceAt.IsZero() || sourceAt.After(clock.sourceAt):
				*clock = nodeDisplayProviderClock{epoch: key.epoch, sourceAt: sourceAt, deadline: candidate}
			case sourceAt.Equal(clock.sourceAt) && clock.epoch == key.epoch:
				if candidate.Before(clock.deadline) {
					clock.deadline = candidate
				}
			default:
				p.State = "stale"
				continue
			}
			if !observedAt.Before(clock.deadline) {
				p.State = "stale"
				continue
			}
			leases[index] = clock.deadline
		}
	}
	o.mu.Unlock()

	v := selectNodeDisplay(s, "fresh", eligible, wallNow)
	for i, lease := range leases {
		selected := v.DirectorySource != nil && (i == 0 && *v.DirectorySource == "solstat" || i == 1 && *v.DirectorySource == "titan")
		if !lease.IsZero() && (selected || v.State == "conflict") && lease.Before(deadline) {
			deadline = lease
		}
	}
	frame, err := encodeNodeDisplay(v)
	if err != nil {
		return nodeDisplayFrame{}, err
	}
	frame.deadline, frame.revision = deadline, revision
	return frame, nil
}

// publish copies a prepared header after encoding. A caller must pass a fresh
// raw clock here; preparation, status changes and scheduler delays consume time.
func (o *nodeDisplayOwner) publish(frame nodeDisplayFrame, now time.Time) bool {
	if o == nil {
		return false
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.ready || frame.revision != o.revision || frame.revision == math.MaxUint64 || len(frame.body) == 0 || len(frame.body) > nodeDisplayBytes ||
		!nodeDisplayRawClock(now) || now.Before(o.observedAt) || !now.Before(frame.deadline) || frame.deadline.After(o.statusDeadline) {
		return false
	}
	o.current = frame
	return true
}

func (o *nodeDisplayOwner) selectFrame(now time.Time) nodeDisplayFrame {
	if o == nil {
		return nodeDisplayFrame{}
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	if !o.ready || !nodeDisplayRawClock(now) || now.Before(o.observedAt) || !now.Before(o.current.deadline) {
		return nodeDisplayFrame{}
	}
	return o.current
}

// admitFrame is the final cached-reply check. A status A→B→A transition cannot
// revive a previously selected A frame, even if its text and dates match again.
func (o *nodeDisplayOwner) admitFrame(frame nodeDisplayFrame, now time.Time) string {
	if o == nil {
		return nodeDisplayUnavailableBody
	}
	o.mu.RLock()
	defer o.mu.RUnlock()
	if !o.ready || frame != o.current || frame.revision != o.revision || !nodeDisplayRawClock(now) ||
		now.Before(o.observedAt) || !now.Before(frame.deadline) {
		return nodeDisplayUnavailableBody
	}
	return frame.body
}

// admitNodeDisplay combines the selected display revision with one current
// parent status frame. Neither a viewer nor this check renews either lease.
func (c *collector) admitNodeDisplay(o *nodeDisplayOwner, frame nodeDisplayFrame) (time.Duration, bool) {
	if c == nil || o == nil || frame.body == "" || frame.body == nodeDisplayUnavailableBody {
		return 0, false
	}
	for range 2 {
		status := c.latest.Load()
		if status == nil || frame.parent == nil || status.lease != frame.parent || status.freshUntil.IsZero() {
			return 0, false
		}
		now := time.Now()
		if o.admitFrame(frame, now) != frame.body {
			return 0, false
		}
		if c.latest.Load() != status {
			continue
		}
		if now = time.Now(); o.admitFrame(frame, now) != frame.body {
			return 0, false
		}
		if c.latest.Load() != status {
			continue
		}
		parentRemaining, valid := status.remainingStatus(0, now)
		displayRemaining := min(frame.deadline.Sub(now), frame.deadline.Round(0).Sub(now.Round(0)))
		remaining := min(parentRemaining, displayRemaining)
		return remaining, valid && remaining.Milliseconds() > 0
	}
	return 0, false
}

// handler serves only a preencoded frame. Viewers cannot load a directory,
// sample node state, select a name or renew either source's lease.
func (o *nodeDisplayOwner) handler(allowed func(*http.Request) bool) http.HandlerFunc {
	limit := rate.NewLimiter(16, 32)
	return func(w http.ResponseWriter, r *http.Request) {
		if !allowed(r) {
			http.Error(w, "host or origin not allowed", http.StatusForbidden)
			return
		}
		if r.Method != http.MethodGet || r.URL.Path != nodeDisplayPublicPath || r.URL.EscapedPath() != nodeDisplayPublicPath ||
			r.URL.RawQuery != "" || r.URL.ForceQuery || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
			http.Error(w, "unsupported request", http.StatusBadRequest)
			return
		}
		if !limit.Allow() {
			http.Error(w, "node display query limit reached", http.StatusTooManyRequests)
			return
		}
		frame := o.selectFrame(time.Now())
		now := time.Now()
		body := o.admitFrame(frame, now)
		if body != nodeDisplayUnavailableBody {
			var remaining time.Duration
			var ok bool
			if o.admit != nil {
				remaining, ok = o.admit(frame)
			}
			if !ok || remaining.Milliseconds() < 1 {
				body = nodeDisplayUnavailableBody
			} else {
				w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(remaining.Milliseconds(), 10))
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if _, err := io.WriteString(w, body); err != nil {
			return
		}
	}
}

func nodeDisplayContextFor(s *Snapshot, state string, now time.Time) (nodeDisplayContext, bool) {
	v := selectNodeDisplay(s, state, nil, now)
	if v.Reason != "directory_unavailable" || !validPublicTime(s.StartedAt, now) {
		return nodeDisplayContext{}, false
	}
	return nodeDisplayContext{runID: s.RunID, identity: *v.Identity, genesis: *v.GenesisHash, startedAt: s.StartedAt.UTC(), epoch: s.Replay.Completed.Epoch}, true
}

func nodeDisplayRawClock(at time.Time) bool {
	return !at.IsZero() && at != at.Round(0)
}

func nodeDisplayLease(receivedAt, wallNow, sourceAt time.Time, limit time.Duration) time.Time {
	age := wallNow.Sub(sourceAt)
	if age < 0 {
		age = 0
	}
	return receivedAt.Add(limit - age)
}
