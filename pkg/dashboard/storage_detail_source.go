package dashboard

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

type storageDetailGate struct {
	runID         string
	nodeStartedAt time.Time
	genesisHash   string
	observedAt    time.Time
	reason        string
	stamp         statusStamp
}

func storageGate(s Snapshot, now time.Time) storageDetailGate {
	g := storageDetailGate{runID: s.RunID, nodeStartedAt: s.StartedAt, observedAt: now}
	if s.GenesisHash != nil {
		g.genesisHash = *s.GenesisHash
	}
	if s.GenesisHash == nil {
		g.reason = "waiting_source"
	}
	return g
}

func (g storageDetailGate) failure(now time.Time) string {
	if g.observedAt.IsZero() || now.Before(g.observedAt) {
		return "source_time_invalid"
	}
	if now.Sub(g.observedAt) > StaleAfter {
		return "status_stale"
	}
	return g.reason
}

func (g storageDetailGate) matches(b StorageDetailBinding) bool {
	return g.runID == b.RunID && g.nodeStartedAt.Equal(b.NodeStartedAt) && g.genesisHash == b.GenesisHash
}

type storageDetailPublication struct {
	body       string
	freshUntil time.Time
	binding    StorageDetailBinding
	capturedAt time.Time
	reason     string
}

type storageDetailSource struct {
	sample         StorageDetailSampler
	gate           atomic.Pointer[storageDetailGate]
	latest         atomic.Pointer[storageDetailPublication]
	highwaterGate  storageDetailGate
	highwaterGen   uint64
	highwaterStart time.Time
	lastCapture    *storageDetailPublication
}

// update runs serially after accepted status. It copies the owner once and
// publishes only cached bytes; HTTP never invokes the owner sampler.
func (s *storageDetailSource) update(snapshot Snapshot, now time.Time) {
	g := storageGate(snapshot, now)
	s.gate.Store(&g)
	p := &storageDetailPublication{binding: StorageDetailBinding{
		RunID: g.runID, NodeStartedAt: g.nodeStartedAt, GenesisHash: g.genesisHash,
	}, reason: g.failure(now)}
	defer func() { s.latest.Store(p) }()
	if p.reason != "" {
		return
	}
	capture, err := s.sample(now)
	completedAt := time.Now()
	if err != nil {
		p.reason = storageDetailReason(err.Error())
		return
	}
	if !g.matches(capture.Detail.Binding) {
		p.reason = "binding_changed"
		return
	}
	if validateStorageDetail(capture.Detail) != nil {
		p.reason = "source_invalid"
		return
	}
	if storageDetailTimeInvalid(capture, completedAt) {
		p.reason = "source_time_invalid"
		return
	}
	binding := capture.Detail.Binding
	if s.highwaterGen != 0 && sameStorageNode(s.highwaterGate, g) && (binding.DBGeneration < s.highwaterGen ||
		binding.DBGeneration == s.highwaterGen && !binding.DBOpenedAt.Equal(s.highwaterStart)) {
		p.reason = "binding_changed"
		return
	}
	body, err := json.Marshal(capture.Detail)
	if err != nil {
		p.reason = "source_invalid"
		return
	}
	if len(body) > MaxStorageDetailBytes {
		p.reason = "response_limit"
		return
	}
	finishedAt := time.Now()
	current := s.gate.Load()
	if current == nil {
		p.reason = "waiting_source"
		return
	}
	if reason := current.failure(finishedAt); reason != "" {
		p.reason = reason
		return
	}
	if !current.matches(capture.Detail.Binding) {
		p.reason = "binding_changed"
		return
	}
	if !capture.FreshUntil.After(finishedAt) {
		p.reason = "source_time_invalid"
		return
	}
	p.body, p.freshUntil = string(body), capture.FreshUntil
	p.binding, p.capturedAt = capture.Detail.Binding, capture.Detail.CapturedAt
	if previous := s.lastCapture; previous != nil && sameStorageCapture(previous, p) {
		if previous.body != p.body {
			p.body, p.freshUntil, p.reason = "", time.Time{}, "source_invalid"
			return
		}
		if previous.freshUntil.Before(p.freshUntil) {
			p.freshUntil = previous.freshUntil
		}
	}
	if s.highwaterGen == 0 || !sameStorageNode(s.highwaterGate, *current) || binding.DBGeneration > s.highwaterGen {
		s.highwaterGate, s.highwaterGen, s.highwaterStart = *current, binding.DBGeneration, binding.DBOpenedAt
	}
	s.lastCapture = p
}

func sameStorageCapture(a, b *storageDetailPublication) bool {
	return a.binding.RunID == b.binding.RunID && a.binding.NodeStartedAt.Equal(b.binding.NodeStartedAt) &&
		a.binding.GenesisHash == b.binding.GenesisHash && a.binding.DBGeneration == b.binding.DBGeneration &&
		a.binding.DBOpenedAt.Equal(b.binding.DBOpenedAt) && a.capturedAt.Equal(b.capturedAt)
}

func sameStorageNode(a, b storageDetailGate) bool {
	return a.runID == b.runID && a.nodeStartedAt.Equal(b.nodeStartedAt) && a.genesisHash == b.genesisHash
}

func storageDetailReason(reason string) string {
	switch reason {
	case "waiting_source", "status_stale", "source_unavailable", "source_invalid", "source_time_invalid", "binding_changed", "response_limit", "invalid_request":
		return reason
	default:
		return "source_unavailable"
	}
}

func (s *storageDetailSource) serveHTTP(w http.ResponseWriter, r *http.Request) {
	status, reason, body := http.StatusServiceUnavailable, "waiting_source", ""
	var remaining int64
	if r.Method != http.MethodGet || r.URL.Path != "/details/storage" || r.URL.EscapedPath() != "/details/storage" ||
		r.URL.RawQuery != "" || r.URL.ForceQuery || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
		status, reason = http.StatusBadRequest, "invalid_request"
	} else if g := s.gate.Load(); g != nil {
		now := time.Now()
		reason = g.failure(now)
		if reason == "" {
			p := s.latest.Load()
			switch {
			case p == nil:
				reason = "waiting_source"
			case !g.matches(p.binding):
				reason = "binding_changed"
			case p.reason != "":
				reason = p.reason
			case !p.freshUntil.After(now):
				reason = "status_stale"
			default:
				remaining = p.freshUntil.Sub(now).Milliseconds()
				if remaining <= 0 {
					reason = "status_stale"
				} else {
					status, body = http.StatusOK, p.body
				}
			}
		}
	}
	if body == "" {
		body = `{"schema_version":1,"family":"storage","state":"unavailable","reason":"` + storageDetailReason(reason) + `"}`
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if status == http.StatusOK {
		w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(remaining, 10))
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}
