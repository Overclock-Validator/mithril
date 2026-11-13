package dashboard

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

type productionDetailGate struct {
	runID         string
	nodeStartedAt time.Time
	genesisHash   string
	observedAt    time.Time
	reason        string
	stamp         statusStamp
}

func productionGate(s Snapshot, now time.Time) productionDetailGate {
	g := productionDetailGate{runID: s.RunID, nodeStartedAt: s.StartedAt, observedAt: now}
	if s.GenesisHash != nil {
		g.genesisHash = *s.GenesisHash
	}
	switch {
	case s.Mode != "validator" || !s.Alpenglow:
		g.reason = "not_applicable"
	case s.GenesisHash == nil:
		g.reason = "waiting_source"
	}
	return g
}

func (g productionDetailGate) failure(now time.Time) string {
	if g.observedAt.IsZero() || now.Before(g.observedAt) {
		return "source_time_invalid"
	}
	if now.Sub(g.observedAt) > StaleAfter {
		return "status_stale"
	}
	return g.reason
}

func (g productionDetailGate) matches(b ProductionDetailBinding) bool {
	return g.runID == b.RunID && g.nodeStartedAt.Equal(b.NodeStartedAt) && g.genesisHash == b.GenesisHash
}

type productionDetailPublication struct {
	body       string
	freshUntil time.Time
	binding    ProductionDetailBinding
	capturedAt time.Time
	reason     string
}

type productionDetailSource struct {
	sample         ProductionDetailSampler
	gate           atomic.Pointer[productionDetailGate]
	latest         atomic.Pointer[productionDetailPublication]
	highwaterGate  productionDetailGate
	highwaterGen   uint64
	highwaterStart time.Time
	lastCapture    *productionDetailPublication
}

// update runs serially after accepted status. It copies the owner once and
// publishes only cached bytes; HTTP never invokes the owner sampler.
func (s *productionDetailSource) update(snapshot Snapshot, now time.Time) {
	g := productionGate(snapshot, now)
	s.gate.Store(&g)
	p := &productionDetailPublication{binding: ProductionDetailBinding{
		RunID: g.runID, NodeStartedAt: g.nodeStartedAt, GenesisHash: g.genesisHash,
	}, reason: g.failure(now)}
	defer func() { s.latest.Store(p) }()
	if p.reason != "" {
		return
	}
	capture, err := s.sample(now)
	completedAt := time.Now()
	if err != nil {
		p.reason = productionDetailReason(err.Error())
		return
	}
	if !g.matches(capture.Detail.Binding) {
		p.reason = "binding_changed"
		return
	}
	if validateProductionDetail(capture.Detail) != nil {
		p.reason = "source_invalid"
		return
	}
	if productionDetailTimeInvalid(capture, completedAt) {
		p.reason = "source_time_invalid"
		return
	}
	binding := capture.Detail.Binding
	if s.highwaterGen != 0 && sameProductionNode(s.highwaterGate, g) && (binding.ProducerGeneration < s.highwaterGen ||
		binding.ProducerGeneration == s.highwaterGen && !binding.ProducerStartedAt.Equal(s.highwaterStart)) {
		p.reason = "binding_changed"
		return
	}
	body, err := json.Marshal(capture.Detail)
	if err != nil {
		p.reason = "source_invalid"
		return
	}
	if len(body) > MaxProductionDetailBytes {
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
	if previous := s.lastCapture; previous != nil && sameProductionCapture(previous, p) {
		if previous.body != p.body {
			p.body, p.freshUntil, p.reason = "", time.Time{}, "source_invalid"
			return
		}
		if previous.freshUntil.Before(p.freshUntil) {
			p.freshUntil = previous.freshUntil
		}
	}
	if s.highwaterGen == 0 || !sameProductionNode(s.highwaterGate, *current) || binding.ProducerGeneration > s.highwaterGen {
		s.highwaterGate, s.highwaterGen, s.highwaterStart = *current, binding.ProducerGeneration, binding.ProducerStartedAt
	}
	s.lastCapture = p
}

func productionDetailTimeInvalid(c ProductionDetailCapture, completedAt time.Time) bool {
	d, limit := c.Detail, completedAt.Add(time.Second)
	if c.FreshUntil.IsZero() || !c.FreshUntil.After(completedAt) || c.FreshUntil.After(completedAt.Add(ProductionDetailFreshFor)) ||
		c.FreshUntil.Sub(d.CapturedAt) > ProductionDetailFreshFor ||
		d.Binding.NodeStartedAt.After(limit) || d.Binding.ProducerStartedAt.After(limit) || d.CapturedAt.After(limit) ||
		completedAt.Sub(d.CapturedAt) >= ProductionDetailFreshFor {
		return true
	}
	if e := d.Engine; e != nil && (e.StartedAt.After(limit) || e.SampledAt.After(limit) ||
		completedAt.Sub(e.SampledAt) >= ProductionDetailFreshFor || c.FreshUntil.After(e.SampledAt.Add(ProductionDetailFreshFor)) ||
		e.LastSlot != nil && e.LastSlot.ObservedAt.After(limit)) {
		return true
	}
	if s := d.SchedulerBudget; s != nil && (s.SampledAt.After(limit) || completedAt.Sub(s.SampledAt) >= ProductionDetailFreshFor ||
		c.FreshUntil.After(s.SampledAt.Add(ProductionDetailFreshFor))) {
		return true
	}
	if p := d.ParentReady; p != nil && (p.ReadyAt.After(limit) || p.ObservedAt.After(limit) ||
		p.ReadyAt.After(d.CapturedAt.Add(time.Second)) || p.ObservedAt.After(d.CapturedAt.Add(time.Second))) {
		return true
	}
	if t := d.LastTerminal; t != nil && t.Finalization != nil && t.Finalization.WindowCompletion != nil {
		w := t.Finalization.WindowCompletion
		for _, at := range []time.Time{w.ReadyAt, w.NetworkCompletedAt} {
			if at.After(limit) || at.After(d.CapturedAt.Add(time.Second)) {
				return true
			}
		}
	}
	return d.LastTerminal != nil && (d.LastTerminal.ObservedAt.After(limit) || d.LastTerminal.ObservedAt.After(d.CapturedAt.Add(time.Second)))
}

func sameProductionCapture(a, b *productionDetailPublication) bool {
	return a.binding.RunID == b.binding.RunID && a.binding.NodeStartedAt.Equal(b.binding.NodeStartedAt) &&
		a.binding.GenesisHash == b.binding.GenesisHash && a.binding.ProducerGeneration == b.binding.ProducerGeneration &&
		a.binding.ProducerStartedAt.Equal(b.binding.ProducerStartedAt) && a.capturedAt.Equal(b.capturedAt)
}

func sameProductionNode(a, b productionDetailGate) bool {
	return a.runID == b.runID && a.nodeStartedAt.Equal(b.nodeStartedAt) && a.genesisHash == b.genesisHash
}

func productionDetailReason(reason string) string {
	switch reason {
	case "waiting_source", "status_stale", "source_unavailable", "source_invalid", "source_time_invalid", "binding_changed", "response_limit", "invalid_request", "not_applicable":
		return reason
	default:
		return "source_unavailable"
	}
}

func (s *productionDetailSource) serveHTTP(w http.ResponseWriter, r *http.Request) {
	status, reason, body := http.StatusServiceUnavailable, "waiting_source", ""
	var remaining int64
	if r.Method != http.MethodGet || r.URL.Path != "/details/production" || r.URL.EscapedPath() != "/details/production" ||
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
		body = `{"schema_version":1,"family":"production","state":"unavailable","reason":"` + productionDetailReason(reason) + `"}`
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if status == http.StatusOK {
		w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(remaining, 10))
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}
