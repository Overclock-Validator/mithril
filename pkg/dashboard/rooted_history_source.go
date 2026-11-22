package dashboard

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
)

type rootedHistoryPublication struct {
	detail     RootedHistoryDetail
	body       string
	freshUntil time.Time
	reason     string
}

type rootedHistorySource struct {
	sample   RootedHistorySampler
	gate     atomic.Pointer[replayDetailGate]
	latest   atomic.Pointer[rootedHistoryPublication]
	lastGood *rootedHistoryPublication // Owned by the serial Feed sampling loop.
}

// update uses the existing Feed cycle. Only immutable bytes reach HTTP; failed
// captures replace availability without discarding the last binding fence.
func (s *rootedHistorySource) update(snapshot Snapshot, now time.Time) {
	g := replayGate(snapshot, now)
	s.gate.Store(&g)
	p := &rootedHistoryPublication{reason: g.failure(now)}
	defer func() { s.latest.Store(p) }()
	if p.reason != "" {
		return
	}
	capture, err := s.sample(now)
	if err != nil {
		p.reason = rootedHistoryReason(err.Error())
		return
	}
	d := capture.Detail
	if !g.matches(d.Binding) {
		p.reason = "binding_changed"
		return
	}
	if validateRootedHistoryDetail(d) != nil {
		p.reason = "source_invalid"
		return
	}
	completedAt := time.Now()
	if !d.CapturedAt.Equal(now.UTC()) || capture.FreshUntil == capture.FreshUntil.Round(0) ||
		!capture.FreshUntil.After(completedAt) || capture.FreshUntil.After(now.Add(RootedHistoryFreshFor)) {
		p.reason = "source_time_invalid"
		return
	}
	if previous := s.lastGood; previous != nil && g.matches(previous.detail.Binding) {
		old := previous.detail
		if !sameReplayDetailBinding(old.Binding, d.Binding) || d.BankSlot < old.BankSlot {
			p.reason = "binding_changed"
			return
		}
		if d.BankSlot == old.BankSlot && !sameRootedHistoryBank(d, old) {
			p.reason = "source_invalid"
			return
		}
	}
	body, err := json.Marshal(d)
	if err != nil || len(body) > MaxRootedHistoryBytes {
		p.reason = "response_limit"
		return
	}
	if !capture.FreshUntil.After(time.Now()) {
		p.reason = "source_time_invalid"
		return
	}
	if d.Binding.GenesisHash != nil {
		value := *d.Binding.GenesisHash
		d.Binding.GenesisHash = &value
	}
	if d.Epoch != nil {
		value := *d.Epoch
		d.Epoch = &value
	}
	p.detail, p.body, p.freshUntil = d, string(body), capture.FreshUntil
	if previous := s.lastGood; previous != nil && previous.body == p.body && previous.freshUntil.Before(p.freshUntil) {
		p.freshUntil = previous.freshUntil
	}
	s.lastGood = p
}

func rootedHistoryReason(reason string) string {
	switch reason {
	case "waiting_source", "status_stale", "source_unavailable", "source_invalid", "source_time_invalid", "binding_changed", "response_limit", "invalid_request":
		return reason
	default:
		return "source_unavailable"
	}
}

func (s *rootedHistorySource) serveHTTP(w http.ResponseWriter, r *http.Request) {
	status, reason, body := http.StatusServiceUnavailable, "waiting_source", ""
	remaining := int64(0)
	if r.Method != http.MethodGet || r.URL.Path != "/details/slots" || r.URL.EscapedPath() != "/details/slots" ||
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
			case p.reason != "":
				reason = p.reason
			case !g.matches(p.detail.Binding):
				reason = "binding_changed"
			default:
				remaining = p.freshUntil.Sub(now).Milliseconds()
				if remaining < 1 {
					reason = "status_stale"
				} else {
					status, body = http.StatusOK, p.body
				}
			}
		}
	}
	if body == "" {
		body = `{"schema_version":1,"family":"rooted_history","state":"unavailable","reason":"` + rootedHistoryReason(reason) + `"}`
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if status == http.StatusOK {
		w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(remaining, 10))
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}
