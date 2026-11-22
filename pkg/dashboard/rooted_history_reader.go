package dashboard

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// PublicRootedHistoryDetail allowlists the rooted-bank facts a reader may
// publish. CollectorAt dates receipt, never the bank or its source observation.
type PublicRootedHistoryDetail struct {
	SchemaVersion int                 `json:"schema_version"`
	Family        string              `json:"family"`
	Scope         string              `json:"scope"`
	State         string              `json:"state"`
	Reason        string              `json:"reason"`
	CollectorAt   time.Time           `json:"collector_at"`
	Binding       ReplayDetailBinding `json:"binding"`
	CapturedAt    time.Time           `json:"captured_at"`
	ObservedAt    time.Time           `json:"observed_at"`
	BankSlot      uint64              `json:"bank_slot,string"`
	Bankhash      string              `json:"bankhash"`
	OldestSlot    uint64              `json:"oldest_slot,string"`
	NextSlot      uint64              `json:"next_slot,string"`
	Capacity      int                 `json:"capacity"`
	Encoding      string              `json:"encoding"`
	Bitmap        string              `json:"bitmap"`
	Epoch         *RootedHistoryEpoch `json:"epoch,omitempty"`
}

type publicRootedHistory struct {
	detail     RootedHistoryDetail
	body       string
	digest     [sha256.Size]byte
	freshUntil time.Time
	stamp      statusStamp
}

// rootedHistoryReader runs serially in the existing replay-detail worker and
// shares that worker's fixed, bounded Unix client. Viewers only read a cache.
type rootedHistoryReader struct {
	client   *http.Client
	gate     *atomic.Pointer[replayDetailGate]
	limit    *rate.Limiter
	latest   atomic.Pointer[publicRootedHistory]
	failure  atomic.Pointer[replayDetailCaptureFailure]
	lastGood *publicRootedHistory
}

func (r *rootedHistoryReader) collect(ctx context.Context) {
	before := r.gate.Load()
	if before == nil || ctx.Err() != nil {
		return
	}
	fail := func(reason string) {
		if ctx.Err() == nil {
			r.failure.Store(&replayDetailCaptureFailure{gate: *before, reason: reason})
		}
	}
	if reason := before.failure(time.Now()); reason != "" {
		fail(reason)
		return
	}
	if before.State != "fresh" {
		fail("status_stale")
		return
	}
	if !before.stamp.valid() || before.retired || before.stopping {
		fail("waiting_source")
		return
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://node/details/slots", nil)
	if err != nil {
		fail("source_invalid")
		return
	}
	started := time.Now()
	response, err := r.client.Do(request)
	if err != nil {
		fail("source_unavailable")
		return
	}
	defer response.Body.Close()
	limit := int64(MaxRootedHistoryBytes + 1)
	if response.StatusCode != http.StatusOK {
		limit = 513
	}
	if response.ContentLength >= limit {
		fail("response_limit")
		return
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, limit))
	if err != nil {
		fail("source_unavailable")
		return
	}
	if len(body) >= int(limit) {
		fail("response_limit")
		return
	}
	if response.StatusCode == http.StatusNotFound {
		fail("unsupported_source")
		return
	}
	if response.StatusCode != http.StatusOK {
		if response.StatusCode != http.StatusServiceUnavailable || len(response.Header.Values(replayDetailRemainingHeader)) != 0 {
			fail("source_invalid")
			return
		}
		fail(decodeRootedHistoryFailure(body))
		return
	}
	values := response.Header.Values(replayDetailRemainingHeader)
	if len(values) != 1 {
		fail("source_invalid")
		return
	}
	// The same 30-second ceiling and canonical integer format apply here.
	remaining, err := parseDetailRemaining(values[0], RootedHistoryFreshFor)
	if err != nil {
		fail("source_invalid")
		return
	}
	d, err := decodeRootedHistoryDetail(body)
	if err != nil {
		fail("source_invalid")
		return
	}
	if !before.matches(d.Binding) {
		fail("binding_changed")
		return
	}
	if d.CapturedAt.After(time.Now().Add(time.Second)) {
		fail("source_time_invalid")
		return
	}
	freshUntil := started.Add(remaining)
	if !freshUntil.After(time.Now()) {
		fail("source_time_invalid")
		return
	}
	if old := r.lastGood; old != nil && before.matches(old.detail.Binding) {
		if !sameReplayDetailBinding(old.detail.Binding, d.Binding) || d.BankSlot < old.detail.BankSlot {
			fail("binding_changed")
			return
		}
		if d.BankSlot == old.detail.BankSlot && !sameRootedHistoryBank(d, old.detail) {
			fail("source_invalid")
			return
		}
	}
	digest := sha256.Sum256(body)
	if old := r.lastGood; old != nil && sameReplayDetailBinding(old.detail.Binding, d.Binding) && old.detail.CapturedAt.Equal(d.CapturedAt) {
		if old.digest != digest {
			fail("source_invalid")
			return
		}
		if old.freshUntil.Before(freshUntil) {
			freshUntil = old.freshUntil
		}
	}
	now := time.Now()
	public := PublicRootedHistoryDetail{SchemaVersion: 1, Family: d.Family, Scope: d.Scope, State: "fresh", CollectorAt: now.UTC(),
		Binding: d.Binding, CapturedAt: d.CapturedAt, ObservedAt: d.ObservedAt, BankSlot: d.BankSlot, Bankhash: d.Bankhash,
		OldestSlot: d.OldestSlot, NextSlot: d.NextSlot, Capacity: d.Capacity, Encoding: d.Encoding, Bitmap: d.Bitmap, Epoch: d.Epoch}
	encoded, err := json.Marshal(public)
	if err != nil || len(encoded) > MaxRootedHistoryBytes {
		fail("response_limit")
		return
	}
	after := r.gate.Load()
	if after == nil || !sameReplayDetailStatusContext(*before, *after) || !after.matches(d.Binding) || after.retired || after.stopping {
		fail("binding_changed")
		return
	}
	if reason := after.failure(time.Now()); reason != "" {
		fail(reason)
		return
	}
	if after.State != "fresh" {
		fail("status_stale")
		return
	}
	if !freshUntil.After(time.Now()) {
		fail("source_time_invalid")
		return
	}
	if ctx.Err() != nil {
		return
	}
	p := &publicRootedHistory{detail: d, body: string(encoded), digest: digest, freshUntil: freshUntil, stamp: before.stamp}
	if old := r.lastGood; old != nil && sameReplayDetailBinding(old.detail.Binding, d.Binding) && old.detail.CapturedAt.Equal(d.CapturedAt) {
		copy := *old
		copy.freshUntil, copy.stamp = freshUntil, before.stamp
		p = &copy // Preserve the body/date, but honor an earlier advertised expiry.
	}
	r.lastGood = p
	r.latest.Store(p)
	r.failure.Store(nil)
}

func decodeRootedHistoryFailure(body []byte) string {
	return decodeDetailFailure(body, "rooted_history", func(reason string) bool {
		return reason != "invalid_request" && rootedHistoryReason(reason) == reason
	})
}

func (r *rootedHistoryReader) serveHTTP(w http.ResponseWriter, request *http.Request, admit func(statusStamp, func() (statusStamp, bool)) (time.Duration, bool)) {
	status, reason, body := http.StatusServiceUnavailable, "waiting_source", ""
	remaining := int64(0)
	var selected *publicRootedHistory
	var selectedFailure *replayDetailCaptureFailure
	if request.Method != http.MethodGet || request.URL.Path != "/api/v1/details/slots" || request.URL.EscapedPath() != "/api/v1/details/slots" || request.URL.RawQuery != "" || request.URL.ForceQuery || request.ContentLength != 0 || len(request.TransferEncoding) != 0 {
		status, reason = http.StatusBadRequest, "invalid_request"
	} else if !r.limit.Allow() {
		status, reason = http.StatusTooManyRequests, "rate_limited"
	} else if g := r.gate.Load(); g != nil {
		now := time.Now()
		reason = g.failure(now)
		if reason == "" && (g.State != "fresh" || g.retired || g.stopping || !g.stamp.valid()) {
			reason = "status_stale"
		}
		if reason == "" {
			// Collectors publish before clearing failure; read failure first.
			failure, p := r.failure.Load(), r.latest.Load()
			switch {
			case failure != nil && sameReplayDetailStatusContext(failure.gate, *g):
				reason = failure.reason
			case p == nil:
				reason = "waiting_source"
			case !g.matches(p.detail.Binding) || p.stamp != g.stamp:
				reason = "binding_changed"
			default:
				remaining = p.freshUntil.Sub(now).Milliseconds()
				if remaining < 1 {
					reason = "status_stale"
				} else {
					status, body = http.StatusOK, p.body
					selected, selectedFailure = p, failure
				}
			}
		}
	}
	var statusRemaining time.Duration
	if status == http.StatusOK {
		var ok bool
		if admit != nil {
			statusRemaining, ok = admit(selected.stamp, func() (statusStamp, bool) {
				gate := r.gate.Load()
				if gate == nil {
					return statusStamp{}, false
				}
				return gate.stamp, !gate.retired && !gate.stopping && gate.State == "fresh" && gate.failure(time.Now()) == "" &&
					r.latest.Load() == selected && r.failure.Load() == selectedFailure && time.Now().Before(selected.freshUntil)
			})
		}
		remaining = min(remaining, time.Until(selected.freshUntil).Milliseconds())
		if !ok || statusRemaining.Milliseconds() < 1 || remaining < 1 {
			status, reason, body = http.StatusServiceUnavailable, "status_stale", ""
		}
	}
	if body == "" {
		if reason != "rate_limited" && reason != "unsupported_source" {
			reason = rootedHistoryReason(reason)
		}
		body = `{"schema_version":1,"family":"rooted_history","state":"unavailable","reason":"` + reason + `"}`
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if status == http.StatusOK {
		w.Header().Set(statusRemainingHeader, strconv.FormatInt(statusRemaining.Milliseconds(), 10))
		w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(remaining, 10))
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}
