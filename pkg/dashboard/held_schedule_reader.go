package dashboard

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// PublicHeldScheduleManifest explicitly lists public metadata. The dataset is
// already the canonical public-intended HeldScheduleData schema, not a proxy
// of arbitrary private JSON. Its digest names identical private/public bytes.
type PublicHeldScheduleManifest struct {
	SchemaVersion        int                  `json:"schema_version"`
	Family               string               `json:"family"`
	Scope                string               `json:"scope"`
	State                string               `json:"state"`
	Reason               string               `json:"reason"`
	CollectorAt          time.Time            `json:"collector_at"`
	Binding              ReplayDetailBinding  `json:"binding"`
	CapturedAt           time.Time            `json:"captured_at"`
	CaptureSequence      uint64               `json:"capture_sequence,string"`
	Digest               string               `json:"digest"`
	Bytes                int                  `json:"bytes"`
	ObservedAt           time.Time            `json:"observed_at"`
	ReferenceSlot        uint64               `json:"reference_slot,string"`
	ReferenceBankhash    string               `json:"reference_bankhash"`
	ReferenceCompletedAt time.Time            `json:"reference_completed_at"`
	Current              HeldScheduleSummary  `json:"current"`
	Next                 *HeldScheduleSummary `json:"next"`
	NextReason           string               `json:"next_reason"`
}

type publicHeldSchedule struct {
	manifest       HeldScheduleManifest
	body           string
	data           *PreparedHeldScheduleData
	manifestDigest [sha256.Size]byte
	freshUntil     time.Time
	stamp          statusStamp
}

// heldScheduleReader runs serially in the existing joined detail reader. It
// fetches a small manifest each cycle and data only when its digest changes.
// Viewers never perform source reads, decode datasets or create workers.
type heldScheduleReader struct {
	client        *http.Client
	gate          *atomic.Pointer[replayDetailGate]
	latest        atomic.Pointer[publicHeldSchedule]
	failure       atomic.Pointer[replayDetailCaptureFailure]
	manifestLimit *rate.Limiter
	dataLimit     *rate.Limiter
	active        chan struct{}
}

func (r *heldScheduleReader) fetch(ctx context.Context, path string, limit int, digest string) ([]byte, time.Time, time.Duration, string) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://node"+path, nil)
	if err != nil {
		return nil, time.Time{}, 0, "source_invalid"
	}
	started := time.Now()
	response, err := r.client.Do(request)
	if err != nil {
		return nil, started, 0, "source_unavailable"
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		limit = 512
	}
	if response.ContentLength > int64(limit) {
		return nil, started, 0, "response_limit"
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, int64(limit)+1))
	if err != nil {
		return nil, started, 0, "source_unavailable"
	}
	if len(body) > limit {
		return nil, started, 0, "response_limit"
	}
	if response.StatusCode != http.StatusOK {
		if len(response.Header.Values(replayDetailRemainingHeader)) != 0 {
			return nil, started, 0, "source_invalid"
		}
		if response.StatusCode == http.StatusNotFound {
			return nil, started, 0, "unsupported_source"
		}
		if response.StatusCode == http.StatusTooManyRequests {
			return nil, started, 0, "source_unavailable"
		}
		if response.StatusCode != http.StatusServiceUnavailable {
			return nil, started, 0, "source_invalid"
		}
		return nil, started, 0, decodeHeldScheduleFailure(body)
	}
	values := response.Header.Values(replayDetailRemainingHeader)
	if len(values) != 1 {
		return nil, started, 0, "source_invalid"
	}
	remaining, err := parseDetailRemaining(values[0], HeldScheduleFreshFor)
	if err != nil {
		return nil, started, 0, "source_invalid"
	}
	if digest != "" {
		etags := response.Header.Values("ETag")
		if len(etags) != 1 || etags[0] != `"`+digest+`"` {
			return nil, started, 0, "source_invalid"
		}
	}
	return body, started, remaining, ""
}

func decodeHeldScheduleFailure(body []byte) string {
	reason := decodeDetailFailure(body, "held_schedule", func(reason string) bool {
		return reason != "invalid_request" && heldScheduleReason(reason) == reason
	})
	if reason == "rate_limited" {
		return "source_unavailable"
	}
	return reason
}

func (r *heldScheduleReader) collect(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	before := r.gate.Load()
	if before == nil {
		return
	}
	fail := func(reason string) {
		if ctx.Err() == nil {
			r.failure.Store(&replayDetailCaptureFailure{gate: *before, reason: heldScheduleReason(reason)})
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
	body, started, remaining, reason := r.fetch(ctx, "/details/schedule", MaxSnapshotBytes, "")
	if reason != "" {
		fail(reason)
		return
	}
	freshUntil := started.Add(remaining)
	m, err := decodeHeldScheduleManifest(body)
	if err != nil {
		fail("source_invalid")
		return
	}
	if !before.matches(m.Binding) {
		fail("binding_changed")
		return
	}
	if m.CapturedAt.After(time.Now().Add(time.Second)) || !freshUntil.After(time.Now()) {
		fail("source_time_invalid")
		return
	}
	manifestDigest := sha256.Sum256(body)
	old := r.latest.Load() // Single collection owner; failures never discard this high water.
	repeated := false
	if old != nil && before.matches(old.manifest.Binding) {
		previous := old.manifest
		if m.CaptureSequence < previous.CaptureSequence || m.Binding.SourceGeneration < previous.Binding.SourceGeneration {
			fail("binding_changed")
			return
		}
		if m.Binding.SourceGeneration == previous.Binding.SourceGeneration &&
			(!sameReplayDetailBinding(m.Binding, previous.Binding) || m.Digest != previous.Digest) {
			fail("source_invalid")
			return
		}
		repeated = m.CaptureSequence == previous.CaptureSequence
		if repeated {
			if manifestDigest != old.manifestDigest {
				fail("source_invalid")
				return
			}
			if old.freshUntil.Before(freshUntil) {
				freshUntil = old.freshUntil
			}
		}
	}
	if !freshUntil.After(time.Now()) {
		fail("source_time_invalid")
		return
	}
	var data *PreparedHeldScheduleData
	if old != nil && old.data.digest == m.Digest {
		data = old.data
	} else {
		// This request cannot outlive the original manifest lease. No retry or
		// later manifest is used to rescue an expired collection.
		dataCtx, cancel := context.WithDeadline(ctx, freshUntil)
		raw, dataStarted, dataRemaining, reason := r.fetch(dataCtx, "/details/schedule/data/"+m.Digest, m.Bytes, m.Digest)
		cancel()
		if !freshUntil.After(time.Now()) {
			fail("source_time_invalid")
			return
		}
		if reason != "" {
			fail(reason)
			return
		}
		dataUntil := dataStarted.Add(dataRemaining)
		if dataUntil.Before(freshUntil) {
			freshUntil = dataUntil
		}
		if len(raw) != m.Bytes {
			fail("source_invalid")
			return
		}
		data, err = decodeHeldScheduleData(raw)
		if err != nil {
			fail("source_invalid")
			return
		}
	}
	if !heldScheduleDataMatchesManifest(data, m) {
		fail("source_invalid")
		return
	}
	publicBody, err := publicHeldScheduleManifest(m, time.Now().UTC())
	if err != nil {
		fail("response_limit")
		return
	}
	after := r.gate.Load()
	if after == nil || !sameReplayDetailStatusContext(*before, *after) || !after.matches(m.Binding) || after.retired || after.stopping {
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
	p := &publicHeldSchedule{manifest: m, body: publicBody, data: data, manifestDigest: manifestDigest, freshUntil: freshUntil, stamp: before.stamp}
	if repeated {
		p.body = old.body // Preserve the original CollectorAt for this capture.
	}
	r.latest.Store(p)
	r.failure.Store(nil)
}

func heldScheduleDataMatchesManifest(p *PreparedHeldScheduleData, m HeldScheduleManifest) bool {
	return p != nil && p.digest == m.Digest && len(p.body) == m.Bytes && sameReplayDetailBinding(p.binding, m.Binding) &&
		p.observedAt.Equal(m.ObservedAt) && p.current == m.Current && p.nextReason == m.NextReason &&
		(p.next == nil && m.Next == nil || p.next != nil && m.Next != nil && *p.next == *m.Next)
}

func publicHeldScheduleManifest(m HeldScheduleManifest, collectedAt time.Time) (string, error) {
	p := PublicHeldScheduleManifest{SchemaVersion: 1, Family: m.Family, Scope: m.Scope, State: "fresh", CollectorAt: collectedAt,
		Binding: m.Binding, CapturedAt: m.CapturedAt, CaptureSequence: m.CaptureSequence, Digest: m.Digest, Bytes: m.Bytes,
		ObservedAt: m.ObservedAt, ReferenceSlot: m.ReferenceSlot, ReferenceBankhash: m.ReferenceBankhash, ReferenceCompletedAt: m.ReferenceCompletedAt,
		Current: m.Current, Next: m.Next, NextReason: m.NextReason}
	body, err := json.Marshal(p)
	if err != nil || len(body) > MaxSnapshotBytes {
		return "", errors.New("response_limit")
	}
	return string(body), nil
}

func (r *heldScheduleReader) serveHTTP(w http.ResponseWriter, request *http.Request, admit func(statusStamp, func() (statusStamp, bool)) (time.Duration, bool)) {
	const path = "/api/v1/details/schedule"
	digest := ""
	if strings.HasPrefix(request.URL.Path, path+"/data/") {
		digest = strings.TrimPrefix(request.URL.Path, path+"/data/")
	}
	if request.Method != http.MethodGet || request.URL.Path != request.URL.EscapedPath() ||
		(request.URL.Path != path && (request.URL.Path != path+"/data/"+digest || !validHeldScheduleDigest(digest))) ||
		request.URL.RawQuery != "" || request.URL.ForceQuery || request.ContentLength != 0 || len(request.TransferEncoding) != 0 {
		writeHeldScheduleReply(w, http.StatusBadRequest, "invalid_request", "", 0, "")
		return
	}
	limit := r.manifestLimit
	if digest != "" {
		limit = r.dataLimit
	}
	if !limit.Allow() {
		writeHeldScheduleReply(w, http.StatusTooManyRequests, "rate_limited", "", 0, "")
		return
	}
	g := r.gate.Load()
	reason := "waiting_source"
	var selected *publicHeldSchedule
	var selectedFailure *replayDetailCaptureFailure
	var remaining int64
	if g != nil {
		reason = g.failure(time.Now())
		if reason == "" && (g.State != "fresh" || g.retired || g.stopping || !g.stamp.valid()) {
			reason = "status_stale"
		}
		// Successful collections publish before clearing failure.
		failure, p := r.failure.Load(), r.latest.Load()
		if reason == "" {
			switch {
			case failure != nil && sameReplayDetailStatusContext(failure.gate, *g):
				reason = failure.reason
			case p == nil:
				reason = "waiting_source"
			case !g.matches(p.manifest.Binding) || p.stamp != g.stamp:
				reason = "binding_changed"
			}
		}
		if reason == "" {
			remaining = p.freshUntil.Sub(time.Now()).Milliseconds()
			if remaining < 1 {
				reason = "status_stale"
			} else if digest != "" && digest != p.data.digest {
				reason = "binding_changed"
			} else if digest == "" {
				selected, selectedFailure = p, failure
			} else {
				select {
				case r.active <- struct{}{}:
					defer func() { <-r.active }()
					selected, selectedFailure = p, failure
				default:
					writeHeldScheduleReply(w, http.StatusTooManyRequests, "rate_limited", "", 0, "")
					return
				}
			}
		}
	}
	if selected != nil {
		var statusRemaining time.Duration
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
		if ok && statusRemaining.Milliseconds() > 0 && remaining > 0 {
			w.Header().Set(statusRemainingHeader, strconv.FormatInt(statusRemaining.Milliseconds(), 10))
			body := selected.body
			if digest != "" {
				body = selected.data.body
			}
			writeHeldScheduleReply(w, http.StatusOK, "", body, remaining, digest)
			return
		}
		reason = "status_stale"
	}
	writeHeldScheduleReply(w, http.StatusServiceUnavailable, reason, "", 0, "")
}
