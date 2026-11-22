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

type heldSchedulePublication struct {
	manifest   HeldScheduleManifest
	body       string
	data       *PreparedHeldScheduleData
	freshUntil time.Time
}

// heldScheduleSource is polled by the existing joined detail worker. Only its
// small gate is shared with one-second status publication; HTTP reads a cache.
type heldScheduleSource struct {
	sample   HeldScheduleSampler
	gate     *atomic.Pointer[replayDetailGate]
	gateMu   *sync.Mutex
	latest   atomic.Pointer[heldSchedulePublication]
	failure  atomic.Pointer[replayDetailCaptureFailure]
	active   chan struct{}
	sequence uint64 // Owned only by the joined worker.
}

func (s *heldScheduleSource) poll(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	before := s.gate.Load()
	if before == nil {
		return
	}
	fail := func(reason string) {
		if ctx.Err() == nil {
			s.failure.Store(&replayDetailCaptureFailure{gate: *before, reason: heldScheduleReason(reason)})
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
	if s.sequence == math.MaxUint64 {
		fail("source_unavailable")
		return
	}
	now := time.Now()
	capture, err := s.sample(now)
	if ctx.Err() != nil {
		return
	}
	if err != nil {
		fail(err.Error())
		return
	}
	if capture.Data == nil || !before.matches(capture.Data.binding) {
		fail("binding_changed")
		return
	}
	manifest, body, err := prepareHeldScheduleManifest(capture, s.sequence+1, now)
	if err != nil {
		fail(err.Error())
		return
	}
	if previous := s.latest.Load(); previous != nil && before.matches(previous.manifest.Binding) {
		old := previous.manifest
		if manifest.Binding.SourceGeneration < old.Binding.SourceGeneration {
			fail("binding_changed")
			return
		}
		if manifest.Binding.SourceGeneration == old.Binding.SourceGeneration &&
			(!sameReplayDetailBinding(manifest.Binding, old.Binding) || manifest.Digest != old.Digest) {
			fail("source_invalid")
			return
		}
	}
	s.gateMu.Lock()
	defer s.gateMu.Unlock()
	after := s.gate.Load()
	if after == nil || !sameReplayDetailGateContext(*before, *after) || !after.matches(manifest.Binding) {
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
	if !capture.FreshUntil.After(time.Now()) {
		fail("source_time_invalid")
		return
	}
	if ctx.Err() != nil {
		return
	}
	s.sequence++
	s.latest.Store(&heldSchedulePublication{manifest: manifest, body: body, data: capture.Data, freshUntil: capture.FreshUntil})
	s.failure.Store(nil)
}

func heldScheduleReason(reason string) string {
	switch reason {
	case "waiting_source", "status_stale", "source_unavailable", "source_invalid", "source_time_invalid", "binding_changed", "response_limit", "invalid_request", "rate_limited", "unsupported_source":
		return reason
	default:
		return "source_unavailable"
	}
}

func validHeldScheduleDigest(value string) bool {
	return validDetailDigest(value)
}

func (s *heldScheduleSource) serveHTTP(w http.ResponseWriter, r *http.Request) {
	const path = "/details/schedule"
	digest := ""
	if strings.HasPrefix(r.URL.Path, path+"/data/") {
		digest = strings.TrimPrefix(r.URL.Path, path+"/data/")
	}
	if r.Method != http.MethodGet || r.URL.EscapedPath() != r.URL.Path ||
		(r.URL.Path != path && (r.URL.Path != path+"/data/"+digest || !validHeldScheduleDigest(digest))) ||
		r.URL.RawQuery != "" || r.URL.ForceQuery || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
		writeHeldScheduleReply(w, http.StatusBadRequest, "invalid_request", "", 0, "")
		return
	}
	g := s.gate.Load()
	reason := "waiting_source"
	if g != nil {
		reason = g.failure(time.Now())
		if reason == "" && g.State != "fresh" {
			reason = "status_stale"
		}
		// Successful polls publish before clearing failure. Read in reverse
		// order so recovery cannot revive an earlier failed publication.
		failure, p := s.failure.Load(), s.latest.Load()
		if reason == "" {
			switch {
			case failure != nil && sameReplayDetailGateContext(failure.gate, *g):
				reason = failure.reason
			case p == nil:
				reason = "waiting_source"
			case !g.matches(p.manifest.Binding):
				reason = "binding_changed"
			}
		}
		if reason == "" {
			remaining := p.freshUntil.Sub(time.Now()).Milliseconds()
			if remaining < 1 {
				reason = "status_stale"
			} else if digest != "" && digest != p.data.digest {
				reason = "binding_changed"
			} else if digest == "" {
				writeHeldScheduleReply(w, http.StatusOK, "", p.body, remaining, "")
				return
			} else {
				select {
				case s.active <- struct{}{}:
					defer func() { <-s.active }()
					writeHeldScheduleReply(w, http.StatusOK, "", p.data.body, remaining, digest)
				default:
					writeHeldScheduleReply(w, http.StatusTooManyRequests, "rate_limited", "", 0, "")
				}
				return
			}
		}
	}
	writeHeldScheduleReply(w, http.StatusServiceUnavailable, reason, "", 0, "")
}

func writeHeldScheduleReply(w http.ResponseWriter, status int, reason, body string, remaining int64, digest string) {
	if body == "" {
		body = `{"schema_version":1,"family":"held_schedule","state":"unavailable","reason":"` + heldScheduleReason(reason) + `"}`
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if status == http.StatusOK {
		w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(remaining, 10))
		if digest != "" {
			w.Header().Set("ETag", `"`+digest+`"`)
		}
	}
	w.WriteHeader(status)
	_, _ = io.WriteString(w, body)
}
