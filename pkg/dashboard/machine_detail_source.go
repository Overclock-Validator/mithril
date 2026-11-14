package dashboard

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	machineDetailPath    = "/details/machine"
	machineDetailHoldFor = 20 * time.Second
	machineDetailMinimum = 9 * time.Second
	// MachineDetailFreshFor is the maximum original metadata-owner lease.
	MachineDetailFreshFor = 90 * time.Second
)

type machineDetailReply struct {
	status                int
	body                  string
	remainingMilliseconds int64
}

func validMachineDigest(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, c := range value {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

func parseMachineDetailQuery(raw string, force bool) (generation string, page int, pageRequest, ok bool) {
	if raw == "" {
		return "", 0, false, !force
	}
	first, second, found := strings.Cut(raw, "&")
	if len(raw) > 128 || !found || strings.Contains(second, "&") {
		return "", 0, false, false
	}
	var pageText string
	for _, part := range [...]string{first, second} {
		switch {
		case strings.HasPrefix(part, "generation=") && generation == "":
			generation = strings.TrimPrefix(part, "generation=")
		case strings.HasPrefix(part, "page=") && pageText == "":
			pageText = strings.TrimPrefix(part, "page=")
		default:
			return "", 0, false, false
		}
	}
	if !validMachineDigest(generation) || pageText == "" || len(pageText) > 3 || len(pageText) > 1 && pageText[0] == '0' {
		return "", 0, false, false
	}
	for _, c := range pageText {
		if c < '0' || c > '9' {
			return "", 0, false, false
		}
	}
	page, err := strconv.Atoi(pageText)
	return generation, page, true, err == nil && page < machineMaxPages
}

// MachineDetailCapture shares an immutable, already encoded owner capture.
// FreshUntil retains its original monotonic component; it is never reconstructed
// from the UTC capture date or advanced by a Feed or viewer read.
type MachineDetailCapture struct {
	Prepared   *PreparedMachineDetail
	FreshUntil time.Time
}

// MachineDetailSampler reads an existing in-memory publication only. It must not
// sample the OS, encode, or wait for new metadata. Feed.Close joins this callback.
type MachineDetailSampler func(time.Time) (MachineDetailCapture, error)

type machineDetailGate struct {
	binding    MachineDetailBinding
	observedAt time.Time
	reason     string
	stamp      statusStamp
}

func machineGate(snapshot Snapshot, now time.Time) machineDetailGate {
	g := machineDetailGate{binding: MachineDetailBinding{RunID: snapshot.RunID, NodeStartedAt: snapshot.StartedAt.UTC()}, observedAt: now}
	if snapshot.GenesisHash != nil {
		g.binding.GenesisHash = *snapshot.GenesisHash
	}
	if snapshot.Phase == "stopping" {
		g.reason = "source_unavailable"
	} else if !validReplayDetailHash(g.binding.GenesisHash, true) {
		g.reason = "waiting_source"
	}
	return g
}

func (g machineDetailGate) matches(b MachineDetailBinding) bool {
	return g.binding.RunID == b.RunID && g.binding.NodeStartedAt.Equal(b.NodeStartedAt) && g.binding.GenesisHash == b.GenesisHash
}

func (g machineDetailGate) failure(now time.Time) string {
	if g.observedAt.IsZero() || now.Before(g.observedAt) {
		return "source_time_invalid"
	}
	if now.Sub(g.observedAt) > StaleAfter {
		return "status_stale"
	}
	return g.reason
}

type machineDetailEntry struct {
	prepared   *PreparedMachineDetail
	binding    MachineDetailBinding
	capturedAt time.Time
	freshUntil time.Time
	holdUntil  time.Time
}

// machineDetailSource is updated by the serial Feed owner, not by HTTP. Only two
// prepared families are cached. Two admitted replies can retain old strings;
// preparation/decoding and network writes never hold mu.
type machineDetailSource struct {
	sample           MachineDetailSampler
	limit            *rate.Limiter
	active           chan struct{}
	mu               sync.Mutex
	gate             machineDetailGate
	items            [2]machineDetailEntry
	reason           string
	closed           bool
	sourceGeneration uint64
}

func newMachineDetailSource(sample MachineDetailSampler) *machineDetailSource {
	return &machineDetailSource{sample: sample, limit: rate.NewLimiter(16, 32), active: make(chan struct{}, 2)}
}

func (s *machineDetailSource) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.items = [2]machineDetailEntry{}
}

func (s *machineDetailSource) update(ctx context.Context, snapshot Snapshot, now time.Time) {
	if ctx.Err() != nil {
		return
	}
	g := machineGate(snapshot, now)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	if !s.gate.matches(g.binding) {
		s.items = [2]machineDetailEntry{}
		s.sourceGeneration = 0
	}
	s.gate, s.reason = g, g.failure(now)
	if s.reason != "" {
		s.items = [2]machineDetailEntry{}
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	capture, err := s.sample(now)
	completedAt := time.Now()
	reason := ""
	var manifest MachineDetailManifest
	// Same-token reads need no metadata decoding. These scalar facts are copied
	// from our accepted entry, never from a caller-owned manifest header.
	s.mu.Lock()
	if current := s.items[0]; current.prepared != nil && capture.Prepared != nil && current.prepared.Generation() == capture.Prepared.Generation() {
		manifest.Binding, manifest.CapturedAt, manifest.Generation = current.binding, current.capturedAt, current.prepared.Generation()
	}
	s.mu.Unlock()
	if err != nil {
		reason = "source_unavailable"
	} else if capture.Prepared == nil || len(capture.Prepared.Manifest()) == 0 || len(capture.Prepared.Manifest()) > MaxMachineManifestBytes {
		reason = "source_invalid"
	} else if manifest.Generation == "" && (json.Unmarshal([]byte(capture.Prepared.Manifest()), &manifest) != nil || manifest.SchemaVersion != 1 || manifest.Family != "machine" || manifest.Generation != capture.Prepared.Generation() ||
		len(manifest.Generation) != 64 || manifest.Bytes <= 0 || manifest.Bytes > MaxMachineDetailBytes || len(manifest.Pages) == 0 || len(manifest.Pages) > machineMaxPages ||
		manifest.Binding.SourceGeneration == 0 || manifest.Binding.SourceGeneration == ^uint64(0) || !validReplayDetailTime(manifest.CapturedAt)) {
		reason = "source_invalid"
	} else if !g.matches(manifest.Binding) {
		reason = "binding_changed"
	} else if capture.FreshUntil.IsZero() || !capture.FreshUntil.After(completedAt) || capture.FreshUntil.After(completedAt.Add(MachineDetailFreshFor)) ||
		manifest.CapturedAt.After(completedAt.Add(time.Second)) || completedAt.Sub(manifest.CapturedAt) >= MachineDetailFreshFor {
		reason = "source_time_invalid"
	}
	if ctx.Err() != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || ctx.Err() != nil || !s.gate.matches(g.binding) || s.gate.observedAt != g.observedAt {
		return
	}
	if gateReason := s.gate.failure(time.Now()); gateReason != "" {
		reason = gateReason
	}
	s.reason = reason
	if reason != "" {
		return
	}
	if p := &s.items[0]; p.prepared != nil && p.prepared.Generation() == manifest.Generation && manifest.Binding.SourceGeneration == s.sourceGeneration {
		if capture.FreshUntil.Before(p.freshUntil) {
			p.freshUntil = capture.FreshUntil
		}
		return
	}
	// SourceGeneration is a per-publication counter, not a UTC timestamp or a
	// topology subcapture generation. Even evicted tokens cannot regain a hold.
	if manifest.Binding.SourceGeneration <= s.sourceGeneration {
		s.reason = "binding_changed"
		return
	}
	s.sourceGeneration = manifest.Binding.SourceGeneration
	s.items[1] = s.items[0]
	s.items[0] = machineDetailEntry{prepared: capture.Prepared, binding: manifest.Binding, capturedAt: manifest.CapturedAt,
		freshUntil: capture.FreshUntil, holdUntil: completedAt.Add(machineDetailHoldFor)}
}

// lookup copies only one immutable response string. Historical pages retain
// their original hold even after source failure; status and binding still gate them.
func (s *machineDetailSource) lookup(generation string, page int, pageRequest bool, now time.Time) machineDetailReply {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return machineDetailFailure("source_unavailable")
	}
	if reason := s.gate.failure(now); reason != "" {
		return machineDetailFailure(reason)
	}
	if !pageRequest && s.reason != "" {
		return machineDetailFailure(s.reason)
	}
	for i := range s.items {
		p := &s.items[i]
		if p.prepared == nil || pageRequest && p.prepared.Generation() != generation || !pageRequest && i != 0 {
			continue
		}
		if !s.gate.matches(p.binding) {
			return machineDetailFailure("binding_changed")
		}
		if !now.Before(p.holdUntil) {
			if pageRequest {
				return machineDetailFailure("source_generation_expired")
			}
			return machineDetailFailure("generation_not_ready")
		}
		if pageRequest {
			body, ok := p.prepared.Page(page)
			if !ok {
				return machineDetailFailure("invalid_request")
			}
			return machineDetailReply{status: http.StatusOK, body: body}
		}
		remaining := min(p.freshUntil.Sub(now), p.holdUntil.Sub(now))
		if remaining < machineDetailMinimum {
			return machineDetailFailure("generation_not_ready")
		}
		return machineDetailReply{status: http.StatusOK, body: p.prepared.Manifest(), remainingMilliseconds: remaining.Milliseconds()}
	}
	if pageRequest {
		return machineDetailFailure("source_generation_expired")
	}
	return machineDetailFailure("waiting_source")
}

func machineDetailFailure(reason string) machineDetailReply {
	const prefix = `{"schema_version":1,"family":"machine","state":"unavailable","reason":"`
	const suffix = `"}`
	switch reason {
	case "invalid_request":
		return machineDetailReply{status: 400, body: prefix + "invalid_request" + suffix}
	case "rate_limited":
		return machineDetailReply{status: 429, body: prefix + "rate_limited" + suffix}
	case "binding_changed":
		return machineDetailReply{status: 409, body: prefix + "binding_changed" + suffix}
	case "source_generation_expired":
		return machineDetailReply{status: 409, body: prefix + "source_generation_expired" + suffix}
	case "waiting_source":
		return machineDetailReply{status: 503, body: prefix + "waiting_source" + suffix}
	case "source_invalid":
		return machineDetailReply{status: 503, body: prefix + "source_invalid" + suffix}
	case "source_time_invalid":
		return machineDetailReply{status: 503, body: prefix + "source_time_invalid" + suffix}
	case "status_stale":
		return machineDetailReply{status: 503, body: prefix + "status_stale" + suffix}
	case "generation_not_ready":
		return machineDetailReply{status: 503, body: prefix + "generation_not_ready" + suffix}
	default:
		return machineDetailReply{status: 503, body: prefix + "source_unavailable" + suffix}
	}
}

func (s *machineDetailSource) serveHTTP(w http.ResponseWriter, r *http.Request) {
	generation, page, pageRequest, valid := parseMachineDetailQuery(r.URL.RawQuery, r.URL.ForceQuery)
	reply := machineDetailFailure("invalid_request")
	if r.Method == http.MethodGet && r.URL.Path == machineDetailPath && r.URL.EscapedPath() == machineDetailPath &&
		r.ContentLength == 0 && len(r.TransferEncoding) == 0 && valid && (!pageRequest || page < machineMaxPages) {
		if !s.limit.Allow() {
			reply = machineDetailFailure("rate_limited")
		} else {
			select {
			case s.active <- struct{}{}:
				defer func() { <-s.active }()
				// Header preparation precedes final cached selection, so a concurrent
				// stop or context replacement cannot borrow an earlier admission.
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Cache-Control", "no-store")
				reply = s.lookup(generation, page, pageRequest, time.Now())
			default:
				reply = machineDetailFailure("rate_limited")
			}
		}
	}
	if r.Context().Err() != nil {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if reply.status == http.StatusOK && reply.remainingMilliseconds > 0 {
		w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(reply.remainingMilliseconds, 10))
	}
	w.WriteHeader(reply.status)
	_, _ = io.WriteString(w, reply.body)
}
