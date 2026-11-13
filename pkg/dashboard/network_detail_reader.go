package dashboard

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

const (
	networkDetailCollectInterval = 10 * time.Second
	networkDetailFamilyBytes     = 64 << 10
)

// PublicNetworkDetail adds only this reader's collection time to the source
// document. Socket observations and their clocks are copied without redating.
type PublicNetworkDetail struct {
	SchemaVersion  int                        `json:"schema_version"`
	Family         string                     `json:"family"`
	Scope          string                     `json:"scope"`
	State          string                     `json:"state"`
	Reason         string                     `json:"reason"`
	CollectorAt    time.Time                  `json:"collector_at"`
	Binding        NetworkDetailBinding       `json:"binding"`
	UpdatedAt      time.Time                  `json:"updated_at"`
	Live           SocketDetail               `json:"live"`
	Repair         SocketDetail               `json:"repair"`
	Reconstruction *ReconstructionDetail      `json:"reconstruction,omitempty"`
	ArrivalTiming  *ArrivalTimingDetail       `json:"arrival_timing,omitempty"`
	RepairRequests *ApplicationPayloadCounter `json:"repair_requests,omitempty"`
}

var networkDetailPublicReasons = [...]string{
	"", "status_stale", "source_unavailable", "source_invalid", "source_time_invalid",
	"unsupported_source", "binding_changed", "response_limit", "waiting_source",
}

type publicNetworkDetailFamily struct {
	binding          NetworkDetailBinding
	sourceDigest     [sha256.Size]byte
	bodies           [len(networkDetailPublicReasons)]string
	publishedAt      time.Time
	sourceFreshUntil time.Time
	node             *nodeNetworkPublicFamily
	stamp            statusStamp
	nodeStamp        statusStamp
}

type networkDetailReaderFailure struct {
	gate       networkDetailGate
	reason     string
	nodeScoped bool
}

type networkDetailReader struct {
	client  *http.Client
	gate    *atomic.Pointer[networkDetailGate]
	admit   func(networkDetailReply) (time.Duration, bool) // installed before the public server starts
	done    chan struct{}
	limit   *rate.Limiter
	latest  atomic.Pointer[publicNetworkDetailFamily]
	failure atomic.Pointer[networkDetailReaderFailure]
}

type networkDetailReply struct {
	status                int
	body                  string
	tail                  string // Optional shared v2 observation bytes; never concatenated per viewer.
	remainingMilliseconds int64
	stamp                 statusStamp
	nodeScoped            bool
	family                *publicNetworkDetailFamily
	failure               *networkDetailReaderFailure
}

func newNetworkDetailReader(client *http.Client, gate *atomic.Pointer[networkDetailGate]) (*networkDetailReader, error) {
	if client == nil || gate == nil {
		return nil, errors.New("network detail reader requires a client and status gate")
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport == nil || transport.DialContext == nil {
		return nil, errors.New("network detail reader requires a fixed Unix transport")
	}
	return &networkDetailReader{
		client: detailHTTPClient(transport, 1, time.Second),
		gate:   gate, done: make(chan struct{}), limit: rate.NewLimiter(16, 32),
	}, nil
}

func (r *networkDetailReader) run(ctx context.Context) {
	runDetailReader(ctx, r.done, r.client, networkDetailCollectInterval, r.collect)
}

func (r *networkDetailReader) fetch(parent context.Context) ([]byte, time.Duration, time.Time, string) {
	return fetchDetail(parent, r.client, "network", MaxNetworkDetailBytes, http.Header{networkPullRequestsHeader: {"1"}, networkMTUHeader: {"1"}, networkRepairGateHeader: {"1"}}, parseNetworkDetailRemaining, decodeNetworkDetailFailure)
}

func parseNetworkDetailRemaining(value string) (time.Duration, error) {
	return parseDetailRemaining(value, NetworkDetailFreshFor)
}

func decodeNetworkDetailFailure(body []byte) string {
	return decodeDetailFailure(body, "network", networkDetailSourceReason)
}

func networkDetailSourceReason(reason string) bool {
	switch reason {
	case "waiting_source", "status_stale", "source_unavailable", "source_invalid", "source_time_invalid", "unsupported_source", "binding_changed", "response_limit":
		return true
	default:
		return false
	}
}

func (r *networkDetailReader) collect(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	before := r.gate.Load()
	if before == nil {
		return
	}
	fail := func(reason string) {
		if ctx.Err() == nil {
			r.failure.Store(&networkDetailReaderFailure{gate: cloneNetworkDetailGate(*before), reason: reason})
		}
	}
	if reason := before.nodeFailure(time.Now()); reason != "" {
		fail(reason)
		return
	}
	if !before.nodeStamp.valid() {
		fail("waiting_source")
		return
	}
	body, remaining, started, reason := r.fetch(ctx)
	if reason != "" {
		fail(reason)
		return
	}
	var header struct {
		SchemaVersion int `json:"schema_version"`
	}
	if json.Unmarshal(body, &header) == nil && header.SchemaVersion == 2 {
		r.collectNode(ctx, *before, body, remaining, started)
		return
	}
	if reason := before.failure(time.Now()); reason != "" {
		fail(reason)
		return
	}
	if !before.stamp.valid() {
		fail("waiting_source")
		return
	}
	detail, err := decodeNetworkDetail(body)
	if err != nil {
		fail("source_invalid")
		return
	}
	if !before.matches(detail.Binding) {
		fail("binding_changed")
		return
	}
	now := time.Now()
	freshUntil := started.Add(remaining)
	if !freshUntil.After(now) {
		fail("source_time_invalid")
		return
	}
	after := r.gate.Load()
	if after == nil || !sameNetworkDetailGateContext(*before, *after) || !after.matches(detail.Binding) {
		fail("binding_changed")
		return
	}
	if reason := after.failure(now); reason != "" {
		fail(reason)
		return
	}
	sourceDigest := sha256.Sum256(body)
	if current := r.latest.Load(); current != nil && current.sourceDigest == sourceDigest {
		if current.stamp != before.stamp || current.nodeStamp != before.nodeStamp || freshUntil.Before(current.sourceFreshUntil) {
			if current.sourceFreshUntil.Before(freshUntil) {
				freshUntil = current.sourceFreshUntil
			}
			if !freshUntil.After(time.Now()) {
				fail("status_stale")
				return
			}
			latestGate := r.gate.Load()
			if latestGate == nil || !sameNetworkDetailGateContext(*before, *latestGate) || latestGate.failure(time.Now()) != "" {
				fail("binding_changed")
				return
			}
			copy := *current
			copy.stamp, copy.nodeStamp, copy.sourceFreshUntil = before.stamp, before.nodeStamp, freshUntil
			r.latest.Store(&copy)
		}
		r.failure.Store(nil)
		return
	}
	family, err := preparePublicNetworkDetail(detail, now, freshUntil)
	if err != nil {
		fail(err.Error())
		return
	}
	if !freshUntil.After(time.Now()) {
		fail("source_time_invalid")
		return
	}
	if ctx.Err() != nil {
		return
	}
	latestGate := r.gate.Load()
	if latestGate == nil || !sameNetworkDetailGateContext(*before, *latestGate) || latestGate.failure(time.Now()) != "" {
		fail("binding_changed")
		return
	}
	family.sourceDigest, family.stamp, family.nodeStamp = sourceDigest, before.stamp, before.nodeStamp
	r.latest.Store(family)
	r.failure.Store(nil)
}

func preparePublicNetworkDetail(detail NetworkDetail, collectedAt, freshUntil time.Time) (*publicNetworkDetailFamily, error) {
	if validateNetworkDetail(detail) != nil || collectedAt.IsZero() || freshUntil.IsZero() || !freshUntil.After(collectedAt) ||
		detail.Reconstruction != nil && detail.Reconstruction.ObservedAt.After(collectedAt.Add(time.Second)) {
		return nil, errors.New("source_invalid")
	}
	if !validApplicationPayloadLease(detail.RepairRequests, collectedAt, freshUntil) {
		return nil, errors.New("source_time_invalid")
	}
	binding := detail.Binding
	if binding.GenesisHash != nil {
		value := *binding.GenesisHash
		binding.GenesisHash = &value
	}
	family := &publicNetworkDetailFamily{binding: binding, publishedAt: collectedAt, sourceFreshUntil: freshUntil}
	public := PublicNetworkDetail{
		SchemaVersion: 1, Family: "network", Scope: detail.Scope, State: "fresh", CollectorAt: collectedAt.UTC(),
		Binding: binding, UpdatedAt: detail.UpdatedAt, Live: detail.Live, Repair: detail.Repair,
		Reconstruction: detail.Reconstruction, ArrivalTiming: detail.ArrivalTiming, RepairRequests: detail.RepairRequests,
	}
	total := 0
	for i, reason := range networkDetailPublicReasons {
		public.Reason = reason
		if reason == "" {
			public.State = "fresh"
		} else {
			public.State = "stale"
		}
		body, err := json.Marshal(public)
		if err != nil || len(body) > MaxNetworkDetailBytes {
			return nil, errors.New("response_limit")
		}
		family.bodies[i], total = string(body), total+len(body)
	}
	if total > networkDetailFamilyBytes {
		return nil, errors.New("response_limit")
	}
	return family, nil
}

func (r *networkDetailReader) lookup(now time.Time, wantExtended ...bool) networkDetailReply {
	gate := r.gate.Load()
	if gate == nil {
		return networkDetailFailure("waiting_source")
	}
	if !gate.nodeStamp.valid() {
		return networkDetailFailure("waiting_source")
	}
	// Keep one failure-first view for both node and receiver projections.
	failure, family := r.failure.Load(), r.latest.Load()
	if family != nil && family.node != nil {
		return r.lookupNode(*gate, family, failure, now, len(wantExtended) > 0 && wantExtended[0], len(wantExtended) > 1 && wantExtended[1], len(wantExtended) > 2 && wantExtended[2])
	}
	if failure != nil && failure.nodeScoped && sameNetworkNodeGateContext(failure.gate, *gate) {
		if reason := gate.nodeFailure(now); reason != "" {
			return networkDetailFailure(reason)
		}
		return networkDetailFailure(failure.reason)
	}
	if reason := gate.failure(now); reason != "" {
		return networkDetailFailure(reason)
	}
	if !gate.stamp.valid() {
		return networkDetailFailure("waiting_source")
	}
	if family == nil {
		if failure != nil && sameNetworkDetailGateContext(failure.gate, *gate) {
			return networkDetailFailure(failure.reason)
		}
		return networkDetailFailure("waiting_source")
	}
	if !gate.matches(family.binding) || gate.stamp != family.stamp || gate.nodeStamp != family.nodeStamp {
		return networkDetailFailure("binding_changed")
	}
	if now.Before(family.publishedAt) {
		return networkDetailFailure("source_time_invalid")
	}
	reason := ""
	remainingMilliseconds := family.sourceFreshUntil.Sub(now).Milliseconds()
	if remainingMilliseconds < 1 {
		reason = "status_stale"
	}
	if failure != nil && sameNetworkDetailGateContext(failure.gate, *gate) {
		reason = failure.reason
	}
	for i, candidate := range networkDetailPublicReasons {
		if candidate == reason {
			reply := networkDetailReply{status: http.StatusOK, body: family.bodies[i], stamp: gate.stamp, family: family, failure: failure}
			if reason == "" {
				reply.remainingMilliseconds = remainingMilliseconds
			}
			return reply
		}
	}
	return networkDetailFailure("source_invalid")
}

// nodeFamily is the separate status-fence slot for node-wide observations.
// The receiver slot remains statusNetwork.
func (c *collector) admitNetworkDetail(r *networkDetailReader, reply networkDetailReply, nodeFamily statusFamily) (time.Duration, bool) {
	family := statusNetwork
	if reply.nodeScoped {
		family = nodeFamily
	}
	return c.admitLiveDetail(family, reply.stamp, func() (statusStamp, bool) {
		gate := r.gate.Load()
		if gate == nil || r.latest.Load() != reply.family || r.failure.Load() != reply.failure {
			return statusStamp{}, false
		}
		if reply.nodeScoped {
			return gate.nodeStamp, gate.nodeFailure(time.Now()) == ""
		}
		return gate.stamp, gate.failure(time.Now()) == "" && reply.family != nil && gate.nodeStamp == reply.family.nodeStamp
	}, time.Now())
}

func (r *networkDetailReader) serveHTTP(w http.ResponseWriter, request *http.Request) {
	reply := networkDetailFailure("invalid_request")
	if validDetailGet(request, "/api/v1/details/network") {
		if r.limit.Allow() {
			pull := request.Header.Get(networkPullRequestsHeader) == "1"
			mtu := pull && request.Header.Get(networkMTUHeader) == "1"
			reply = r.lookup(time.Now(), pull, mtu, mtu && request.Header.Get(networkRepairGateHeader) == "1")
		} else {
			reply = networkDetailFailure("rate_limited")
		}
	}
	var statusRemaining time.Duration
	if reply.status == http.StatusOK {
		var ok bool
		if r.admit != nil {
			statusRemaining, ok = r.admit(reply)
		}
		if !ok || statusRemaining.Milliseconds() < 1 || reply.family == nil {
			reply = networkDetailFailure("status_stale")
		} else if reply.remainingMilliseconds > 0 {
			remaining := time.Until(reply.family.sourceFreshUntil).Milliseconds()
			if remaining < 1 {
				reply = networkDetailFailure("status_stale")
			} else {
				reply.remainingMilliseconds = min(reply.remainingMilliseconds, remaining)
			}
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if reply.status == http.StatusOK {
		w.Header().Set(statusRemainingHeader, strconv.FormatInt(statusRemaining.Milliseconds(), 10))
	}
	if reply.status == http.StatusOK && reply.remainingMilliseconds > 0 {
		w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(reply.remainingMilliseconds, 10))
	}
	w.WriteHeader(reply.status)
	if _, err := io.WriteString(w, reply.body); err == nil && reply.tail != "" {
		if remaining, ok := r.admit(reply); !ok || remaining.Milliseconds() < 1 ||
			reply.remainingMilliseconds > 0 && !reply.family.sourceFreshUntil.After(time.Now()) {
			panic(http.ErrAbortHandler)
		}
		_, _ = io.WriteString(w, reply.tail)
	}
}

func networkDetailFailure(reason string) networkDetailReply {
	status := http.StatusServiceUnavailable
	body := `{"schema_version":1,"family":"network","state":"unavailable","reason":"source_invalid"}`
	switch reason {
	case "invalid_request":
		status = http.StatusBadRequest
		body = `{"schema_version":1,"family":"network","state":"unavailable","reason":"invalid_request"}`
	case "rate_limited":
		status = http.StatusTooManyRequests
		body = `{"schema_version":1,"family":"network","state":"unavailable","reason":"rate_limited"}`
	case "waiting_source":
		body = `{"schema_version":1,"family":"network","state":"unavailable","reason":"waiting_source"}`
	case "status_stale":
		body = `{"schema_version":1,"family":"network","state":"unavailable","reason":"status_stale"}`
	case "source_unavailable":
		body = `{"schema_version":1,"family":"network","state":"unavailable","reason":"source_unavailable"}`
	case "source_invalid":
	case "source_time_invalid":
		body = `{"schema_version":1,"family":"network","state":"unavailable","reason":"source_time_invalid"}`
	case "unsupported_source":
		body = `{"schema_version":1,"family":"network","state":"unavailable","reason":"unsupported_source"}`
	case "binding_changed":
		body = `{"schema_version":1,"family":"network","state":"unavailable","reason":"binding_changed"}`
	case "response_limit":
		body = `{"schema_version":1,"family":"network","state":"unavailable","reason":"response_limit"}`
	}
	return networkDetailReply{status: status, body: body}
}

func sameNetworkDetailGateContext(a, b networkDetailGate) bool {
	return a.matches(b.binding) && b.matches(a.binding) && a.stamp == b.stamp && a.nodeStamp == b.nodeStamp
}

func sameNetworkNodeGateContext(a, b networkDetailGate) bool {
	return sameNodeNetworkBinding(a.nodeBinding(), b.nodeBinding()) && a.nodeStamp == b.nodeStamp
}

func cloneNetworkDetailGate(g networkDetailGate) networkDetailGate {
	if g.binding.GenesisHash != nil {
		value := *g.binding.GenesisHash
		g.binding.GenesisHash = &value
	}
	return g
}
