package dashboard

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"net/http"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

const (
	cacheDetailCollectInterval = 10 * time.Second
	cacheDetailFamilyBytes     = 32 << 10
)

// PublicCacheDetail adds the reader's collection time without redating gauges.
// Capacities keep their source units and entry counts remain approximate.
type PublicCacheDetail struct {
	SchemaVersion int                  `json:"schema_version"`
	Family        string               `json:"family"`
	Scope         string               `json:"scope"`
	State         string               `json:"state"`
	Reason        string               `json:"reason"`
	CollectorAt   time.Time            `json:"collector_at"`
	Binding       CacheDetailBinding   `json:"binding"`
	CapturedAt    time.Time            `json:"captured_at"`
	Vote          CacheVoteDetail      `json:"vote"`
	Program       CacheProgramDetail   `json:"program"`
	Common        CacheCommonDetail    `json:"common"`
	Activity      *CacheActivityDetail `json:"activity,omitempty"`
}

var cacheDetailPublicReasons = [...]string{
	"", "status_stale", "source_unavailable", "source_invalid", "source_time_invalid",
	"binding_changed", "response_limit", "waiting_source", "unsupported_source",
}

type publicCacheDetailFamily struct {
	binding          CacheDetailBinding
	sourceDigest     [sha256.Size]byte
	bodies           [len(cacheDetailPublicReasons)]string
	publishedAt      time.Time
	sourceFreshUntil time.Time
	stamp            statusStamp
}

type cacheDetailReaderFailure struct {
	gate   cacheDetailGate
	reason string
}

type cacheDetailReader struct {
	client  *http.Client
	gate    *atomic.Pointer[cacheDetailGate]
	done    chan struct{}
	limit   *rate.Limiter
	latest  atomic.Pointer[publicCacheDetailFamily]
	failure atomic.Pointer[cacheDetailReaderFailure]
}

type cacheDetailReply struct {
	status                int
	body                  string
	remainingMilliseconds int64
	stamp                 statusStamp
	family                *publicCacheDetailFamily
	failure               *cacheDetailReaderFailure
}

func newCacheDetailReader(client *http.Client, gate *atomic.Pointer[cacheDetailGate]) (*cacheDetailReader, error) {
	if client == nil || gate == nil {
		return nil, errors.New("cache detail reader requires a client and status gate")
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport == nil || transport.DialContext == nil {
		return nil, errors.New("cache detail reader requires a fixed Unix transport")
	}
	return &cacheDetailReader{
		client: detailHTTPClient(transport, 1, time.Second),
		gate:   gate, done: make(chan struct{}), limit: rate.NewLimiter(16, 32),
	}, nil
}

func (r *cacheDetailReader) run(ctx context.Context) {
	runDetailReader(ctx, r.done, r.client, cacheDetailCollectInterval, r.collect)
}

func (r *cacheDetailReader) fetch(parent context.Context) ([]byte, time.Duration, time.Time, string) {
	return fetchDetail(parent, r.client, "cache", MaxCacheDetailBytes, nil, parseCacheDetailRemaining, decodeCacheDetailFailure)
}

func parseCacheDetailRemaining(value string) (time.Duration, error) {
	return parseDetailRemaining(value, CacheDetailFreshFor)
}

func decodeCacheDetailFailure(body []byte) string {
	return decodeDetailFailure(body, "accounts_cache", cacheDetailSourceReason)
}

func cacheDetailSourceReason(reason string) bool {
	return reason != "invalid_request" && cacheDetailReason(reason) == reason
}

func (r *cacheDetailReader) collect(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	before := r.gate.Load()
	if before == nil {
		return
	}
	fail := func(reason string) {
		if ctx.Err() == nil {
			r.failure.Store(&cacheDetailReaderFailure{gate: cloneCacheDetailGate(*before), reason: reason})
		}
	}
	if reason := before.failure(time.Now()); reason != "" {
		fail(reason)
		return
	}
	if !before.stamp.valid() {
		fail("waiting_source")
		return
	}
	body, remaining, started, reason := r.fetch(ctx)
	if reason != "" {
		fail(reason)
		return
	}
	detail, err := decodeCacheDetail(body)
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
	if after == nil || !sameCacheDetailGateContext(*before, *after) || !after.matches(detail.Binding) {
		fail("binding_changed")
		return
	}
	if reason := after.failure(now); reason != "" {
		fail(reason)
		return
	}
	if ctx.Err() != nil {
		return
	}
	sourceDigest := sha256.Sum256(body)
	if current := r.latest.Load(); current != nil && current.sourceDigest == sourceDigest {
		// A later valid fetch may rebind an identical capture, but must not
		// renew its original deadline or collection timestamp.
		if current.stamp != before.stamp || freshUntil.Before(current.sourceFreshUntil) {
			if current.sourceFreshUntil.Before(freshUntil) {
				freshUntil = current.sourceFreshUntil
			}
			if !freshUntil.After(time.Now()) {
				fail("status_stale")
				return
			}
			latestGate := r.gate.Load()
			if latestGate == nil || !sameCacheDetailGateContext(*before, *latestGate) || latestGate.failure(time.Now()) != "" {
				fail("binding_changed")
				return
			}
			copy := *current
			copy.stamp, copy.sourceFreshUntil = before.stamp, freshUntil
			r.latest.Store(&copy)
		}
		r.failure.Store(nil)
		return
	}
	family, err := preparePublicCacheDetail(detail, now, freshUntil)
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
	if latestGate == nil || !sameCacheDetailGateContext(*before, *latestGate) || latestGate.failure(time.Now()) != "" {
		fail("binding_changed")
		return
	}
	family.sourceDigest, family.stamp = sourceDigest, before.stamp
	r.latest.Store(family)
	r.failure.Store(nil)
}

func preparePublicCacheDetail(detail CacheDetail, collectedAt, freshUntil time.Time) (*publicCacheDetailFamily, error) {
	if validateCacheDetail(detail) != nil || collectedAt.IsZero() || freshUntil.IsZero() || !freshUntil.After(collectedAt) {
		return nil, errors.New("source_invalid")
	}
	binding := detail.Binding
	if binding.GenesisHash != nil {
		value := *binding.GenesisHash
		binding.GenesisHash = &value
	}
	family := &publicCacheDetailFamily{binding: binding, publishedAt: collectedAt, sourceFreshUntil: freshUntil}
	public := PublicCacheDetail{
		SchemaVersion: 1, Family: "accounts_cache", Scope: detail.Scope, State: "fresh", CollectorAt: collectedAt.UTC(),
		Binding: binding, CapturedAt: detail.CapturedAt, Vote: detail.Vote, Program: detail.Program, Common: detail.Common,
		Activity: detail.Activity,
	}
	total := 0
	for i, reason := range cacheDetailPublicReasons {
		public.Reason = reason
		if reason == "" {
			public.State = "fresh"
		} else {
			public.State = "stale"
		}
		body, err := json.Marshal(public)
		if err != nil || len(body) > MaxCacheDetailBytes {
			return nil, errors.New("response_limit")
		}
		family.bodies[i], total = string(body), total+len(body)
	}
	if total > cacheDetailFamilyBytes {
		return nil, errors.New("response_limit")
	}
	return family, nil
}

func (r *cacheDetailReader) lookup(now time.Time) cacheDetailReply {
	gate := r.gate.Load()
	if gate == nil {
		return cacheDetailFailure("waiting_source")
	}
	if !gate.stamp.valid() {
		return cacheDetailFailure("waiting_source")
	}
	if reason := gate.failure(now); reason != "" {
		return cacheDetailFailure(reason)
	}
	// Collectors publish before clearing failure; read failure only once first.
	failure, family := r.failure.Load(), r.latest.Load()
	if family == nil {
		if failure != nil && sameCacheDetailGateContext(failure.gate, *gate) {
			return cacheDetailFailure(failure.reason)
		}
		return cacheDetailFailure("waiting_source")
	}
	if !gate.matches(family.binding) || gate.stamp != family.stamp {
		return cacheDetailFailure("binding_changed")
	}
	if now.Before(family.publishedAt) {
		return cacheDetailFailure("source_time_invalid")
	}
	reason := ""
	remainingMilliseconds := family.sourceFreshUntil.Sub(now).Milliseconds()
	if remainingMilliseconds < 1 {
		reason = "status_stale"
	}
	if failure != nil && sameCacheDetailGateContext(failure.gate, *gate) {
		reason = failure.reason
	}
	for i, candidate := range cacheDetailPublicReasons {
		if candidate == reason {
			reply := cacheDetailReply{status: http.StatusOK, body: family.bodies[i], stamp: gate.stamp, family: family, failure: failure}
			if reason == "" {
				reply.remainingMilliseconds = remainingMilliseconds
			}
			return reply
		}
	}
	return cacheDetailFailure("source_invalid")
}

func (c *collector) admitCacheDetail(r *cacheDetailReader, reply cacheDetailReply) (time.Duration, bool) {
	return c.admitLiveDetail(statusCache, reply.stamp, func() (statusStamp, bool) {
		gate := r.gate.Load()
		if gate == nil {
			return statusStamp{}, false
		}
		return gate.stamp, gate.failure(time.Now()) == "" &&
			r.latest.Load() == reply.family && r.failure.Load() == reply.failure
	}, time.Now())
}

func (r *cacheDetailReader) serveHTTP(w http.ResponseWriter, request *http.Request, admit func(cacheDetailReply) (time.Duration, bool)) {
	reply := cacheDetailFailure("invalid_request")
	if validDetailGet(request, "/api/v1/details/cache") {
		if r.limit.Allow() {
			reply = r.lookup(time.Now())
		} else {
			reply = cacheDetailFailure("rate_limited")
		}
	}
	var statusRemaining time.Duration
	if reply.status == http.StatusOK {
		var ok bool
		if admit != nil {
			statusRemaining, ok = admit(reply)
		}
		if !ok || statusRemaining.Milliseconds() < 1 || reply.family == nil {
			reply = cacheDetailFailure("status_stale")
		} else if reply.remainingMilliseconds > 0 {
			remaining := time.Until(reply.family.sourceFreshUntil).Milliseconds()
			if remaining < 1 {
				reply = cacheDetailFailure("status_stale")
			} else {
				reply.remainingMilliseconds = min(reply.remainingMilliseconds, remaining)
			}
		}
	}
	writeDetailResponse(w, reply.status, reply.body, statusRemaining, reply.remainingMilliseconds)
}

func cacheDetailFailure(reason string) cacheDetailReply {
	status := http.StatusServiceUnavailable
	switch reason {
	case "invalid_request":
		status = http.StatusBadRequest
	case "rate_limited":
		status = http.StatusTooManyRequests
	default:
		if reason != "unsupported_source" && !cacheDetailSourceReason(reason) {
			reason = "source_invalid"
		}
	}
	body := `{"schema_version":1,"family":"accounts_cache","state":"unavailable","reason":"` + reason + `"}`
	return cacheDetailReply{status: status, body: body}
}

func sameCacheDetailGateContext(a, b cacheDetailGate) bool {
	return a.matches(b.binding) && b.matches(a.binding) && a.stamp == b.stamp
}

func cloneCacheDetailGate(g cacheDetailGate) cacheDetailGate {
	if g.binding.GenesisHash != nil {
		value := *g.binding.GenesisHash
		g.binding.GenesisHash = &value
	}
	return g
}
