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
	productionDetailCollectInterval = 10 * time.Second
	productionDetailFamilyBytes     = 60 << 10
)

// PublicProductionDetail adds the reader's collection time without redating
// the producer lifecycle or its historical terminal observation.
type PublicProductionDetail struct {
	SchemaVersion   int                          `json:"schema_version"`
	Family          string                       `json:"family"`
	Scope           string                       `json:"scope"`
	State           string                       `json:"state"`
	Reason          string                       `json:"reason"`
	CollectorAt     time.Time                    `json:"collector_at"`
	Binding         ProductionDetailBinding      `json:"binding"`
	CapturedAt      time.Time                    `json:"captured_at"`
	ProducerState   string                       `json:"producer_state"`
	LastTerminal    *ProductionTerminalDetail    `json:"last_terminal"`
	Engine          *ProductionEngineDetail      `json:"engine,omitempty"`
	SchedulerBudget *ProductionSchedulerDetail   `json:"scheduler_budget,omitempty"`
	ParentReady     *ProductionParentReadyDetail `json:"parent_ready,omitempty"`
}

var productionDetailPublicReasons = [...]string{
	"", "status_stale", "source_unavailable", "source_invalid", "source_time_invalid",
	"binding_changed", "response_limit", "waiting_source", "unsupported_source", "not_applicable",
}

type publicProductionDetailFamily struct {
	binding          ProductionDetailBinding
	capturedAt       time.Time
	sourceDigest     [sha256.Size]byte
	bodies           [len(productionDetailPublicReasons)]string
	publishedAt      time.Time
	sourceFreshUntil time.Time
	stamp            statusStamp
}

type productionDetailReaderFailure struct {
	gate   productionDetailGate
	reason string
}

type productionDetailReader struct {
	client  *http.Client
	gate    *atomic.Pointer[productionDetailGate]
	done    chan struct{}
	limit   *rate.Limiter
	latest  atomic.Pointer[publicProductionDetailFamily]
	failure atomic.Pointer[productionDetailReaderFailure]

	highwaterNode  productionDetailGate
	highwaterGen   uint64
	highwaterStart time.Time
	lastCapture    *publicProductionDetailFamily
}

type productionDetailReply struct {
	status                int
	body                  string
	remainingMilliseconds int64
	stamp                 statusStamp
	family                *publicProductionDetailFamily
	failure               *productionDetailReaderFailure
}

func newProductionDetailReader(client *http.Client, gate *atomic.Pointer[productionDetailGate]) (*productionDetailReader, error) {
	if client == nil || gate == nil {
		return nil, errors.New("production detail reader requires a client and status gate")
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport == nil || transport.DialContext == nil {
		return nil, errors.New("production detail reader requires a fixed Unix transport")
	}
	return &productionDetailReader{
		client: detailHTTPClient(transport, 1, time.Second),
		gate:   gate, done: make(chan struct{}), limit: rate.NewLimiter(16, 32),
	}, nil
}

func (r *productionDetailReader) run(ctx context.Context) {
	runDetailReader(ctx, r.done, r.client, productionDetailCollectInterval, r.collect)
}

func (r *productionDetailReader) fetch(parent context.Context) ([]byte, time.Duration, time.Time, string) {
	return fetchDetail(parent, r.client, "production", MaxProductionDetailBytes, nil, parseProductionDetailRemaining, decodeProductionDetailFailure)
}

func parseProductionDetailRemaining(value string) (time.Duration, error) {
	return parseDetailRemaining(value, ProductionDetailFreshFor)
}

func decodeProductionDetailFailure(body []byte) string {
	return decodeDetailFailure(body, "production", productionDetailSourceReason)
}

func productionDetailSourceReason(reason string) bool {
	return reason != "invalid_request" && productionDetailReason(reason) == reason
}

func (r *productionDetailReader) collect(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	before := r.gate.Load()
	if before == nil {
		return
	}
	fail := func(reason string) {
		if ctx.Err() == nil {
			r.failure.Store(&productionDetailReaderFailure{gate: *before, reason: reason})
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
	detail, err := decodeProductionDetail(body)
	if err != nil {
		fail("source_invalid")
		return
	}
	if !before.matches(detail.Binding) {
		fail("binding_changed")
		return
	}
	receivedAt := time.Now()
	freshUntil := started.Add(remaining)
	if productionDetailTimeInvalid(ProductionDetailCapture{Detail: detail, FreshUntil: freshUntil}, receivedAt) {
		fail("source_time_invalid")
		return
	}
	after := r.gate.Load()
	if after == nil || !sameProductionDetailGateContext(*before, *after) || !after.matches(detail.Binding) {
		fail("binding_changed")
		return
	}
	if reason := after.failure(receivedAt); reason != "" {
		fail(reason)
		return
	}
	if ctx.Err() != nil {
		return
	}
	if r.highwaterGen != 0 && sameProductionNode(r.highwaterNode, *before) &&
		(detail.Binding.ProducerGeneration < r.highwaterGen || detail.Binding.ProducerGeneration == r.highwaterGen &&
			!detail.Binding.ProducerStartedAt.Equal(r.highwaterStart)) {
		fail("binding_changed")
		return
	}
	digest := sha256.Sum256(body)
	if previous := r.lastCapture; previous != nil && samePublicProductionCapture(previous, detail) {
		if previous.sourceDigest != digest {
			fail("source_invalid")
			return
		}
		finishedAt := time.Now()
		current := r.gate.Load()
		if current == nil || !sameProductionDetailGateContext(*after, *current) || !current.matches(detail.Binding) {
			fail("binding_changed")
			return
		}
		if reason := current.failure(finishedAt); reason != "" {
			fail(reason)
			return
		}
		if previous.sourceFreshUntil.Before(freshUntil) {
			freshUntil = previous.sourceFreshUntil
		}
		if !freshUntil.After(finishedAt) {
			fail("status_stale")
			return
		}
		if previous.stamp != before.stamp || freshUntil.Before(previous.sourceFreshUntil) {
			copy := *previous
			copy.stamp, copy.sourceFreshUntil = before.stamp, freshUntil
			r.latest.Store(&copy)
			r.lastCapture = &copy
		}
		r.failure.Store(nil)
		return
	}
	family, err := preparePublicProductionDetail(detail, receivedAt, freshUntil)
	if err != nil {
		fail(err.Error())
		return
	}
	finishedAt := time.Now()
	current := r.gate.Load()
	if current == nil || !sameProductionDetailGateContext(*after, *current) || !current.matches(detail.Binding) {
		fail("binding_changed")
		return
	}
	if reason := current.failure(finishedAt); reason != "" {
		fail(reason)
		return
	}
	if !freshUntil.After(finishedAt) {
		fail("source_time_invalid")
		return
	}
	if ctx.Err() != nil {
		return
	}
	family.sourceDigest, family.stamp = digest, before.stamp
	r.latest.Store(family)
	r.failure.Store(nil)
	r.lastCapture = family
	if r.highwaterGen == 0 || !sameProductionNode(r.highwaterNode, *current) || detail.Binding.ProducerGeneration > r.highwaterGen {
		r.highwaterNode, r.highwaterGen, r.highwaterStart = *current, detail.Binding.ProducerGeneration, detail.Binding.ProducerStartedAt
	}
}

func preparePublicProductionDetail(detail ProductionDetail, collectedAt, freshUntil time.Time) (*publicProductionDetailFamily, error) {
	if validateProductionDetail(detail) != nil || collectedAt.IsZero() || freshUntil.IsZero() || !freshUntil.After(collectedAt) {
		return nil, errors.New("source_invalid")
	}
	family := &publicProductionDetailFamily{binding: detail.Binding, capturedAt: detail.CapturedAt,
		publishedAt: collectedAt, sourceFreshUntil: freshUntil}
	public := PublicProductionDetail{
		SchemaVersion: 1, Family: "production", Scope: detail.Scope, State: "fresh", CollectorAt: collectedAt.UTC(),
		Binding: detail.Binding, CapturedAt: detail.CapturedAt, ProducerState: detail.ProducerState, LastTerminal: detail.LastTerminal,
		Engine:          detail.Engine,
		SchedulerBudget: detail.SchedulerBudget,
		ParentReady:     detail.ParentReady,
	}
	total := 0
	for i, reason := range productionDetailPublicReasons {
		public.Reason = reason
		if reason == "" {
			public.State = "fresh"
		} else {
			public.State = "stale"
		}
		body, err := json.Marshal(public)
		if err != nil || len(body) > MaxProductionDetailBytes {
			return nil, errors.New("response_limit")
		}
		family.bodies[i], total = string(body), total+len(body)
	}
	if total > productionDetailFamilyBytes {
		return nil, errors.New("response_limit")
	}
	return family, nil
}

func samePublicProductionCapture(family *publicProductionDetailFamily, detail ProductionDetail) bool {
	b := detail.Binding
	return family.binding.RunID == b.RunID && family.binding.NodeStartedAt.Equal(b.NodeStartedAt) &&
		family.binding.GenesisHash == b.GenesisHash && family.binding.ProducerGeneration == b.ProducerGeneration &&
		family.binding.ProducerStartedAt.Equal(b.ProducerStartedAt) && family.capturedAt.Equal(detail.CapturedAt)
}

func sameProductionDetailGateContext(a, b productionDetailGate) bool {
	return sameProductionNode(a, b) && a.stamp == b.stamp
}

func (r *productionDetailReader) lookup(now time.Time) productionDetailReply {
	gate := r.gate.Load()
	if gate == nil {
		return productionDetailFailure("waiting_source")
	}
	if reason := gate.failure(now); reason != "" {
		return productionDetailFailure(reason)
	}
	if !gate.stamp.valid() {
		return productionDetailFailure("waiting_source")
	}
	// Collectors publish before clearing failure; read failure only once first.
	failure, family := r.failure.Load(), r.latest.Load()
	if family == nil {
		if failure != nil && sameProductionDetailGateContext(failure.gate, *gate) {
			return productionDetailFailure(failure.reason)
		}
		return productionDetailFailure("waiting_source")
	}
	if !gate.matches(family.binding) || gate.stamp != family.stamp {
		return productionDetailFailure("binding_changed")
	}
	if now.Before(family.publishedAt) {
		return productionDetailFailure("source_time_invalid")
	}
	reason := ""
	remainingMilliseconds := family.sourceFreshUntil.Sub(now).Milliseconds()
	if remainingMilliseconds < 1 {
		reason = "status_stale"
	}
	if failure != nil && sameProductionDetailGateContext(failure.gate, *gate) {
		reason = failure.reason
	}
	for i, candidate := range productionDetailPublicReasons {
		if candidate == reason {
			reply := productionDetailReply{status: http.StatusOK, body: family.bodies[i], stamp: gate.stamp, family: family, failure: failure}
			if reason == "" {
				reply.remainingMilliseconds = remainingMilliseconds
			}
			return reply
		}
	}
	return productionDetailFailure("source_invalid")
}

func (c *collector) admitProductionDetail(r *productionDetailReader, reply productionDetailReply) (time.Duration, bool) {
	return c.admitLiveDetail(statusProduction, reply.stamp, func() (statusStamp, bool) {
		gate := r.gate.Load()
		if gate == nil {
			return statusStamp{}, false
		}
		return gate.stamp, gate.failure(time.Now()) == "" &&
			r.latest.Load() == reply.family && r.failure.Load() == reply.failure
	}, time.Now())
}

func (r *productionDetailReader) serveHTTP(w http.ResponseWriter, request *http.Request, admit func(productionDetailReply) (time.Duration, bool)) {
	reply := productionDetailFailure("invalid_request")
	if validDetailGet(request, "/api/v1/details/production") {
		if r.limit.Allow() {
			reply = r.lookup(time.Now())
		} else {
			reply = productionDetailFailure("rate_limited")
		}
	}
	var statusRemaining time.Duration
	if reply.status == http.StatusOK {
		var ok bool
		if admit != nil {
			statusRemaining, ok = admit(reply)
		}
		if !ok || statusRemaining.Milliseconds() < 1 || reply.family == nil {
			reply = productionDetailFailure("status_stale")
		} else if reply.remainingMilliseconds > 0 {
			remaining := time.Until(reply.family.sourceFreshUntil).Milliseconds()
			if remaining < 1 {
				reply = productionDetailFailure("status_stale")
			} else {
				reply.remainingMilliseconds = min(reply.remainingMilliseconds, remaining)
			}
		}
	}
	writeDetailResponse(w, reply.status, reply.body, statusRemaining, reply.remainingMilliseconds)
}

func productionDetailFailure(reason string) productionDetailReply {
	status := http.StatusServiceUnavailable
	switch reason {
	case "invalid_request":
		status = http.StatusBadRequest
	case "rate_limited":
		status = http.StatusTooManyRequests
	default:
		if reason != "unsupported_source" && !productionDetailSourceReason(reason) {
			reason = "source_invalid"
		}
	}
	body := `{"schema_version":1,"family":"production","state":"unavailable","reason":"` + reason + `"}`
	return productionDetailReply{status: status, body: body}
}
