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
	storageDetailCollectInterval = 10 * time.Second
	storageDetailFamilyBytes     = 80 << 10
)

// PublicStorageDetail adds the reader's collection time without redating
// the database lifetime or original stall events.
type PublicStorageDetail struct {
	SchemaVersion int                  `json:"schema_version"`
	Family        string               `json:"family"`
	Scope         string               `json:"scope"`
	State         string               `json:"state"`
	Reason        string               `json:"reason"`
	CollectorAt   time.Time            `json:"collector_at"`
	Binding       StorageDetailBinding `json:"binding"`
	CapturedAt    time.Time            `json:"captured_at"`
	Index         StorageStoreDetail   `json:"index"`
	BankHash      StorageStoreDetail   `json:"bankhash"`
	IO            *StorageIODetail     `json:"io,omitempty"`
	Inventory     *StorageInventory    `json:"inventory,omitempty"`
	Liveness      *StorageLiveness     `json:"liveness,omitempty"`
}

var storageDetailPublicReasons = [...]string{
	"", "status_stale", "source_unavailable", "source_invalid", "source_time_invalid",
	"binding_changed", "response_limit", "waiting_source", "unsupported_source",
}

type publicStorageDetailFamily struct {
	binding          StorageDetailBinding
	capturedAt       time.Time
	sourceDigest     [sha256.Size]byte
	bodies           [len(storageDetailPublicReasons)]string
	publishedAt      time.Time
	sourceFreshUntil time.Time
	stamp            statusStamp
}

type storageDetailReaderFailure struct {
	gate   storageDetailGate
	reason string
}

type storageDetailReader struct {
	client  *http.Client
	gate    *atomic.Pointer[storageDetailGate]
	done    chan struct{}
	limit   *rate.Limiter
	latest  atomic.Pointer[publicStorageDetailFamily]
	failure atomic.Pointer[storageDetailReaderFailure]

	highwaterNode  storageDetailGate
	highwaterGen   uint64
	highwaterStart time.Time
	lastCapture    *publicStorageDetailFamily
}

type storageDetailReply struct {
	status                int
	body                  string
	remainingMilliseconds int64
	stamp                 statusStamp
	family                *publicStorageDetailFamily
	failure               *storageDetailReaderFailure
}

func newStorageDetailReader(client *http.Client, gate *atomic.Pointer[storageDetailGate]) (*storageDetailReader, error) {
	if client == nil || gate == nil {
		return nil, errors.New("storage detail reader requires a client and status gate")
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport == nil || transport.DialContext == nil {
		return nil, errors.New("storage detail reader requires a fixed Unix transport")
	}
	return &storageDetailReader{
		client: detailHTTPClient(transport, 1, time.Second),
		gate:   gate, done: make(chan struct{}), limit: rate.NewLimiter(16, 32),
	}, nil
}

func (r *storageDetailReader) run(ctx context.Context) {
	runDetailReader(ctx, r.done, r.client, storageDetailCollectInterval, r.collect)
}

func (r *storageDetailReader) fetch(parent context.Context) ([]byte, time.Duration, time.Time, string) {
	return fetchDetail(parent, r.client, "storage", MaxStorageDetailBytes, nil, parseStorageDetailRemaining, decodeStorageDetailFailure)
}

func parseStorageDetailRemaining(value string) (time.Duration, error) {
	return parseDetailRemaining(value, StorageDetailFreshFor)
}

func decodeStorageDetailFailure(body []byte) string {
	return decodeDetailFailure(body, "storage", storageDetailSourceReason)
}

func storageDetailSourceReason(reason string) bool {
	return reason != "invalid_request" && storageDetailReason(reason) == reason
}

func (r *storageDetailReader) collect(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	before := r.gate.Load()
	if before == nil {
		return
	}
	fail := func(reason string) {
		if ctx.Err() == nil {
			r.failure.Store(&storageDetailReaderFailure{gate: *before, reason: reason})
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
	detail, err := decodeStorageDetail(body)
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
	if storageDetailTimeInvalid(StorageDetailCapture{Detail: detail, FreshUntil: freshUntil}, receivedAt) {
		fail("source_time_invalid")
		return
	}
	after := r.gate.Load()
	if after == nil || !sameStorageDetailGateContext(*before, *after) || !after.matches(detail.Binding) {
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
	if r.highwaterGen != 0 && sameStorageNode(r.highwaterNode, *before) &&
		(detail.Binding.DBGeneration < r.highwaterGen || detail.Binding.DBGeneration == r.highwaterGen &&
			!detail.Binding.DBOpenedAt.Equal(r.highwaterStart)) {
		fail("binding_changed")
		return
	}
	digest := sha256.Sum256(body)
	if previous := r.lastCapture; previous != nil && samePublicStorageCapture(previous, detail) {
		if previous.sourceDigest != digest {
			fail("source_invalid")
			return
		}
		finishedAt := time.Now()
		current := r.gate.Load()
		if current == nil || !sameStorageDetailGateContext(*after, *current) || !current.matches(detail.Binding) {
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
	family, err := preparePublicStorageDetail(detail, receivedAt, freshUntil)
	if err != nil {
		fail(err.Error())
		return
	}
	finishedAt := time.Now()
	current := r.gate.Load()
	if current == nil || !sameStorageDetailGateContext(*after, *current) || !current.matches(detail.Binding) {
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
	if r.highwaterGen == 0 || !sameStorageNode(r.highwaterNode, *current) || detail.Binding.DBGeneration > r.highwaterGen {
		r.highwaterNode, r.highwaterGen, r.highwaterStart = *current, detail.Binding.DBGeneration, detail.Binding.DBOpenedAt
	}
}

func preparePublicStorageDetail(detail StorageDetail, collectedAt, freshUntil time.Time) (*publicStorageDetailFamily, error) {
	if validateStorageDetail(detail) != nil || collectedAt.IsZero() || freshUntil.IsZero() || !freshUntil.After(collectedAt) {
		return nil, errors.New("source_invalid")
	}
	family := &publicStorageDetailFamily{binding: detail.Binding, capturedAt: detail.CapturedAt,
		publishedAt: collectedAt, sourceFreshUntil: freshUntil}
	public := PublicStorageDetail{
		SchemaVersion: 1, Family: "storage", Scope: detail.Scope, State: "fresh", CollectorAt: collectedAt.UTC(),
		Binding: detail.Binding, CapturedAt: detail.CapturedAt, Index: detail.Index, BankHash: detail.BankHash,
		IO: detail.IO, Inventory: detail.Inventory, Liveness: detail.Liveness,
	}
	total := 0
	for i, reason := range storageDetailPublicReasons {
		public.Reason = reason
		if reason == "" {
			public.State = "fresh"
		} else {
			public.State = "stale"
		}
		body, err := json.Marshal(public)
		if err != nil || len(body) > MaxStorageDetailBytes {
			return nil, errors.New("response_limit")
		}
		family.bodies[i], total = string(body), total+len(body)
	}
	if total > storageDetailFamilyBytes {
		return nil, errors.New("response_limit")
	}
	return family, nil
}

func samePublicStorageCapture(family *publicStorageDetailFamily, detail StorageDetail) bool {
	b := detail.Binding
	return family.binding.RunID == b.RunID && family.binding.NodeStartedAt.Equal(b.NodeStartedAt) &&
		family.binding.GenesisHash == b.GenesisHash && family.binding.DBGeneration == b.DBGeneration &&
		family.binding.DBOpenedAt.Equal(b.DBOpenedAt) && family.capturedAt.Equal(detail.CapturedAt)
}

func sameStorageDetailGateContext(a, b storageDetailGate) bool {
	return sameStorageNode(a, b) && a.stamp == b.stamp
}

func (r *storageDetailReader) lookup(now time.Time) storageDetailReply {
	gate := r.gate.Load()
	if gate == nil {
		return storageDetailFailure("waiting_source")
	}
	if reason := gate.failure(now); reason != "" {
		return storageDetailFailure(reason)
	}
	if !gate.stamp.valid() {
		return storageDetailFailure("waiting_source")
	}
	// Collectors publish before clearing failure; read failure only once first.
	failure, family := r.failure.Load(), r.latest.Load()
	if family == nil {
		if failure != nil && sameStorageDetailGateContext(failure.gate, *gate) {
			return storageDetailFailure(failure.reason)
		}
		return storageDetailFailure("waiting_source")
	}
	if !gate.matches(family.binding) || gate.stamp != family.stamp {
		return storageDetailFailure("binding_changed")
	}
	if now.Before(family.publishedAt) {
		return storageDetailFailure("source_time_invalid")
	}
	reason := ""
	remainingMilliseconds := family.sourceFreshUntil.Sub(now).Milliseconds()
	if remainingMilliseconds < 1 {
		reason = "status_stale"
	}
	if failure != nil && sameStorageDetailGateContext(failure.gate, *gate) {
		reason = failure.reason
	}
	for i, candidate := range storageDetailPublicReasons {
		if candidate == reason {
			reply := storageDetailReply{status: http.StatusOK, body: family.bodies[i], stamp: gate.stamp, family: family, failure: failure}
			if reason == "" {
				reply.remainingMilliseconds = remainingMilliseconds
			}
			return reply
		}
	}
	return storageDetailFailure("source_invalid")
}

func (c *collector) admitStorageDetail(r *storageDetailReader, reply storageDetailReply) (time.Duration, bool) {
	return c.admitLiveDetail(statusStorage, reply.stamp, func() (statusStamp, bool) {
		gate := r.gate.Load()
		if gate == nil {
			return statusStamp{}, false
		}
		return gate.stamp, gate.failure(time.Now()) == "" &&
			r.latest.Load() == reply.family && r.failure.Load() == reply.failure
	}, time.Now())
}

func (r *storageDetailReader) serveHTTP(w http.ResponseWriter, request *http.Request, admit func(storageDetailReply) (time.Duration, bool)) {
	reply := storageDetailFailure("invalid_request")
	if validDetailGet(request, "/api/v1/details/storage") {
		if r.limit.Allow() {
			reply = r.lookup(time.Now())
		} else {
			reply = storageDetailFailure("rate_limited")
		}
	}
	var statusRemaining time.Duration
	if reply.status == http.StatusOK {
		var ok bool
		if admit != nil {
			statusRemaining, ok = admit(reply)
		}
		if !ok || statusRemaining.Milliseconds() < 1 || reply.family == nil {
			reply = storageDetailFailure("status_stale")
		} else if reply.remainingMilliseconds > 0 {
			remaining := time.Until(reply.family.sourceFreshUntil).Milliseconds()
			if remaining < 1 {
				reply = storageDetailFailure("status_stale")
			} else {
				reply.remainingMilliseconds = min(reply.remainingMilliseconds, remaining)
			}
		}
	}
	writeDetailResponse(w, reply.status, reply.body, statusRemaining, reply.remainingMilliseconds)
}

func storageDetailFailure(reason string) storageDetailReply {
	status := http.StatusServiceUnavailable
	switch reason {
	case "invalid_request":
		status = http.StatusBadRequest
	case "rate_limited":
		status = http.StatusTooManyRequests
	default:
		if reason != "unsupported_source" && !storageDetailSourceReason(reason) {
			reason = "source_invalid"
		}
	}
	body := `{"schema_version":1,"family":"storage","state":"unavailable","reason":"` + reason + `"}`
	return storageDetailReply{status: status, body: body}
}
