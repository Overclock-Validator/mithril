package dashboard

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

const localPeerPublicPath = "/api/v1/details/peers"

type localPeerReaderFailure struct {
	binding   NodeNetworkBinding
	nodeEpoch uint64
	revision  uint64
	reason    string
}

type localPeerReply struct {
	stamp       statusStamp
	publication *localPeerPublication
	failure     *localPeerReaderFailure
}

// The collection loop owns this high-water mark. It survives transient source
// errors; a repeated sample cannot extend its original lease.
type localPeerAccepted struct {
	binding                   NodeNetworkBinding
	client, run, sequence     uint64
	clientStarted, runStarted time.Time
	freshUntil                time.Time
	digest                    [32]byte
}

// localPeerReader collects one bounded body over the fixed Unix transport.
// Public handlers read only the immutable cached body and the status gate.
type localPeerReader struct {
	client   *http.Client
	gate     *atomic.Pointer[localPeerGate]
	admit    func(localPeerReply) (time.Duration, bool) // installed before the public server starts
	latest   atomic.Pointer[localPeerPublication]
	failure  atomic.Pointer[localPeerReaderFailure]
	limit    *rate.Limiter
	active   chan struct{}
	done     chan struct{}
	accepted *localPeerAccepted // owned only by the collection loop
}

func newLocalPeerReader(client *http.Client, gate *atomic.Pointer[localPeerGate]) (*localPeerReader, error) {
	if client == nil || gate == nil {
		return nil, errors.New("local peer reader requires a client and status gate")
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport == nil || transport.DialContext == nil {
		return nil, errors.New("local peer reader requires a fixed Unix transport")
	}
	return &localPeerReader{client: detailHTTPClient(transport, 1, 3*time.Second),
		gate: gate, limit: rate.NewLimiter(2, 4), active: make(chan struct{}, 2), done: make(chan struct{})}, nil
}

func (r *localPeerReader) run(ctx context.Context) {
	runDetailReader(ctx, r.done, r.client, 10*time.Second, r.collect)
}

func (r *localPeerReader) collect(ctx context.Context) {
	before := r.gate.Load()
	if before == nil || ctx.Err() != nil {
		return
	}
	fail := func(reason string) {
		after := r.gate.Load()
		if ctx.Err() == nil && after != nil && before.nodeEpoch == after.nodeEpoch && before.revision == after.revision && sameNodeNetworkBinding(before.binding, after.binding) {
			r.failure.Store(&localPeerReaderFailure{binding: before.binding, nodeEpoch: before.nodeEpoch, revision: before.revision, reason: networkDetailReason(reason)})
		}
	}
	if reason := before.failure(time.Now()); reason != "" {
		fail(reason)
		return
	}
	if !(statusStamp{nodeEpoch: before.nodeEpoch, familyEpoch: before.revision}).valid() {
		fail("waiting_source")
		return
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://node"+localPeerPath, nil)
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
	limit := int64(MaxLocalPeerBytes + 1)
	if response.StatusCode != http.StatusOK {
		limit = 513
	}
	if response.ContentLength > limit-1 {
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
	if response.StatusCode != http.StatusOK {
		if len(response.Header.Values(replayDetailRemainingHeader)) != 0 {
			fail("source_invalid")
		} else if response.StatusCode == http.StatusNotFound {
			fail("unsupported_source")
		} else if response.StatusCode == http.StatusServiceUnavailable {
			fail(decodeLocalPeerFailure(body))
		} else {
			fail("source_unavailable")
		}
		return
	}
	values := response.Header.Values(replayDetailRemainingHeader)
	if len(values) != 1 {
		fail("source_invalid")
		return
	}
	remaining, err := parseDetailRemaining(values[0], NetworkDetailFreshFor)
	if err != nil {
		fail("source_invalid")
		return
	}
	detail, err := DecodeLocalPeers(body)
	if err != nil || !sameNodeNetworkBinding(before.binding, detail.Binding) {
		fail("source_invalid")
		return
	}
	now := time.Now()
	until := started.Add(remaining)
	after := r.gate.Load()
	if !until.After(now) || after == nil || after.nodeEpoch != before.nodeEpoch || after.revision != before.revision ||
		!sameNodeNetworkBinding(before.binding, after.binding) || after.failure(now) != "" || ctx.Err() != nil {
		fail("binding_changed")
		return
	}
	digest := sha256.Sum256(body)
	if last := r.accepted; last != nil && sameNodeNetworkBinding(last.binding, detail.Binding) {
		switch {
		case detail.ClientGeneration < last.client:
			fail("binding_changed")
			return
		case detail.ClientGeneration == last.client && !detail.ClientStartedAt.Equal(last.clientStarted):
			fail("source_invalid")
			return
		case detail.ClientGeneration == last.client && detail.RunGeneration < last.run:
			fail("binding_changed")
			return
		case detail.ClientGeneration == last.client && detail.RunGeneration == last.run:
			// Publication order is source-owned, independent of serialized UTC.
			// An equal sequence is immutable and cannot renew its original lease.
			if !detail.RunStartedAt.Equal(last.runStarted) || detail.PublicationSequence < last.sequence ||
				detail.PublicationSequence == last.sequence && digest != last.digest {
				fail("source_invalid")
				return
			}
			if detail.PublicationSequence == last.sequence && last.freshUntil.Before(until) {
				until = last.freshUntil
			}
		}
	}
	if !until.After(now) {
		fail("status_stale")
		return
	}
	finalGate := r.gate.Load()
	if finalGate == nil || finalGate.nodeEpoch != before.nodeEpoch || finalGate.revision != before.revision ||
		!sameNodeNetworkBinding(before.binding, finalGate.binding) || finalGate.failure(time.Now()) != "" || ctx.Err() != nil {
		fail("binding_changed")
		return
	}
	r.accepted = &localPeerAccepted{binding: cloneLocalPeerBinding(detail.Binding), client: detail.ClientGeneration,
		run: detail.RunGeneration, clientStarted: detail.ClientStartedAt, runStarted: detail.RunStartedAt,
		sequence: detail.PublicationSequence, freshUntil: until, digest: digest}
	r.latest.Store(&localPeerPublication{body: string(body), binding: before.binding, nodeEpoch: before.nodeEpoch, revision: before.revision, freshUntil: until})
	r.failure.Store(nil)
}

func decodeLocalPeerFailure(body []byte) string {
	return decodeDetailFailure(body, "local_peers", func(reason string) bool {
		return reason != "invalid_request" && networkDetailReason(reason) == reason
	})
}

func (c *collector) admitLocalPeers(r *localPeerReader, reply localPeerReply) (time.Duration, bool) {
	return c.admitLiveDetail(statusPeers, reply.stamp, func() (statusStamp, bool) {
		gate := r.gate.Load()
		if gate == nil || r.latest.Load() != reply.publication || r.failure.Load() != reply.failure {
			return statusStamp{}, false
		}
		return statusStamp{nodeEpoch: gate.nodeEpoch, familyEpoch: gate.revision}, gate.failure(time.Now()) == ""
	}, time.Now())
}

func (r *localPeerReader) serveHTTP(w http.ResponseWriter, request *http.Request) {
	status, reason, body, remaining := http.StatusServiceUnavailable, "waiting_source", "", int64(0)
	var selected localPeerReply
	if request.Method != http.MethodGet || request.URL.Path != localPeerPublicPath || request.URL.EscapedPath() != localPeerPublicPath ||
		request.URL.RawQuery != "" || request.URL.ForceQuery || request.ContentLength != 0 || len(request.TransferEncoding) != 0 {
		status, reason = http.StatusBadRequest, "invalid_request"
	} else if !r.limit.Allow() {
		status, reason = http.StatusTooManyRequests, "rate_limited"
	} else if gate := r.gate.Load(); gate != nil {
		now := time.Now()
		reason = gate.failure(now)
		if reason == "" {
			failure, p := r.failure.Load(), r.latest.Load()
			switch {
			case failure != nil && failure.nodeEpoch == gate.nodeEpoch && failure.revision == gate.revision && sameNodeNetworkBinding(failure.binding, gate.binding):
				reason = failure.reason
			case p == nil:
				reason = "waiting_source"
			case p.nodeEpoch != gate.nodeEpoch || p.revision != gate.revision || !sameNodeNetworkBinding(p.binding, gate.binding):
				reason = "binding_changed"
			case !p.freshUntil.After(now):
				reason = "status_stale"
			default:
				remaining = p.freshUntil.Sub(now).Milliseconds()
				if remaining < 1 {
					reason = "status_stale"
				} else {
					select {
					case r.active <- struct{}{}:
						defer func() { <-r.active }()
						status, body = http.StatusOK, p.body
						selected = localPeerReply{stamp: statusStamp{nodeEpoch: gate.nodeEpoch, familyEpoch: gate.revision}, publication: p, failure: failure}
					default:
						status, reason = http.StatusTooManyRequests, "rate_limited"
					}
				}
			}
		}
	}
	var statusRemaining time.Duration
	if status == http.StatusOK {
		var ok bool
		if r.admit != nil {
			statusRemaining, ok = r.admit(selected)
		}
		if !ok || statusRemaining.Milliseconds() < 1 || selected.publication == nil {
			status, reason, body, remaining = http.StatusServiceUnavailable, "status_stale", "", 0
		} else if deadline := time.Until(selected.publication.freshUntil).Milliseconds(); deadline < 1 {
			status, reason, body, remaining = http.StatusServiceUnavailable, "status_stale", "", 0
		} else {
			remaining = min(remaining, deadline)
		}
	}
	if body == "" {
		if reason != "rate_limited" {
			reason = networkDetailReason(reason)
		}
		body = `{"schema_version":1,"family":"local_peers","state":"unavailable","reason":"` + reason + `"}`
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
