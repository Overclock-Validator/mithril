package dashboard

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// selectedWindowReader is designed for the replay reader's joined worker and
// fixed Unix client. Viewers only read complete, re-encoded cached generations.
type selectedWindowReader struct {
	client   *http.Client
	gate     *atomic.Pointer[replayDetailGate]
	limit    *rate.Limiter
	active   chan struct{}
	mu       sync.Mutex
	cache    replayDetailCache
	revision uint64
	failure  *selectedWindowFailure
	last     *selectedWindowHighWater // owned by the collection worker
}

func (r *selectedWindowReader) fetch(ctx context.Context, query string) ([]byte, time.Time, time.Duration, string) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://node/details/selected"+query, nil)
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
		switch response.StatusCode {
		case http.StatusNotFound:
			return nil, started, 0, "unsupported_source"
		case http.StatusConflict:
			return nil, started, 0, "source_generation_expired"
		default:
			return nil, started, 0, "source_unavailable"
		}
	}
	if response.ContentLength > MaxSnapshotBytes {
		return nil, started, 0, "response_limit"
	}
	var remaining time.Duration
	if query == "" {
		values := response.Header.Values(replayDetailRemainingHeader)
		if len(values) != 1 {
			return nil, started, 0, "source_invalid"
		}
		ms, err := strconv.ParseInt(values[0], 10, 64)
		if err != nil || ms < 13000 || ms > 30000 || strconv.FormatInt(ms, 10) != values[0] {
			return nil, started, 0, "source_invalid"
		}
		remaining = time.Duration(ms) * time.Millisecond
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, MaxSnapshotBytes+1))
	if err != nil {
		return nil, started, 0, "source_unavailable"
	}
	if len(body) > MaxSnapshotBytes {
		return nil, started, 0, "response_limit"
	}
	return body, started, remaining, ""
}

func (r *selectedWindowReader) collect(parent context.Context) {
	if parent.Err() != nil {
		return
	}
	before := r.gate.Load()
	if before == nil {
		return
	}
	fail := func(reason string) {
		r.mu.Lock()
		defer r.mu.Unlock()
		after := r.gate.Load()
		if parent.Err() == nil && after != nil && !after.retired && after.revision == before.revision && sameReplayDetailStatusContext(*before, *after) {
			r.failure = &selectedWindowFailure{revision: before.revision, stamp: before.stamp, reason: reason}
		}
	}
	if before.retired || before.stopping || !before.stamp.valid() {
		fail("source_unavailable")
		return
	}
	if before.State != "fresh" {
		fail("status_stale")
		return
	}
	if reason := before.failure(time.Now()); reason != "" {
		fail(reason)
		return
	}
	ctx, cancel := context.WithTimeout(parent, replayDetailCollectionLimit)
	defer cancel()
	manifestBody, started, remaining, reason := r.fetch(ctx, "")
	if reason != "" {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			reason = "collection_deadline"
		}
		fail(reason)
		return
	}
	m, err := decodeSelectedWindowManifest(manifestBody)
	if err != nil || !before.matches(m.Binding) {
		fail("source_invalid")
		return
	}
	deadline := started.Add(remaining)
	pages := make([][]byte, m.PageCount)
	jobs := make(chan int, m.PageCount)
	for i := range m.PageCount {
		jobs <- i
	}
	close(jobs)
	var total atomic.Int64
	total.Store(int64(len(manifestBody)))
	failures := make(chan string, 1)
	var workers sync.WaitGroup
	for range min(2, m.PageCount) {
		workers.Go(func() {
			for page := range jobs {
				if ctx.Err() != nil {
					return
				}
				query := "?generation=" + m.Digest + "&page=" + strconv.Itoa(page)
				body, _, _, reason := r.fetch(ctx, query)
				if reason == "" && total.Add(int64(len(body))) > selectedWindowMaxBytes {
					reason = "response_limit"
				}
				if reason != "" {
					select {
					case failures <- reason:
					default:
					}
					cancel()
					return
				}
				pages[page] = body
			}
		})
	}
	workers.Wait()
	select {
	case reason := <-failures:
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			reason = "collection_deadline"
		}
		fail(reason)
		return
	default:
	}
	if ctx.Err() != nil {
		fail("collection_deadline")
		return
	}
	w, err := decodeSelectedWindow(manifestBody, pages)
	now := time.Now()
	if err != nil {
		fail("source_invalid")
		return
	}
	if !deadline.After(now) || w.CapturedAt.After(now.UTC().Add(time.Second)) {
		fail("source_time_invalid")
		return
	}
	if selectedWindowRegressed(r.last, w.Binding, w.CapturedAt, m.Digest) {
		fail("source_invalid")
		return
	}
	prepared, err := prepareSelectedWindow(w)
	if err != nil {
		fail("response_limit")
		return
	}
	if ctx.Err() != nil {
		fail("collection_deadline")
		return
	}
	p := &replayDetailPrepared{binding: w.Binding, generation: m.Digest,
		capturedAt: deadline.Add(-replayDetailFreshFor), pages: prepared.pages, bytes: prepared.bytes}
	for i := range p.manifests {
		p.manifests[i] = prepared.manifest
	}
	r.mu.Lock()
	after := r.gate.Load()
	if ctx.Err() != nil || after == nil || after.retired || after.stopping || after.State != "fresh" ||
		after.revision != before.revision || !sameReplayDetailStatusContext(*before, *after) || !deadline.After(time.Now()) {
		if ctx.Err() != nil && parent.Err() == nil && after != nil && after.revision == before.revision && sameReplayDetailStatusContext(*before, *after) {
			r.failure = &selectedWindowFailure{revision: after.revision, stamp: after.stamp, reason: "collection_deadline"}
		}
		r.mu.Unlock()
		return
	}
	if r.revision != after.revision {
		r.cache.mu.Lock()
		r.cache.items = [replayDetailGenerations]*replayDetailPublication{}
		r.cache.mu.Unlock()
		r.revision = after.revision
	}
	if err := r.cache.publish(p, *after, time.Now()); err != nil {
		r.failure = &selectedWindowFailure{revision: after.revision, stamp: after.stamp, reason: replayDetailCaptureReason(err)}
		r.mu.Unlock()
		return
	}
	r.failure = nil
	r.mu.Unlock()
	last := &selectedWindowHighWater{binding: w.Binding, at: w.CapturedAt, digest: m.Digest}
	if w.Binding.GenesisHash != nil {
		genesis := *w.Binding.GenesisHash
		last.binding.GenesisHash = &genesis
	}
	r.last = last
}

func (r *selectedWindowReader) serveHTTP(w http.ResponseWriter, request *http.Request,
	admit func(statusStamp, func() (statusStamp, bool)) (time.Duration, bool)) {
	reply := replayDetailFailure("invalid_request")
	var selectedFailure *selectedWindowFailure
	_, _, page, ok := parseReplayDetailQuery(request.URL.RawQuery, request.URL.ForceQuery)
	if request.Method == http.MethodGet && request.URL.Path == "/api/v1/details/selected" &&
		request.URL.EscapedPath() == request.URL.Path && request.ContentLength == 0 && len(request.TransferEncoding) == 0 && ok {
		if !r.limit.Allow() {
			reply = replayDetailReply{status: http.StatusTooManyRequests, body: `{"schema_version":1,"family":"selected_slots","state":"unavailable","reason":"rate_limited"}`}
		} else {
			select {
			case r.active <- struct{}{}:
				defer func() { <-r.active }()
				r.mu.Lock()
				gate := r.gate.Load()
				switch {
				case gate == nil:
					reply = replayDetailFailure("waiting_source")
				case gate.retired || !gate.stamp.valid():
					reply = replayDetailFailure("source_unavailable")
				case gate.stopping:
					reply = replayDetailFailure("source_unavailable")
				case gate.State != "fresh":
					reply = replayDetailFailure("status_stale")
				case r.failure != nil && r.failure.revision == gate.revision && r.failure.stamp == gate.stamp && !page:
					reply = replayDetailReaderFailure(r.failure.reason)
				case r.revision != gate.revision:
					reply = replayDetailFailure("waiting_source")
				default:
					reply = r.cache.lookup(request.URL.RawQuery, request.URL.ForceQuery, *gate, time.Now())
					selectedFailure = r.failure
				}
				r.mu.Unlock()
			default:
				reply = replayDetailReply{status: http.StatusTooManyRequests, body: `{"schema_version":1,"family":"selected_slots","state":"unavailable","reason":"rate_limited"}`}
			}
		}
	}
	var statusRemaining time.Duration
	if reply.status == http.StatusOK {
		var ok bool
		publication := reply.publication
		if publication != nil && admit != nil {
			statusRemaining, ok = admit(publication.stamp, func() (statusStamp, bool) {
				gate := r.gate.Load()
				if gate == nil {
					return statusStamp{}, false
				}
				r.mu.Lock()
				valid := r.revision == gate.revision && (page || r.failure == selectedFailure)
				r.cache.mu.RLock()
				present := false
				for _, item := range r.cache.items {
					if item == publication || page && item != nil && item.stamp == publication.stamp &&
						item.prepared.generation == publication.prepared.generation && item.publishedAt.Equal(publication.publishedAt) &&
						item.holdUntil.Equal(publication.holdUntil) {
						present = true
						break
					}
				}
				r.cache.mu.RUnlock()
				r.mu.Unlock()
				now := time.Now()
				return gate.stamp, valid && present && !gate.retired && !gate.stopping && gate.State == "fresh" &&
					gate.failure(now) == "" && gate.matches(publication.prepared.binding) && now.Before(publication.holdUntil) &&
					(page || now.Before(publication.prepared.capturedAt.Add(replayDetailFreshFor)))
			})
		}
		if !page && publication != nil {
			reply.remainingMilliseconds = min(reply.remainingMilliseconds, time.Until(publication.holdUntil).Milliseconds(),
				time.Until(publication.prepared.capturedAt.Add(replayDetailFreshFor)).Milliseconds())
		}
		if !ok || statusRemaining.Milliseconds() < 1 || !page && reply.remainingMilliseconds < 1 {
			reply = replayDetailFailure("status_stale")
		}
	}
	if reply.status != http.StatusOK {
		reply.body = strings.Replace(reply.body, `"family":"replay"`, `"family":"selected_slots"`, 1)
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
	if _, err := io.WriteString(w, reply.body); err != nil {
		return
	}
}
