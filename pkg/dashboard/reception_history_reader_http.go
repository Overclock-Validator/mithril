package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

const receptionHistoryPublicPath = "/api/v1/details/reception"

type receptionPublicRequest struct {
	receptionHistoryRequest
	start, end time.Time
}

// Remapping is local and fixed; no supplied URL becomes a private request.
func parseReceptionPublicRequest(r *http.Request) (receptionPublicRequest, int) {
	var out receptionPublicRequest
	if r.Method != http.MethodGet || r.URL.Path != r.URL.EscapedPath() || r.ContentLength != 0 || len(r.TransferEncoding) != 0 ||
		r.URL.ForceQuery || len(r.URL.RawQuery) > 512 ||
		(r.URL.Path != receptionHistoryPublicPath && !strings.HasPrefix(r.URL.Path, receptionHistoryPublicPath+"/")) {
		return out, http.StatusBadRequest
	}
	suffix := strings.TrimPrefix(r.URL.Path, receptionHistoryPublicPath)
	parts := strings.Split(suffix, "/")
	if len(parts) < 2 || parts[1] != "cursor" && parts[1] != "query" {
		copy, target := *r, *r.URL
		target.Path, target.RawPath = receptionHistoryPath+suffix, ""
		copy.URL = &target
		var status int
		out.receptionHistoryRequest, status = parseReceptionHistoryRequest(&copy)
		return out, status
	}
	if len(parts) != 3 || !validHeldScheduleDigest(parts[2]) {
		return out, http.StatusBadRequest
	}
	values, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || values.Encode() != r.URL.RawQuery || len(values["after"]) != 1 {
		return out, http.StatusBadRequest
	}
	after, err := strconv.ParseUint(values.Get("after"), 10, 64)
	if err != nil || strconv.FormatUint(after, 10) != values.Get("after") {
		return out, http.StatusBadRequest
	}
	out.kind, out.recording, out.after = parts[1], parts[2], after
	if out.kind == "cursor" {
		if len(values) != 1 {
			return receptionPublicRequest{}, http.StatusBadRequest
		}
		return out, 0
	}
	if len(values) != 3 || len(values["start_time"]) != 1 || len(values["end_time"]) != 1 {
		return receptionPublicRequest{}, http.StatusBadRequest
	}
	out.start, err = time.Parse(time.RFC3339Nano, values.Get("start_time"))
	if err != nil || !validReceptionQueryTime(out.start) || out.start.UTC().Format(time.RFC3339Nano) != values.Get("start_time") {
		return receptionPublicRequest{}, http.StatusBadRequest
	}
	out.end, err = time.Parse(time.RFC3339Nano, values.Get("end_time"))
	if err != nil || !validReceptionQueryTime(out.end) || out.end.UTC().Format(time.RFC3339Nano) != values.Get("end_time") ||
		!out.start.Before(out.end) || out.end.Sub(out.start) > time.Minute {
		return receptionPublicRequest{}, http.StatusBadRequest
	}
	return out, 0
}

// This independently dated source copy is not the atomic instant of a later
// cache scan. In particular, its consumed-through does not bound that scan.
type receptionPublicSourceCapture struct {
	PublishedAt     time.Time               `json:"published_at"`
	ConsumedThrough uint64                  `json:"consumed_through,string"`
	SourceBounds    ReceptionSequenceBounds `json:"source_bounds"`
	Closed          bool                    `json:"closed"`
	Interrupted     bool                    `json:"interrupted"`
	Exhausted       bool                    `json:"exhausted"`
}

type receptionPublicRows struct {
	SchemaVersion  int                          `json:"schema_version"`
	Family         string                       `json:"family"`
	Scope          string                       `json:"scope"`
	State          string                       `json:"state"`
	RecordingID    string                       `json:"recording_id"`
	Kind           string                       `json:"kind"`
	After          uint64                       `json:"after,string"`
	ScannedThrough uint64                       `json:"scanned_through,string"`
	SourceCapture  receptionPublicSourceCapture `json:"source_capture"`
	CacheBounds    ReceptionSequenceBounds      `json:"cache_bounds"`
	CacheGaps      uint64                       `json:"cache_gaps,string"`
	StartTime      *time.Time                   `json:"start_time"`
	EndTime        *time.Time                   `json:"end_time"`
	ScannedRows    int                          `json:"scanned_rows"`
	ScannedChunks  int                          `json:"scanned_chunks"`
	More           bool                         `json:"more"`
	Columns        [10]string                   `json:"columns"`
	Rows           []ReceptionHistoryRow        `json:"rows"`
}

func prepareReceptionPublicRows(q receptionPublicRequest, source receptionPublicSourceCapture, gaps uint64, read *ReceptionHistoryRead) ([]byte, error) {
	if read == nil || q.kind != "cursor" && q.kind != "query" || read.RecordingID != q.recording || !validHeldScheduleDigest(read.RecordingID) ||
		read.After != q.after || read.ScannedThrough < read.After || read.Count < 0 || read.Count > receptionReaderPageRows ||
		read.ScannedRows < read.Count || read.ScannedRows > maxReceptionChunks*128 || read.ScannedChunks < 0 || read.ScannedChunks > maxReceptionChunks ||
		!validReceptionBounds(read.CacheBounds, math.MaxUint64) || !validReplayDetailTime(source.PublishedAt) ||
		!validReceptionBounds(source.SourceBounds, source.ConsumedThrough) {
		return nil, errors.New("source_invalid")
	}
	out := receptionPublicRows{SchemaVersion: 1, Family: "reception", Scope: "authenticated_socket_occurrences", State: "historical",
		RecordingID: read.RecordingID, Kind: q.kind, After: read.After, ScannedThrough: read.ScannedThrough,
		SourceCapture: source, CacheBounds: read.CacheBounds, CacheGaps: gaps,
		ScannedRows: read.ScannedRows, ScannedChunks: read.ScannedChunks, More: read.More,
		Columns: receptionChunkColumns, Rows: read.Rows[:read.Count]}
	if q.kind == "query" {
		if !validReceptionQueryTime(q.start) || !validReceptionQueryTime(q.end) || !q.start.Before(q.end) || q.end.Sub(q.start) > time.Minute {
			return nil, errors.New("source_invalid")
		}
		out.StartTime, out.EndTime = &q.start, &q.end
	}
	previous := read.After
	for _, row := range out.Rows {
		if row.Sequence <= previous || row.Sequence > read.ScannedThrough || read.CacheBounds.First == nil ||
			row.Sequence < *read.CacheBounds.First || row.Sequence > *read.CacheBounds.Last ||
			q.kind == "query" && (row.At.Before(q.start) || !row.At.Before(q.end)) {
			return nil, errors.New("source_invalid")
		}
		previous = row.Sequence
	}
	if read.More != (read.CacheBounds.Last != nil && read.ScannedThrough < *read.CacheBounds.Last) || read.More && read.Count != receptionReaderPageRows {
		return nil, errors.New("source_invalid")
	}
	body, err := json.Marshal(out)
	if err != nil || len(body) > MaxReceptionChunkBytes {
		return nil, errors.New("response_limit")
	}
	return body, nil
}

// One selected response owns one string OR encoded byte buffer, never a cache
// generation or a second row array. Gate admission also fences historical errors.
type receptionPublicSelection struct {
	gate              receptionHistoryGate
	recording         string
	bound, root       bool
	status            int
	reason            string
	bounds            ReceptionSequenceBounds
	next              *uint64
	body, etag, stale string
	encoded           []byte
	metadata          string
	publicationEpoch  uint64
	expires           time.Time
	remaining         int64
	statusRemaining   int64
}

func receptionPublicBounds(b ReceptionSequenceBounds) ReceptionSequenceBounds {
	if b.First == nil {
		return ReceptionSequenceBounds{}
	}
	first, last := *b.First, *b.Last
	return ReceptionSequenceBounds{First: &first, Last: &last}
}

// Return only the original deadline, not a bundle/header copy. The existing
// metadataAt owns expiry and its monotonic high-water; the HTTP layer does not
// renew it or retain a pointer after releasing the cache lock.
func receptionPublicMetadataUntil(c *ReceptionHistoryReaderCache, id, token string, now time.Time) time.Time {
	if c == nil {
		return time.Time{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.metadataAt(id, token, now)
	if p != nil {
		for _, entry := range c.metadata {
			if entry.bundle == p {
				return entry.installedAt.Add(receptionReaderMetadataTTL)
			}
		}
	}
	return time.Time{}
}

func (r *receptionHistoryReader) selectPublic(ctx context.Context, q receptionPublicRequest, now time.Time) receptionPublicSelection {
	r.gate.mu.RLock()
	r.mu.Lock()
	g, cache, publication := r.gate.value, r.cache, r.publication
	id, binding, cursor, gaps, failure := r.last.RecordingID, r.last.Binding, r.cursor, r.cacheGaps, r.failure
	source := receptionPublicSourceCapture{PublishedAt: r.last.PublishedAt, ConsumedThrough: r.last.ConsumedThrough,
		SourceBounds: receptionPublicBounds(r.last.SourceBounds), Closed: r.last.Closed, Interrupted: r.last.Interrupted, Exhausted: r.last.Exhausted}
	r.mu.Unlock()
	r.gate.mu.RUnlock()
	out := receptionPublicSelection{gate: g, recording: id, bound: id != "", root: q.kind == "root", status: http.StatusOK}
	if cache == nil || id == "" {
		out.status, out.reason = http.StatusServiceUnavailable, "waiting_source"
		return out
	}
	if g.epoch == 0 || g.epoch == math.MaxUint64 || !g.matches(binding) || !out.root && q.recording != id {
		out.status, out.reason = http.StatusConflict, "binding_changed"
		return out
	}
	if out.root {
		if publication.body == "" {
			out.status, out.reason = http.StatusServiceUnavailable, "waiting_source"
			return out
		}
		out.publicationEpoch = publication.epoch
		if publication.epoch != g.epoch || publication.root.RecordingID != id || !g.matches(publication.root.Binding) {
			out.status, out.reason = http.StatusConflict, "binding_changed"
			return out
		}
		out.metadata = publication.root.Metadata.Digest
		if receptionPublicMetadataUntil(cache, id, out.metadata, now).IsZero() {
			out.status, out.reason = http.StatusServiceUnavailable, "metadata_expired"
			return out
		}
		out.body = publication.body
		if publication.root.State == "fresh" {
			out.expires, out.stale = publication.until, publication.stale
			out.remaining = publication.until.Sub(now).Milliseconds()
			if failure != "" || !g.fresh(now) || out.remaining < 1 || out.remaining > receptionHistoryFreshFor.Milliseconds() {
				out.body, out.remaining = out.stale, 0
			}
		}
		return out
	}
	if q.kind == "cursor" || q.kind == "query" {
		var read ReceptionHistoryRead
		var err error
		if q.kind == "cursor" {
			read, err = cache.Cursor(ctx, id, q.after)
		} else {
			read, err = cache.Query(ctx, id, q.start, q.end, q.after)
		}
		out.bounds, out.next = read.CacheBounds, read.NextAvailable
		if err != nil {
			out.status, out.reason = http.StatusServiceUnavailable, "source_unavailable"
			switch err.Error() {
			case "cache_gap", "binding_changed":
				out.status, out.reason = http.StatusConflict, err.Error()
			case "query_budget":
				out.status, out.reason = http.StatusTooManyRequests, "query_budget"
			case "invalid_query":
				out.status, out.reason = http.StatusBadRequest, "invalid_request"
			}
			return out
		}
		if !read.More && read.ScannedThrough < cursor {
			out.status, out.reason = http.StatusConflict, "cache_gap"
			return out
		}
		out.encoded, err = prepareReceptionPublicRows(q, source, gaps, &read)
		if err != nil {
			out.status, out.reason = http.StatusServiceUnavailable, receptionHistoryReason(err.Error())
		}
		return out
	}
	index, gap, ok := cache.Index(id, q.after)
	if !ok {
		out.status, out.reason = http.StatusConflict, "binding_changed"
		return out
	}
	out.bounds = index.CacheBounds
	switch q.kind {
	case "index":
		if gap || len(index.Chunks) == 0 && q.after < cursor {
			out.status, out.reason = http.StatusConflict, "cache_gap"
			if len(index.Chunks) != 0 {
				next := index.Chunks[0].FirstSequence
				out.next = &next
			}
			return out
		}
		var err error
		out.body, err = PrepareReceptionHistoryIndex(index)
		if err != nil {
			out.status, out.reason = http.StatusServiceUnavailable, "source_invalid"
		}
	case "chunk":
		out.body, ok = cache.ChunkBody(id, q.digest)
		if !ok {
			out.status, out.reason = http.StatusConflict, "chunk_unavailable"
		} else {
			out.etag = q.digest
		}
	case "metadata":
		var descriptor ReceptionMetadataPageDescriptor
		out.body, descriptor, ok = cache.MetadataPage(id, q.digest, q.metadataKind, q.page, now)
		if !ok {
			out.status, out.reason = http.StatusConflict, "metadata_expired"
		} else {
			out.etag, out.metadata = descriptor.Digest, q.digest
		}
	default:
		out.status, out.reason = http.StatusBadRequest, "invalid_request"
	}
	return out
}

// Admission is the last context check, not a lock held over a network write.
// A caller can still read explicit historical IDs after same-identity status
// failure; this never makes an old publication current or supplies a live lease.
func (r *receptionHistoryReader) admitPublic(out receptionPublicSelection, now time.Time) receptionPublicSelection {
	if !out.bound {
		return out
	}
	r.gate.mu.RLock()
	r.mu.Lock()
	g, id, cache := r.gate.value, r.last.RecordingID, r.cache
	compatible := sameReceptionHistoryGate(out.gate, g) && g.epoch != 0 && g.epoch != math.MaxUint64 &&
		out.recording == id && g.matches(r.last.Binding)
	if out.publicationEpoch != 0 {
		compatible = compatible && out.publicationEpoch == g.epoch && r.publication.epoch == g.epoch
	}
	r.mu.Unlock()
	r.gate.mu.RUnlock()
	if !compatible {
		return receptionPublicSelection{status: http.StatusConflict, reason: "binding_changed"}
	}
	if clock := time.Now(); clock.After(now) {
		now = clock
	}
	var metadataUntil time.Time
	if out.status == http.StatusOK && out.metadata != "" {
		metadataUntil = receptionPublicMetadataUntil(cache, id, out.metadata, now)
	}
	// Cache work above can race a reset. Recheck after it, for successful bodies
	// and cache-derived failures alike, without holding either lock while writing.
	r.gate.mu.RLock()
	r.mu.Lock()
	compatible = sameReceptionHistoryGate(out.gate, r.gate.value) && r.last.RecordingID == id &&
		r.gate.value.matches(r.last.Binding) && (out.publicationEpoch == 0 || out.publicationEpoch == r.gate.value.epoch && r.publication.epoch == r.gate.value.epoch)
	g, failure := r.gate.value, r.failure
	acceptedUntil := r.until
	r.mu.Unlock()
	r.gate.mu.RUnlock()
	if !compatible {
		return receptionPublicSelection{status: http.StatusConflict, reason: "binding_changed"}
	}
	if clock := time.Now(); clock.After(now) {
		now = clock
	}
	if out.status == http.StatusOK && out.metadata != "" && (metadataUntil.IsZero() || !now.Before(metadataUntil)) {
		out.status, out.reason = http.StatusConflict, "metadata_expired"
		if out.root {
			out.status = http.StatusServiceUnavailable
		}
		out.body, out.encoded, out.remaining = "", nil, 0
		return out
	}
	if out.status == http.StatusOK && !out.expires.IsZero() {
		if acceptedUntil.IsZero() || acceptedUntil.Before(out.expires) {
			out.expires = acceptedUntil
		}
		remaining := out.expires.Sub(now).Milliseconds()
		if acceptedUntil.IsZero() || out.remaining == 0 || failure != "" || !g.fresh(now) || remaining < 1 || remaining > receptionHistoryFreshFor.Milliseconds() {
			out.body, out.remaining = out.stale, 0
		} else {
			out.remaining = remaining
		}
	}
	return out
}

func writeReceptionPublicReply(w http.ResponseWriter, out receptionPublicSelection) {
	if out.reason != "" {
		out.body = receptionHistoryError(out.status, out.reason, out.recording, out.bounds, out.next).body
		out.encoded, out.etag, out.remaining = nil, "", 0
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if out.etag != "" {
		w.Header().Set("ETag", `"`+out.etag+`"`)
	}
	if out.remaining > 0 {
		w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(out.remaining, 10))
	}
	if out.statusRemaining > 0 {
		w.Header().Set(statusRemainingHeader, strconv.FormatInt(out.statusRemaining, 10))
	}
	w.WriteHeader(out.status)
	if out.encoded != nil {
		_, _ = w.Write(out.encoded)
	} else {
		_, _ = io.WriteString(w, out.body)
	}
}

func (r *receptionHistoryReader) publicHandler() http.HandlerFunc {
	queryLimit, bodyLimit, metadataLimit := rate.NewLimiter(2, 2), rate.NewLimiter(64, 16), rate.NewLimiter(64, 16)
	return func(w http.ResponseWriter, request *http.Request) {
		if request.Context().Err() != nil {
			return
		}
		q, status := parseReceptionPublicRequest(request)
		if status != 0 {
			writeReceptionPublicReply(w, receptionPublicSelection{status: status, reason: "invalid_request"})
			return
		}
		limit := metadataLimit
		if q.kind == "query" {
			limit = queryLimit
		} else if q.kind == "cursor" || q.kind == "chunk" {
			limit = bodyLimit
		}
		if !limit.Allow() {
			writeReceptionPublicReply(w, receptionPublicSelection{status: http.StatusTooManyRequests, reason: "rate_limited"})
			return
		}
		out := r.selectPublic(request.Context(), q, time.Now())
		if request.Context().Err() != nil {
			return
		}
		out = r.admitPublic(out, time.Now())
		if out.root && out.status == http.StatusOK && out.remaining > 0 {
			var remaining time.Duration
			var ok bool
			if r.admitStatus != nil {
				remaining, ok = r.admitStatus(out)
			}
			if !ok || remaining.Milliseconds() < 1 {
				out = receptionPublicSelection{status: http.StatusServiceUnavailable, reason: "status_stale"}
			} else {
				out.statusRemaining = remaining.Milliseconds()
			}
		}
		if out.status == http.StatusOK && out.remaining > 0 {
			remaining := time.Until(out.expires).Milliseconds()
			if remaining < 1 {
				out.body, out.remaining, out.statusRemaining = out.stale, 0, 0
			} else {
				out.remaining = min(out.remaining, remaining)
			}
		}
		if request.Context().Err() == nil {
			writeReceptionPublicReply(w, out)
		}
	}
}

func (c *collector) admitReceptionHistory(r *receptionHistoryReader, selected receptionPublicSelection) (time.Duration, bool) {
	stamp := statusStamp{nodeEpoch: selected.gate.nodeEpoch, familyEpoch: selected.gate.epoch}
	return c.admitLiveDetail(statusReception, stamp, func() (statusStamp, bool) {
		r.gate.mu.RLock()
		defer r.gate.mu.RUnlock()
		r.mu.Lock()
		defer r.mu.Unlock()
		g := r.gate.value
		eligible := sameReceptionHistoryGate(selected.gate, g) && g.fresh(time.Now()) &&
			r.failure == "" && r.publication.epoch == g.epoch && r.last.RecordingID == selected.recording &&
			r.publication.body == selected.body && !r.publication.until.Before(selected.expires) &&
			!r.until.IsZero() && !r.until.Before(selected.expires) && selected.expires.After(time.Now())
		return statusStamp{nodeEpoch: g.nodeEpoch, familyEpoch: g.epoch}, eligible
	}, time.Now())
}
