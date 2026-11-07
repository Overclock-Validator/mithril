package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const receptionHistoryInterval = 250 * time.Millisecond
const receptionHistoryFreshFor = 3 * time.Second
const receptionHistoryPath = "/details/reception"
const receptionMetadataRefresh = 3 * receptionMetadataHold

// ReceptionHistoryCapture is a detached, body-free source publication. PublishedAt
// retains the source's original monotonic clock; Metadata.PublishedAt is its UTC
// representation. The preparation owner assigns Metadata.Revision. Receipt dates
// and marker/loss facts remain original, including reversed wall dates.
type ReceptionHistoryCapture struct {
	Metadata        ReceptionMetadata
	Chunks          []ReceptionChunkDescriptor
	PublishedAt     time.Time
	ConsumedThrough uint64
	Closed          bool
	Interrupted     bool
	Exhausted       bool
}

// ReceptionHistorySampler provides only bounded in-memory copies and one-body
// lookups. ChunkBody must check the complete recording binding before reading.
// Neither callback may do I/O or wait for progress. The Feed owns their single
// preparation caller and joins it on Close; HTTP never calls either callback.
type ReceptionHistorySampler struct {
	Metadata  func() (ReceptionHistoryCapture, error)
	ChunkBody func(ReceptionRecordingBinding, ReceptionChunkDescriptor) (string, bool)
}

type receptionHistoryGate struct {
	run       string
	started   time.Time
	genesis   string
	at        time.Time
	known     bool
	nodeEpoch uint64
	epoch     uint64 // Zero is uninitialized; MaxUint64 permanently retires pending work.
}

func (g receptionHistoryGate) matches(b ReceptionRecordingBinding) bool {
	return g.run == b.RunID && g.started.Equal(b.NodeStartedAt) && g.genesis == b.GenesisHash
}

func (g receptionHistoryGate) valid() bool {
	return g.known && g.epoch != 0 && g.epoch != math.MaxUint64
}

func (g receptionHistoryGate) fresh(now time.Time) bool {
	return g.valid() && !now.Before(g.at) && now.Sub(g.at) <= StaleAfter
}

func sameReceptionHistoryIdentity(a, b receptionHistoryGate) bool {
	return a.run == b.run && a.started.Equal(b.started) && a.genesis == b.genesis
}

func sameReceptionHistoryGate(a, b receptionHistoryGate) bool {
	return a.epoch == b.epoch && sameReceptionHistoryIdentity(a, b)
}

// receptionHistorySource owns one mutable encoded cache, not a chain of cache
// snapshots. Its lock covers status fences and small cached reply selection;
// callbacks, chunk validation, metadata preparation and writes run outside it.
type receptionHistorySource struct {
	sample            ReceptionHistorySampler
	done              chan struct{}
	limit             *rate.Limiter
	active            chan struct{}
	mu                sync.RWMutex
	gate              receptionHistoryGate
	cache             *ReceptionHistoryCache
	id                string
	cursor            uint64
	last              ReceptionHistoryRoot
	body              string
	staleBody         string
	freshUntil        time.Time
	metadataCheckedAt time.Time
	failure           string
	failureLogged     bool
}

func newReceptionHistorySource(sample ReceptionHistorySampler) *receptionHistorySource {
	return &receptionHistorySource{sample: sample, done: make(chan struct{}), limit: rate.NewLimiter(64, 16),
		active: make(chan struct{}, 4), failure: "waiting_source"}
}

func (s *receptionHistorySource) observeStatus(snapshot Snapshot, at time.Time) {
	g := receptionHistoryGate{run: snapshot.RunID, started: snapshot.StartedAt.UTC(), at: at}
	if snapshot.GenesisHash != nil {
		g.genesis = *snapshot.GenesisHash
	}
	_, err := ReceptionRecordingID(ReceptionRecordingBinding{RunID: g.run, NodeStartedAt: g.started, GenesisHash: g.genesis, RecordingGeneration: 1})
	g.known = err == nil && !at.IsZero() && at != at.Round(0)
	s.mu.Lock()
	defer s.mu.Unlock()
	g.epoch = s.gate.epoch
	if !sameReceptionHistoryIdentity(g, s.gate) {
		// An A -> B -> A transition must invalidate work captured under the first
		// A. Normal status refreshes preserve this token and the source lease.
		if g.epoch < math.MaxUint64-1 {
			g.epoch++
		} else {
			g.epoch = math.MaxUint64
		}
		s.cache, s.id, s.cursor = nil, "", 0
		s.last, s.body, s.staleBody = ReceptionHistoryRoot{}, "", ""
		s.freshUntil, s.metadataCheckedAt = time.Time{}, time.Time{}
		s.failure = "waiting_source"
		s.failureLogged = false
	}
	s.gate = g
}

func (s *receptionHistorySource) run(ctx context.Context) {
	defer close(s.done)
	ticker := time.NewTicker(receptionHistoryInterval)
	defer ticker.Stop()
	s.poll(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.poll(ctx)
		}
	}
}

func (s *receptionHistorySource) current(g receptionHistoryGate, ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return sameReceptionHistoryGate(g, s.gate) && s.gate.fresh(time.Now())
}

func (s *receptionHistorySource) fail(g receptionHistoryGate, reason string) {
	s.mu.Lock()
	logFailure := sameReceptionHistoryGate(g, s.gate) && !s.failureLogged
	if sameReceptionHistoryGate(g, s.gate) {
		s.failure, s.failureLogged = receptionHistoryReason(reason), true
	}
	s.mu.Unlock()
	if logFailure {
		attrs := []any{"component", "dashboard", "operation", "reception_prepare", "entrypoint", "private_feed", "reason", receptionHistoryReason(reason)}
		if g.valid() {
			attrs = append(attrs, "run_id", g.run)
		}
		slog.Warn("dashboard reception capture rejected", attrs...)
	}
}

// poll has one serial worker caller. Tests may drive it without changing the
// production interval. It never retains the capture's source/range slices.
func (s *receptionHistorySource) poll(ctx context.Context) {
	s.mu.RLock()
	g, oldID, cursor, checkedAt, cache := s.gate, s.id, s.cursor, s.metadataCheckedAt, s.cache
	var manifest ReceptionMetadataManifest
	var metadataInstalledAt time.Time
	if s.cache != nil {
		manifest, _ = s.cache.MetadataManifest(oldID)
		s.cache.mu.RLock()
		metadataInstalledAt = s.cache.metadataInstalledAt
		s.cache.mu.RUnlock()
	}
	s.mu.RUnlock()
	if ctx.Err() != nil || !g.fresh(time.Now()) {
		return
	}
	capture, err := s.sample.Metadata()
	if !s.current(g, ctx) {
		return
	}
	if err != nil {
		s.fail(g, "source_unavailable")
		return
	}
	id, err := validateReceptionHistoryCapture(capture, time.Now())
	if err != nil {
		s.fail(g, err.Error())
		return
	}
	if !g.matches(capture.Metadata.Binding) {
		s.fail(g, "binding_changed")
		return
	}
	if id != oldID {
		cursor, checkedAt, manifest = 0, time.Time{}, ReceptionMetadataManifest{}
		metadataInstalledAt = time.Time{}
	}
	var incoming [8]PreparedReceptionChunk
	count, attempts, through := 0, 0, cursor
	for _, descriptor := range capture.Chunks {
		if descriptor.LastSequence <= cursor {
			continue
		}
		if attempts == len(incoming) {
			break
		}
		if !s.current(g, ctx) {
			return
		}
		body, ok := s.sample.ChunkBody(capture.Metadata.Binding, descriptor)
		if !s.current(g, ctx) {
			return
		}
		attempts++
		through = descriptor.LastSequence
		if !ok {
			continue
		} // This interval remains a cache gap, not packet loss.
		prepared, err := PrepareReceptionHistoryChunk(descriptor, body)
		if err != nil {
			s.fail(g, "source_invalid")
			return
		}
		incoming[count], count = prepared, count+1
	}
	checkMetadata := checkedAt.IsZero() || time.Since(checkedAt) >= receptionMetadataHold
	var preparedMetadata *PreparedReceptionMetadata
	if checkMetadata {
		same, err := s.sameMetadata(id, manifest, capture.Metadata)
		if err != nil {
			s.fail(g, "source_invalid")
			return
		}
		// Readers expire immutable metadata tokens. Reissue unchanged metadata
		// only while the sampler is fresh; original event dates remain intact.
		refresh := !metadataInstalledAt.IsZero() && time.Since(metadataInstalledAt) >= receptionMetadataRefresh &&
			time.Since(capture.PublishedAt) < receptionHistoryFreshFor
		if !same || refresh {
			if manifest.Revision == math.MaxUint64 {
				s.fail(g, "response_limit")
				return
			}
			capture.Metadata.Revision = manifest.Revision + 1
			preparedMetadata, err = PrepareReceptionMetadata(capture.Metadata)
			if err != nil {
				s.fail(g, err.Error())
				return
			}
		}
	}
	if !s.current(g, ctx) {
		return
	}
	// Exactly one worker mutates this cache. Reset it in place, never create a
	// second retained body dictionary. Context changes hide the old root before
	// reset; same-recording incremental imports may advance ahead of its root.
	if cache == nil {
		cache, err = NewReceptionHistoryCache(true, capture.Metadata.Binding)
		if err != nil {
			s.fail(g, err.Error())
			return
		}
	}
	s.mu.Lock()
	if ctx.Err() != nil || !sameReceptionHistoryGate(g, s.gate) || !s.gate.fresh(time.Now()) {
		s.mu.Unlock()
		return
	}
	if s.id != "" && s.id != id && capture.Metadata.Binding.RecordingGeneration <= s.last.Binding.RecordingGeneration {
		s.mu.Unlock()
		s.fail(g, "binding_changed")
		return
	}
	if s.id == id && s.body != "" && (capture.ConsumedThrough < s.last.ConsumedThrough ||
		!receptionStartupCompatible(s.last.Startup, capture.Metadata.StartupAtCapture) ||
		s.last.Closed && !capture.Closed || s.last.Interrupted && !capture.Interrupted || s.last.Exhausted && !capture.Exhausted) {
		s.mu.Unlock()
		s.fail(g, "source_invalid")
		return
	}
	if s.id != id {
		s.id, s.cursor, s.last = id, 0, ReceptionHistoryRoot{}
		s.body, s.staleBody, s.freshUntil = "", "", time.Time{}
		s.metadataCheckedAt = time.Time{}
	}
	s.cache = cache
	s.mu.Unlock()
	if err = cache.Reset(capture.Metadata.Binding); err != nil {
		s.fail(g, err.Error())
		return
	}
	if !s.current(g, ctx) {
		return
	}
	if err = cache.InstallChunks(id, incoming[:count]); err != nil {
		s.fail(g, err.Error())
		return
	}
	now := time.Now()
	if preparedMetadata != nil {
		if err = cache.InstallMetadata(id, preparedMetadata, now); err != nil {
			s.fail(g, err.Error())
			return
		}
	}
	manifest, ok := cache.MetadataManifest(id)
	if !ok {
		s.fail(g, "waiting_source")
		return
	}
	index, _, _ := cache.Index(id, math.MaxUint64)
	root := ReceptionHistoryRoot{SchemaVersion: 1, Family: "reception", Scope: "authenticated_socket_occurrences",
		Binding: capture.Metadata.Binding, RecordingID: id, State: "fresh", PublishedAt: capture.PublishedAt.UTC(),
		ConsumedThrough: capture.ConsumedThrough, CacheBounds: index.CacheBounds, Closed: capture.Closed,
		Interrupted: capture.Interrupted, Exhausted: capture.Exhausted, Startup: cloneReceptionStartup(capture.Metadata.StartupAtCapture), Metadata: manifest}
	if len(capture.Chunks) > 0 {
		first, last := capture.Chunks[0].FirstSequence, capture.Chunks[len(capture.Chunks)-1].LastSequence
		root.SourceBounds = ReceptionSequenceBounds{First: &first, Last: &last}
	}
	switch {
	case root.Exhausted:
		root.State = "exhausted"
	case root.Interrupted:
		root.State = "interrupted"
	case root.Closed:
		root.State = "closed"
	}
	body, err := PrepareReceptionHistoryRoot(root)
	if err != nil {
		s.fail(g, err.Error())
		return
	}
	stale := body
	if root.State == "fresh" {
		root.State = "stale"
		stale, err = PrepareReceptionHistoryRoot(root)
		root.State = "fresh"
		if err != nil {
			s.fail(g, err.Error())
			return
		}
	}
	// No other writer can alter this cache between installation and encoding.
	// Accepted status replacement may detach it; do not resurrect that recording.
	s.mu.Lock()
	if ctx.Err() != nil || !sameReceptionHistoryGate(g, s.gate) || !s.gate.fresh(time.Now()) || s.cache != cache || s.id != id {
		s.mu.Unlock()
		return
	}
	s.cursor = through
	if checkMetadata {
		s.metadataCheckedAt = now
	}
	s.last, s.body, s.staleBody = root, body, stale
	recovered := s.failureLogged
	s.freshUntil, s.failure = capture.PublishedAt.Add(receptionHistoryFreshFor), ""
	s.failureLogged = false
	s.mu.Unlock()
	if recovered {
		slog.Info("dashboard reception capture recovered", "component", "dashboard", "operation", "reception_prepare", "entrypoint", "private_feed", "reason", "capture_accepted", "run_id", g.run)
	}
}

func validateReceptionHistoryCapture(c ReceptionHistoryCapture, now time.Time) (string, error) {
	id, err := ReceptionRecordingID(c.Metadata.Binding)
	if err != nil || c.PublishedAt.IsZero() || c.PublishedAt == c.PublishedAt.Round(0) || c.PublishedAt.After(now) ||
		!validReplayDetailTime(c.Metadata.PublishedAt) || !c.Metadata.PublishedAt.Equal(c.PublishedAt.UTC()) ||
		!validReceptionStartup(c.Metadata.StartupAtCapture) || c.Metadata.StartupAtCapture.PrefixCutoff > c.ConsumedThrough ||
		(c.Interrupted || c.Exhausted) && !c.Metadata.StartupAtCapture.Limited ||
		len(c.Metadata.Sources) > maxReceptionSources || !validReceptionSources(c.Metadata.Sources) || ValidateReceptionChunkDescriptors(c.Chunks) != nil {
		return "", errors.New("source_invalid")
	}
	for _, ranges := range c.Metadata.Ranges {
		if len(ranges) > maxReceptionRanges || !validReceptionRanges(ranges) {
			return "", errors.New("source_invalid")
		}
	}
	if len(c.Chunks) > 0 && c.Chunks[len(c.Chunks)-1].LastSequence > c.ConsumedThrough {
		return "", errors.New("source_invalid")
	}
	return id, nil
}

// Compare one bounded page at a time using the retained revision. An unchanged
// source/range/marker bundle avoids a second whole-metadata encoding until its
// token needs renewal. Receipt and marker dates always stay original.
func (s *receptionHistorySource) sameMetadata(id string, old ReceptionMetadataManifest, m ReceptionMetadata) (bool, error) {
	if old.Revision == 0 || old.StartupAtCapture.State != m.StartupAtCapture.State ||
		!receptionStartupCompatible(old.StartupAtCapture, m.StartupAtCapture) || !receptionStartupCompatible(m.StartupAtCapture, old.StartupAtCapture) {
		return false, nil
	}
	seen := 0
	for kindIndex, kind := range receptionMetadataKinds {
		count, size := len(m.Sources), 64
		if kindIndex > 0 {
			count, size = len(m.Ranges[kindIndex-1]), 128
		}
		for first, page := 0, 0; first < count; first, page = first+size, page+1 {
			if seen >= len(old.Pages) || old.Pages[seen].Kind != kind || old.Pages[seen].Page != page {
				return false, nil
			}
			value := ReceptionMetadataPage{Revision: old.Revision, Kind: kind, Page: page}
			if kindIndex == 0 {
				value.Sources = m.Sources[first:min(first+size, count)]
			} else {
				value.Ranges = m.Ranges[kindIndex-1][first:min(first+size, count)]
			}
			body, err := json.Marshal(value)
			if err != nil || len(body) > MaxReceptionMetadataPageBytes {
				return false, errors.New("source_invalid")
			}
			s.mu.RLock()
			var cached string
			var ok bool
			if s.id == id && s.cache != nil {
				cached, ok = s.cache.MetadataPage(id, old.Digest, kind, page)
			}
			s.mu.RUnlock()
			if !ok || cached != string(body) {
				return false, nil
			}
			seen++
		}
	}
	return seen == len(old.Pages), nil
}

type receptionHistoryRequest struct {
	kind, recording, digest, metadataKind string
	page                                  int
	after                                 uint64
}

func parseReceptionHistoryRequest(r *http.Request) (receptionHistoryRequest, int) {
	var q receptionHistoryRequest
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, receptionHistoryPath), "/")
	if len(parts) > 1 && (parts[1] == "cursor" || parts[1] == "query") {
		return q, http.StatusNotFound
	}
	if r.Method != http.MethodGet || r.URL.EscapedPath() != r.URL.Path || r.ContentLength != 0 || len(r.TransferEncoding) != 0 ||
		r.URL.ForceQuery || len(r.URL.RawQuery) > 512 {
		return q, http.StatusBadRequest
	}
	if r.URL.Path == receptionHistoryPath && r.URL.RawQuery == "" {
		q.kind = "root"
		return q, 0
	}
	if !strings.HasPrefix(r.URL.Path, receptionHistoryPath+"/") || len(parts) < 3 || !validHeldScheduleDigest(parts[2]) {
		return q, http.StatusBadRequest
	}
	q.kind, q.recording = parts[1], parts[2]
	switch {
	case q.kind == "index" && len(parts) == 3 && strings.HasPrefix(r.URL.RawQuery, "after="):
		value := strings.TrimPrefix(r.URL.RawQuery, "after=")
		after, err := strconv.ParseUint(value, 10, 64)
		if err == nil && strconv.FormatUint(after, 10) == value {
			q.after = after
			return q, 0
		}
	case q.kind == "chunk" && len(parts) == 4 && r.URL.RawQuery == "" && validHeldScheduleDigest(parts[3]):
		q.digest = parts[3]
		return q, 0
	case q.kind == "metadata" && len(parts) == 6 && r.URL.RawQuery == "" && validHeldScheduleDigest(parts[3]):
		kind := receptionKindIndex(parts[4])
		page, err := strconv.ParseUint(parts[5], 10, 8)
		if err == nil && strconv.FormatUint(page, 10) == parts[5] && kind >= 0 && (kind == 0 && page < 4 || kind > 0 && page < 16) {
			q.digest, q.metadataKind, q.page = parts[3], parts[4], int(page)
			return q, 0
		}
	}
	return q, http.StatusBadRequest
}

type receptionHistoryReply struct {
	status     int
	body, etag string
	remaining  int64
	gate       receptionHistoryGate
	recording  string
	expires    time.Time
	stale      string
}

// Selection contains only scalars, one string or at most eight descriptors.
// It cannot keep the cache or a metadata bundle alive while encoding/writing.
type receptionHistorySelection struct {
	reply      receptionHistoryReply
	reason, id string
	bounds     ReceptionSequenceBounds
	next       *uint64
	index      *ReceptionHistoryIndex
}

func receptionHistoryReason(reason string) string {
	switch reason {
	case context.Canceled.Error(), context.DeadlineExceeded.Error():
		return "source_unavailable"
	case "invalid_request", "rate_limited", "query_budget", "binding_changed", "cache_gap", "metadata_expired", "chunk_unavailable", "waiting_source", "source_invalid", "source_unavailable", "source_time_invalid", "status_stale", "response_limit":
		return reason
	default:
		return "source_invalid"
	}
}

func receptionHistoryError(status int, reason, id string, bounds ReceptionSequenceBounds, next *uint64) receptionHistoryReply {
	value := struct {
		Schema    int                     `json:"schema_version"`
		Family    string                  `json:"family"`
		State     string                  `json:"state"`
		Reason    string                  `json:"reason"`
		Recording string                  `json:"recording_id"`
		Bounds    ReceptionSequenceBounds `json:"cache_bounds"`
		Next      *uint64                 `json:"next_sequence,string"`
	}{1, "reception", "unavailable", receptionHistoryReason(reason), id, bounds, next}
	body, err := json.Marshal(value)
	if err != nil || len(body) > 512 {
		return receptionHistoryReply{status: http.StatusServiceUnavailable, body: `{"schema_version":1,"family":"reception","state":"unavailable","reason":"response_limit","recording_id":"","cache_bounds":{"first":null,"last":null},"next_sequence":null}`}
	}
	return receptionHistoryReply{status: status, body: string(body)}
}

func (s *receptionHistorySource) reply(q receptionHistoryRequest, now time.Time) receptionHistoryReply {
	s.mu.RLock()
	selected := s.selectReply(q, now)
	s.mu.RUnlock()
	if selected.reason != "" {
		return receptionHistoryError(selected.reply.status, selected.reason, selected.id, selected.bounds, selected.next)
	}
	if selected.index != nil {
		body, err := PrepareReceptionHistoryIndex(*selected.index)
		if err != nil {
			return receptionHistoryError(http.StatusServiceUnavailable, "source_invalid", selected.id, selected.bounds, nil)
		}
		selected.reply.body = body
	}
	return selected.reply
}

// selectReply is called with the source read lock; it does no encoding/hashing.
func (s *receptionHistorySource) selectReply(q receptionHistoryRequest, now time.Time) receptionHistorySelection {
	out := receptionHistorySelection{reply: receptionHistoryReply{status: http.StatusOK, gate: s.gate, recording: s.id}, id: s.id}
	if !s.gate.valid() || s.cache == nil || s.body == "" {
		out.reply.status, out.reason = http.StatusServiceUnavailable, receptionHistoryReason(s.failure)
		return out
	}
	if q.kind == "root" {
		out.reply.body = s.body
		if s.last.State == "fresh" {
			out.reply.expires, out.reply.stale = s.freshUntil, s.staleBody
			out.reply.remaining = s.freshUntil.Sub(now).Milliseconds()
			if s.failure != "" || !s.gate.fresh(now) || out.reply.remaining < 1 || out.reply.remaining > receptionHistoryFreshFor.Milliseconds() {
				out.reply.body, out.reply.remaining = s.staleBody, 0
			}
		}
		return out
	}
	index, gap, _ := s.cache.Index(s.id, q.after)
	out.bounds = index.CacheBounds
	if q.recording != s.id {
		out.reply.status, out.reason = http.StatusConflict, "binding_changed"
		return out
	}
	switch q.kind {
	case "index":
		if gap || len(index.Chunks) == 0 && q.after < s.cursor {
			if len(index.Chunks) != 0 {
				n := index.Chunks[0].FirstSequence
				out.next = &n
			}
			out.reply.status, out.reason = http.StatusConflict, "cache_gap"
			return out
		}
		out.index = &index
		return out
	case "chunk":
		if body, ok := s.cache.ChunkBody(s.id, q.digest); ok {
			out.reply.body, out.reply.etag = body, q.digest
			return out
		}
		out.reply.status, out.reason = http.StatusConflict, "chunk_unavailable"
		return out
	case "metadata":
		if body, digest, ok := s.cache.receptionMetadataReply(s.id, q.digest, q.metadataKind, q.page); ok {
			out.reply.body, out.reply.etag = body, digest
			return out
		}
		out.reply.status, out.reason = http.StatusConflict, "metadata_expired"
		return out
	}
	out.reply.status, out.reason = http.StatusBadRequest, "invalid_request"
	return out
}

// Copy one page and its precomputed digest under the cache's own lock. This
// transport accessor does not expose a prepared bundle or rehash its bytes.
func (c *ReceptionHistoryCache) receptionMetadataReply(recording, token, kind string, page int) (string, string, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if recording != c.recordingID {
		return "", "", false
	}
	for _, p := range [2]*PreparedReceptionMetadata{c.currentMetadata, c.previousMetadata} {
		if p == nil || p.manifest.Digest != token {
			continue
		}
		body, ok := p.Page(kind, page)
		if !ok {
			return "", "", false
		}
		for _, d := range p.manifest.Pages {
			if d.Kind == kind && d.Page == page {
				return body, d.Digest, true
			}
		}
	}
	return "", "", false
}

func (s *receptionHistorySource) admitReply(reply receptionHistoryReply, now time.Time) receptionHistoryReply {
	if reply.status != http.StatusOK {
		return reply
	}
	s.mu.RLock()
	compatible := sameReceptionHistoryGate(reply.gate, s.gate) && s.gate.valid() && reply.recording == s.id
	fresh := s.gate.fresh(now) && s.failure == ""
	s.mu.RUnlock()
	if !compatible {
		return receptionHistoryError(http.StatusConflict, "binding_changed", "", ReceptionSequenceBounds{}, nil)
	}
	if !reply.expires.IsZero() {
		remaining := reply.expires.Sub(now).Milliseconds()
		if reply.remaining == 0 || !fresh || remaining < 1 || remaining > receptionHistoryFreshFor.Milliseconds() {
			reply.body, reply.remaining = reply.stale, 0
		} else {
			reply.remaining = remaining
		}
	}
	return reply
}

func (s *receptionHistorySource) serveHTTP(w http.ResponseWriter, r *http.Request) {
	q, status := parseReceptionHistoryRequest(r)
	if status == http.StatusNotFound {
		http.NotFound(w, r)
		return
	}
	reply := receptionHistoryError(http.StatusBadRequest, "invalid_request", "", ReceptionSequenceBounds{}, nil)
	if status == 0 {
		if !s.limit.Allow() {
			reply = receptionHistoryError(http.StatusTooManyRequests, "rate_limited", "", ReceptionSequenceBounds{}, nil)
		} else {
			select {
			case s.active <- struct{}{}:
				defer func() { <-s.active }()
				reply = s.reply(q, time.Now())
			default:
				reply = receptionHistoryError(http.StatusTooManyRequests, "rate_limited", "", ReceptionSequenceBounds{}, nil)
			}
		}
	}
	reply = s.admitReply(reply, time.Now())
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if reply.remaining > 0 {
		w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(reply.remaining, 10))
	}
	if reply.etag != "" {
		w.Header().Set("ETag", `"`+reply.etag+`"`)
	}
	w.WriteHeader(reply.status)
	_, _ = io.WriteString(w, reply.body)
}
