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
	"sync"
	"sync/atomic"
	"time"
)

const (
	receptionReaderCycleLimit   = 225 * time.Millisecond
	receptionReaderRequestLimit = 100 * time.Millisecond
	receptionReaderCycleBytes   = 608 << 10
	receptionReaderCycleStarts  = 14
)

// The status loop is the only gate writer. Holding its read lock through a
// bounded cache commit makes A -> unknown -> A a real fence, not just a pair
// of equal strings observed before and after an unrelated publication.
type receptionReaderStatus struct {
	mu    sync.RWMutex
	value receptionHistoryGate
}

func (c *collector) observeReceptionHistoryStatus(state string, at time.Time) statusStamp {
	g := receptionHistoryGate{at: at, nodeEpoch: c.statusNodeEpoch}
	if c.last != nil {
		g.run, g.started = c.last.RunID, c.last.StartedAt.UTC()
		if c.last.GenesisHash != nil {
			g.genesis = *c.last.GenesisHash
		}
	}
	_, err := ReceptionRecordingID(ReceptionRecordingBinding{RunID: g.run, NodeStartedAt: g.started, GenesisHash: g.genesis, RecordingGeneration: 1})
	g.known = state == "fresh" && err == nil && !at.IsZero() && at != at.Round(0) && g.nodeEpoch != math.MaxUint64
	c.receptionGate.mu.Lock()
	defer c.receptionGate.mu.Unlock()
	old := c.receptionGate.value
	g.epoch = old.epoch
	if g.epoch == 0 || !sameReceptionHistoryIdentity(g, old) || g.known != old.known || g.nodeEpoch != old.nodeEpoch {
		if g.epoch < math.MaxUint64-1 {
			g.epoch++
		} else {
			g.epoch = math.MaxUint64
		}
	}
	c.receptionGate.value = g
	return statusStamp{nodeEpoch: g.nodeEpoch, familyEpoch: g.epoch}
}

type receptionMetadataTransfer struct {
	manifest ReceptionMetadataManifest
	body     string
	pages    [maxReceptionPages]receptionValidatedMetadataPage
}

type receptionReaderPublication struct {
	root        ReceptionHistoryRoot
	body, stale string
	until       time.Time
	epoch       uint64
}

// receptionHistoryReader has one joined collector and two joined request lanes
// per cycle. It owns no source callback, native snapshot or previous cache.
type receptionHistoryReader struct {
	client          *http.Client
	gate            *receptionReaderStatus
	admitStatus     func(receptionPublicSelection) (time.Duration, bool)
	done            chan struct{}
	mu              sync.Mutex
	cache           *ReceptionHistoryReaderCache
	last            ReceptionHistoryRoot // accepted source high-waters, not the public root
	cursor          uint64
	pending         *receptionMetadataTransfer
	pendingEpoch    uint64
	expiredMetadata string
	fingerprint     string
	until           time.Time
	publication     receptionReaderPublication
	failure         string
	failureLogged   bool
	cacheGaps       uint64 // explicit transport availability gaps, never source loss
}

func newReceptionHistoryReader(client *http.Client, gate *receptionReaderStatus) (*receptionHistoryReader, error) {
	if client == nil || gate == nil {
		return nil, errors.New("reception reader requires a client and status gate")
	}
	t, ok := client.Transport.(*http.Transport)
	if !ok || t == nil || t.DialContext == nil {
		return nil, errors.New("reception reader requires a fixed Unix transport")
	}
	readerClient := detailHTTPClient(t, 2, receptionReaderRequestLimit)
	readerClient.Transport.(*http.Transport).DisableCompression = true
	return &receptionHistoryReader{client: readerClient,
		gate: gate, done: make(chan struct{}), failure: "waiting_source"}, nil
}

func (r *receptionHistoryReader) run(ctx context.Context) {
	defer close(r.done)
	defer r.client.CloseIdleConnections()
	ticker := time.NewTicker(receptionHistoryInterval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		r.collect(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *receptionHistoryReader) status() receptionHistoryGate {
	r.gate.mu.RLock()
	defer r.gate.mu.RUnlock()
	return r.gate.value
}

// commit also serializes the two collector lanes. No network call, source call
// or user callback is made while either lock is held.
func (r *receptionHistoryReader) commit(ctx context.Context, g receptionHistoryGate, f func() error) error {
	r.gate.mu.RLock()
	defer r.gate.mu.RUnlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if !sameReceptionHistoryGate(g, r.gate.value) || !r.gate.value.fresh(time.Now()) {
		return errors.New("binding_changed")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return f()
}

func (r *receptionHistoryReader) fail(g receptionHistoryGate, reason string) {
	if reason == "" {
		return
	}
	if reason != "unsupported_source" {
		reason = receptionHistoryReason(reason)
	}
	r.gate.mu.RLock()
	defer r.gate.mu.RUnlock()
	if !sameReceptionHistoryGate(g, r.gate.value) {
		return
	}
	r.mu.Lock()
	logFailure := !r.failureLogged
	r.failure, r.failureLogged = reason, true
	r.mu.Unlock()
	if logFailure {
		slog.Warn("dashboard reception collection rejected", "component", "dashboard", "operation", "reception_collect", "entrypoint", "public_reader", "reason", reason, "run_id", g.run)
	}
}

type receptionCycleBudget struct{ starts, bytes atomic.Int64 }

// All bytes actually returned by Read, including bound+1 probes and error
// bodies, share one budget. Reserving before the call bounds simultaneous reads.
type receptionBudgetReader struct {
	reader io.Reader
	budget *receptionCycleBudget
}

func (b receptionBudgetReader) Read(p []byte) (int, error) {
	for {
		left := int64(receptionReaderCycleBytes) - b.budget.bytes.Load()
		if left <= 0 {
			return 0, errors.New("response_limit")
		}
		n := min(int64(len(p)), left)
		old := int64(receptionReaderCycleBytes) - left
		if !b.budget.bytes.CompareAndSwap(old, old+n) {
			continue
		}
		read, err := b.reader.Read(p[:int(n)])
		b.budget.bytes.Add(int64(read) - n)
		return read, err
	}
}

type receptionReaderError struct {
	Schema    int                     `json:"schema_version"`
	Family    string                  `json:"family"`
	State     string                  `json:"state"`
	Reason    string                  `json:"reason"`
	Recording string                  `json:"recording_id"`
	Bounds    ReceptionSequenceBounds `json:"cache_bounds"`
	Next      *uint64                 `json:"next_sequence,string"`
}

type receptionReaderResponse struct {
	body      []byte
	status    int
	started   time.Time
	remaining time.Duration
	error     receptionReaderError
	reason    string
}

func (r *receptionHistoryReader) fetch(ctx context.Context, budget *receptionCycleBudget, path, etag string, limit int, root bool) receptionReaderResponse {
	out := receptionReaderResponse{}
	if ctx.Err() != nil {
		out.reason = "source_unavailable"
		return out
	}
	for {
		starts := budget.starts.Load()
		if starts >= receptionReaderCycleStarts {
			out.reason = "response_limit"
			return out
		}
		if budget.starts.CompareAndSwap(starts, starts+1) {
			break
		}
	}
	ctx, cancel := context.WithTimeout(ctx, receptionReaderRequestLimit)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://node"+path, nil)
	if err != nil {
		out.reason = "source_invalid"
		return out
	}
	out.started = time.Now()
	response, err := r.client.Do(req)
	if err != nil {
		out.reason = "source_unavailable"
		return out
	}
	defer response.Body.Close()
	out.status = response.StatusCode
	if out.status != http.StatusOK {
		limit = 512
	}
	if response.ContentLength > int64(limit) {
		out.reason = "response_limit"
		return out
	}
	out.body, err = io.ReadAll(io.LimitReader(receptionBudgetReader{response.Body, budget}, int64(limit)+1))
	if len(out.body) > limit || budget.bytes.Load() >= receptionReaderCycleBytes {
		out.reason = "response_limit"
		return out
	}
	if err != nil {
		out.reason = "source_unavailable"
		return out
	}
	if root && out.status == http.StatusNotFound {
		out.reason = "unsupported_source"
		return out
	}
	if len(response.Header.Values("Content-Type")) != 1 || response.Header.Get("Content-Type") != "application/json" ||
		len(response.Header.Values("Content-Encoding")) != 0 {
		out.reason = "source_invalid"
		return out
	}
	remaining := response.Header.Values(replayDetailRemainingHeader)
	if out.status != http.StatusOK {
		if len(remaining) != 0 || json.Unmarshal(out.body, &out.error) != nil || !receptionCanonical(out.body, out.error) ||
			out.error.Schema != 1 || out.error.Family != "reception" || out.error.State != "unavailable" ||
			receptionHistoryReason(out.error.Reason) != out.error.Reason || !validReceptionBounds(out.error.Bounds, math.MaxUint64) ||
			out.error.Recording != "" && !validHeldScheduleDigest(out.error.Recording) || out.error.Next != nil &&
			(out.error.Recording == "" || out.error.Bounds.First == nil || *out.error.Next < *out.error.Bounds.First || *out.error.Next > *out.error.Bounds.Last) {
			out.reason = "source_invalid"
			return out
		}
		switch out.status {
		case http.StatusConflict, http.StatusTooManyRequests, http.StatusServiceUnavailable:
			out.reason = out.error.Reason
		default:
			out.reason = "source_unavailable"
		}
		return out
	}
	if root {
		if len(remaining) > 1 {
			out.reason = "source_invalid"
			return out
		}
		if len(remaining) == 1 {
			lease, err := parseDetailRemaining(remaining[0], receptionHistoryFreshFor)
			if err != nil {
				out.reason = "source_invalid"
				return out
			}
			out.remaining = lease
		}
	} else if len(remaining) != 0 {
		out.reason = "source_invalid"
		return out
	}
	if etag != "" && (len(response.Header.Values("ETag")) != 1 || response.Header.Get("ETag") != `"`+etag+`"`) {
		out.reason = "source_invalid"
	}
	return out
}

func receptionCaptureFingerprint(root ReceptionHistoryRoot) string {
	// Adapter state/cache bounds/metadata can change while the source clock is
	// unchanged. Hash only the exact source fact, never the prepared root bytes.
	body, _ := json.Marshal(struct {
		Binding                        ReceptionRecordingBinding
		At                             time.Time
		Through                        uint64
		Bounds                         ReceptionSequenceBounds
		Closed, Interrupted, Exhausted bool
		Startup                        ReceptionStartup
	}{root.Binding, root.PublishedAt, root.ConsumedThrough, root.SourceBounds, root.Closed, root.Interrupted, root.Exhausted, root.Startup})
	return receptionDigest(body)
}

func (r *receptionHistoryReader) accept(ctx context.Context, g receptionHistoryGate, root ReceptionHistoryRoot, response receptionReaderResponse) error {
	if !g.matches(root.Binding) || (root.State == "fresh") != (response.remaining > 0) {
		return errors.New("source_invalid")
	}
	until := time.Time{}
	if response.remaining > 0 {
		until = response.started.Add(response.remaining)
		if !time.Now().Before(until) {
			return errors.New("source_time_invalid")
		}
	}
	fingerprint := receptionCaptureFingerprint(root)
	metadataBody, err := json.Marshal(root.Metadata)
	if err != nil || len(metadataBody) > MaxReceptionRootBytes {
		return errors.New("source_invalid")
	}
	metadataText := string(metadataBody)
	return r.commit(ctx, g, func() error {
		if response.remaining > 0 && !time.Now().Before(until) {
			return errors.New("source_time_invalid")
		}
		old := r.last
		sameNode := old.Binding.RunID == root.Binding.RunID && old.Binding.NodeStartedAt.Equal(root.Binding.NodeStartedAt) && old.Binding.GenesisHash == root.Binding.GenesisHash
		if old.RecordingID != "" && sameNode && root.Binding.RecordingGeneration < old.Binding.RecordingGeneration {
			return errors.New("binding_changed")
		}
		if old.RecordingID == root.RecordingID {
			if root.ConsumedThrough < old.ConsumedThrough || !receptionStartupCompatible(old.Startup, root.Startup) ||
				!receptionStartupCompatible(old.Metadata.StartupAtCapture, root.Metadata.StartupAtCapture) ||
				old.Closed && !root.Closed || old.Interrupted && !root.Interrupted || old.Exhausted && !root.Exhausted ||
				root.Metadata.Revision < old.Metadata.Revision || root.Metadata.Revision == old.Metadata.Revision && root.Metadata.Digest != old.Metadata.Digest {
				return errors.New("source_invalid")
			}
		}
		if r.cache == nil {
			var err error
			r.cache, err = NewReceptionHistoryReaderCache(true, root.Binding)
			if err != nil {
				return err
			}
		} else if err := r.cache.Reset(root.Binding); err != nil {
			return err
		}
		if old.RecordingID != root.RecordingID {
			r.cursor, r.pending, r.fingerprint, r.until, r.publication, r.cacheGaps = 0, nil, "", time.Time{}, receptionReaderPublication{}, 0
			r.expiredMetadata = ""
		}
		if r.pendingEpoch != g.epoch {
			r.pending = nil
		}
		r.pendingEpoch = g.epoch
		if fingerprint == r.fingerprint && (r.until.IsZero() || !until.IsZero() && r.until.Before(until)) {
			until = r.until
		}
		r.last, r.fingerprint, r.until = root, fingerprint, until
		if r.pending == nil && root.Metadata.Digest != r.expiredMetadata {
			if _, ok := r.cache.MetadataManifest(root.RecordingID, root.Metadata.Digest, time.Now()); !ok {
				r.pending = &receptionMetadataTransfer{manifest: root.Metadata, body: metadataText}
			}
		}
		return nil
	})
}

func (r *receptionHistoryReader) collect(parent context.Context) {
	g := r.status()
	if parent.Err() != nil {
		return
	}
	if !g.fresh(time.Now()) {
		r.fail(g, "status_stale")
		return
	}
	ctx, cancel := context.WithTimeout(parent, receptionReaderCycleLimit)
	defer cancel()
	budget := &receptionCycleBudget{}
	response := r.fetch(ctx, budget, receptionHistoryPath, "", MaxReceptionRootBytes, true)
	if response.reason != "" {
		r.fail(g, response.reason)
		return
	}
	root, err := decodeReceptionHistoryRoot(response.body)
	if err == nil {
		err = r.accept(ctx, g, root, response)
	}
	if err != nil {
		r.fail(g, err.Error())
		return
	}
	// Two joined lanes let metadata attempt up to four pages while the chunk lane
	// drains old occurrences. Both share the cycle deadline and resource limits;
	// neither launches per-response workers.
	results := make(chan string, 2)
	go func() { results <- r.collectChunks(ctx, g, budget, root) }()
	go func() { results <- r.collectMetadata(ctx, g, budget, root.RecordingID) }()
	a, b := <-results, <-results
	if a != "" {
		r.fail(g, a)
	} else if b != "" {
		r.fail(g, b)
	}
	if ctx.Err() != nil {
		r.fail(g, "source_unavailable")
		return
	}
	if err := r.publish(ctx, g, a == "" && b == ""); err != nil {
		r.fail(g, err.Error())
	}
}

func (r *receptionHistoryReader) collectChunks(ctx context.Context, g receptionHistoryGate, budget *receptionCycleBudget, root ReceptionHistoryRoot) string {
	r.mu.Lock()
	after := r.cursor
	r.mu.Unlock()
	path := receptionHistoryPath + "/index/" + root.RecordingID + "?after=" + strconv.FormatUint(after, 10)
	response := r.fetch(ctx, budget, path, "", MaxReceptionRootBytes, false)
	if response.reason != "" {
		if response.status == http.StatusConflict && response.reason == "cache_gap" && response.error.Recording == root.RecordingID {
			next := after
			if response.error.Next != nil && *response.error.Next <= after {
				return "source_invalid"
			}
			if response.error.Next != nil && *response.error.Next > after {
				next = *response.error.Next - 1
			}
			// An empty missing tail supplies no exact next cursor. ConsumedThrough
			// also includes source rows the node adapter has not attempted yet,
			// so keep waiting rather than silently skip those future imports.
			// An index may advance while this root is in flight. Its explicit
			// gap proves absence through the older root's finite frontier too.
			if next > root.ConsumedThrough {
				next = root.ConsumedThrough
			}
			if err := r.commit(ctx, g, func() error {
				if r.last.RecordingID != root.RecordingID {
					return errors.New("binding_changed")
				}
				if next > r.cursor {
					r.cursor = next
					if r.cacheGaps != math.MaxUint64 {
						r.cacheGaps++
					}
				}
				return nil
			}); err != nil {
				return err.Error()
			}
			return ""
		}
		return response.reason
	}
	index, err := decodeReceptionHistoryIndex(response.body)
	if err != nil || index.RecordingID != root.RecordingID || index.After != after {
		return "source_invalid"
	}
	for _, d := range index.Chunks {
		if root.CacheBounds.Last == nil || d.LastSequence > *root.CacheBounds.Last {
			break
		}
		response = r.fetch(ctx, budget, receptionHistoryPath+"/chunk/"+root.RecordingID+"/"+d.Digest, d.Digest, MaxReceptionChunkBytes, false)
		missing := response.status == http.StatusConflict && response.reason == "chunk_unavailable" && response.error.Recording == root.RecordingID
		if response.reason != "" && !missing {
			return response.reason
		}
		var prepared PreparedReceptionChunk
		var rows receptionChunkRows
		if !missing {
			prepared, rows, err = decodeReceptionHistoryChunk(d, string(response.body))
			if err != nil {
				return "source_invalid"
			}
		}
		if err := r.commit(ctx, g, func() error {
			if r.last.RecordingID != root.RecordingID {
				return errors.New("binding_changed")
			}
			if !missing {
				if err := r.cache.installPreparedChunks(root.RecordingID, []PreparedReceptionChunk{prepared}, []receptionChunkRows{rows}); err != nil {
					return err
				}
			}
			if missing || d.FirstSequence > r.cursor && d.FirstSequence-r.cursor > 1 {
				if r.cacheGaps != math.MaxUint64 {
					r.cacheGaps++
				}
			}
			r.cursor = d.LastSequence
			return nil
		}); err != nil {
			return err.Error()
		}
	}
	return ""
}

func (r *receptionHistoryReader) collectMetadata(ctx context.Context, g receptionHistoryGate, budget *receptionCycleBudget, id string) string {
	r.mu.Lock()
	transfer := r.pending
	r.mu.Unlock()
	if transfer == nil {
		return ""
	}
	starts := 0
	for i, d := range transfer.manifest.Pages {
		if transfer.pages[i].body != "" {
			continue
		}
		if starts == 4 {
			break
		}
		starts++
		path := receptionHistoryPath + "/metadata/" + id + "/" + transfer.manifest.Digest + "/" + d.Kind + "/" + strconv.Itoa(d.Page)
		response := r.fetch(ctx, budget, path, d.Digest, MaxReceptionMetadataPageBytes, false)
		if response.reason != "" {
			if response.status == http.StatusConflict && (response.reason == "metadata_expired" || response.reason == "binding_changed") {
				_ = r.commit(ctx, g, func() error {
					if r.pending == transfer {
						r.pending = nil
					}
					return nil
				})
			}
			return response.reason
		}
		page, err := decodeReceptionMetadataPageForCache(transfer.manifest, i, response.body)
		if err != nil {
			return "source_invalid"
		}
		if err := r.commit(ctx, g, func() error {
			if r.last.RecordingID != id || r.pending != transfer {
				return errors.New("binding_changed")
			}
			transfer.pages[i] = page
			return nil
		}); err != nil {
			return err.Error()
		}
	}
	for i := range transfer.manifest.Pages {
		if transfer.pages[i].body == "" {
			return ""
		}
	}
	prepared, err := finishReceptionMetadata(transfer.body, transfer.pages[:len(transfer.manifest.Pages)])
	if err != nil {
		_ = r.commit(ctx, g, func() error {
			if r.pending == transfer {
				r.expiredMetadata, r.pending = transfer.manifest.Digest, nil
			}
			return nil
		})
		return "source_invalid"
	}
	returnErr := r.commit(ctx, g, func() error {
		if r.last.RecordingID != id || r.pending != transfer {
			return errors.New("binding_changed")
		}
		if err := r.cache.InstallMetadata(id, prepared, time.Now()); err != nil {
			if err.Error() == "metadata_expired" {
				r.expiredMetadata, r.pending = transfer.manifest.Digest, nil
			}
			return err
		}
		r.pending = nil
		return nil
	})
	if returnErr != nil {
		return returnErr.Error()
	}
	return ""
}

func (r *receptionHistoryReader) publish(ctx context.Context, g receptionHistoryGate, healthy bool) error {
	r.mu.Lock()
	root, until, cache := r.last, r.until, r.cache
	r.mu.Unlock()
	metadata, ok := cache.MetadataManifest(root.RecordingID, "", time.Now())
	if !ok {
		return errors.New("waiting_source")
	}
	index, _, ok := cache.Index(root.RecordingID, math.MaxUint64)
	if !ok {
		return errors.New("binding_changed")
	}
	root.Metadata, root.CacheBounds = metadata, index.CacheBounds
	body, err := PrepareReceptionHistoryRoot(root)
	if err != nil {
		return err
	}
	stale := body
	if root.State == "fresh" {
		root.State = "stale"
		stale, err = PrepareReceptionHistoryRoot(root)
		root.State = "fresh"
		if err != nil {
			return err
		}
	}
	recovered := false
	err = r.commit(ctx, g, func() error {
		if r.last.RecordingID != root.RecordingID {
			return errors.New("binding_changed")
		}
		if _, ok := cache.MetadataManifest(root.RecordingID, metadata.Digest, time.Now()); !ok {
			return errors.New("metadata_expired")
		}
		r.publication = receptionReaderPublication{root: root, body: body, stale: stale, until: until, epoch: g.epoch}
		if healthy {
			recovered = r.failureLogged
			r.failure, r.failureLogged = "", false
		}
		return nil
	})
	if recovered {
		slog.Info("dashboard reception collection recovered", "component", "dashboard", "operation", "reception_collect", "entrypoint", "public_reader", "reason", "capture_accepted", "run_id", g.run)
	}
	return err
}

// rootBody is a cache-only selection for the later HTTP slice. No route is
// registered yet. Missing metadata removes the advertisement, not history.
func (r *receptionHistoryReader) rootBody(now time.Time) (string, time.Time, string) {
	r.gate.mu.RLock()
	defer r.gate.mu.RUnlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	p := r.publication
	if p.body == "" {
		return "", time.Time{}, "waiting_source"
	}
	if p.epoch != r.gate.value.epoch || !r.gate.value.matches(p.root.Binding) {
		return "", time.Time{}, "binding_changed"
	}
	if _, ok := r.cache.MetadataManifest(p.root.RecordingID, p.root.Metadata.Digest, now); !ok {
		return "", time.Time{}, "metadata_expired"
	}
	if p.root.State != "fresh" {
		return p.body, time.Time{}, ""
	}
	if r.failure != "" || p.epoch != r.gate.value.epoch || !r.gate.value.fresh(now) || !now.Before(p.until) {
		return p.stale, time.Time{}, ""
	}
	return p.body, p.until, ""
}
