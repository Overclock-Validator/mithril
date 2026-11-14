package dashboard

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

type historyStatus struct {
	SchemaVersion    int             `json:"schema_version"`
	State            string          `json:"state"`
	Reason           string          `json:"reason,omitempty"`
	ComputedAt       time.Time       `json:"computed_at"`
	Binding          *historyBinding `json:"binding"`
	BindingScope     string          `json:"binding_scope"`
	First            *time.Time      `json:"first_observation"`
	ObservedThrough  *time.Time      `json:"observed_through"`
	DurableThrough   *time.Time      `json:"durable_through"`
	Dropped          uint64          `json:"dropped_this_reader,string"`
	RecoveredTail    bool            `json:"recovered_tail"`
	SampleSeconds    int             `json:"sample_seconds"`
	RetentionSeconds int64           `json:"retention_seconds"`
	Metrics          []historyMetric `json:"metrics"`
	Ranges           []historyRange  `json:"ranges"`
}

type historyResponse struct {
	historyStatus
	Metric historyMetric   `json:"metric"`
	Range  historyRange    `json:"range"`
	Points []historyBucket `json:"points"`
}

type historyCache struct {
	sourceBinding *historyBinding
	health        []byte
	queries       map[string][]byte
}

type historyService struct {
	store          *historyStore // only run accesses it after startup
	resources      bool
	queue          chan historyRecord
	done           chan struct{}
	current        atomic.Pointer[historyBinding]
	cache          atomic.Pointer[historyCache]
	dropped        atomic.Uint64
	observed       atomic.Int64
	clockRegressed atomic.Bool
	// The collector alone owns these sampling guards.
	lastSeen    time.Time
	lastOffered int64
}

func newHistoryService(path string, resources bool) (*historyService, error) {
	store, err := openHistoryStore(path)
	if err != nil {
		return nil, err
	}
	h := &historyService{store: store, resources: resources, queue: make(chan historyRecord, 1), done: make(chan struct{})}
	h.publish(time.Now().UTC(), "loading", "")
	return h, nil
}

// offer is called only by the collector. It copies fixed public scalars and
// never waits for storage, history queries, or a slow history worker.
func (h *historyService) offer(publicJSON []byte, now time.Time) {
	var p PublicSnapshot
	if len(publicJSON) > MaxSnapshotBytes || json.Unmarshal(publicJSON, &p) != nil {
		return
	}
	r, ok := historyPublicRecord(p, now, h.resources)
	if ok && p.SourceState == "fresh" {
		current := h.current.Load()
		if current == nil || *current != r.Binding {
			binding := r.Binding
			h.current.Store(&binding)
		}
	}
	if now.Before(h.lastSeen) {
		h.clockRegressed.Store(true)
		return
	}
	h.lastSeen = now
	h.clockRegressed.Store(false)
	if !ok || r.At <= h.lastOffered {
		return
	}
	h.lastOffered = r.At
	h.observed.Store(r.ObservedAt)
	select {
	case h.queue <- r:
	default:
		if h.dropped.Load() != math.MaxUint64 {
			h.dropped.Add(1)
		}
	}
}

func (h *historyService) run(ctx context.Context) {
	defer close(h.done)
	defer func() {
		if err := h.store.close(); err != nil {
			h.logFailure("close", "close_failed")
		}
	}()
	failure := ""
	if err := h.store.load(time.Now().UTC()); err != nil && !errors.Is(err, errHistoryClock) {
		failure = historyFailure(err)
		h.logFailure("load", failure)
	}
	if h.store.recoveredTail {
		slog.Info("dashboard history recovered a torn final record", "component", "dashboard", "operation", "history_load")
	}
	appendRecord := func(r historyRecord, now time.Time) {
		if failure != "" || h.clockRegressed.Load() {
			return
		}
		if _, err := h.store.append(r, now); err != nil && !errors.Is(err, errHistoryClock) {
			failure = historyFailure(err)
			h.logFailure("append", failure)
		}
	}
	var previousBinding *historyBinding
	var minute int64
	var dropped uint64
	var paused bool
	update := func(now time.Time, force bool) {
		current := h.current.Load()
		clock := h.clockRegressed.Load() || now.Unix() < h.store.lastAt
		changedBinding := current != previousBinding
		if !force && now.Unix()/60 == minute && dropped == h.dropped.Load() && clock == paused && !changedBinding {
			return
		}
		if !clock && failure == "" {
			if err := h.store.prune(now); err != nil {
				failure = historyFailure(err)
				h.logFailure("retention", failure)
			}
		}
		state, reason := "recording", ""
		switch {
		case failure != "":
			state, reason = "degraded", failure
		case clock:
			state, reason = "paused", "clock_regressed"
		case current == nil:
			state = "waiting_source"
		}
		h.publish(now, state, reason)
		previousBinding, minute, dropped, paused = current, now.Unix()/60, h.dropped.Load(), clock
	}
	update(time.Now().UTC(), true)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			select {
			case r := <-h.queue:
				appendRecord(r, time.Now().UTC())
			default:
			}
			return
		case r := <-h.queue:
			now := time.Now().UTC()
			appendRecord(r, now)
			update(now, true)
		case now := <-ticker.C:
			update(now.UTC(), false)
		}
	}
}

func (h *historyService) publish(now time.Time, state, reason string) {
	sourceBinding := h.current.Load()
	status := historyStatus{SchemaVersion: 1, State: state, Reason: reason, ComputedAt: now.UTC(), Binding: sourceBinding,
		SampleSeconds: 60, RetentionSeconds: int64(historyRetention / time.Second), Dropped: h.dropped.Load(), RecoveredTail: h.store.recoveredTail,
		Ranges: append([]historyRange(nil), historyRanges[:]...)}
	status.BindingScope = "unavailable"
	if sourceBinding != nil {
		status.BindingScope = "observed_source"
	} else if len(h.store.records) > 0 {
		binding := h.store.records[len(h.store.records)-1].Binding
		status.Binding, status.BindingScope = &binding, "last_recorded"
	}
	for _, metric := range historyMetrics {
		if !metric.resources || h.resources {
			status.Metrics = append(status.Metrics, metric)
		}
	}
	if observed := h.observed.Load(); observed > 0 {
		at := time.UnixMilli(observed).UTC()
		status.ObservedThrough = &at
	}
	if status.Binding != nil {
		for _, record := range h.store.records {
			if status.Binding.matches(record.Binding) {
				at := time.UnixMilli(record.ObservedAt).UTC()
				if status.First == nil {
					status.First = &at
				}
				status.DurableThrough = &at
			}
		}
	}
	cache := &historyCache{sourceBinding: sourceBinding, queries: make(map[string][]byte)}
	cache.health, _ = json.Marshal(status)
	if status.Binding != nil {
		for metric, definition := range historyMetrics {
			if definition.resources && !h.resources {
				continue
			}
			for _, span := range historyRanges {
				response := historyResponse{historyStatus: status, Metric: definition, Range: span,
					Points: historyBuckets(h.store.records, *status.Binding, metric, span, now)}
				b, err := json.Marshal(response)
				if err != nil || len(b) > MaxSnapshotBytes {
					status.State, status.Reason = "degraded", "history_response_limit"
					cache.health, _ = json.Marshal(status)
					clear(cache.queries)
					h.cache.Store(cache)
					return
				}
				cache.queries[definition.Name+"/"+span.Name] = b
			}
		}
	}
	h.cache.Store(cache)
}

func (h *historyService) handler(allowed func(*http.Request) bool) http.HandlerFunc {
	queries := make(chan struct{}, 4)
	limit := rate.NewLimiter(8, 16)
	return func(w http.ResponseWriter, r *http.Request) {
		if !allowed(r) {
			http.Error(w, "host or origin not allowed", http.StatusForbidden)
			return
		}
		if r.Method != http.MethodGet || r.ContentLength != 0 || len(r.TransferEncoding) != 0 || len(r.URL.RawQuery) > 256 || r.URL.ForceQuery && r.URL.RawQuery == "" {
			http.Error(w, "unsupported request", http.StatusBadRequest)
			return
		}
		key := ""
		if r.URL.RawQuery != "" {
			q, err := url.ParseQuery(r.URL.RawQuery)
			if err != nil || len(q) != 2 || len(q["metric"]) != 1 || len(q["range"]) != 1 {
				http.Error(w, "unsupported history query", http.StatusBadRequest)
				return
			}
			metricOK, rangeOK := false, false
			for _, metric := range historyMetrics {
				if q.Get("metric") == metric.Name && (!metric.resources || h.resources) {
					metricOK = true
				}
			}
			for _, span := range historyRanges {
				if q.Get("range") == span.Name {
					rangeOK = true
				}
			}
			if !metricOK || !rangeOK {
				http.Error(w, "unsupported history query", http.StatusBadRequest)
				return
			}
			key = q.Get("metric") + "/" + q.Get("range")
		}
		if !limit.Allow() {
			http.Error(w, "history query rate exceeded", http.StatusTooManyRequests)
			return
		}
		select {
		case queries <- struct{}{}:
			defer func() { <-queries }()
		default:
			http.Error(w, "history viewer limit reached", http.StatusServiceUnavailable)
			return
		}
		cache, binding := h.cache.Load(), h.current.Load()
		if cache == nil || cache.sourceBinding != binding {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"schema_version":1,"state":"updating","reason":"binding_changed"}`))
			return
		}
		b := cache.health
		if key != "" {
			b = cache.queries[key]
		}
		if b == nil {
			http.Error(w, "history not available", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(b)
	}
}

func (h *historyService) logFailure(operation, reason string) {
	attrs := []any{"component", "dashboard", "operation", "history_" + operation, "reason", reason}
	if binding := h.current.Load(); binding != nil {
		attrs = append(attrs, "run_id", binding.Run)
	}
	slog.Warn("dashboard history stopped recording", attrs...)
}
