package dashboard

import (
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	replayDetailPageSize    = 2
	replayDetailGenerations = 3
	replayDetailFamilyBytes = 4 << 20
	replayDetailFreshFor    = 30 * time.Second
	replayDetailHoldFor     = 30 * time.Second
	replayDetailMinimum     = 13 * time.Second
)

// ReplayDetailManifest is the concrete private manifest. Reader-derived gaps
// belong to a separate public DTO, not to this producer observation.
type ReplayDetailManifest struct {
	SchemaVersion  int                   `json:"schema_version"`
	Family         string                `json:"family"`
	Scope          string                `json:"scope"`
	State          string                `json:"state"`
	Reason         string                `json:"reason"`
	CollectorAt    time.Time             `json:"collector_at"`
	Binding        ReplayDetailBinding   `json:"binding"`
	Generation     string                `json:"generation"`
	CapturedAt     time.Time             `json:"captured_at"`
	RetainUntil    time.Time             `json:"retain_until"`
	UpdatedAt      time.Time             `json:"updated_at"`
	Capacity       int                   `json:"capacity"`
	Count          int                   `json:"count"`
	PageSize       int                   `json:"page_size"`
	PageCount      int                   `json:"page_count"`
	FirstSequence  uint64                `json:"first_sequence,string"`
	LastSequence   uint64                `json:"last_sequence,string"`
	Evicted        uint64                `json:"evicted,string"`
	Exhausted      bool                  `json:"exhausted"`
	ScheduleWindow *HeldScheduleWindow   `json:"schedule_window,omitempty"`
	FoldCapacity   *ReplayFoldCapacity   `json:"fold_capacity,omitempty"`
	NearReception  *ReplayNearReception  `json:"near_reception,omitempty"`
	LatestSelected *ReplayLatestSelected `json:"latest_selected,omitempty"`
	RecentSelected *ReplayRecentSelected `json:"recent_selected,omitempty"`
}

// ReplayDetailPage contains complete typed rows belonging to one manifest.
// It makes no independent freshness, finality or selected-fork claim.
type ReplayDetailPage struct {
	SchemaVersion int                  `json:"schema_version"`
	Family        string               `json:"family"`
	Generation    string               `json:"generation"`
	Binding       ReplayDetailBinding  `json:"binding"`
	Page          int                  `json:"page"`
	Offset        int                  `json:"offset"`
	NextPage      *int                 `json:"next_page"`
	Records       []ReplayDetailRecord `json:"records"`
}

// replayDetailGate copies only context supplied by the existing status source.
// ObservedAt retains its monotonic clock. No source reset generation is invented
// here; the full source binding remains part of each prepared publication.
type replayDetailGate struct {
	RunID            string
	NodeStartedAt    time.Time
	GenesisHash      string
	HasGenesis       bool
	Attempt          uint64
	Rewinds          uint64
	AttemptStartedAt time.Time
	ObservedAt       time.Time
	State            string
	Reason           string
	stopping         bool   // selected-window lifecycle fence; not serialized
	revision         uint64 // reader-local context fence; not serialized
	retired          bool
	stamp            statusStamp // public-reader status binding; not serialized
}

// replayDetailPrepared owns immutable encoded strings. It retains neither the
// caller's rows nor its genesis pointer. Capture time keeps its monotonic clock.
type replayDetailPrepared struct {
	binding      ReplayDetailBinding
	generation   string
	capturedAt   time.Time
	lastSequence uint64
	manifests    [4]string
	pages        []string
	bytes        int
}

type replayDetailPublication struct {
	prepared    *replayDetailPrepared
	publishedAt time.Time
	holdUntil   time.Time
	stamp       statusStamp
}

// replayDetailCache retains at most three same-context immutable generations.
// It starts no workers and performs no owner reads. The future feed worker must
// preflight before capture/encoding and pass a newly loaded gate at commit.
type replayDetailCache struct {
	// ponytail: one lock and a three-entry scan; change only if measured cache
	// contention matters. Encoding and response writes never hold this lock.
	mu    sync.RWMutex
	items [replayDetailGenerations]*replayDetailPublication
}

type replayDetailReply struct {
	status                int
	body                  string
	remainingMilliseconds int64
	failure               *replayDetailCaptureFailure
	publication           *replayDetailPublication
}

func prepareReplayDetail(core ReplayDetailCore, capturedAt time.Time) (*replayDetailPrepared, error) {
	if !capturedAt.Equal(core.CapturedAt) {
		return nil, errors.New("source_time_invalid")
	}
	generation, err := ReplayDetailGeneration(core)
	if err != nil {
		return nil, errors.New("source_invalid")
	}
	binding := core.Binding
	if binding.GenesisHash != nil {
		value := *binding.GenesisHash
		binding.GenesisHash = &value
	}
	p := &replayDetailPrepared{binding: binding, generation: generation, capturedAt: capturedAt, lastSequence: core.LastSequence}
	pageCount := (core.Count + replayDetailPageSize - 1) / replayDetailPageSize
	p.pages = make([]string, pageCount)
	for page := range pageCount {
		offset := page * replayDetailPageSize
		var next *int
		if page+1 < pageCount {
			value := page + 1
			next = &value
		}
		body, err := json.Marshal(ReplayDetailPage{
			SchemaVersion: core.SchemaVersion, Family: "replay", Generation: generation, Binding: binding,
			Page: page, Offset: offset, NextPage: next,
			Records: core.Records[offset:min(offset+replayDetailPageSize, core.Count)],
		})
		if err != nil || len(body) > MaxSnapshotBytes {
			return nil, errors.New("response_limit")
		}
		p.pages[page], p.bytes = string(body), p.bytes+len(body)
	}
	m := ReplayDetailManifest{
		SchemaVersion: core.SchemaVersion, Family: "replay", Scope: "replay_row_history", State: "fresh",
		CollectorAt: core.CapturedAt, Binding: binding, Generation: generation,
		CapturedAt: core.CapturedAt, UpdatedAt: core.UpdatedAt,
		// This advisory wall date is conservative. The internal hold starts only
		// after all encoding finishes, at publish's monotonic commit time.
		RetainUntil: capturedAt.Add(replayDetailHoldFor).UTC(),
		Capacity:    core.Capacity, Count: core.Count, PageSize: replayDetailPageSize, PageCount: pageCount,
		FirstSequence: core.FirstSequence, LastSequence: core.LastSequence, Evicted: core.Evicted, Exhausted: core.Exhausted,
		ScheduleWindow: core.ScheduleWindow,
		FoldCapacity:   core.FoldCapacity,
		NearReception:  core.NearReception,
		LatestSelected: core.LatestSelected,
		RecentSelected: core.RecentSelected,
	}
	for i, reason := range [...]string{"", "status_stale", "source_unavailable", "source_invalid"} {
		m.Reason = reason
		if i != 0 {
			m.State = "stale"
		}
		body, err := json.Marshal(m)
		if err != nil || len(body) > MaxSnapshotBytes {
			return nil, errors.New("response_limit")
		}
		p.manifests[i], p.bytes = string(body), p.bytes+len(body)
	}
	if p.bytes > replayDetailFamilyBytes {
		return nil, errors.New("response_limit")
	}
	return p, nil
}

func (c *replayDetailCache) canPublish(gate replayDetailGate, now time.Time) bool {
	if c == nil || gate.failure(now) != "" || gate.State != "fresh" {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.prune(gate, now)
	return c.items[len(c.items)-1] == nil
}

// publish installs already encoded bytes. Its now must be sampled after prepare
// finishes; retrying an existing token does not replace either original clock.
func (c *replayDetailCache) publish(p *replayDetailPrepared, gate replayDetailGate, now time.Time) error {
	if c == nil || p == nil {
		return errors.New("source_invalid")
	}
	if reason := gate.failure(now); reason != "" {
		return errors.New(reason)
	}
	if gate.State != "fresh" {
		return errors.New("status_stale")
	}
	if !gate.matches(p.binding) {
		return errors.New("binding_changed")
	}
	if now.Before(p.capturedAt) {
		return errors.New("source_time_invalid")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.prune(gate, now)
	var latest *replayDetailPublication
	for i, item := range c.items {
		if item == nil {
			break
		}
		if item.prepared.generation == p.generation {
			if item.stamp != gate.stamp || p.capturedAt.Before(item.prepared.capturedAt) {
				updated := *item
				updated.stamp = gate.stamp
				if p.capturedAt.Before(item.prepared.capturedAt) {
					prepared := *item.prepared
					prepared.capturedAt = p.capturedAt
					updated.prepared = &prepared
				}
				c.items[i] = &updated
			}
			return nil
		}
		latest = item
	}
	if p.capturedAt.Add(replayDetailFreshFor).Sub(now) < replayDetailMinimum {
		return errors.New("generation_not_ready")
	}
	if latest != nil {
		old := latest.prepared
		if p.capturedAt.Before(old.capturedAt) || p.binding.SourceGeneration < old.binding.SourceGeneration {
			return errors.New("source_invalid")
		}
		if !sameReplayDetailBinding(old.binding, p.binding) {
			if p.binding.SourceGeneration == old.binding.SourceGeneration {
				return errors.New("source_invalid")
			}
			c.items = [replayDetailGenerations]*replayDetailPublication{}
		} else if p.lastSequence < old.lastSequence {
			return errors.New("source_invalid")
		}
	}
	for i := range c.items {
		if c.items[i] == nil {
			owned := *p
			owned.pages = slices.Clone(p.pages)
			if p.binding.GenesisHash != nil {
				value := *p.binding.GenesisHash
				owned.binding.GenesisHash = &value
			}
			c.items[i] = &replayDetailPublication{prepared: &owned, publishedAt: now, holdUntil: now.Add(replayDetailHoldFor), stamp: gate.stamp}
			return nil
		}
	}
	return errors.New("generation_not_ready")
}

// prune runs only in the background publication path, never during lookup.
func (c *replayDetailCache) prune(gate replayDetailGate, now time.Time) {
	var retained [replayDetailGenerations]*replayDetailPublication
	n := 0
	for _, item := range c.items {
		if item != nil && now.Before(item.holdUntil) && gate.matches(item.prepared.binding) {
			retained[n] = item
			n++
		}
	}
	c.items = retained
}

func (c *replayDetailCache) lookup(rawQuery string, forceQuery bool, gate replayDetailGate, now time.Time) replayDetailReply {
	generation, page, pageRequest, ok := parseReplayDetailQuery(rawQuery, forceQuery)
	if !ok {
		return replayDetailFailure("invalid_request")
	}
	if reason := gate.failure(now); reason != "" {
		return replayDetailFailure(reason)
	}
	if c == nil {
		return replayDetailFailure("capture_disabled")
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	var selected *replayDetailPublication
	for _, item := range c.items {
		if item != nil && (!pageRequest || item.prepared.generation == generation) {
			selected = item
		}
	}
	if selected == nil {
		if pageRequest {
			return replayDetailFailure("source_generation_expired")
		}
		return replayDetailFailure("waiting_source")
	}
	if !gate.matches(selected.prepared.binding) || selected.stamp != gate.stamp {
		return replayDetailFailure("binding_changed")
	}
	if now.Before(selected.publishedAt) || now.Before(selected.prepared.capturedAt) {
		return replayDetailFailure("source_time_invalid")
	}
	if !now.Before(selected.holdUntil) {
		if pageRequest {
			return replayDetailFailure("source_generation_expired")
		}
		return replayDetailFailure("generation_not_ready")
	}
	if pageRequest {
		if page >= len(selected.prepared.pages) {
			return replayDetailFailure("invalid_request")
		}
		return replayDetailReply{status: http.StatusOK, body: selected.prepared.pages[page], publication: selected}
	}
	remaining := min(selected.prepared.capturedAt.Add(replayDetailFreshFor).Sub(now), selected.holdUntil.Sub(now))
	if remaining < replayDetailMinimum {
		return replayDetailFailure("generation_not_ready")
	}
	variant := 0
	if gate.State == "stale" {
		switch gate.Reason {
		case "source_unavailable":
			variant = 2
		case "source_invalid":
			variant = 3
		default:
			variant = 1
		}
	}
	return replayDetailReply{status: http.StatusOK, body: selected.prepared.manifests[variant], remainingMilliseconds: remaining.Milliseconds(), publication: selected}
}

func (g replayDetailGate) matches(b ReplayDetailBinding) bool {
	return g.RunID == b.RunID && g.NodeStartedAt.Equal(b.NodeStartedAt) &&
		g.HasGenesis == (b.GenesisHash != nil) && (!g.HasGenesis || g.GenesisHash == *b.GenesisHash) &&
		g.Attempt == b.Attempt && g.Rewinds == b.Rewinds && g.AttemptStartedAt.Equal(b.AttemptStartedAt)
}

func (g replayDetailGate) failure(now time.Time) string {
	if g.State == "unavailable" {
		switch g.Reason {
		case "capture_disabled", "unsupported_source", "waiting_source", "source_unavailable", "source_invalid", "source_time_invalid":
			return g.Reason
		default:
			return "source_unavailable"
		}
	}
	if g.State != "fresh" && g.State != "stale" || g.RunID == "" || len(g.RunID) > 128 || !utf8.ValidString(g.RunID) ||
		!validReplayDetailTime(g.NodeStartedAt) || !validReplayDetailTime(g.AttemptStartedAt) || g.Attempt == 0 ||
		g.HasGenesis && !validReplayDetailHash(g.GenesisHash, false) || !g.HasGenesis && g.GenesisHash != "" ||
		g.ObservedAt.IsZero() || now.IsZero() {
		return "source_invalid"
	}
	if now.Before(g.ObservedAt) {
		return "source_time_invalid"
	}
	if now.Sub(g.ObservedAt) > StaleAfter {
		return "status_stale"
	}
	if g.State == "fresh" && g.Reason != "" {
		return "source_invalid"
	}
	if g.State == "stale" && g.Reason != "" && g.Reason != "status_stale" && g.Reason != "source_unavailable" && g.Reason != "source_invalid" {
		return "source_invalid"
	}
	return ""
}

func sameReplayDetailBinding(a, b ReplayDetailBinding) bool {
	return a.RunID == b.RunID && a.NodeStartedAt.Equal(b.NodeStartedAt) &&
		(a.GenesisHash == nil) == (b.GenesisHash == nil) && (a.GenesisHash == nil || *a.GenesisHash == *b.GenesisHash) &&
		a.SourceGeneration == b.SourceGeneration && a.Attempt == b.Attempt && a.Rewinds == b.Rewinds &&
		a.AttemptStartedAt.Equal(b.AttemptStartedAt) && a.ResetAt.Equal(b.ResetAt)
}

func parseReplayDetailQuery(raw string, force bool) (generation string, page int, pageRequest, ok bool) {
	return parseDetailQuery(raw, force, ReplayDetailCapacity/replayDetailPageSize)
}

// replayDetailFailure returns only static, bounded bodies. Parser errors and
// caller-provided text never enter a response, and lookup performs no encoding.
func replayDetailFailure(reason string) replayDetailReply {
	switch reason {
	case "invalid_request":
		return replayDetailReply{status: 400, body: `{"schema_version":1,"family":"replay","state":"unavailable","reason":"invalid_request"}`}
	case "capture_disabled":
		return replayDetailReply{status: 404, body: `{"schema_version":1,"family":"replay","state":"unavailable","reason":"capture_disabled"}`}
	case "unsupported_source":
		return replayDetailReply{status: 404, body: `{"schema_version":1,"family":"replay","state":"unavailable","reason":"unsupported_source"}`}
	case "binding_changed":
		return replayDetailReply{status: 409, body: `{"schema_version":1,"family":"replay","state":"unavailable","reason":"binding_changed"}`}
	case "source_generation_expired":
		return replayDetailReply{status: 409, body: `{"schema_version":1,"family":"replay","state":"unavailable","reason":"source_generation_expired"}`}
	case "waiting_source":
		return replayDetailReply{status: 503, body: `{"schema_version":1,"family":"replay","state":"unavailable","reason":"waiting_source"}`}
	case "generation_not_ready":
		return replayDetailReply{status: 503, body: `{"schema_version":1,"family":"replay","state":"unavailable","reason":"generation_not_ready"}`}
	case "source_time_invalid":
		return replayDetailReply{status: 503, body: `{"schema_version":1,"family":"replay","state":"unavailable","reason":"source_time_invalid"}`}
	case "status_stale":
		return replayDetailReply{status: 503, body: `{"schema_version":1,"family":"replay","state":"unavailable","reason":"status_stale"}`}
	case "source_unavailable":
		return replayDetailReply{status: 503, body: `{"schema_version":1,"family":"replay","state":"unavailable","reason":"source_unavailable"}`}
	default:
		return replayDetailReply{status: 503, body: `{"schema_version":1,"family":"replay","state":"unavailable","reason":"source_invalid"}`}
	}
}
