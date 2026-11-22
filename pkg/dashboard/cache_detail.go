package dashboard

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

// MaxCacheDetailBytes bounds the complete AccountsDB cache-gauge document.
const MaxCacheDetailBytes = 4096

// CacheDetailFreshFor is the maximum age of a cache-owner observation.
const CacheDetailFreshFor = 30 * time.Second

// CacheDetailBinding identifies a node run and its process-local cache generation.
type CacheDetailBinding struct {
	RunID           string    `json:"run_id"`
	NodeStartedAt   time.Time `json:"node_started_at"`
	GenesisHash     *string   `json:"genesis_hash"`
	CacheGeneration uint64    `json:"cache_generation,string"`
}

// CacheDetail contains configured capacities and approximate entry counts.
// Fixed-tier occupied counts are optional, approximate across shards and never
// reported partially. They do not measure weighted occupancy or cache activity.
type CacheDetail struct {
	SchemaVersion int                  `json:"schema_version"`
	Family        string               `json:"family"`
	Scope         string               `json:"scope"`
	State         string               `json:"state"`
	Reason        string               `json:"reason"`
	Binding       CacheDetailBinding   `json:"binding"`
	CapturedAt    time.Time            `json:"captured_at"`
	Vote          CacheVoteDetail      `json:"vote"`
	Program       CacheProgramDetail   `json:"program"`
	Common        CacheCommonDetail    `json:"common"`
	Activity      *CacheActivityDetail `json:"activity,omitempty"`
}

// CacheActivityDetail carries optional lifetime counts, not per-row work or a
// cache-health verdict. Instance survives Clear and changes on reconstruction.
type CacheActivityDetail struct {
	Instance         uint64                    `json:"instance,string"`
	Vote             CacheLookupDetail         `json:"vote"`
	Program          CacheLookupDetail         `json:"program"`
	CommonLarge      CacheLookupDetail         `json:"common_large"`
	Fixed            *CacheFixedActivityDetail `json:"fixed"`
	Window           *CacheActivityWindow      `json:"window,omitempty"`
	InsertionWeights *CacheInsertionWeights    `json:"insertion_weights,omitempty"`
	ProgramSets      *CacheProgramSets         `json:"program_sets,omitempty"`
	ProgramRefusals  *CacheProgramSets         `json:"program_refusals,omitempty"`
}

// CacheProgramSets counts scoped program-cache Set calls, including updates.
// Accepted and refused calls are not unique programs or current residency;
// refusals are not FD spills. Saturation is unknown, not zero.
// The count shares the enclosing activity instance and cache capture lifetime.
type CacheProgramSets struct {
	Scope  string  `json:"scope"`
	Count  *uint64 `json:"count,string"`
	Reason string  `json:"reason"`
}

// CacheInsertionWeights reports approximate stored insertion weights in each
// cache's native units. A counter fault is unknown, never an inferred zero.
// These weights do not measure eviction-policy occupancy or process memory.
type CacheInsertionWeights struct {
	Scope                     string  `json:"scope"`
	ProgramCostUnits          *uint64 `json:"program_cost_units,string"`
	ProgramReason             string  `json:"program_reason"`
	CommonLargeAdmissionBytes *uint64 `json:"common_large_admission_bytes,string"`
	CommonLargeReason         string  `json:"common_large_reason"`
}

// CacheActivityWindow holds raw observed lookup deltas, not an exact rolling
// minute or hit-ratio verdict. Fixed tiers are not combined with large lookups.
type CacheActivityWindow struct {
	StartedAt           time.Time        `json:"started_at"`
	EndedAt             time.Time        `json:"ended_at"`
	DurationNanoseconds uint64           `json:"duration_nanoseconds,string"`
	Vote                CacheLookupDelta `json:"vote"`
	Program             CacheLookupDelta `json:"program"`
	CommonLarge         CacheLookupDelta `json:"common_large"`
}

type CacheLookupDelta struct {
	Hits   uint64 `json:"hits,string"`
	Misses uint64 `json:"misses,string"`
}

// CacheLookupDetail reports approximate independent Otter counters. Eviction
// weight uses the corresponding cache's native units, not necessarily bytes.
type CacheLookupDetail struct {
	Hits           uint64 `json:"hits,string"`
	Misses         uint64 `json:"misses,string"`
	Evictions      uint64 `json:"evictions,string"`
	EvictionWeight uint64 `json:"eviction_weight,string"`
}

// CacheFixedActivityDetail distinguishes fixed-tier hits from large fallback
// lookups. Replacements exclude refresh, manual deletion and Clear.
type CacheFixedActivityDetail struct {
	EmptyHits         uint64 `json:"empty_hits,string"`
	SmallHits         uint64 `json:"small_hits,string"`
	EmptyReplacements uint64 `json:"empty_replacements,string"`
	SmallReplacements uint64 `json:"small_replacements,string"`
}

// CacheVoteDetail keeps entry capacity separate from an approximate entry count.
type CacheVoteDetail struct {
	CapacityEntries  uint64 `json:"capacity_entries,string"`
	EstimatedEntries uint64 `json:"estimated_entries,string"`
}

// CacheProgramDetail distinguishes configured cost units from entry counts.
type CacheProgramDetail struct {
	CapacityCostUnits uint64 `json:"capacity_cost_units,string"`
	CostUnitBytes     uint64 `json:"cost_unit_bytes,string"`
	EstimatedEntries  uint64 `json:"estimated_entries,string"`
}

// CacheCommonDetail distinguishes provisioned slots, occupied slots and byte
// weights. None measures total used or free memory.
type CacheCommonDetail struct {
	Layout                    string  `json:"layout"`
	ConfiguredBudgetBytes     uint64  `json:"configured_budget_bytes,string"`
	FixedEmptyCapacityEntries uint64  `json:"fixed_empty_capacity_entries,string"`
	FixedSmallCapacityEntries uint64  `json:"fixed_small_capacity_entries,string"`
	LargeCapacityBytes        uint64  `json:"large_capacity_bytes,string"`
	LargeEstimatedEntries     uint64  `json:"large_estimated_entries,string"`
	FixedEmptyOccupiedEntries *uint64 `json:"fixed_empty_occupied_entries,string,omitempty"`
	FixedSmallOccupiedEntries *uint64 `json:"fixed_small_occupied_entries,string,omitempty"`
}

// CacheDetailCapture retains the original monotonic owner deadline.
// FreshUntil is not serialized or reconstructed from CapturedAt.
type CacheDetailCapture struct {
	Detail     CacheDetail
	FreshUntil time.Time
}

// CacheDetailSampler returns a detached cached value without sampling occupancy
// or doing I/O. It runs after accepted Feed status samples, never from HTTP.
type CacheDetailSampler func(now time.Time) (CacheDetailCapture, error)

func validateCacheDetail(d CacheDetail) error {
	b, c := d.Binding, d.Common
	if a := d.Activity; a != nil {
		if a.Instance == 0 || a.Instance == ^uint64(0) || a.Instance > b.CacheGeneration ||
			c.Layout == "flat" && a.Fixed != nil && *a.Fixed != (CacheFixedActivityDetail{}) {
			return errors.New("source_invalid")
		}
		if w := a.InsertionWeights; w != nil {
			validWeight := func(value *uint64, reason string) bool {
				return value != nil && reason == "" || value == nil && reason == "counter_fault"
			}
			if w.Scope != "approximate_stored_insertion_weight" || !validWeight(w.ProgramCostUnits, w.ProgramReason) ||
				!validWeight(w.CommonLargeAdmissionBytes, w.CommonLargeReason) {
				return errors.New("source_invalid")
			}
		}
		if p := a.ProgramSets; p != nil && (p.Scope != "accepted_program_cache_set_calls" ||
			p.Count != nil && (p.Reason != "" || *p.Count == ^uint64(0)) ||
			p.Count == nil && p.Reason != "counter_saturated") {
			return errors.New("source_invalid")
		}
		if p := a.ProgramRefusals; p != nil && (p.Scope != "oversized_program_cache_set_refusals" ||
			p.Count != nil && (p.Reason != "" || *p.Count == ^uint64(0)) ||
			p.Count == nil && p.Reason != "counter_saturated") {
			return errors.New("source_invalid")
		}
		if w := a.Window; w != nil {
			if !validReplayDetailTime(w.StartedAt) || !validReplayDetailTime(w.EndedAt) || !w.EndedAt.Equal(d.CapturedAt) ||
				w.DurationNanoseconds == 0 || w.DurationNanoseconds > uint64(time.Minute) || w.StartedAt.Before(b.NodeStartedAt) ||
				w.Vote.Hits > a.Vote.Hits || w.Vote.Misses > a.Vote.Misses || w.Program.Hits > a.Program.Hits || w.Program.Misses > a.Program.Misses ||
				w.CommonLarge.Hits > a.CommonLarge.Hits || w.CommonLarge.Misses > a.CommonLarge.Misses {
				return errors.New("source_invalid")
			}
		}
	}
	if d.SchemaVersion != 1 || d.Family != "accounts_cache" || d.Scope != "accountsdb_read_cache_gauges" || d.State != "fresh" || d.Reason != "" ||
		!utf8.ValidString(b.RunID) || b.RunID == "" || len(b.RunID) > 128 || b.CacheGeneration == 0 || b.CacheGeneration == ^uint64(0) ||
		!validReplayDetailTime(b.NodeStartedAt) || !validReplayDetailTime(d.CapturedAt) || d.CapturedAt.Before(b.NodeStartedAt) ||
		b.GenesisHash != nil && !validReplayDetailHash(*b.GenesisHash, false) ||
		d.Vote.CapacityEntries == 0 || d.Program.CapacityCostUnits == 0 || d.Program.CostUnitBytes != 1<<20 ||
		c.ConfiguredBudgetBytes == 0 || c.LargeCapacityBytes == 0 {
		return errors.New("source_invalid")
	}
	switch c.Layout {
	case "flat":
		if c.FixedEmptyCapacityEntries != 0 || c.FixedSmallCapacityEntries != 0 || c.LargeCapacityBytes != c.ConfiguredBudgetBytes {
			return errors.New("source_invalid")
		}
	case "fixed":
		if c.FixedEmptyCapacityEntries == 0 || c.FixedSmallCapacityEntries == 0 || c.LargeCapacityBytes >= c.ConfiguredBudgetBytes {
			return errors.New("source_invalid")
		}
	default:
		return errors.New("source_invalid")
	}
	if (c.FixedEmptyOccupiedEntries == nil) != (c.FixedSmallOccupiedEntries == nil) ||
		c.FixedEmptyOccupiedEntries != nil && (*c.FixedEmptyOccupiedEntries > c.FixedEmptyCapacityEntries || *c.FixedSmallOccupiedEntries > c.FixedSmallCapacityEntries) {
		return errors.New("source_invalid")
	}
	// Concurrent admission and eviction can make estimated counts exceed capacity.
	return nil
}

// decodeCacheDetail accepts only this producer's canonical typed JSON.
func decodeCacheDetail(body []byte) (CacheDetail, error) {
	var d CacheDetail
	if len(body) > MaxCacheDetailBytes || json.Unmarshal(body, &d) != nil || validateCacheDetail(d) != nil {
		return CacheDetail{}, errors.New("source_invalid")
	}
	canonical, err := json.Marshal(d)
	if err != nil || !bytes.Equal(body, canonical) {
		return CacheDetail{}, errors.New("source_invalid")
	}
	return d, nil
}

type cacheDetailGate struct {
	binding    CacheDetailBinding
	observedAt time.Time
	reason     string
	stamp      statusStamp
}

func cacheGate(s Snapshot, now time.Time) cacheDetailGate {
	g := cacheDetailGate{binding: CacheDetailBinding{RunID: s.RunID, NodeStartedAt: s.StartedAt}, observedAt: now}
	if s.GenesisHash != nil {
		value := *s.GenesisHash
		g.binding.GenesisHash = &value
	}
	return g
}

func (g cacheDetailGate) failure(now time.Time) string {
	if g.observedAt.IsZero() || now.Before(g.observedAt) {
		return "source_time_invalid"
	}
	if now.Sub(g.observedAt) > StaleAfter {
		return "status_stale"
	}
	return g.reason
}

func (g cacheDetailGate) matches(b CacheDetailBinding) bool {
	a := g.binding
	return a.RunID == b.RunID && a.NodeStartedAt.Equal(b.NodeStartedAt) &&
		(a.GenesisHash == nil) == (b.GenesisHash == nil) && (a.GenesisHash == nil || *a.GenesisHash == *b.GenesisHash)
}

type cacheDetailPublication struct {
	body       string
	freshUntil time.Time
	gate       cacheDetailGate
	reason     string
}

type cacheDetailSource struct {
	sample CacheDetailSampler
	gate   atomic.Pointer[cacheDetailGate]
	latest atomic.Pointer[cacheDetailPublication]
}

// update runs serially after accepted status. A failed copy replaces old gauges;
// reset invalidation is observed on this cycle, not by an HTTP-owned cache read.
func (s *cacheDetailSource) update(snapshot Snapshot, now time.Time) {
	g := cacheGate(snapshot, now)
	s.gate.Store(&g)
	p := &cacheDetailPublication{gate: g, reason: g.failure(now)}
	defer func() { s.latest.Store(p) }()
	if p.reason != "" {
		return
	}
	capture, err := s.sample(now)
	completedAt := time.Now()
	if err != nil {
		p.reason = cacheDetailReason(err.Error())
		return
	}
	if !g.matches(capture.Detail.Binding) {
		p.reason = "binding_changed"
		return
	}
	if validateCacheDetail(capture.Detail) != nil {
		p.reason = "source_invalid"
		return
	}
	// The diagnostics owner can publish while the feed copies its cache. Use
	// completion for validation without replacing the original owner deadline.
	if capture.FreshUntil.IsZero() || !capture.FreshUntil.After(completedAt) || capture.FreshUntil.After(completedAt.Add(CacheDetailFreshFor)) ||
		capture.Detail.CapturedAt.After(completedAt.Add(time.Second)) || completedAt.Sub(capture.Detail.CapturedAt) >= CacheDetailFreshFor {
		p.reason = "source_time_invalid"
		return
	}
	body, err := json.Marshal(capture.Detail)
	if err != nil {
		p.reason = "source_invalid"
		return
	}
	if len(body) > MaxCacheDetailBytes {
		p.reason = "response_limit"
		return
	}
	p.body, p.freshUntil = string(body), capture.FreshUntil
	if previous := s.latest.Load(); previous != nil && previous.body == p.body && previous.freshUntil.Before(p.freshUntil) {
		p.freshUntil = previous.freshUntil
	}
}

func cacheDetailReason(reason string) string {
	switch reason {
	case "waiting_source", "status_stale", "source_unavailable", "source_invalid", "source_time_invalid", "binding_changed", "response_limit", "invalid_request":
		return reason
	default:
		return "source_unavailable"
	}
}

func (s *cacheDetailSource) serveHTTP(w http.ResponseWriter, r *http.Request) {
	status, reason, body := http.StatusServiceUnavailable, "waiting_source", ""
	var remaining int64
	if r.Method != http.MethodGet || r.URL.Path != "/details/cache" || r.URL.EscapedPath() != "/details/cache" ||
		r.URL.RawQuery != "" || r.URL.ForceQuery || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
		status, reason = http.StatusBadRequest, "invalid_request"
	} else if g := s.gate.Load(); g != nil {
		now := time.Now()
		reason = g.failure(now)
		if reason == "" {
			p := s.latest.Load()
			switch {
			case p == nil:
				reason = "waiting_source"
			case !g.matches(p.gate.binding):
				reason = "binding_changed"
			case p.reason != "":
				reason = p.reason
			case !p.freshUntil.After(now):
				reason = "status_stale"
			default:
				remaining = p.freshUntil.Sub(now).Milliseconds()
				if remaining <= 0 {
					reason = "status_stale"
				} else {
					status, body = http.StatusOK, p.body
				}
			}
		}
	}
	if body == "" {
		body = `{"schema_version":1,"family":"accounts_cache","state":"unavailable","reason":"` + reason + `"}`
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if status == http.StatusOK {
		w.Header().Set(replayDetailRemainingHeader, strconv.FormatInt(remaining, 10))
	}
	w.WriteHeader(status)
	if _, err := io.WriteString(w, body); err != nil {
		return
	}
}
