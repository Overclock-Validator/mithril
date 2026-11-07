package node

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
)

func validateDashboardCacheDetails(enabled bool, socket string) error {
	if enabled && strings.TrimSpace(socket) == "" {
		return errors.New("dashboard.cache_details requires dashboard.socket")
	}
	return nil
}

type cacheDashboardObservation struct {
	value         accountsdb.CacheObservation
	activity      accountsdb.CacheActivityObservation
	activityKnown bool
	window        *dashboard.CacheActivityWindow
	startedAt     time.Time
	completedAt   time.Time
}

// cacheDashboardMonitor belongs to one initialized database in one node run.
// Diagnostics owns occupancy reads; the Feed only copies its bounded result.
type cacheDashboardMonitor struct {
	mu             sync.RWMutex
	recordActivity bool // selected once before the runtime cache is constructed
	owner          *accountsdb.AccountsDb
	binding        dashboard.CacheDetailBinding
	stopped        bool
	latest         atomic.Pointer[cacheDashboardObservation]
	activityWindow cacheActivityWindow // Written only by the diagnostics publisher.
}

func (d *nodeDashboard) configureCacheActivity(enabled bool) {
	if d != nil && d.cache != nil {
		d.cache.recordActivity = enabled
	}
}

func (d *nodeDashboard) cacheActivityEnabled() bool {
	return d != nil && d.cache != nil && d.cache.recordActivity
}

func (d *nodeDashboard) cacheBinding() dashboard.CacheDetailBinding {
	d.mu.Lock()
	defer d.mu.Unlock()
	b := dashboard.CacheDetailBinding{RunID: d.state.RunID, NodeStartedAt: d.state.StartedAt}
	if d.state.GenesisHash != nil {
		genesis := *d.state.GenesisHash
		b.GenesisHash = &genesis
	}
	return b
}

func sameCacheNodeBinding(a, b dashboard.CacheDetailBinding) bool {
	return a.RunID == b.RunID && a.NodeStartedAt.Equal(b.NodeStartedAt) && sameReplayDetailGenesis(a.GenesisHash, b.GenesisHash)
}

// attachCacheOwner runs after chain binding and InitCaches. Replacement requires
// a new dashboard run: generation 1 on a different DB is not the old generation.
func (d *nodeDashboard) attachCacheOwner(owner *accountsdb.AccountsDb) bool {
	if d == nil || d.cache == nil {
		return true
	}
	b := d.cacheBinding()
	m := d.cache
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return false
	}
	if m.owner != nil {
		if m.owner == owner && sameCacheNodeBinding(m.binding, b) {
			return true
		}
		m.owner, m.stopped = nil, true
		m.latest.Store(nil)
		return false
	}
	if owner == nil || owner.CacheObservationGeneration() == 0 || b.RunID == "" || b.NodeStartedAt.IsZero() || b.GenesisHash == nil {
		return false
	}
	m.owner, m.binding = owner, b
	return true
}

func (m *cacheDashboardMonitor) observe() {
	if m == nil {
		return
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.stopped || m.owner == nil {
		return
	}
	startedAt := time.Now()
	value, ok := m.owner.CacheObservation()
	activity, activityKnown := m.owner.CacheActivity()
	completedAt := time.Now()
	if !ok {
		m.activityWindow = cacheActivityWindow{}
		m.latest.Store(nil)
		return
	}
	if activityKnown && activity.Generation != value.Generation {
		// A reset between the two reads cannot be presented as one capture.
		activity, activityKnown = accountsdb.CacheActivityObservation{}, false
	}
	window := m.activityWindow.observe(activity, activityKnown, startedAt, completedAt)
	m.latest.Store(&cacheDashboardObservation{value: value, activity: activity, activityKnown: activityKnown, window: window, startedAt: startedAt, completedAt: completedAt})
}

// stop joins an occupancy read before releasing the DB lifetime. The monitor
// cannot be reattached, including after an attempted owner replacement.
func (m *cacheDashboardMonitor) stop() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.owner, m.stopped = nil, true
	m.latest.Store(nil)
	m.mu.Unlock()
}

func (d *nodeDashboard) sampleCacheDetail(_ time.Time) (dashboard.CacheDetailCapture, error) {
	if d == nil || d.cache == nil {
		return dashboard.CacheDetailCapture{}, errors.New("source_unavailable")
	}
	b := d.cacheBinding()
	m := d.cache
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.stopped {
		return dashboard.CacheDetailCapture{}, errors.New("source_unavailable")
	}
	if m.owner == nil {
		return dashboard.CacheDetailCapture{}, errors.New("waiting_source")
	}
	if !sameCacheNodeBinding(b, m.binding) {
		return dashboard.CacheDetailCapture{}, errors.New("binding_changed")
	}
	o := m.latest.Load()
	if o == nil {
		return dashboard.CacheDetailCapture{}, errors.New("waiting_source")
	}
	generation := m.owner.CacheObservationGeneration()
	if generation == 0 || generation != o.value.Generation {
		return dashboard.CacheDetailCapture{}, errors.New("binding_changed")
	}
	capture, err := cacheDetailMeasurement(b, *o, time.Now())
	if err != nil {
		return dashboard.CacheDetailCapture{}, err
	}
	if m.owner.CacheObservationGeneration() != generation || !sameCacheNodeBinding(b, d.cacheBinding()) {
		return dashboard.CacheDetailCapture{}, errors.New("binding_changed")
	}
	return capture, nil
}

func cacheDetailMeasurement(binding dashboard.CacheDetailBinding, o cacheDashboardObservation, now time.Time) (dashboard.CacheDetailCapture, error) {
	if o.startedAt.IsZero() || o.completedAt.Before(o.startedAt) || o.completedAt.Sub(o.startedAt) > time.Second ||
		now.Before(o.completedAt) || o.startedAt.Before(binding.NodeStartedAt) {
		return dashboard.CacheDetailCapture{}, errors.New("source_time_invalid")
	}
	deadline := o.startedAt.Add(dashboard.CacheDetailFreshFor)
	if !deadline.After(now) {
		return dashboard.CacheDetailCapture{}, errors.New("status_stale")
	}
	v := o.value
	if v.Generation == 0 || v.Generation == ^uint64(0) {
		return dashboard.CacheDetailCapture{}, errors.New("source_invalid")
	}
	binding.CacheGeneration = v.Generation
	var emptyUsed, smallUsed *uint64
	if v.CommonFixedOccupancyKnown {
		empty, small := v.CommonFixedEmptyOccupiedEntries, v.CommonFixedSmallOccupiedEntries
		emptyUsed, smallUsed = &empty, &small
	}
	var activity *dashboard.CacheActivityDetail
	if o.activityKnown {
		a := o.activity
		if a.Generation != v.Generation || a.Instance == 0 || a.Instance > a.Generation || a.Instance == ^uint64(0) {
			return dashboard.CacheDetailCapture{}, errors.New("source_invalid")
		}
		activity = &dashboard.CacheActivityDetail{Instance: a.Instance,
			Vote: cacheLookupMeasurement(a.Vote), Program: cacheLookupMeasurement(a.Program), CommonLarge: cacheLookupMeasurement(a.CommonLarge)}
		activity.ProgramSets = &dashboard.CacheProgramSets{Scope: "accepted_program_cache_set_calls", Reason: "counter_saturated"}
		if a.ProgramAcceptedSetsKnown {
			count := a.ProgramAcceptedSets
			activity.ProgramSets.Count, activity.ProgramSets.Reason = &count, ""
		}
		activity.ProgramRefusals = &dashboard.CacheProgramSets{Scope: "oversized_program_cache_set_refusals", Reason: "counter_saturated"}
		if a.ProgramRefusedSetsKnown {
			count := a.ProgramRefusedSets
			activity.ProgramRefusals.Count, activity.ProgramRefusals.Reason = &count, ""
		}
		if a.FixedKnown {
			activity.Fixed = &dashboard.CacheFixedActivityDetail{EmptyHits: a.FixedEmptyHits, SmallHits: a.FixedSmallHits,
				EmptyReplacements: a.FixedEmptyReplacements, SmallReplacements: a.FixedSmallReplacements}
		}
		if o.window != nil {
			window := *o.window
			activity.Window = &window
		}
		if a.InsertionWeightsRecorded {
			program, programReason := cacheInsertionWeightMeasurement(a.ProgramInsertionWeight)
			large, largeReason := cacheInsertionWeightMeasurement(a.CommonLargeInsertionWeight)
			activity.InsertionWeights = &dashboard.CacheInsertionWeights{Scope: "approximate_stored_insertion_weight",
				ProgramCostUnits: program, ProgramReason: programReason, CommonLargeAdmissionBytes: large, CommonLargeReason: largeReason}
		}
	}
	return dashboard.CacheDetailCapture{FreshUntil: deadline, Detail: dashboard.CacheDetail{
		SchemaVersion: 1, Family: "accounts_cache", Scope: "accountsdb_read_cache_gauges", State: "fresh",
		Binding: binding, CapturedAt: o.completedAt.UTC(),
		Activity: activity,
		Vote:     dashboard.CacheVoteDetail{CapacityEntries: v.VoteCapacityEntries, EstimatedEntries: v.VoteEstimatedEntries},
		Program: dashboard.CacheProgramDetail{CapacityCostUnits: v.ProgramCapacityCostUnits,
			CostUnitBytes: v.ProgramCostUnitBytes, EstimatedEntries: v.ProgramEstimatedEntries},
		Common: dashboard.CacheCommonDetail{Layout: v.CommonLayout, ConfiguredBudgetBytes: v.CommonConfiguredBudgetBytes,
			FixedEmptyCapacityEntries: v.CommonFixedEmptyCapacityEntries, FixedSmallCapacityEntries: v.CommonFixedSmallCapacityEntries,
			FixedEmptyOccupiedEntries: emptyUsed, FixedSmallOccupiedEntries: smallUsed,
			LargeCapacityBytes: v.CommonLargeCapacityBytes, LargeEstimatedEntries: v.CommonLargeEstimatedEntries},
	}}, nil
}

func cacheLookupMeasurement(o accountsdb.CacheLookupActivity) dashboard.CacheLookupDetail {
	return dashboard.CacheLookupDetail{Hits: o.Hits, Misses: o.Misses, Evictions: o.Evictions, EvictionWeight: o.EvictionWeight}
}

func cacheInsertionWeightMeasurement(o accountsdb.CacheInsertionWeightObservation) (*uint64, string) {
	if !o.KnownApproximate || o.Faulted {
		return nil, "counter_fault"
	}
	value := o.ApproximateWeight
	return &value, ""
}
