package node

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/gagliardetto/solana-go"
)

const (
	machineMetadataEvery = 30 * time.Second
	machineRSSFreshFor   = 30 * time.Second
)

type machineDashboardMetadata struct {
	topology metrics.TopologyObservation
	affinity metrics.ThreadAffinityObservation
	links    metrics.LinkTopologyObservation
	routes   metrics.RouteSummaryObservation
}

type machineDashboardPublication struct {
	capture dashboard.MachineDetailCapture
}

// machineDashboardMonitor is observed serially by the existing diagnostics
// owner. Only stop/sample are concurrent. No OS work or encoding holds mu.
type machineDashboardMonitor struct {
	mu       sync.Mutex
	inflight sync.WaitGroup
	stopped  atomic.Bool
	latest   atomic.Pointer[machineDashboardPublication]

	// This concrete capture seam exists only to control cadence and shutdown in
	// tests. Production uses the four samplers below with their shared budgets.
	captureMetadata       func() machineDashboardMetadata
	history               *metrics.RSSHourHistory
	binding               metrics.RSSHistoryBinding
	publicationGeneration uint64
	metadata              *machineDashboardMetadata
	metadataAttemptAt     time.Time
	workers               []dashboard.MachineWorker // Immutable after publication under mu.
}

func newMachineDashboardMonitor(enabled bool) *machineDashboardMonitor {
	if !enabled {
		return nil
	}
	topology := metrics.NewTopologySampler(true)
	affinity := metrics.NewThreadAffinitySampler(true)
	links := metrics.NewLinkTopologySampler(true)
	routes := metrics.NewRouteSummarySampler(true)
	m := &machineDashboardMonitor{history: metrics.NewRSSHourHistory(true)}
	m.captureMetadata = func() machineDashboardMetadata {
		// These enabled samplers always return an attempt, including unsupported
		// platforms or exhaustion. Each consumes the previous original budget.
		t, _ := topology.Sample()
		a, _ := affinity.Sample(t)
		l, _ := links.Sample(t, a)
		r, _ := routes.Sample(t, a, l)
		return machineDashboardMetadata{t, a, l, r}
	}
	return m
}

// observe consumes the original RSS read, not a ticker timestamp or a new poll.
// A zero native RSS is unavailable. Publication generation is supplied by the
// node owner and must strictly advance even across metadata-only cache reuse.
func (m *machineDashboardMonitor) observe(b metrics.RSSHistoryBinding, publicationGeneration uint64, point metrics.RSSHistoryPoint, gomaxprocs uint32) {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.stopped.Load() {
		m.mu.Unlock()
		return
	}
	m.inflight.Add(1)
	m.mu.Unlock()
	defer m.inflight.Done()

	// Binding dates identify a run; normalizing them does not establish a lease.
	b.StartedAt = b.StartedAt.UTC()
	if m.binding != b {
		m.latest.Store(nil)
		m.binding = b
		m.history = metrics.NewRSSHourHistory(true)
		m.metadata, m.metadataAttemptAt = nil, time.Time{}
	}
	if publicationGeneration == 0 || publicationGeneration <= m.publicationGeneration || publicationGeneration == math.MaxUint64 {
		m.latest.Store(nil)
		if publicationGeneration == math.MaxUint64 {
			m.stopped.Store(true)
		}
		return
	}
	m.publicationGeneration = publicationGeneration
	point.Known = point.Known && point.Bytes != 0
	rss := m.history.Observe(b, point)
	if rss.Reason == "binding_unavailable" || rss.Reason == "clock_unavailable" || rss.Reason == "clock_discontinuity" {
		m.latest.Store(nil)
		m.metadata = nil
		return
	}
	if m.metadataAttemptAt.IsZero() || point.At.Sub(m.metadataAttemptAt) >= machineMetadataEvery {
		m.metadataAttemptAt = point.At
		value := m.captureMetadata()
		// Replace failures too: no old usable rows are redated or carried over.
		m.metadata = &value
	}
	if m.stopped.Load() {
		return
	}
	var split *dashboard.ProcessRSSBreakdown
	if value, ok := metrics.ReadProcessRSSBreakdown(); ok {
		split = &dashboard.ProcessRSSBreakdown{SampledAt: time.Now().UTC(), AnonymousBytes: value.AnonymousBytes, FileBytes: value.FileBytes, SharedBytes: value.SharedBytes}
	}
	capturedAt := time.Now()
	deadline, ok := machineDashboardDeadline(point.At, m.metadata)
	if !ok {
		m.latest.Store(nil)
		return
	}
	v := m.metadata
	detail, err := dashboard.NewMachineDetail(dashboard.MachineDetailBinding{
		RunID: b.RunID, NodeStartedAt: b.StartedAt, GenesisHash: solana.Hash(b.GenesisHash).String(), SourceGeneration: publicationGeneration,
	}, capturedAt, &v.topology, &v.affinity, &v.links, &v.routes, &rss)
	if err != nil {
		m.latest.Store(nil)
		return
	}
	detail.RSSBreakdown = split
	if gomaxprocs != 0 {
		resolved := gomaxprocs
		detail.GOMAXPROCS = &resolved
	}
	m.mu.Lock()
	workers := m.workers
	m.mu.Unlock()
	if len(workers) != 0 {
		detail.Workers = workers
		detail.PoolSettingsState = "resolved_constructors"
	}
	prepared, err := dashboard.PrepareMachineDetail(detail)
	if err != nil {
		m.latest.Store(nil)
		return
	}
	m.mu.Lock()
	if !m.stopped.Load() {
		m.latest.Store(&machineDashboardPublication{dashboard.MachineDetailCapture{Prepared: prepared, FreshUntil: deadline}})
	}
	m.mu.Unlock()
}

// resolvedWorker records a constructor's limit, not live goroutines or pinning.
func (m *machineDashboardMonitor) resolvedWorker(name string, count int) {
	if m == nil || count < 0 || uint64(count) > math.MaxUint32 ||
		name != "replay_transactions" && name != "tpu_sigverify" && name != "harmonic_bundles" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped.Load() {
		return
	}
	rows := append([]dashboard.MachineWorker(nil), m.workers...)
	row := dashboard.MachineWorker{Name: name, Enabled: count > 0, Resolved: uint32(count), Affinity: "not_bound_by_node"}
	for i := range rows {
		if rows[i].Name == name {
			rows[i] = row
			m.workers = rows
			return
		}
	}
	m.workers = append(rows, row)
}

// machineDashboardDeadline uses only original raw monotonic clocks. Failed
// metadata has no old values to lease; available/partial values cap the family.
func machineDashboardDeadline(at time.Time, v *machineDashboardMetadata) (time.Time, bool) {
	valid := func(at time.Time) bool {
		return !at.IsZero() && at.Year() >= 1 && at.Year() <= 9999 && at != at.Round(0)
	}
	if !valid(at) || v == nil {
		return time.Time{}, false
	}
	deadline := at.Add(machineRSSFreshFor)
	usable := func(state metrics.TopologyState) bool {
		return state == metrics.TopologyAvailable || state == metrics.TopologyPartial
	}
	for _, clock := range []struct {
		needed     bool
		start, end time.Time
	}{
		{usable(v.topology.CPUState) || usable(v.topology.NUMAState), v.topology.StartedAt, v.topology.SampledAt},
		{usable(v.affinity.State), v.affinity.StartedAt, v.affinity.SampledAt},
		{usable(v.links.State), v.links.StartedAt, v.links.SampledAt},
		{usable(v.routes.State), v.routes.StartedAt, v.routes.SampledAt},
	} {
		if !clock.needed {
			continue
		}
		if !valid(clock.start) || !valid(clock.end) || clock.end.Before(clock.start) || clock.end.Sub(clock.start) > time.Second {
			return time.Time{}, false
		}
		until := clock.start.Add(dashboard.MachineDetailFreshFor)
		if until.Before(deadline) {
			deadline = until
		}
	}
	return deadline, true
}

// sample copies only an immutable prepared pointer and its original deadline.
// Expiry is evaluated by the Feed using that deadline, never renewed here.
func (m *machineDashboardMonitor) sample() (dashboard.MachineDetailCapture, error) {
	if m == nil || m.stopped.Load() {
		return dashboard.MachineDetailCapture{}, errors.New("source_unavailable")
	}
	p := m.latest.Load()
	if p == nil {
		return dashboard.MachineDetailCapture{}, errors.New("waiting_source")
	}
	if m.stopped.Load() || m.latest.Load() != p {
		return dashboard.MachineDetailCapture{}, errors.New("source_unavailable")
	}
	return p.capture, nil
}

// stop fences publication immediately and joins the one serial capture before
// returning. The source cannot restart; subsequent callers need a new monitor.
func (m *machineDashboardMonitor) stop() {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.stopped.Store(true)
	m.latest.Store(nil)
	m.mu.Unlock()
	m.inflight.Wait()
	m.mu.Lock()
	m.metadata = nil
	m.history = nil
	m.workers = nil
	m.mu.Unlock()
}
