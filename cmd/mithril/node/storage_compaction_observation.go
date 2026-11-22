package node

import (
	"math"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
)

// compactionObservation is one returned background cycle, including partial
// work on failure. Reclaimed bytes are the source's net accounting, not physical
// disk writes or a filesystem free-space measurement.
type compactionObservation struct {
	SessionStartedAt time.Time
	StartedAt        time.Time
	CompletedAt      time.Time
	Duration         time.Duration
	Failed           bool
	Stats            accountsdb.CompactStats
}

// compactionMonitor has one worker writer; readers only copy retained values.
// Its zero value has no completed cycle. A new worker clears it before launch.
type compactionMonitor struct {
	state atomic.Pointer[compactionState]
}

type compactionState struct {
	running    bool
	active     bool
	latest     *compactionObservation
	relocation *compactionRelocation
}

type compactionRelocation struct {
	started time.Time
	bytes   atomic.Uint64
}

func (m *compactionMonitor) snapshot() (compactionObservation, bool) {
	if m != nil {
		if s := m.state.Load(); s != nil && s.latest != nil {
			return *s.latest, true
		}
	}
	return compactionObservation{}, false
}

func (m *compactionMonitor) begin() {
	if m == nil {
		return
	}
	s := &compactionState{running: true, active: true}
	if previous := m.state.Load(); previous != nil {
		s.latest = previous.latest
		s.relocation = previous.relocation
	}
	m.state.Store(s)
}

func (m *compactionMonitor) stopped() {
	if m == nil {
		return
	}
	s := &compactionState{}
	if previous := m.state.Load(); previous != nil {
		s.latest = previous.latest
	}
	m.state.Store(s)
}

func (m *compactionMonitor) completed(session, started, completed time.Time, stats accountsdb.CompactStats, err error) {
	if m == nil {
		return
	}
	s := &compactionState{latest: &compactionObservation{
		SessionStartedAt: session, StartedAt: started, CompletedAt: completed,
		Duration: completed.Sub(started), Failed: err != nil, Stats: stats,
	}}
	if previous := m.state.Load(); previous != nil {
		s.running = previous.running
		s.relocation = previous.relocation
	}
	m.state.Store(s)
}

// configureCompaction binds one joined-before-replacement worker to this node.
// No private dashboard means no additional observer or owner clock calls.
func (d *nodeDashboard) configureCompaction(enabled bool) *compactionMonitor {
	if d == nil {
		return nil
	}
	var monitor *compactionMonitor
	if enabled {
		monitor = &compactionMonitor{}
	}
	d.mu.Lock()
	d.compactionConfigured, d.compaction = true, monitor
	d.mu.Unlock()
	return monitor
}

func compactionMeasurement(m *compactionMonitor) *dashboard.Compaction {
	p := &dashboard.Compaction{Enabled: m != nil}
	if m == nil {
		return p
	}
	started := time.Now()
	s := m.state.Load()
	if s == nil {
		return p
	}
	var bytes uint64
	if s.relocation != nil {
		bytes = s.relocation.bytes.Load()
	}
	completed := time.Now()
	var sessionElapsed time.Duration
	if s.relocation != nil {
		sessionElapsed = completed.Sub(s.relocation.started)
	}
	elapsed := completed.Sub(started)
	started, completed = started.UTC(), completed.UTC()
	if s.running && elapsed >= 0 && elapsed <= time.Second && !completed.Before(started) && completed.Sub(started) <= time.Second {
		active := s.active
		p.Active = &active
		p.ActiveSampleStartedAt, p.ActiveSampleCompletedAt = &started, &completed
		if s.relocation != nil && bytes != math.MaxUint64 && sessionElapsed >= 0 && m.state.Load() == s {
			p.Relocation = &dashboard.CompactionRelocation{
				SessionStartedAt: s.relocation.started.UTC(), ElapsedNanoseconds: int64(sessionElapsed), Bytes: bytes,
			}
		}
	}
	if s.latest == nil {
		return p
	}
	v := s.latest
	stats := v.Stats
	if v.Duration < 0 || stats.CandidatesScanned < 0 || stats.FilesCompacted < 0 || stats.FilesDeleted < 0 ||
		stats.FilesSparsified < 0 || stats.LiveBytesMoved < 0 || stats.BytesReclaimed < 0 {
		return nil
	}
	p.LastCompleted = &dashboard.CompactionCycle{
		SessionStartedAt: v.SessionStartedAt.UTC(), StartedAt: v.StartedAt.UTC(), CompletedAt: v.CompletedAt.UTC(),
		DurationNanoseconds: int64(v.Duration), Failed: v.Failed,
		CandidatesScanned: int64(stats.CandidatesScanned), FilesCompacted: int64(stats.FilesCompacted),
		FilesDeleted: int64(stats.FilesDeleted), FilesSparsified: int64(stats.FilesSparsified),
		LiveBytesMoved: stats.LiveBytesMoved, BytesReclaimed: stats.BytesReclaimed,
	}
	return p
}
