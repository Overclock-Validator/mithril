package node

import (
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
	latest atomic.Pointer[compactionObservation]
}

func (m *compactionMonitor) snapshot() (compactionObservation, bool) {
	if m != nil {
		if v := m.latest.Load(); v != nil {
			return *v, true
		}
	}
	return compactionObservation{}, false
}

func (m *compactionMonitor) completed(session, started, completed time.Time, stats accountsdb.CompactStats, err error) {
	if m == nil {
		return
	}
	m.latest.Store(&compactionObservation{
		SessionStartedAt: session, StartedAt: started, CompletedAt: completed,
		Duration: completed.Sub(started), Failed: err != nil, Stats: stats,
	})
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
	v, ok := m.snapshot()
	if !ok {
		return p
	}
	s := v.Stats
	if v.Duration < 0 || s.CandidatesScanned < 0 || s.FilesCompacted < 0 || s.FilesDeleted < 0 ||
		s.FilesSparsified < 0 || s.LiveBytesMoved < 0 || s.BytesReclaimed < 0 {
		return nil
	}
	p.LastCompleted = &dashboard.CompactionCycle{
		SessionStartedAt: v.SessionStartedAt.UTC(), StartedAt: v.StartedAt.UTC(), CompletedAt: v.CompletedAt.UTC(),
		DurationNanoseconds: int64(v.Duration), Failed: v.Failed,
		CandidatesScanned: int64(s.CandidatesScanned), FilesCompacted: int64(s.FilesCompacted),
		FilesDeleted: int64(s.FilesDeleted), FilesSparsified: int64(s.FilesSparsified),
		LiveBytesMoved: s.LiveBytesMoved, BytesReclaimed: s.BytesReclaimed,
	}
	return p
}
