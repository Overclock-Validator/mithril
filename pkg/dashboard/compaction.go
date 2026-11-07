package dashboard

import "time"

// Compaction records configuration and one historical returned worker cycle.
// Enabled means configured, not that the worker is currently running or healthy.
type Compaction struct {
	Enabled       bool             `json:"enabled"`
	LastCompleted *CompactionCycle `json:"last_completed"`
}

// CompactionCycle preserves one cycle's returned accounting, including partial
// work on failure. Byte counts are not physical writes or filesystem free space.
type CompactionCycle struct {
	SessionStartedAt    time.Time `json:"session_started_at"`
	StartedAt           time.Time `json:"started_at"`
	CompletedAt         time.Time `json:"completed_at"`
	DurationNanoseconds int64     `json:"duration_ns,string"`
	Failed              bool      `json:"failed"`
	CandidatesScanned   int64     `json:"candidates_scanned,string"`
	FilesCompacted      int64     `json:"files_compacted,string"`
	FilesDeleted        int64     `json:"files_deleted,string"`
	FilesSparsified     int64     `json:"files_sparsified,string"`
	LiveBytesMoved      int64     `json:"live_bytes_moved,string"`
	BytesReclaimed      int64     `json:"bytes_reclaimed,string"`
}

// PublicCompaction contains the explicitly selected compaction fields.
type PublicCompaction struct {
	Enabled       bool                   `json:"enabled"`
	LastCompleted *PublicCompactionCycle `json:"last_completed"`
}

type PublicCompactionCycle struct {
	SessionStartedAt    time.Time `json:"session_started_at"`
	StartedAt           time.Time `json:"started_at"`
	CompletedAt         time.Time `json:"completed_at"`
	DurationNanoseconds int64     `json:"duration_ns,string"`
	Failed              bool      `json:"failed"`
	CandidatesScanned   int64     `json:"candidates_scanned,string"`
	FilesCompacted      int64     `json:"files_compacted,string"`
	FilesDeleted        int64     `json:"files_deleted,string"`
	FilesSparsified     int64     `json:"files_sparsified,string"`
	LiveBytesMoved      int64     `json:"live_bytes_moved,string"`
	BytesReclaimed      int64     `json:"bytes_reclaimed,string"`
}

func publicCompaction(c *Compaction, sampledAt, now time.Time) *PublicCompaction {
	if c == nil || !c.Enabled && c.LastCompleted != nil {
		return nil
	}
	p := &PublicCompaction{Enabled: c.Enabled}
	if c.LastCompleted == nil {
		return p
	}
	v := *c.LastCompleted
	for _, n := range [...]int64{v.DurationNanoseconds, v.CandidatesScanned, v.FilesCompacted,
		v.FilesDeleted, v.FilesSparsified, v.LiveBytesMoved, v.BytesReclaimed} {
		if n < 0 {
			return nil
		}
	}
	if !validPublicTime(v.SessionStartedAt, now) || !validPublicTime(v.StartedAt, now) ||
		!validPublicTime(v.CompletedAt, now) || v.CompletedAt.After(sampledAt.Add(time.Second)) {
		return nil
	}
	// Duration is measured with the producer's monotonic clock. A wall-clock
	// adjustment can reorder the serialized dates without changing that duration.
	p.LastCompleted = &PublicCompactionCycle{
		SessionStartedAt: v.SessionStartedAt, StartedAt: v.StartedAt, CompletedAt: v.CompletedAt,
		DurationNanoseconds: v.DurationNanoseconds, Failed: v.Failed,
		CandidatesScanned: v.CandidatesScanned, FilesCompacted: v.FilesCompacted,
		FilesDeleted: v.FilesDeleted, FilesSparsified: v.FilesSparsified,
		LiveBytesMoved: v.LiveBytesMoved, BytesReclaimed: v.BytesReclaimed,
	}
	return p
}
