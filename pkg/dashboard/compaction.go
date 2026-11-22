package dashboard

import (
	"math"
	"time"
)

// Compaction records configuration and one historical returned worker cycle.
// Enabled means configured, not that the worker is currently running or healthy.
// Active is sampled with resources and does not establish the worker's state now.
// Its optional sample dates bracket one atomic owner-state read, not a cycle.
type Compaction struct {
	Enabled                 bool                  `json:"enabled"`
	Active                  *bool                 `json:"active,omitempty"`
	ActiveSampleStartedAt   *time.Time            `json:"active_sample_started_at,omitempty"`
	ActiveSampleCompletedAt *time.Time            `json:"active_sample_completed_at,omitempty"`
	Relocation              *CompactionRelocation `json:"relocation,omitempty"`
	LastCompleted           *CompactionCycle      `json:"last_completed"`
}

// CompactionRelocation counts committed record bytes in one worker session.
// ElapsedNanoseconds is monotonic within the session; the active sample dates
// bracket this read. Bytes include record overhead, not physical disk writes.
type CompactionRelocation struct {
	SessionStartedAt   time.Time `json:"session_started_at"`
	ElapsedNanoseconds int64     `json:"elapsed_ns,string"`
	Bytes              uint64    `json:"bytes,string"`
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
	Enabled                 bool                        `json:"enabled"`
	Active                  *bool                       `json:"active,omitempty"`
	ActiveSampleStartedAt   *time.Time                  `json:"active_sample_started_at,omitempty"`
	ActiveSampleCompletedAt *time.Time                  `json:"active_sample_completed_at,omitempty"`
	Relocation              *PublicCompactionRelocation `json:"relocation,omitempty"`
	LastCompleted           *PublicCompactionCycle      `json:"last_completed"`
}

// PublicCompactionRelocation reports dated committed record bytes in one worker session.
type PublicCompactionRelocation struct {
	SessionStartedAt   time.Time `json:"session_started_at"`
	ElapsedNanoseconds int64     `json:"elapsed_ns,string"`
	Bytes              uint64    `json:"bytes,string"`
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
	if c == nil || !c.Enabled && (c.LastCompleted != nil || c.Active != nil || c.ActiveSampleStartedAt != nil || c.ActiveSampleCompletedAt != nil || c.Relocation != nil) {
		return nil
	}
	p := &PublicCompaction{Enabled: c.Enabled}
	if c.Active != nil {
		legacy := c.ActiveSampleStartedAt == nil && c.ActiveSampleCompletedAt == nil
		if legacy || c.ActiveSampleStartedAt != nil && c.ActiveSampleCompletedAt != nil &&
			validPublicTime(*c.ActiveSampleStartedAt, now) && validPublicTime(*c.ActiveSampleCompletedAt, now) &&
			!c.ActiveSampleStartedAt.Before(sampledAt) && !c.ActiveSampleCompletedAt.Before(*c.ActiveSampleStartedAt) &&
			c.ActiveSampleCompletedAt.Sub(*c.ActiveSampleStartedAt) <= time.Second {
			active := *c.Active
			p.Active = &active
			if !legacy {
				started, completed := *c.ActiveSampleStartedAt, *c.ActiveSampleCompletedAt
				p.ActiveSampleStartedAt, p.ActiveSampleCompletedAt = &started, &completed
				if r := c.Relocation; r != nil && r.Bytes != math.MaxUint64 && r.ElapsedNanoseconds >= 0 &&
					validPublicTime(r.SessionStartedAt, now) && !r.SessionStartedAt.After(completed) {
					p.Relocation = &PublicCompactionRelocation{
						SessionStartedAt: r.SessionStartedAt, ElapsedNanoseconds: r.ElapsedNanoseconds, Bytes: r.Bytes,
					}
				}
			}
		}
	}
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
