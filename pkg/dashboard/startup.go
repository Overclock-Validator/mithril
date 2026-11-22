package dashboard

import "time"

// PublicStartup combines dated lifecycle observations, not a readiness verdict.
type PublicStartup struct {
	StartedAt          time.Time                 `json:"started_at"`
	Recovery           *PublicRecovery           `json:"recovery"`
	Replay             *PublicReplayStart        `json:"replay"`
	Input              *PublicCatchup            `json:"input"`
	SnapshotManifests  *PublicSnapshotManifests  `json:"snapshot_manifests,omitempty"`
	SnapshotArchive    *PublicSnapshotArchive    `json:"snapshot_archive,omitempty"`
	IncrementalArchive *PublicIncrementalArchive `json:"incremental_archive,omitempty"`
	SnapshotShardFlush *PublicSnapshotShardFlush `json:"snapshot_shard_flush,omitempty"`
}

// SnapshotShardFlush reports successful shard-write function returns and the
// aggregate flush call's result, not SST durability or bootstrap readiness.
type SnapshotShardFlush struct {
	Attempt             uint64     `json:"attempt,string"`
	StartedAt           time.Time  `json:"started_at"`
	SampledAt           time.Time  `json:"sampled_at"`
	Phase               string     `json:"phase"`
	FullSnapshotSlot    uint64     `json:"full_snapshot_slot,string"`
	IncrementalSlot     *uint64    `json:"incremental_slot,string"`
	CompletedShards     uint32     `json:"completed_shards,string"`
	TotalShards         uint32     `json:"total_shards,string"`
	SSTIngestReturnedAt *time.Time `json:"sst_ingest_returned_at,omitempty"`
}

type PublicSnapshotShardFlush struct {
	PublicObservation
	Attempt             uint64     `json:"attempt,string"`
	StartedAt           time.Time  `json:"started_at"`
	Phase               string     `json:"phase"`
	FullSnapshotSlot    uint64     `json:"full_snapshot_slot,string"`
	IncrementalSlot     *uint64    `json:"incremental_slot,string"`
	CompletedShards     uint32     `json:"completed_shards,string"`
	TotalShards         uint32     `json:"total_shards,string"`
	SSTIngestReturnedAt *time.Time `json:"sst_ingest_returned_at,omitempty"`
}

func validSnapshotShardFlush(a *SnapshotShardFlush) bool {
	return a != nil && a.Attempt != 0 && validReplayDetailTime(a.StartedAt) && validReplayDetailTime(a.SampledAt) &&
		!a.SampledAt.Before(a.StartedAt) && (a.Phase == "active" || a.Phase == "returned" || a.Phase == "error") &&
		a.TotalShards >= 1 && a.TotalShards <= 1000 && a.CompletedShards <= a.TotalShards &&
		(a.IncrementalSlot == nil || *a.IncrementalSlot > a.FullSnapshotSlot) &&
		(a.SSTIngestReturnedAt == nil || a.Phase != "active" && validReplayDetailTime(*a.SSTIngestReturnedAt) && !a.SSTIngestReturnedAt.Before(a.SampledAt))
}

// SnapshotManifestParse is the successful return of a manifest parse. It does
// not mean that the account database was built or is ready for replay.
type SnapshotManifestParse struct {
	Slot     uint64    `json:"slot,string"`
	ParsedAt time.Time `json:"parsed_at"`
}

// SnapshotManifests belongs to a single bootstrap attempt. Missing entries
// are unknown, not evidence that an archive did not exist.
type SnapshotManifests struct {
	Attempt     uint64                 `json:"attempt,string"`
	Full        *SnapshotManifestParse `json:"full"`
	Incremental *SnapshotManifestParse `json:"incremental"`
	ReadyMarker *SnapshotReadyMarker   `json:"ready_marker,omitempty"`
}

// SnapshotReadyMarker observes a saved ready-state marker for this bootstrap.
// It is not proof of current replay progress or validator readiness.
type SnapshotReadyMarker struct {
	Slot       uint64    `json:"slot,string"`
	ObservedAt time.Time `json:"observed_at"`
}

// PublicSnapshotManifests contains the manifest fields shared with viewers.
type PublicSnapshotManifests struct {
	Attempt     uint64                       `json:"attempt,string"`
	Full        *PublicSnapshotManifestParse `json:"full"`
	Incremental *PublicSnapshotManifestParse `json:"incremental"`
	ReadyMarker *PublicSnapshotReadyMarker   `json:"ready_marker,omitempty"`
}

// PublicSnapshotManifestParse records a successfully parsed manifest.
type PublicSnapshotManifestParse struct {
	Slot     uint64    `json:"slot,string"`
	ParsedAt time.Time `json:"parsed_at"`
}

// PublicSnapshotReadyMarker records a ready marker for the selected snapshot.
type PublicSnapshotReadyMarker struct {
	Slot       uint64    `json:"slot,string"`
	ObservedAt time.Time `json:"observed_at"`
}

// SnapshotArchive carries one archive read and worker-drain attempt's fields.
// The snapshot_archive child is full-only; IncrementalArchive adds its end slot.
// Completion does not establish index ingestion or node readiness.
type SnapshotArchive struct {
	Attempt                 uint64    `json:"attempt,string"`
	StartedAt               time.Time `json:"started_at"`
	SampledAt               time.Time `json:"sampled_at"`
	Phase                   string    `json:"phase"`
	FullSnapshotSlot        uint64    `json:"full_snapshot_slot,string"`
	InputKind               string    `json:"input_kind"`
	CompressedInputBytes    uint64    `json:"compressed_input_bytes,string"`
	CompressedReportedTotal *uint64   `json:"compressed_reported_total,string"`
	AccountPayloadBytes     uint64    `json:"account_payload_bytes,string"`
}

// IncrementalArchive distinguishes incremental bytes from full-archive work.
// FullSnapshotSlot is the selected base; IncrementalSlot is the parsed end slot.
type IncrementalArchive struct {
	SnapshotArchive
	IncrementalSlot uint64 `json:"incremental_slot,string"`
}

// PublicSnapshotArchive retains the original owner date, including terminal
// history. State describes observation freshness, not progress or node health.
type PublicSnapshotArchive struct {
	PublicObservation
	Attempt                 uint64    `json:"attempt,string"`
	StartedAt               time.Time `json:"started_at"`
	Phase                   string    `json:"phase"`
	FullSnapshotSlot        uint64    `json:"full_snapshot_slot,string"`
	InputKind               string    `json:"input_kind"`
	CompressedInputBytes    uint64    `json:"compressed_input_bytes,string"`
	CompressedReportedTotal *uint64   `json:"compressed_reported_total,string"`
	AccountPayloadBytes     uint64    `json:"account_payload_bytes,string"`
}

// PublicIncrementalArchive is a distinct child so older readers cannot label
// incremental byte counters as full-snapshot work.
type PublicIncrementalArchive struct {
	PublicSnapshotArchive
	IncrementalSlot uint64 `json:"incremental_slot,string"`
}

// PublicRecovery is a historical startup selection, not the current disk tip.
type PublicRecovery struct {
	SelectedAt   time.Time `json:"selected_at"`
	SnapshotSlot uint64    `json:"snapshot_slot,string"`
	StoredSlot   *uint64   `json:"stored_slot,string"`
	RootedSlot   *uint64   `json:"rooted_slot,string"`
}

// PublicReplayStart describes one replay attempt's initialization.
// InitializedAt means the execution loop was entered, not that it caught up.
type PublicReplayStart struct {
	PublicObservation
	Attempt       uint64     `json:"attempt,string"`
	StartedAt     time.Time  `json:"started_at"`
	InitializedAt *time.Time `json:"initialized_at"`
	StartSlot     uint64     `json:"start_slot,string"`
	ResumeParent  *uint64    `json:"resume_parent,string"`
}

// PublicCatchup reports independently sampled input state without an ETA.
// The repair range is a requested interval, not a measured download progress.
type PublicCatchup struct {
	PublicObservation
	StartedAt            time.Time `json:"started_at"`
	ConfiguredSource     string    `json:"configured_source"`
	RPCBlockFetchAllowed bool      `json:"rpc_block_fetch_allowed"`
	RepairPending        bool      `json:"repair_pending"`
	RepairFrom           *uint64   `json:"repair_from,string"`
	RepairUntil          *uint64   `json:"repair_until,string"`
}

func publicStartup(s *Snapshot, state string, now time.Time) *PublicStartup {
	p := &PublicStartup{StartedAt: s.StartedAt}
	withinRun := func(at time.Time) bool {
		return validPublicTime(at, now) && !at.Before(s.StartedAt) && !at.After(s.SampledAt.Add(time.Second))
	}
	copySlot := func(slot *uint64) *uint64 {
		if slot == nil {
			return nil
		}
		x := *slot
		return &x
	}
	if m := s.SnapshotManifests; m != nil && m.Attempt != 0 && (m.Full != nil || m.Incremental != nil) &&
		(m.Full == nil || withinRun(m.Full.ParsedAt)) && (m.Incremental == nil || withinRun(m.Incremental.ParsedAt)) {
		copy := &PublicSnapshotManifests{Attempt: m.Attempt}
		if m.Full != nil {
			copy.Full = &PublicSnapshotManifestParse{Slot: m.Full.Slot, ParsedAt: m.Full.ParsedAt}
		}
		if m.Incremental != nil {
			copy.Incremental = &PublicSnapshotManifestParse{Slot: m.Incremental.Slot, ParsedAt: m.Incremental.ParsedAt}
		}
		selected := m.Full
		if m.Incremental != nil {
			selected = m.Incremental
		}
		if r := m.ReadyMarker; r != nil && selected != nil && r.Slot == selected.Slot &&
			withinRun(r.ObservedAt) && !r.ObservedAt.Before(selected.ParsedAt) {
			copy.ReadyMarker = &PublicSnapshotReadyMarker{Slot: r.Slot, ObservedAt: r.ObservedAt}
		}
		p.SnapshotManifests = copy
	}
	archive := func(a *SnapshotArchive) *PublicSnapshotArchive {
		if a == nil || a.Attempt == 0 || !withinRun(a.StartedAt) || !withinRun(a.SampledAt) ||
			a.SampledAt.Before(a.StartedAt) || a.InputKind != "local" && a.InputKind != "remote" ||
			a.Phase != "active" && a.Phase != "complete" && a.Phase != "interrupted" ||
			a.CompressedReportedTotal != nil && *a.CompressedReportedTotal == 0 {
			return nil
		}
		observation := PublicObservation{State: state, SampledAt: a.SampledAt}
		if state == "fresh" && now.Sub(a.SampledAt) > StaleAfter {
			observation.State = "stale"
		}
		return &PublicSnapshotArchive{PublicObservation: observation,
			Attempt: a.Attempt, StartedAt: a.StartedAt, Phase: a.Phase,
			FullSnapshotSlot: a.FullSnapshotSlot, InputKind: a.InputKind,
			CompressedInputBytes: a.CompressedInputBytes, CompressedReportedTotal: copySlot(a.CompressedReportedTotal),
			AccountPayloadBytes: a.AccountPayloadBytes}
	}
	if s.SnapshotShardFlush == nil && s.IncrementalArchive == nil {
		p.SnapshotArchive = archive(s.SnapshotArchive)
	} else if s.SnapshotShardFlush == nil && s.SnapshotArchive == nil && s.IncrementalArchive.IncrementalSlot > s.IncrementalArchive.FullSnapshotSlot {
		if a := archive(&s.IncrementalArchive.SnapshotArchive); a != nil {
			p.IncrementalArchive = &PublicIncrementalArchive{PublicSnapshotArchive: *a, IncrementalSlot: s.IncrementalArchive.IncrementalSlot}
		}
	}
	if a := s.SnapshotShardFlush; a != nil && s.SnapshotArchive == nil && s.IncrementalArchive == nil &&
		validSnapshotShardFlush(a) && withinRun(a.StartedAt) && withinRun(a.SampledAt) && (a.SSTIngestReturnedAt == nil || withinRun(*a.SSTIngestReturnedAt)) {
		observation := PublicObservation{State: state, SampledAt: a.SampledAt}
		if state == "fresh" && now.Sub(a.SampledAt) > StaleAfter {
			observation.State = "stale"
		}
		p.SnapshotShardFlush = &PublicSnapshotShardFlush{PublicObservation: observation,
			Attempt: a.Attempt, StartedAt: a.StartedAt, Phase: a.Phase, FullSnapshotSlot: a.FullSnapshotSlot,
			IncrementalSlot: copySlot(a.IncrementalSlot), CompletedShards: a.CompletedShards, TotalShards: a.TotalShards}
		if a.SSTIngestReturnedAt != nil {
			at := *a.SSTIngestReturnedAt
			p.SnapshotShardFlush.SSTIngestReturnedAt = &at
		}
	}
	if r := s.Recovery; r != nil && withinRun(r.SelectedAt) {
		p.Recovery = &PublicRecovery{SelectedAt: r.SelectedAt, SnapshotSlot: r.SnapshotSlot,
			StoredSlot: copySlot(r.StoredSlot), RootedSlot: copySlot(r.RootedSlot)}
	}
	if r := s.Replay; r != nil && withinRun(r.StartedAt) && withinRun(r.SampledAt) && !r.SampledAt.Before(r.StartedAt) {
		p.Replay = &PublicReplayStart{PublicObservation: publicObservation(r.SampledAt, state, now),
			Attempt: r.Attempt, StartedAt: r.StartedAt, StartSlot: r.StartSlot, ResumeParent: copySlot(r.ResumeParent)}
		if r.ReadyAt != nil && !r.ReadyAt.Before(r.StartedAt) && !r.ReadyAt.After(r.SampledAt) {
			at := *r.ReadyAt
			p.Replay.InitializedAt = &at
		}
	}
	if b := s.BlockSource; b != nil && withinRun(b.SampledAt) && withinRun(b.StartedAt) && !b.SampledAt.Before(b.StartedAt) {
		source := "unknown"
		switch b.ConfiguredSource {
		case "rpc", "file", "lightbringer", "turbine":
			source = b.ConfiguredSource
		}
		p.Input = &PublicCatchup{PublicObservation: publicObservation(b.SampledAt, state, now), StartedAt: b.StartedAt,
			ConfiguredSource: source, RPCBlockFetchAllowed: b.RPCBlockFetchAllowed, RepairPending: b.RepairCatchupPending}
		if b.RepairCatchupPending && b.RepairCatchupFrom != nil && b.RepairCatchupUntil != nil && *b.RepairCatchupUntil >= *b.RepairCatchupFrom {
			p.Input.RepairFrom, p.Input.RepairUntil = copySlot(b.RepairCatchupFrom), copySlot(b.RepairCatchupUntil)
		}
	}
	return p
}
