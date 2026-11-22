package dashboard

import (
	"math"
	"time"

	"github.com/gagliardetto/solana-go"
)

const (
	// BlockLifecycleCapacity bounds the engine's retained exact block identities.
	BlockLifecycleCapacity = 128
	// BlockLifecycleVisibleRows bounds the existing feed, not source retention.
	BlockLifecycleVisibleRows = 4
)

// BlockLifecycle is copied from one engine session. It is independent of the
// current identity, selected fork and replay attempt. Rows have nil padding.
type BlockLifecycle struct {
	SampledAt time.Time                                     `json:"sampled_at"`
	StartedAt time.Time                                     `json:"started_at"`
	UpdatedAt *time.Time                                    `json:"updated_at"`
	Retained  int                                           `json:"retained"`
	Evicted   uint64                                        `json:"evicted,string"`
	RowCount  int                                           `json:"row_count"`
	Rows      [BlockLifecycleVisibleRows]*BlockLifecycleRow `json:"rows"`
}

// BlockLifecycleRow retains first local events for an exact Alpenglow block.
// Assembly timestamps belong to its completed assembly generation, not proven
// first receipt of that exact block. Intervals were measured before UTC conversion.
type BlockLifecycleRow struct {
	Slot                    uint64     `json:"slot,string"`
	BlockID                 string     `json:"block_id"`
	FirstObservedAt         time.Time  `json:"first_observed_at"`
	LastObservedAt          time.Time  `json:"last_observed_at"`
	AssemblyFirstAcceptedAt *time.Time `json:"assembly_first_accepted_at"`
	AssemblyFullAt          *time.Time `json:"assembly_full_at"`
	ReadyAt                 *time.Time `json:"ready_at"`
	ReplayAcceptedAt        *time.Time `json:"replay_accepted_at"`
	DirectFinalizedAt       *time.Time `json:"direct_finalized_at"`
	FinalityKind            *string    `json:"finality_kind"`
	AssemblyFirstToFullNS   *uint64    `json:"assembly_first_to_full_ns,string"`
	FullToReadyNS           *uint64    `json:"full_to_ready_ns,string"`
	ReadyToReplayNS         *uint64    `json:"ready_to_replay_ns,string"`
	ReplayToFinalityNS      *uint64    `json:"replay_to_finality_ns,string"`
	FinalityToReplayNS      *uint64    `json:"finality_to_replay_ns,string"`
	FinalityBeforeReplay    *bool      `json:"finality_before_replay"`
	WallOrderConflict       bool       `json:"wall_order_conflict"`
}

// PublicBlockLifecycle exposes bounded historical engine observations, not
// readiness, current-fork status, durability or actions by the current identity.
type PublicBlockLifecycle struct {
	PublicObservation
	ActivityScope string                    `json:"activity_scope"`
	StartedAt     time.Time                 `json:"started_at"`
	UpdatedAt     *time.Time                `json:"updated_at"`
	Capacity      int                       `json:"capacity"`
	Retained      int                       `json:"retained"`
	Evicted       uint64                    `json:"evicted,string"`
	SafetyFault   bool                      `json:"safety_fault"`
	Rows          []PublicBlockLifecycleRow `json:"rows"`
}

// PublicBlockLifecycleRow is an explicit allowlist of local historical facts.
// Nullable nanoseconds preserve measured zero and unknown as different values.
type PublicBlockLifecycleRow struct {
	Slot                    uint64     `json:"slot,string"`
	BlockID                 string     `json:"block_id"`
	FirstObservedAt         time.Time  `json:"first_observed_at"`
	LastObservedAt          time.Time  `json:"last_observed_at"`
	AssemblyFirstAcceptedAt *time.Time `json:"assembly_first_accepted_at"`
	AssemblyFullAt          *time.Time `json:"assembly_full_at"`
	ReadyAt                 *time.Time `json:"ready_at"`
	ReplayAcceptedAt        *time.Time `json:"replay_accepted_at"`
	DirectFinalizedAt       *time.Time `json:"direct_finalized_at"`
	FinalityKind            *string    `json:"finality_kind"`
	AssemblyFirstToFullNS   *uint64    `json:"assembly_first_to_full_ns,string"`
	FullToReadyNS           *uint64    `json:"full_to_ready_ns,string"`
	ReadyToReplayNS         *uint64    `json:"ready_to_replay_ns,string"`
	ReplayToFinalityNS      *uint64    `json:"replay_to_finality_ns,string"`
	FinalityToReplayNS      *uint64    `json:"finality_to_replay_ns,string"`
	FinalityBeforeReplay    *bool      `json:"finality_before_replay"`
	WallOrderConflict       bool       `json:"wall_order_conflict"`
}

func publicBlockLifecycle(s *Snapshot, state string, now time.Time) *PublicBlockLifecycle {
	c := s.Consensus
	if c == nil || c.SafetyFault == nil || c.BlockLifecycle == nil || !s.Alpenglow || s.RunID == "" || s.GenesisHash == nil {
		return nil
	}
	l := c.BlockLifecycle
	if !validPublicTime(s.StartedAt, now) || !validPublicTime(s.SampledAt, now) || s.SampledAt.Before(s.StartedAt) ||
		!validPublicTime(l.StartedAt, now) || l.StartedAt.Before(s.StartedAt) ||
		!validPublicTime(l.SampledAt, now) || l.SampledAt.Before(l.StartedAt) || !l.SampledAt.Equal(c.SampledAt) ||
		l.SampledAt.After(s.SampledAt.Add(time.Second)) || l.Retained < 0 || l.Retained > BlockLifecycleCapacity ||
		l.RowCount != min(l.Retained, BlockLifecycleVisibleRows) || l.Evicted != 0 && l.Retained != BlockLifecycleCapacity ||
		(l.UpdatedAt == nil) != (l.Retained == 0) {
		return nil
	}
	if genesis, err := solana.HashFromBase58(*s.GenesisHash); err != nil || genesis == (solana.Hash{}) {
		return nil
	}
	if s.IdentityKey != nil {
		if identity, err := solana.PublicKeyFromBase58(*s.IdentityKey); err != nil || identity.IsZero() {
			return nil
		}
	}
	p := &PublicBlockLifecycle{PublicObservation: publicObservation(l.SampledAt.UTC(), state, now), ActivityScope: "engine_session",
		StartedAt: l.StartedAt.UTC(), Capacity: BlockLifecycleCapacity, Retained: l.Retained, Evicted: l.Evicted,
		SafetyFault: *c.SafetyFault, Rows: make([]PublicBlockLifecycleRow, 0, l.RowCount)}
	// The one-second tolerance is absolute to the node and collector clocks,
	// not cumulative through parent, owner and event observations.
	eventClock := now
	if s.SampledAt.Before(eventClock) {
		eventClock = s.SampledAt
	}
	if l.UpdatedAt != nil {
		if !validLifecycleEvent(*l.UpdatedAt, l.SampledAt, eventClock) {
			return nil
		}
		at := l.UpdatedAt.UTC()
		p.UpdatedAt = &at
	}
	for i, row := range l.Rows {
		if i >= l.RowCount {
			if row != nil {
				return nil
			}
			continue
		}
		if row == nil {
			return nil
		}
		// ponytail: scan at most four rows; add an index only if this cap grows.
		for _, previous := range p.Rows {
			if previous.Slot == row.Slot && previous.BlockID == row.BlockID {
				return nil
			}
		}
		value, ok := publicLifecycleRow(row, l.SampledAt, eventClock)
		if !ok {
			return nil
		}
		p.Rows = append(p.Rows, value)
	}
	return p
}

func publicLifecycleRow(r *BlockLifecycleRow, sampledAt, now time.Time) (PublicBlockLifecycleRow, bool) {
	p := PublicBlockLifecycleRow{Slot: r.Slot, BlockID: r.BlockID, FirstObservedAt: r.FirstObservedAt.UTC(),
		LastObservedAt: r.LastObservedAt.UTC(), WallOrderConflict: r.WallOrderConflict}
	if hash, err := solana.HashFromBase58(r.BlockID); err != nil || hash == (solana.Hash{}) {
		return p, false
	}
	ingress := r.ReadyAt != nil
	joined := r.ReplayAcceptedAt != nil && r.DirectFinalizedAt != nil
	if !validLifecycleEvent(r.FirstObservedAt, sampledAt, now) || !validLifecycleEvent(r.LastObservedAt, sampledAt, now) ||
		(r.AssemblyFirstAcceptedAt != nil) != ingress || (r.AssemblyFullAt != nil) != ingress ||
		!ingress && r.ReplayAcceptedAt == nil && r.DirectFinalizedAt == nil ||
		(r.FinalityKind == nil) != (r.DirectFinalizedAt == nil) || (r.FinalityBeforeReplay != nil) != joined {
		return p, false
	}
	if r.FinalityKind != nil {
		kind := *r.FinalityKind
		if kind != "fast" && kind != "slow" && kind != "genesis" {
			return p, false
		}
		p.FinalityKind = &kind
	}
	for _, pair := range []struct {
		source *time.Time
		target **time.Time
	}{
		{r.AssemblyFirstAcceptedAt, &p.AssemblyFirstAcceptedAt}, {r.AssemblyFullAt, &p.AssemblyFullAt},
		{r.ReadyAt, &p.ReadyAt}, {r.ReplayAcceptedAt, &p.ReplayAcceptedAt}, {r.DirectFinalizedAt, &p.DirectFinalizedAt},
	} {
		if pair.source != nil {
			if !validLifecycleEvent(*pair.source, sampledAt, now) {
				return p, false
			}
			at := pair.source.UTC()
			*pair.target = &at
		}
	}
	for _, interval := range []struct {
		source *uint64
		target **uint64
		bound  bool
	}{
		{r.AssemblyFirstToFullNS, &p.AssemblyFirstToFullNS, ingress},
		{r.FullToReadyNS, &p.FullToReadyNS, ingress},
		{r.ReadyToReplayNS, &p.ReadyToReplayNS, ingress && r.ReplayAcceptedAt != nil},
		{r.ReplayToFinalityNS, &p.ReplayToFinalityNS, joined},
		{r.FinalityToReplayNS, &p.FinalityToReplayNS, joined},
	} {
		if interval.source != nil {
			if !interval.bound || *interval.source >= math.MaxInt64 {
				return p, false
			}
			ns := *interval.source
			*interval.target = &ns
		}
	}
	if joined {
		before := *r.FinalityBeforeReplay
		if before && r.ReplayToFinalityNS != nil || !before && r.FinalityToReplayNS != nil {
			return p, false
		}
		p.FinalityBeforeReplay = &before
	}
	return p, true
}

// validLifecycleEvent does not lower-bound historical UTC by session dates:
// the source checks its monotonic session before stripping clocks. Callback
// dates can arrive out of order, and wall steps do not invalidate measured spans.
func validLifecycleEvent(at, sampledAt, now time.Time) bool {
	return validPublicTime(at, now) && !at.After(sampledAt.Add(time.Second))
}
