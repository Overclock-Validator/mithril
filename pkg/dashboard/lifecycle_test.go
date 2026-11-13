package dashboard

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
)

func lifecycleFixture(at time.Time) Snapshot {
	s := testSnapshot(at)
	genesis, identity := (solana.Hash{8}).String(), (solana.PublicKey{9}).String()
	s.GenesisHash, s.IdentityKey = &genesis, &identity
	first, full, ready, replayed, finalized := at.Add(-5*time.Second), at.Add(-4*time.Second), at.Add(-3*time.Second), at.Add(-2*time.Second), at.Add(-time.Second)
	fast, slow, genesisKind := "fast", "slow", "genesis"
	second, three, zero := uint64(time.Second), uint64(3*time.Second), uint64(0)
	before, after := true, false
	l := &BlockLifecycle{SampledAt: at, StartedAt: at.Add(-time.Minute), UpdatedAt: &finalized,
		Retained: BlockLifecycleCapacity, Evicted: math.MaxUint64, RowCount: BlockLifecycleVisibleRows}
	l.Rows[0] = &BlockLifecycleRow{BlockID: (solana.Hash{1}).String(), FirstObservedAt: first, LastObservedAt: finalized,
		AssemblyFirstAcceptedAt: &first, AssemblyFullAt: &full, ReadyAt: &ready, ReplayAcceptedAt: &replayed,
		DirectFinalizedAt: &finalized, FinalityKind: &fast, AssemblyFirstToFullNS: &second, FullToReadyNS: &second,
		ReadyToReplayNS: &second, ReplayToFinalityNS: &second, FinalityBeforeReplay: &after}
	l.Rows[1] = &BlockLifecycleRow{Slot: math.MaxUint64, BlockID: (solana.Hash{2}).String(), FirstObservedAt: first, LastObservedAt: replayed,
		ReplayAcceptedAt: &replayed, DirectFinalizedAt: &first, FinalityKind: &slow, FinalityBeforeReplay: &before, FinalityToReplayNS: &three}
	l.Rows[2] = &BlockLifecycleRow{BlockID: (solana.Hash{3}).String(), FirstObservedAt: finalized, LastObservedAt: finalized,
		DirectFinalizedAt: &finalized, FinalityKind: &genesisKind}
	l.Rows[3] = &BlockLifecycleRow{Slot: math.MaxUint64, BlockID: (solana.Hash{4}).String(), FirstObservedAt: ready, LastObservedAt: ready,
		AssemblyFirstAcceptedAt: &first, AssemblyFullAt: &first, ReadyAt: &ready, AssemblyFirstToFullNS: &zero}
	fault := false
	s.Consensus = &Consensus{SampledAt: at, SafetyFault: &fault, BlockLifecycle: l}
	return s
}

func TestPublicBlockLifecycleExactHistoryAndClockSemantics(t *testing.T) {
	at := time.Now().UTC()
	s := lifecycleFixture(at)
	p, raw := advancedPublicValue(t, &s, "fresh", at)
	l := p.Consensus.BlockLifecycle
	if l == nil || l.ActivityScope != "engine_session" || l.Capacity != 128 || l.Retained != 128 || l.Evicted != math.MaxUint64 || len(l.Rows) != 4 || l.State != "fresh" {
		t.Fatalf("lifecycle scope/bounds lost: %+v", l)
	}
	if l.Rows[0].Slot != 0 || *l.Rows[0].FinalityBeforeReplay || l.Rows[1].Slot != math.MaxUint64 || !*l.Rows[1].FinalityBeforeReplay ||
		l.Rows[2].ReplayAcceptedAt != nil || l.Rows[2].FinalityBeforeReplay != nil || l.Rows[3].FullToReadyNS != nil || *l.Rows[3].AssemblyFirstToFullNS != 0 ||
		!strings.Contains(string(raw), `"slot":"18446744073709551615"`) || !strings.Contains(string(raw), `"assembly_first_to_full_ns":"0"`) {
		t.Fatal("zero, unknown, exact identity or finality direction lost")
	}
	// UTC clock steps are not an authority for source-monotonic intervals or
	// ordering. This history remains valid even before the wall session marker.
	old := s.StartedAt.Add(-time.Hour)
	s.Consensus.BlockLifecycle.Rows[0].DirectFinalizedAt = &old
	s.Consensus.BlockLifecycle.Rows[0].AssemblyFullAt = &old
	s.Consensus.BlockLifecycle.Rows[0].FirstObservedAt = old
	s.Consensus.BlockLifecycle.Rows[0].WallOrderConflict = true
	s.Consensus.BlockLifecycle.UpdatedAt = &old
	p, _ = advancedPublicValue(t, &s, "fresh", at)
	r := p.Consensus.BlockLifecycle.Rows[0]
	if !r.WallOrderConflict || *r.FinalityBeforeReplay || *r.ReplayToFinalityNS != uint64(time.Second) || *r.AssemblyFirstToFullNS != uint64(time.Second) || !r.DirectFinalizedAt.Equal(old) {
		t.Fatal("projection recomputed monotonic facts from UTC or rejected historical dates")
	}
	// No replay, voter or current identity attribution is required. Changing the
	// header identity does not erase or relabel an engine-session history.
	other := (solana.PublicKey{10}).String()
	s.IdentityKey, s.Mode, s.Replay, s.Consensus.VotingEnabled = &other, "validator", nil, true
	fault := true
	s.Consensus.SafetyFault = &fault
	p, _ = advancedPublicValue(t, &s, "stale", at)
	if got := p.Consensus.BlockLifecycle; got == nil || got.State != "stale" || !got.SafetyFault || len(got.Rows) != 4 || got.ActivityScope != "engine_session" {
		t.Fatal("faulted/stale session history was lost or made current")
	}
	s.IdentityKey = nil
	p, _ = advancedPublicValue(t, &s, "fresh", at.Add(31*time.Second))
	if p.Consensus.BlockLifecycle == nil || p.Consensus.BlockLifecycle.State != "stale" {
		t.Fatal("owner-sample age or passive-node support was lost")
	}
}

func TestPublicBlockLifecycleRejectsInvalidBindingAndRows(t *testing.T) {
	at := time.Now().UTC()
	for name, mutate := range map[string]func(*Snapshot){
		"run":                     func(s *Snapshot) { s.RunID = "" },
		"non-alpenglow":           func(s *Snapshot) { s.Alpenglow = false },
		"unknown-genesis":         func(s *Snapshot) { s.GenesisHash = nil },
		"zero-genesis":            func(s *Snapshot) { v := (solana.Hash{}).String(); s.GenesisHash = &v },
		"bad-identity":            func(s *Snapshot) { v := "not a key"; s.IdentityKey = &v },
		"zero-start":              func(s *Snapshot) { s.Consensus.BlockLifecycle.StartedAt = time.Time{} },
		"old-session":             func(s *Snapshot) { s.Consensus.BlockLifecycle.StartedAt = s.StartedAt.Add(-time.Nanosecond) },
		"future-session":          func(s *Snapshot) { s.Consensus.BlockLifecycle.StartedAt = at.Add(time.Second) },
		"different-parent-sample": func(s *Snapshot) { s.Consensus.BlockLifecycle.SampledAt = at.Add(time.Nanosecond) },
		"future-node-relative":    func(s *Snapshot) { s.SampledAt = at.Add(-time.Second - time.Nanosecond) },
		"future-collector-relative": func(s *Snapshot) {
			s.Consensus.SampledAt = at.Add(time.Second + time.Nanosecond)
			s.Consensus.BlockLifecycle.SampledAt = s.Consensus.SampledAt
		},
		"negative-retained": func(s *Snapshot) { s.Consensus.BlockLifecycle.Retained = -1 },
		"capacity":          func(s *Snapshot) { s.Consensus.BlockLifecycle.Retained = 129 },
		"row-count":         func(s *Snapshot) { s.Consensus.BlockLifecycle.RowCount = 5 },
		"missing-row":       func(s *Snapshot) { s.Consensus.BlockLifecycle.Rows[0] = nil },
		"nonnull-padding": func(s *Snapshot) {
			s.Consensus.BlockLifecycle.Retained = 1
			s.Consensus.BlockLifecycle.RowCount = 1
			s.Consensus.BlockLifecycle.Evicted = 0
		},
		"evicted-unfilled": func(s *Snapshot) { s.Consensus.BlockLifecycle.Retained = 127 },
		"missing-update":   func(s *Snapshot) { s.Consensus.BlockLifecycle.UpdatedAt = nil },
		"future-update": func(s *Snapshot) {
			v := at.Add(time.Second + time.Nanosecond)
			s.Consensus.BlockLifecycle.UpdatedAt = &v
		},
		"duplicate-id": func(s *Snapshot) { s.Consensus.BlockLifecycle.Rows[1] = s.Consensus.BlockLifecycle.Rows[0] },
		"zero-id":      func(s *Snapshot) { s.Consensus.BlockLifecycle.Rows[0].BlockID = (solana.Hash{}).String() },
		"bad-id":       func(s *Snapshot) { s.Consensus.BlockLifecycle.Rows[0].BlockID = "bankhash?" },
		"zero-first":   func(s *Snapshot) { s.Consensus.BlockLifecycle.Rows[0].FirstObservedAt = time.Time{} },
		"future-event": func(s *Snapshot) {
			v := at.Add(time.Second + time.Nanosecond)
			s.Consensus.BlockLifecycle.Rows[0].ReadyAt = &v
		},
		"partial-ingress":          func(s *Snapshot) { s.Consensus.BlockLifecycle.Rows[0].AssemblyFullAt = nil },
		"missing-kind":             func(s *Snapshot) { s.Consensus.BlockLifecycle.Rows[0].FinalityKind = nil },
		"unknown-kind":             func(s *Snapshot) { v := "notarize"; s.Consensus.BlockLifecycle.Rows[0].FinalityKind = &v },
		"missing-finality":         func(s *Snapshot) { s.Consensus.BlockLifecycle.Rows[0].DirectFinalizedAt = nil },
		"missing-direction":        func(s *Snapshot) { s.Consensus.BlockLifecycle.Rows[0].FinalityBeforeReplay = nil },
		"direction-without-replay": func(s *Snapshot) { v := false; s.Consensus.BlockLifecycle.Rows[2].FinalityBeforeReplay = &v },
		"interval-without-ingress": func(s *Snapshot) { v := uint64(0); s.Consensus.BlockLifecycle.Rows[1].AssemblyFirstToFullNS = &v },
		"interval-without-replay":  func(s *Snapshot) { v := uint64(0); s.Consensus.BlockLifecycle.Rows[3].ReadyToReplayNS = &v },
		"saturated-interval":       func(s *Snapshot) { v := uint64(math.MaxInt64); s.Consensus.BlockLifecycle.Rows[0].ReadyToReplayNS = &v },
		"max-uint-interval": func(s *Snapshot) {
			v := uint64(math.MaxUint64)
			s.Consensus.BlockLifecycle.Rows[0].ReadyToReplayNS = &v
		},
		"wrong-direction-interval":    func(s *Snapshot) { v := uint64(0); s.Consensus.BlockLifecycle.Rows[0].FinalityToReplayNS = &v },
		"opposite-direction-interval": func(s *Snapshot) { v := uint64(0); s.Consensus.BlockLifecycle.Rows[1].ReplayToFinalityNS = &v },
	} {
		t.Run(name, func(t *testing.T) {
			s := lifecycleFixture(at)
			mutate(&s)
			if got := publicBlockLifecycle(&s, "fresh", at); got != nil {
				t.Fatalf("invalid lifecycle accepted: %+v", got)
			}
		})
	}
}

func TestBlockLifecycleFutureToleranceDoesNotChain(t *testing.T) {
	at := time.Now().UTC()
	s := lifecycleFixture(at.Add(-10 * time.Second))
	l := s.Consensus.BlockLifecycle
	l.SampledAt = s.SampledAt.Add(time.Second)
	s.Consensus.SampledAt = l.SampledAt
	event := s.SampledAt.Add(time.Second)
	l.Rows[0].ReadyAt = &event
	if publicBlockLifecycle(&s, "fresh", at) == nil {
		t.Fatal("inclusive one-second node tolerance rejected")
	}
	event = event.Add(time.Nanosecond)
	if publicBlockLifecycle(&s, "fresh", at) != nil {
		t.Fatal("node→owner→event tolerances admitted an event beyond node+1s")
	}
}

func TestPublicBlockLifecycleEmptyOldProducerAndDetachedAllowlist(t *testing.T) {
	at := time.Now().UTC()
	s := lifecycleFixture(at)
	p := publicBlockLifecycle(&s, "fresh", at)
	private := s.Consensus.BlockLifecycle
	*private.Rows[0].AssemblyFirstToFullNS = 99
	*private.Rows[0].ReadyAt = at
	*private.Rows[0].FinalityBeforeReplay = true
	*private.Rows[0].FinalityKind = "changed"
	*private.UpdatedAt = at
	if *p.Rows[0].AssemblyFirstToFullNS != uint64(time.Second) || p.Rows[0].ReadyAt.Equal(at) || *p.Rows[0].FinalityBeforeReplay || *p.Rows[0].FinalityKind != "fast" || p.UpdatedAt.Equal(at) {
		t.Fatal("public lifecycle aliases private storage")
	}
	b, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	var group map[string]json.RawMessage
	if err := json.Unmarshal(b, &group); err != nil {
		t.Fatal(err)
	}
	assertKeys := func(value map[string]json.RawMessage, expected string) {
		t.Helper()
		keys := strings.Fields(expected)
		if len(value) != len(keys) {
			t.Fatalf("allowlist length: %v", value)
		}
		for _, key := range keys {
			if _, ok := value[key]; !ok {
				t.Fatalf("missing allowlisted %s", key)
			}
		}
	}
	assertKeys(group, "state sampled_at activity_scope started_at updated_at capacity retained evicted safety_fault rows")
	var rows []map[string]json.RawMessage
	if err := json.Unmarshal(group["rows"], &rows); err != nil {
		t.Fatal(err)
	}
	assertKeys(rows[0], "slot block_id first_observed_at last_observed_at assembly_first_accepted_at assembly_full_at ready_at replay_accepted_at direct_finalized_at finality_kind assembly_first_to_full_ns full_to_ready_ns ready_to_replay_ns replay_to_finality_ns finality_to_replay_ns finality_before_replay wall_order_conflict")
	s.Consensus.BlockLifecycle = &BlockLifecycle{SampledAt: at, StartedAt: at.Add(-time.Second)}
	public, raw := advancedPublicValue(t, &s, "fresh", at)
	if got := public.Consensus.BlockLifecycle; got == nil || got.UpdatedAt != nil || got.Retained != 0 || len(got.Rows) != 0 || !strings.Contains(string(raw), `"rows":[]`) {
		t.Fatal("enabled empty recorder fabricated history or became disabled")
	}
	s.Consensus.BlockLifecycle = nil
	public, raw = advancedPublicValue(t, &s, "fresh", at)
	if public.Consensus.BlockLifecycle != nil || strings.Contains(string(raw), "block_lifecycle") {
		t.Fatal("old producer lost omission compatibility")
	}
}

func TestBlockLifecycleCollectorSourceLossAndNewSession(t *testing.T) {
	path, body := fixtureFeed(t)
	c := newCollector(path)
	c.public = true
	defer c.client.CloseIdleConnections()
	at := time.Now().UTC()
	s := lifecycleFixture(at)
	setFixture(t, body, s)
	c.poll(context.Background(), at)
	read := func() PublicSnapshot {
		t.Helper()
		var p PublicSnapshot
		if err := json.Unmarshal(c.latest.Load().advancedPublicJSON, &p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	if read().Consensus.BlockLifecycle == nil {
		t.Fatal("collector lost lifecycle")
	}
	body.Store(nil)
	c.poll(context.Background(), at.Add(2*time.Second))
	if p := read(); p.Consensus == nil || p.Consensus.BlockLifecycle.State != "stale" {
		t.Fatal("source loss did not mark history stale")
	}
	c.poll(context.Background(), at.Add(time.Minute))
	if p := read(); p.Consensus == nil || p.Consensus.BlockLifecycle.State != "stale" {
		t.Fatal("old history did not remain explicitly stale")
	}
	if p, _ := advancedPublicValue(t, &s, "unavailable", at); p.Consensus != nil {
		t.Fatal("unavailable source retained lifecycle")
	}
	s = lifecycleFixture(at.Add(2 * time.Minute))
	s.RunID, s.StartedAt = "new-run", at.Add(time.Minute)
	s.Consensus.BlockLifecycle = &BlockLifecycle{SampledAt: s.SampledAt, StartedAt: s.StartedAt.Add(time.Second)}
	setFixture(t, body, s)
	c.poll(context.Background(), s.SampledAt)
	p := read()
	if p.Node.RunID != "new-run" || p.Consensus.BlockLifecycle == nil || p.Consensus.BlockLifecycle.Retained != 0 || !reflect.DeepEqual(p.Consensus.BlockLifecycle.Rows, []PublicBlockLifecycleRow{}) {
		t.Fatal("new engine/run retained old rows")
	}
}
