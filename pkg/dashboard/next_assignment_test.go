package dashboard

import (
	"encoding/json"
	"math"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestPublicNextKnownAssignment(t *testing.T) {
	now := time.Now().UTC()
	s := publicTestSnapshot(now)
	s.Mode, s.Consensus.Voting = "verifying", nil
	s.Replay.StartedAt = now.Add(-time.Hour)
	slot := uint64(1<<53 + 1)
	a := &NextKnownAssignment{SampledAt: now, Identity: *s.IdentityKey, ReferenceSlot: slot, SearchedThrough: math.MaxUint64, NextKnownSlot: &slot}
	s.Replay.NextKnownAssignment = a
	p, raw := publicValue(t, &s, "fresh", now)
	if p.Replay.NextKnownAssignment == nil || *p.Replay.NextKnownAssignment.NextKnownSlot != slot {
		t.Fatal("identity-bound schedule observation missing on nonvoting node")
	}
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	fields := wire["replay"].(map[string]any)["next_known_assignment"].(map[string]any)
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	want := []string{"identity", "next_known_slot", "reference_slot", "sampled_at", "searched_through_slot", "state"}
	if !reflect.DeepEqual(keys, want) || fields["next_known_slot"] != "9007199254740993" || fields["searched_through_slot"] != "18446744073709551615" {
		t.Fatalf("keys or precision: %v", fields)
	}
	private, err := json.Marshal(a)
	if err != nil {
		t.Fatal(err)
	}
	var privateFields map[string]any
	if err := json.Unmarshal(private, &privateFields); err != nil || len(privateFields) != 5 {
		t.Fatalf("private DTO keys: %s, %v", private, err)
	}
	// No hit remains null, not slot zero or a claim of no assignment.
	a.NextKnownSlot = nil
	p, _ = publicValue(t, &s, "fresh", now)
	if p.Replay.NextKnownAssignment == nil || p.Replay.NextKnownAssignment.NextKnownSlot != nil {
		t.Fatal("unknown converted into absence or zero")
	}
	a.SampledAt = now.Add(-31 * time.Second)
	p, _ = publicValue(t, &s, "fresh", now)
	if p.Replay.NextKnownAssignment == nil || p.Replay.NextKnownAssignment.State != "stale" {
		t.Fatal("sample timestamp did not expire")
	}
	for _, bad := range []func(){
		func() { a.Identity = "previous identity" },
		func() { s.IdentityKey = nil },
		func() { a.SampledAt = time.Time{} },
		func() { a.SampledAt = now.Add(2 * time.Second) },
	} {
		identity := "current identity"
		s.IdentityKey = &identity
		a.Identity, a.SampledAt = identity, now
		bad()
		p, _ = publicValue(t, &s, "fresh", now)
		if p.Replay.NextKnownAssignment != nil {
			t.Fatal("invalid identity/time binding published")
		}
	}
	p, _ = publicValue(t, &s, "unavailable", now)
	if p.Replay != nil {
		t.Fatal("unavailable source retained observation")
	}
}

func TestPublicNextKnownAssignmentRejectsOutOfRangeSlot(t *testing.T) {
	now := time.Now().UTC()
	s := publicTestSnapshot(now)
	s.Replay.StartedAt = now.Add(-time.Hour)
	for name, mutate := range map[string]func(*NextKnownAssignment){
		"before range": func(a *NextKnownAssignment) { a.NextKnownSlot = new(uint64(9)) },
		"after range":  func(a *NextKnownAssignment) { a.NextKnownSlot = new(uint64(21)) },
		"reversed":     func(a *NextKnownAssignment) { a.SearchedThrough = 9 },
		"old attempt":  func(a *NextKnownAssignment) { a.SampledAt = now.Add(-2 * time.Hour) },
	} {
		t.Run(name, func(t *testing.T) {
			a := &NextKnownAssignment{SampledAt: now, Identity: *s.IdentityKey, ReferenceSlot: 10, SearchedThrough: 20, NextKnownSlot: new(uint64(10))}
			mutate(a)
			s.Replay.NextKnownAssignment = a
			p, _ := publicValue(t, &s, "fresh", now)
			if p.Replay.NextKnownAssignment != nil {
				t.Fatal("invalid assignment was published")
			}
		})
	}
}

func TestNextKnownAssignmentWithFullPublicHistoryFitsBudget(t *testing.T) {
	now := sizingTime(time.Now())
	s := publicTestSnapshot(now)
	s.Replay.NextKnownAssignment.Identity = *s.IdentityKey
	s.Replay.CertificateOutcomes = validCertificateOutcomeFixture(now, s.Consensus.Voting.VoteAccount)
	s.Replay.LeaderOutcomes.Provenance, s.Replay.LeaderOutcomes.Identity = "selected_and_folded", *s.IdentityKey
	l := s.Replay.LeaderOutcomes
	l.HasWindow, l.WindowStart, l.WindowEnd = true, math.MaxUint64-511, math.MaxUint64
	l.HasCoveredRange, l.CoveredStart, l.CoveredEnd = true, l.WindowStart, l.WindowEnd
	l.Produced, l.SelectedForkSkipped, l.ScheduledUnknown = 256, 256, 0
	l.UnknownScheduleSlots, l.UnknownAncestrySlots = 0, 0
	s.Replay.Phase, s.Replay.WorkCountersSaturated = "running", false
	var acc Throughput
	var throughput PublicThroughput
	for i := 0; i < throughputSamples; i++ {
		s.SampledAt = now.Add(time.Duration(i) * time.Second)
		s.Replay.SampledAt = s.SampledAt
		s.Replay.ExecutedTransactions = uint64(i) * (math.MaxUint64 / throughputSamples)
		s.Replay.ExecutedComputeUnits = s.Replay.ExecutedTransactions
		throughput = acc.Observe(&s, "fresh", s.SampledAt)
	}
	b, err := encodePublicSnapshot(&s, "fresh", s.SampledAt, &throughput, nil, nil, nil)
	if err != nil || len(b) > MaxSnapshotBytes || len(throughput.History) != throughputSamples-1 {
		t.Fatalf("public history and next assignment: bytes=%d err=%v", len(b), err)
	}
	var p PublicSnapshot
	if err := json.Unmarshal(b, &p); err != nil || p.Replay.NextKnownAssignment == nil || p.Replay.NextKnownAssignment.NextKnownSlot == nil || p.Replay.CertificateOutcomes == nil || p.Replay.LeaderOutcomes == nil {
		t.Fatalf("full public groups missing: %v", err)
	}
	t.Logf("all outcome groups plus public history: %d/%d bytes", len(b), MaxSnapshotBytes)
}
