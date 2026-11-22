package dashboard

import (
	"encoding/json"
	"math"
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestPublicLeaderProjection(t *testing.T) {
	now := time.Now().UTC()
	fixture := func() Snapshot {
		s := publicTestSnapshot(now)
		s.Replay.StartedAt = now.Add(-time.Hour)
		s.Replay.LeaderOutcomes = &LeaderOutcomes{SampledAt: now, Provenance: "selected_and_folded", Identity: s.Consensus.Voting.Identity,
			HasWindow: true, WindowStart: 1<<53 + 1 - 511, WindowEnd: 1<<53 + 1,
			UnknownScheduleSlots: 512, UnknownAncestrySlots: 512, CoverageIncomplete: true}
		return s
	}
	s := fixture()
	p, raw := publicValue(t, &s, "fresh", now)
	if p.Replay.LeaderOutcomes == nil || p.Replay.LeaderOutcomes.WindowEnd != 1<<53+1 {
		t.Fatal("leader outcome missing or rounded")
	}
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	fields := wire["replay"].(map[string]any)["leader_outcomes"].(map[string]any)
	want := []string{"state", "sampled_at", "provenance", "identity", "has_window", "window_start", "window_end", "has_covered_range", "covered_start", "covered_end", "produced", "selected_fork_skipped", "scheduled_unknown", "unknown_schedule_slots", "unknown_ancestry_slots", "coverage_incomplete"}
	got := make([]string, 0, len(fields))
	for k := range fields {
		got = append(got, k)
	}
	sort.Strings(want)
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) || fields["window_end"] != "9007199254740993" {
		t.Fatalf("fields or precision: %v", fields)
	}
	s.Replay.LeaderOutcomes.SampledAt = now.Add(-31 * time.Second)
	p, _ = publicValue(t, &s, "fresh", now)
	if p.Replay.LeaderOutcomes == nil || p.Replay.LeaderOutcomes.State != "stale" {
		t.Fatal("leader freshness")
	}
	s = fixture()
	s.Consensus.Voting.SampledAt = now.Add(-31 * time.Second)
	p, _ = publicValue(t, &s, "fresh", now)
	if p.Replay.LeaderOutcomes == nil || p.Replay.LeaderOutcomes.State != "stale" {
		t.Fatal("voter freshness")
	}
	for _, mutate := range []func(*Snapshot){
		func(s *Snapshot) { s.Replay.LeaderOutcomes.Identity = "old" },
		func(s *Snapshot) { s.Replay.LeaderOutcomes.Provenance = "unknown" },
		func(s *Snapshot) { s.Replay.LeaderOutcomes.SampledAt = time.Time{} },
		func(s *Snapshot) { s.Consensus.Voting = nil },
		func(s *Snapshot) { s.Consensus.Voting.Identity = "old" },
		func(s *Snapshot) { s.Mode = "verifying" },
		func(s *Snapshot) { s.Alpenglow = false },
	} {
		s = fixture()
		mutate(&s)
		p, _ = publicValue(t, &s, "fresh", now)
		if p.Replay.LeaderOutcomes != nil {
			t.Fatal("invalid leader outcome published")
		}
	}
}

func TestLeaderOutcomeSnapshotBudgets(t *testing.T) {
	now := sizingTime(time.Now())
	s := fullTestSnapshot(now)
	b, err := json.Marshal(s)
	if err != nil || len(b) > MaxPrivateSnapshotBytes || s.Replay.LeaderOutcomes.WindowEnd != math.MaxUint64 {
		t.Fatalf("private bytes=%d err=%v", len(b), err)
	}
	s = publicTestSnapshot(now)
	s.Replay.LeaderOutcomes = &LeaderOutcomes{SampledAt: now, Provenance: "selected_and_folded", Identity: s.Consensus.Voting.Identity,
		HasWindow: true, WindowStart: math.MaxUint64 - 511, WindowEnd: math.MaxUint64,
		HasCoveredRange: true, CoveredStart: math.MaxUint64 - 511, CoveredEnd: math.MaxUint64,
		Produced: 256, SelectedForkSkipped: 256}
	s.Replay.CertificateOutcomes = validCertificateOutcomeFixture(now, s.Consensus.Voting.VoteAccount)
	s.Replay.Phase = "running"
	s.Replay.WorkCountersSaturated = false
	var acc Throughput
	var throughput PublicThroughput
	for i := 0; i < throughputSamples; i++ {
		s.SampledAt = now.Add(time.Duration(i) * time.Second)
		s.Replay.SampledAt = s.SampledAt
		s.Replay.ExecutedTransactions = uint64(i) * (math.MaxUint64 / throughputSamples)
		s.Replay.ExecutedComputeUnits = s.Replay.ExecutedTransactions
		throughput = acc.Observe(&s, "fresh", s.SampledAt)
	}
	b, err = encodePublicSnapshot(&s, "fresh", s.SampledAt, &throughput, nil, nil, nil)
	if err != nil || len(b) > MaxSnapshotBytes || len(throughput.History) != throughputSamples-1 {
		t.Fatalf("public bytes=%d err=%v", len(b), err)
	}
	var p PublicSnapshot
	if err := json.Unmarshal(b, &p); err != nil || p.Replay == nil || p.Replay.LeaderOutcomes == nil || p.Replay.CertificateOutcomes == nil {
		t.Fatalf("public outcome groups missing: %v", err)
	}
}
