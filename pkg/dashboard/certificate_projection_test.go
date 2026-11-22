package dashboard

import (
	"encoding/json"
	"math"
	"reflect"
	"sort"
	"testing"
	"time"
)

func validCertificateOutcomeFixture(at time.Time, vote string) *CertificateOutcomes {
	return &CertificateOutcomes{SampledAt: at, Provenance: "selected_and_folded", VoteAccount: vote, CoverageIncomplete: true,
		HasWindow: true, FirstCarrier: math.MaxUint64 - 511, LastCarrier: math.MaxUint64, Carriers: 512, Pending: 1024, Dropped: math.MaxUint64,
		RewardIncluded: 128, RewardExcluded: 128, RewardUnknown: 128, RewardIneligible: 128,
		FinalFast: 256, FinalSlow: 256, FastMember: 128, FastNonmember: 64, FastMembershipUnknown: 64}
}

func TestPublicCertificateProjection(t *testing.T) {
	now := time.Now().UTC()
	s := publicTestSnapshot(now)
	s.Replay.StartedAt = now.Add(-time.Hour)
	s.Replay.CertificateOutcomes = validCertificateOutcomeFixture(now, s.Consensus.Voting.VoteAccount)
	c := s.Replay.CertificateOutcomes
	c.Dropped = 1<<53 + 1
	p, raw := publicValue(t, &s, "fresh", now)
	if p.Replay.CertificateOutcomes == nil || p.Replay.CertificateOutcomes.Dropped != 1<<53+1 {
		t.Fatal("missing or rounded outcome")
	}
	var wire map[string]any
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	fields := wire["replay"].(map[string]any)["certificate_outcomes"].(map[string]any)
	want := []string{"state", "sampled_at", "provenance", "vote_account", "coverage_incomplete", "has_window", "first_carrier", "last_carrier", "carriers", "gaps", "slots_without_carrier", "pending", "dropped_this_attempt", "reward_included", "reward_excluded", "reward_unknown", "reward_ineligible", "final_fast", "final_slow", "fast_member", "fast_nonmember", "fast_membership_unknown", "final_duplicates", "final_variants", "final_conflict"}
	got := make([]string, 0, len(fields))
	for key := range fields {
		got = append(got, key)
	}
	sort.Strings(want)
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) || fields["dropped_this_attempt"] != "9007199254740993" {
		t.Fatalf("unexpected fields or precision: %v %v", got, fields["dropped_this_attempt"])
	}
	if len(raw) > MaxSnapshotBytes {
		t.Fatalf("public snapshot exceeds budget: %d", len(raw))
	}
	private, err := json.Marshal(s)
	if err != nil || len(private) > MaxPrivateSnapshotBytes {
		t.Fatalf("private snapshot exceeds budget: %d, %v", len(private), err)
	}

	c.SampledAt = now.Add(-31 * time.Second)
	p, _ = publicValue(t, &s, "fresh", now)
	if p.Replay.CertificateOutcomes == nil || p.Replay.CertificateOutcomes.State != "stale" {
		t.Fatal("outcome timestamp did not expire independently")
	}
	c.SampledAt = now
	s.Consensus.Voting.SampledAt = now.Add(-31 * time.Second)
	p, _ = publicValue(t, &s, "fresh", now)
	if p.Replay.CertificateOutcomes == nil || p.Replay.CertificateOutcomes.State != "stale" {
		t.Fatal("stale voter binding was presented as fresh")
	}
	s.Consensus.Voting.SampledAt = now
	for _, mutate := range []func(){
		func() { c.VoteAccount = "different" },
		func() { c.Provenance = "unknown" },
		func() { c.SampledAt = time.Time{} },
		func() { s.Consensus.Voting = nil },
		func() { s.Consensus.Voting.Identity = "other" },
		func() { s.Mode = "verifying" },
		func() { s.Alpenglow = false },
	} {
		fixture := publicTestSnapshot(now)
		s = fixture
		s.Replay.StartedAt = now.Add(-time.Hour)
		s.Replay.CertificateOutcomes = validCertificateOutcomeFixture(now, s.Consensus.Voting.VoteAccount)
		c = s.Replay.CertificateOutcomes
		mutate()
		p, _ = publicValue(t, &s, "fresh", now)
		if p.Replay.CertificateOutcomes != nil {
			t.Fatal("unbound or invalid outcome was published")
		}
	}
}

func TestCertificateOutcomeAndFullPublicHistoryFitBudget(t *testing.T) {
	at := sizingTime(time.Now())
	s := publicTestSnapshot(at)
	s.Replay.CertificateOutcomes = validCertificateOutcomeFixture(at, s.Consensus.Voting.VoteAccount)
	s.Replay.Phase, s.Replay.WorkCountersSaturated = "running", false
	var acc Throughput
	var throughput PublicThroughput
	for i := 0; i < throughputSamples; i++ {
		s.SampledAt = at.Add(time.Duration(i) * time.Second)
		s.Replay.SampledAt = s.SampledAt
		s.Replay.ExecutedTransactions = uint64(i) * (math.MaxUint64 / throughputSamples)
		s.Replay.ExecutedComputeUnits = s.Replay.ExecutedTransactions
		throughput = acc.Observe(&s, "fresh", s.SampledAt)
	}
	b, err := encodePublicSnapshot(&s, "fresh", s.SampledAt, &throughput, nil, nil, nil)
	if err != nil || len(b) > MaxSnapshotBytes || len(throughput.History) != throughputSamples-1 {
		t.Fatalf("public history plus outcomes: bytes=%d err=%v", len(b), err)
	}
	var p PublicSnapshot
	if err := json.Unmarshal(b, &p); err != nil || p.Replay.CertificateOutcomes == nil {
		t.Fatalf("outcome missing: %v", err)
	}
	t.Logf("public history plus outcomes: %d/%d bytes", len(b), MaxSnapshotBytes)
}

func TestCertificateOutcomePrivateFullWidth(t *testing.T) {
	s := fullTestSnapshot(sizingTime(time.Now()))
	b, err := json.Marshal(s)
	if err != nil || len(b) > MaxPrivateSnapshotBytes {
		t.Fatalf("private snapshot: bytes=%d err=%v", len(b), err)
	}
	if s.Replay.CertificateOutcomes.RewardIncluded != math.MaxUint64 {
		t.Fatal("fixture did not fill all outcome counters")
	}
}
