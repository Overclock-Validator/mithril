package dashboard

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

func rewardHealthFixture(at time.Time) Snapshot {
	s := publicTestSnapshot(at)
	s.Phase, s.Replay.Phase = "replaying", "running"
	s.Replay.StartedAt = at.Add(-time.Minute)
	s.StartedAt = at.Add(-2 * time.Minute)
	first, last, durable := uint64(0), uint64(127), uint64(135)
	s.Replay.DurableSlot = &durable
	s.Replay.RewardHealth = &RewardHealth{SampledAt: at, Identity: *s.IdentityKey, VoteAccount: s.Consensus.Voting.VoteAccount,
		Through: durable, WindowFirst: &first, WindowLast: &last, Included: 1, Excluded: 127, CoverageComplete: true, Health: "included_recently",
		LatestResolved: &RewardTarget{Slot: last, Epoch: 3, Outcome: "excluded"}, LatestIncluded: &RewardTarget{Slot: 0, Epoch: 0, Outcome: "included"}}
	return s
}

func TestPublicRewardHealthBindingPrecisionAndFreshness(t *testing.T) {
	at := time.Now().UTC()
	s := rewardHealthFixture(at)
	p, _ := publicValue(t, &s, "fresh", at)
	h := p.Replay.RewardHealth
	if h == nil || h.Health != "included_recently" || h.LatestIncluded == nil || h.LatestIncluded.Slot != 0 || *h.WindowFirst != 0 {
		t.Fatalf("known target zero or health lost: %+v", h)
	}
	s.Replay.RewardHealth.LatestIncluded.Slot = 1
	*s.Replay.RewardHealth.WindowFirst = 1
	if h.LatestIncluded.Slot != 0 || *h.WindowFirst != 0 {
		t.Fatal("reward projection aliases source")
	}
	for _, state := range []string{"fresh", "stale"} {
		s = rewardHealthFixture(at)
		p, _ = publicValue(t, &s, state, at.Add(31*time.Second))
		if p.Replay.RewardHealth.State != "stale" || p.Replay.RewardHealth.Health != "unknown" || p.Replay.RewardHealth.LatestIncluded == nil {
			t.Fatal("old evidence either looked healthy or lost historical context")
		}
	}
	s = rewardHealthFixture(at)
	s.Consensus.Voting.SampledAt = at.Add(-time.Minute)
	p, _ = publicValue(t, &s, "fresh", at)
	if p.Replay.RewardHealth.Health != "unknown" || p.Replay.RewardHealth.State != "stale" {
		t.Fatal("stale voter binding promoted")
	}
	s = rewardHealthFixture(at)
	s.Replay.Phase = "stopped"
	p, _ = publicValue(t, &s, "fresh", at)
	if p.Replay.RewardHealth.Health != "unknown" {
		t.Fatal("stopped replay retained an active health claim")
	}
	s = rewardHealthFixture(at)
	huge := s.Replay.RewardHealth
	huge.Through = math.MaxUint64
	*s.Replay.DurableSlot = huge.Through
	*huge.WindowLast, *huge.WindowFirst = huge.Through-8, huge.Through-135
	huge.LatestResolved.Slot = *huge.WindowLast
	huge.LatestIncluded.Slot = *huge.WindowFirst
	_, raw := publicValue(t, &s, "fresh", at)
	if !strings.Contains(string(raw), `"through_slot":"18446744073709551615"`) {
		t.Fatal("fold frontier lost uint64 precision")
	}
}

func TestPublicRewardHealthRejectsInvalidEvidence(t *testing.T) {
	at := time.Now().UTC()
	for name, mutate := range map[string]func(*Snapshot){
		"role":                       func(s *Snapshot) { s.Mode = "verifying" },
		"classic":                    func(s *Snapshot) { s.Alpenglow = false },
		"attempt":                    func(s *Snapshot) { s.Replay.Attempt = 0 },
		"identity":                   func(s *Snapshot) { s.Replay.RewardHealth.Identity = "other" },
		"vote":                       func(s *Snapshot) { s.Replay.RewardHealth.VoteAccount = "other" },
		"durability":                 func(s *Snapshot) { *s.Replay.DurableSlot = 134 },
		"future":                     func(s *Snapshot) { s.Replay.RewardHealth.SampledAt = at.Add(time.Second) },
		"old attempt":                func(s *Snapshot) { s.Replay.RewardHealth.SampledAt = s.Replay.StartedAt.Add(-time.Nanosecond) },
		"paired bounds":              func(s *Snapshot) { s.Replay.RewardHealth.WindowLast = nil },
		"wrong bounds":               func(s *Snapshot) { *s.Replay.RewardHealth.WindowFirst = 1 },
		"overflow":                   func(s *Snapshot) { s.Replay.RewardHealth.Included = math.MaxUint64 },
		"partition":                  func(s *Snapshot) { s.Replay.RewardHealth.Excluded-- },
		"false negative":             func(s *Snapshot) { s.Replay.RewardHealth.Health = "no_inclusion_128" },
		"future reference":           func(s *Snapshot) { s.Replay.RewardHealth.LatestResolved.Slot++ },
		"wrong reference outcome":    func(s *Snapshot) { s.Replay.RewardHealth.LatestIncluded.Outcome = "excluded" },
		"missing inclusion":          func(s *Snapshot) { s.Replay.RewardHealth.LatestIncluded = nil },
		"impossible inclusion count": func(s *Snapshot) { h := s.Replay.RewardHealth; h.Included = 2; h.Excluded = 126 },
		"impossible resolved count":  func(s *Snapshot) { h := s.Replay.RewardHealth; h.LatestResolved.Slot = 126; h.Health = "unknown" },
		"future inclusion epoch":     func(s *Snapshot) { s.Replay.RewardHealth.LatestIncluded.Epoch = 4 },
		"false latest inclusion":     func(s *Snapshot) { s.Replay.RewardHealth.LatestResolved.Outcome = "included" },
		"unknown complete":           func(s *Snapshot) { h := s.Replay.RewardHealth; h.Excluded--; h.Unknown++ },
	} {
		t.Run(name, func(t *testing.T) {
			s := rewardHealthFixture(at)
			mutate(&s)
			p, _ := publicValue(t, &s, "fresh", at)
			if p.Replay != nil && p.Replay.RewardHealth != nil {
				t.Fatalf("invalid reward evidence published: %+v", p.Replay.RewardHealth)
			}
		})
	}
	// An old included reference is context only when the newest target is unknown.
	s := rewardHealthFixture(at)
	h := s.Replay.RewardHealth
	h.LatestResolved.Slot--
	h.Excluded--
	h.Unknown++
	h.CoverageComplete = false
	h.Health = "unknown"
	if p, _ := publicValue(t, &s, "fresh", at); p.Replay.RewardHealth == nil || p.Replay.RewardHealth.Health != "unknown" {
		t.Fatal("missing newest evidence was hidden by older inclusion")
	}
	h.LatestIncluded = nil
	h.Included = 0
	h.Excluded = 128
	h.Unknown = 0
	h.CoverageComplete = true
	h.LatestResolved.Slot++
	h.Health = "no_inclusion_128"
	if p, _ := publicValue(t, &s, "fresh", at); p.Replay.RewardHealth == nil || p.Replay.RewardHealth.Health != "no_inclusion_128" {
		t.Fatal("complete 128-target negative was lost")
	}
}

func TestRewardHealthCollectorSourceLossAndOldProducer(t *testing.T) {
	path, body := fixtureFeed(t)
	c := newCollector(path)
	c.public = true
	defer c.client.CloseIdleConnections()
	at := time.Now().UTC()
	s := rewardHealthFixture(at)
	read := func() *PublicRewardHealth {
		t.Helper()
		var p PublicSnapshot
		if err := json.Unmarshal(c.latest.Load().publicJSON, &p); err != nil {
			t.Fatal(err)
		}
		return p.Replay.RewardHealth
	}
	setFixture(t, body, s)
	c.poll(context.Background(), at)
	if read() == nil || read().Health != "included_recently" {
		t.Fatal("collector lost known membership")
	}
	body.Store(nil)
	c.poll(context.Background(), at.Add(time.Second))
	if read().Health != "unknown" || read().State != "stale" {
		t.Fatal("source loss retained a fresh health claim")
	}
	s.SampledAt = at.Add(2 * time.Second)
	s.Replay.RewardHealth = nil
	setFixture(t, body, s)
	c.poll(context.Background(), at.Add(2*time.Second))
	if read() != nil {
		t.Fatal("old producer inherited prior reward health")
	}
}
