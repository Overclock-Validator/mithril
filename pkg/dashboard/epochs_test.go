package dashboard

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"
)

func epochFixture(at time.Time) Snapshot {
	s := publicTestSnapshot(at)
	s.Replay.StartedAt = at.Add(-time.Minute)
	s.StartedAt = s.Replay.StartedAt.Add(-time.Minute)
	durable := uint64(39)
	s.Replay.DurableSlot = &durable
	s.Replay.EpochOutcomes = &EpochOutcomes{SampledAt: at, Identity: *s.IdentityKey, VoteAccount: s.Consensus.Voting.VoteAccount, Through: 39,
		Epochs: []EpochOutcome{
			{Epoch: 1, FirstSlot: 32, LastSlot: 63, Leaders: &EpochLeaderWindow{First: 32, Last: 39, Produced: 8}},
			{Epoch: 0, FirstSlot: 0, LastSlot: 31, Leaders: &EpochLeaderWindow{First: 0, Last: 31, Produced: 30, Skipped: 2, CompleteEpoch: true}, Rewards: &EpochRewardWindow{First: 0, Last: 31, Included: 29, Excluded: 2, Ineligible: 1, CompleteEpoch: true}},
		}}
	return s
}

func TestPublicEpochOutcomesCoverageAndBinding(t *testing.T) {
	at := time.Now().UTC()
	s := epochFixture(at)
	p, _ := publicValue(t, &s, "fresh", at)
	e := p.Replay.EpochOutcomes
	if e == nil || len(e.Epochs) != 2 || !e.Epochs[1].Rewards.CompleteEpoch || e.Epochs[0].Rewards != nil || e.Epochs[0].Leaders.CompleteEpoch {
		t.Fatalf("wrong coverage: %+v", e)
	}
	s.Replay.EpochOutcomes.Epochs[1].Rewards.Included = 0
	if e.Epochs[1].Rewards.Included != 29 {
		t.Fatal("reward projection aliases source")
	}
	s.Replay.EpochOutcomes.Epochs[1].Leaders.Produced = 0
	if e.Epochs[1].Leaders.Produced != 30 {
		t.Fatal("leader projection aliases source")
	}
	s = epochFixture(at)
	p, _ = publicValue(t, &s, "fresh", at.Add(31*time.Second))
	if p.Replay.EpochOutcomes.State != "stale" {
		t.Fatal("old epoch observation is fresh")
	}
	s.Replay.EpochOutcomes.SampledAt = at
	s.Consensus.Voting.SampledAt = at.Add(-time.Minute)
	p, _ = publicValue(t, &s, "fresh", at)
	if p.Replay.EpochOutcomes.State != "stale" {
		t.Fatal("stale voter binding promoted")
	}
}

func TestPublicEpochOutcomesRejectInvalidRangesAndCounts(t *testing.T) {
	at := time.Now().UTC()
	for name, change := range map[string]func(*Snapshot){
		"classic":                           func(s *Snapshot) { s.Alpenglow = false },
		"verifying":                         func(s *Snapshot) { s.Mode = "verifying" },
		"identity":                          func(s *Snapshot) { s.Replay.EpochOutcomes.Identity = "different" },
		"vote":                              func(s *Snapshot) { s.Replay.EpochOutcomes.VoteAccount = "different" },
		"future sample":                     func(s *Snapshot) { s.Replay.EpochOutcomes.SampledAt = at.Add(2 * time.Second) },
		"old attempt":                       func(s *Snapshot) { s.Replay.EpochOutcomes.SampledAt = s.Replay.StartedAt.Add(-time.Second) },
		"old run":                           func(s *Snapshot) { s.StartedAt = at },
		"future fold":                       func(s *Snapshot) { s.Replay.EpochOutcomes.Through = 40 },
		"no durable":                        func(s *Snapshot) { s.Replay.DurableSlot = nil },
		"too many epochs":                   func(s *Snapshot) { e := s.Replay.EpochOutcomes; e.Epochs = append(e.Epochs, e.Epochs[1]) },
		"wrong epoch":                       func(s *Snapshot) { s.Replay.EpochOutcomes.Epochs[1].Epoch = 1 },
		"overlapping epoch":                 func(s *Snapshot) { s.Replay.EpochOutcomes.Epochs[1].LastSlot = 32 },
		"wrong current epoch":               func(s *Snapshot) { s.Replay.EpochOutcomes.Epochs[0].FirstSlot = 40 },
		"leader overcount":                  func(s *Snapshot) { s.Replay.EpochOutcomes.Epochs[0].Leaders.Produced = 9 },
		"leader unknown schedule":           func(s *Snapshot) { s.Replay.EpochOutcomes.Epochs[0].Leaders.UnknownSchedule = 1 },
		"leader unknown ancestry":           func(s *Snapshot) { s.Replay.EpochOutcomes.Epochs[1].Leaders.UnknownAncestry = 1 },
		"partial known and unknown overlap": func(s *Snapshot) { s.Replay.EpochOutcomes.Epochs[0].Leaders.UnknownAncestry = 8 },
		"leader partial complete":           func(s *Snapshot) { s.Replay.EpochOutcomes.Epochs[0].Leaders.CompleteEpoch = true },
		"reward missing count":              func(s *Snapshot) { s.Replay.EpochOutcomes.Epochs[1].Rewards.Included-- },
		"reward overflow":                   func(s *Snapshot) { s.Replay.EpochOutcomes.Epochs[1].Rewards.Included = math.MaxUint64 },
		"reward unknown complete":           func(s *Snapshot) { w := s.Replay.EpochOutcomes.Epochs[1].Rewards; w.Included--; w.Unknown++ },
		"reward reversed":                   func(s *Snapshot) { s.Replay.EpochOutcomes.Epochs[1].Rewards.First = 32 },
		"reward too recent": func(s *Snapshot) {
			s.Replay.EpochOutcomes.Epochs[0].Rewards = &EpochRewardWindow{First: 32, Last: 32, Included: 1}
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := epochFixture(at)
			change(&s)
			p, _ := publicValue(t, &s, "fresh", at)
			if p.Replay.EpochOutcomes != nil {
				t.Fatalf("invalid epoch data published: %+v", p.Replay.EpochOutcomes)
			}
		})
	}
}

func TestEpochCollectorStaleAndReplacement(t *testing.T) {
	path, body := fixtureFeed(t)
	c := newCollector(path)
	c.public = true
	defer c.client.CloseIdleConnections()
	at := time.Now().UTC()
	s := epochFixture(at)
	read := func() PublicSnapshot {
		t.Helper()
		var p PublicSnapshot
		if err := json.Unmarshal(c.latest.Load().publicJSON, &p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	setFixture(t, body, s)
	c.poll(context.Background(), at)
	if read().Replay.EpochOutcomes.Epochs[1].Rewards.Included != 29 {
		t.Fatal("transport changed reward count")
	}
	body.Store(nil)
	c.poll(context.Background(), at.Add(time.Second))
	if read().Replay.EpochOutcomes.State != "stale" {
		t.Fatal("source loss retained fresh epoch")
	}
	s.RunID = "replacement"
	s.SampledAt = at.Add(time.Millisecond)
	s.Replay.EpochOutcomes = nil
	setFixture(t, body, s)
	c.poll(context.Background(), at.Add(2*time.Second))
	if read().Replay.EpochOutcomes != nil {
		t.Fatal("replacement reused prior epoch evidence")
	}
}
