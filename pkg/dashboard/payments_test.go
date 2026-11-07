package dashboard

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"
)

func paymentFixture(at time.Time) Snapshot {
	s := epochFixture(at)
	s.Replay.PaymentOutcomes = &PaymentOutcomes{SampledAt: at, Identity: *s.IdentityKey, VoteAccount: s.Consensus.Voting.VoteAccount,
		FirstCarrier: 32, LastCarrier: 39, Carriers: 8, Known: 8,
		Totals: &PaymentTotals{Fees: 100, FeeBurned: 20, IncineratorDeposits: 5, IdentityFeeDeposits: 7, VoteFeeDeposits: 3, OtherLeaderFeeDeposits: 11, VotingRewardsPaid: math.MaxUint64}}
	return s
}

func TestPublicPaymentOutcomesExactReceiptsAndCoverage(t *testing.T) {
	at := time.Now().UTC()
	s := paymentFixture(at)
	p, body := advancedPublicValue(t, &s, "fresh", at)
	got := p.Replay.PaymentOutcomes
	if got == nil || got.Totals == nil || *got.Totals != PublicPaymentTotals(*s.Replay.PaymentOutcomes.Totals) {
		t.Fatalf("receipt values changed: %+v", got)
	}
	var roundTrip PublicSnapshot
	if err := json.Unmarshal(body, &roundTrip); err != nil || roundTrip.Replay.PaymentOutcomes.Totals.VotingRewardsPaid != math.MaxUint64 {
		t.Fatalf("uint64 receipt lost precision: %v", err)
	}
	s.Replay.PaymentOutcomes.Totals.VotingRewardsPaid = 0
	if got.Totals.VotingRewardsPaid != math.MaxUint64 {
		t.Fatal("projection aliases receipt")
	}
	s = paymentFixture(at)
	s.Replay.PaymentOutcomes.Known, s.Replay.PaymentOutcomes.Unknown = 7, 1
	s.Replay.PaymentOutcomes.CoverageIncomplete = true
	p, _ = advancedPublicValue(t, &s, "fresh", at)
	if p.Replay.PaymentOutcomes.Totals == nil || !p.Replay.PaymentOutcomes.CoverageIncomplete {
		t.Fatal("known partial sum disappeared")
	}
	s.Replay.PaymentOutcomes.Totals = nil // Overflow cannot be presented as zero.
	p, _ = advancedPublicValue(t, &s, "fresh", at)
	if p.Replay.PaymentOutcomes == nil || p.Replay.PaymentOutcomes.Totals != nil {
		t.Fatal("unknown totals became money")
	}
	p, _ = advancedPublicValue(t, &s, "fresh", at.Add(31*time.Second))
	if p.Replay.PaymentOutcomes.State != "stale" {
		t.Fatal("old receipts became current")
	}
}

func TestPublicPaymentOutcomesRejectContradictions(t *testing.T) {
	at := time.Now().UTC()
	for name, mutate := range map[string]func(*Snapshot){
		"classic":           func(s *Snapshot) { s.Alpenglow = false },
		"nonvoting":         func(s *Snapshot) { s.Mode = "verifying" },
		"identity":          func(s *Snapshot) { s.Replay.PaymentOutcomes.Identity = "other" },
		"vote":              func(s *Snapshot) { s.Replay.PaymentOutcomes.VoteAccount = "other" },
		"future":            func(s *Snapshot) { s.Replay.PaymentOutcomes.SampledAt = at.Add(2 * time.Second) },
		"old attempt":       func(s *Snapshot) { s.Replay.PaymentOutcomes.SampledAt = s.Replay.StartedAt.Add(-time.Second) },
		"old run":           func(s *Snapshot) { s.StartedAt = at },
		"no durable":        func(s *Snapshot) { s.Replay.DurableSlot = nil },
		"not folded":        func(s *Snapshot) { s.Replay.PaymentOutcomes.LastCarrier = 40 },
		"reversed":          func(s *Snapshot) { s.Replay.PaymentOutcomes.FirstCarrier = 40 },
		"missing":           func(s *Snapshot) { s.Replay.PaymentOutcomes.Carriers = 0 },
		"too many":          func(s *Snapshot) { s.Replay.PaymentOutcomes.Carriers = 513 },
		"range too small":   func(s *Snapshot) { s.Replay.PaymentOutcomes.FirstCarrier = 33 },
		"range gap":         func(s *Snapshot) { s.Replay.PaymentOutcomes.FirstCarrier = 31 },
		"overlap":           func(s *Snapshot) { s.Replay.PaymentOutcomes.Unknown = 1 },
		"count overflow":    func(s *Snapshot) { s.Replay.PaymentOutcomes.Known = math.MaxUint64 },
		"missing counts":    func(s *Snapshot) { s.Replay.PaymentOutcomes.Known-- },
		"false complete":    func(s *Snapshot) { v := s.Replay.PaymentOutcomes; v.Known--; v.Unknown++ },
		"burn":              func(s *Snapshot) { s.Replay.PaymentOutcomes.Totals.FeeBurned = 101 },
		"duplicate deposit": func(s *Snapshot) { s.Replay.PaymentOutcomes.Totals.IdentityFeeDeposits = 79 },
		"deposit overflow":  func(s *Snapshot) { s.Replay.PaymentOutcomes.Totals.IdentityFeeDeposits = math.MaxUint64 },
		"amount without known": func(s *Snapshot) {
			v := s.Replay.PaymentOutcomes
			v.Known = 0
			v.Unknown = 8
			v.CoverageIncomplete = true
		},
		"missing amount complete": func(s *Snapshot) { s.Replay.PaymentOutcomes.Totals = nil },
	} {
		t.Run(name, func(t *testing.T) {
			s := paymentFixture(at)
			mutate(&s)
			p, _ := advancedPublicValue(t, &s, "fresh", at)
			if p.Replay.PaymentOutcomes != nil {
				t.Fatalf("invalid payments published: %+v", p.Replay.PaymentOutcomes)
			}
		})
	}
}

func TestPaymentCollectorSourceLossAndReplacement(t *testing.T) {
	path, body := fixtureFeed(t)
	c := newCollector(path)
	c.public = true
	defer c.client.CloseIdleConnections()
	at := time.Now().UTC()
	s := paymentFixture(at)
	read := func() PublicSnapshot {
		t.Helper()
		var p PublicSnapshot
		if err := json.Unmarshal(c.latest.Load().advancedPublicJSON, &p); err != nil {
			t.Fatal(err)
		}
		return p
	}
	setFixture(t, body, s)
	c.poll(context.Background(), at)
	if read().Replay.PaymentOutcomes.Totals.IdentityFeeDeposits != 7 {
		t.Fatal("transport changed receipts")
	}
	body.Store(nil)
	c.poll(context.Background(), at.Add(time.Second))
	if read().Replay.PaymentOutcomes.State != "stale" {
		t.Fatal("source loss retained current receipts")
	}
	s.RunID = "replacement"
	s.SampledAt = at.Add(time.Millisecond)
	s.Replay.PaymentOutcomes = nil
	setFixture(t, body, s)
	c.poll(context.Background(), at.Add(2*time.Second))
	if read().Replay.PaymentOutcomes != nil {
		t.Fatal("old payments survived replacement")
	}
}
