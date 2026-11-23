package dashboard

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

func activityFixture(at time.Time) Snapshot {
	s := slotHistoryFixture(at)
	s.Replay.ChainActivity = &ChainActivity{SampledAt: at, Available: true, AnchorSlot: math.MaxUint64 - 10, LastSlot: math.MaxUint64,
		StartTime: 1000, EndTime: 1010, Blocks: 9, Transactions: 20, NonVoteTransactions: 18, FailedTransactions: 3, FailedNonVote: 2, ComputeUnits: 3000}
	return s
}

func TestPublicChainActivityExactCountsAndDistinctTime(t *testing.T) {
	at := time.Now().UTC()
	s := activityFixture(at)
	p, raw := publicValue(t, &s, "fresh", at)
	a := p.Replay.ChainActivity
	if a == nil || a.Interval == nil || a.Provenance != "selected_and_folded" || a.TimeSource != "bank_clock" || a.State != "fresh" {
		t.Fatalf("missing chain interval: %+v", a)
	}
	i := *a.Interval
	if i.LastSlot != math.MaxUint64 || i.Transactions != 20 || i.TransactionsPerSec != 2 || i.NonVotePerSec != 1.8 || i.FailedPerSec != .3 || i.ComputeUnitsPerSec != 300 ||
		!strings.Contains(string(raw), `"last_slot":"18446744073709551615"`) {
		t.Fatalf("count/rate/precision mismatch: %+v", i)
	}
	s.Replay.ChainActivity.Transactions = 99
	if *a.Interval != i {
		t.Fatal("projection aliases producer")
	}
	if got := publicChainActivity(&s, "fresh", at.Add(31*time.Second)); got.State != "stale" {
		t.Fatal("stopped observation remains fresh")
	}
	if got := publicChainActivity(&s, "stale", at); got.State != "stale" {
		t.Fatal("stale source promoted")
	}
	s.Replay.ChainActivity.Available = false
	if got := publicChainActivity(&s, "fresh", at); got == nil || got.Interval != nil {
		t.Fatal("unknown interval became zero activity")
	}
	p, _ = publicValue(t, &s, "unavailable", at)
	if p.Replay != nil {
		t.Fatal("unavailable source retained activity")
	}
}

func TestPublicChainActivityRejectsInvalidIntervals(t *testing.T) {
	at := time.Now().UTC()
	for name, change := range map[string]func(*Snapshot){
		"classic":                func(s *Snapshot) { s.Alpenglow = false },
		"not durable":            func(s *Snapshot) { s.Replay.DurableSlot = nil },
		"future slot":            func(s *Snapshot) { *s.Replay.DurableSlot-- },
		"empty interval":         func(s *Snapshot) { s.Replay.ChainActivity.StartTime = 1010 },
		"negative time":          func(s *Snapshot) { s.Replay.ChainActivity.StartTime = -1 },
		"reversed slots":         func(s *Snapshot) { s.Replay.ChainActivity.LastSlot = 1 },
		"too many blocks":        func(s *Snapshot) { s.Replay.ChainActivity.Blocks = 512 },
		"more blocks than slots": func(s *Snapshot) { s.Replay.ChainActivity.Blocks = 11 },
		"no blocks":              func(s *Snapshot) { s.Replay.ChainActivity.Blocks = 0 },
		"invalid nonvote":        func(s *Snapshot) { s.Replay.ChainActivity.NonVoteTransactions = 21 },
		"invalid failures":       func(s *Snapshot) { s.Replay.ChainActivity.FailedTransactions = 21 },
		"invalid failed nonvote": func(s *Snapshot) { s.Replay.ChainActivity.FailedNonVote = 4 },
		"invalid failed votes":   func(s *Snapshot) { s.Replay.ChainActivity.FailedNonVote = 0 },
		"future sample":          func(s *Snapshot) { s.Replay.ChainActivity.SampledAt = at.Add(2 * time.Second) },
		"old attempt":            func(s *Snapshot) { s.Replay.ChainActivity.SampledAt = s.Replay.StartedAt.Add(-time.Second) },
		"old run":                func(s *Snapshot) { s.StartedAt = at },
	} {
		t.Run(name, func(t *testing.T) {
			s := activityFixture(at)
			change(&s)
			if got := publicChainActivity(&s, "fresh", at); got != nil {
				t.Fatalf("invalid projection: %+v", got)
			}
		})
	}
}

func TestChainActivityCollectorSourceLossAndReplacement(t *testing.T) {
	path, body := fixtureFeed(t)
	c := newCollector(path)
	c.public = true
	defer c.client.CloseIdleConnections()
	at := time.Now().UTC()
	s := activityFixture(at)
	setFixture(t, body, s)
	c.poll(context.Background(), at)
	var first PublicSnapshot
	if err := json.Unmarshal(c.latest.Load().publicJSON, &first); err != nil {
		t.Fatal(err)
	}
	if first.Replay.ChainActivity.Interval.Transactions != 20 {
		t.Fatal("collector lost exact interval")
	}
	body.Store(nil)
	c.poll(context.Background(), at.Add(time.Second))
	var stale PublicSnapshot
	if err := json.Unmarshal(c.latest.Load().publicJSON, &stale); err != nil {
		t.Fatal(err)
	}
	if stale.Replay.ChainActivity.State != "stale" {
		t.Fatal("broken feed kept activity fresh")
	}
	s.RunID = "replacement"
	s.SampledAt = at.Add(time.Millisecond)
	s.Replay.ChainActivity = nil
	setFixture(t, body, s)
	c.poll(context.Background(), at.Add(2*time.Second))
	var next PublicSnapshot
	if err := json.Unmarshal(c.latest.Load().publicJSON, &next); err != nil {
		t.Fatal(err)
	}
	if next.Replay.ChainActivity != nil {
		t.Fatal("old node's interval survived replacement")
	}
}
