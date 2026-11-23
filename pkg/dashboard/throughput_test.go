package dashboard

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

func TestThroughputRejectsReplayBeyondClockTolerance(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := throughputFixture(now)
	var acc Throughput
	acc.Observe(&s, "fresh", now)
	now = now.Add(time.Second)
	s.SampledAt = now.Add(time.Second)
	s.Replay.SampledAt = now.Add(2 * time.Second)
	s.Replay.ExecutedTransactions++
	if got := acc.Observe(&s, "fresh", now); got.State != "unavailable" || got.Samples != 0 {
		t.Fatalf("future replay observation accepted: %+v", got)
	}
	acc = Throughput{}
	s = throughputFixture(now)
	acc.Observe(&s, "fresh", now)
	now = now.Add(time.Second)
	s.SampledAt = now
	s.Replay.SampledAt = now.Add(time.Second)
	s.Replay.ExecutedTransactions++
	if got := acc.Observe(&s, "fresh", now); got.State != "available" {
		t.Fatalf("observation at tolerance rejected: %+v", got)
	}
}

func TestThroughputBoundedAndDetached(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := throughputFixture(at)
	var acc Throughput
	var v PublicThroughput
	for i := 0; i < 40; i++ {
		s.SampledAt = at.Add(time.Duration(i) * time.Second)
		s.Replay.SampledAt = s.SampledAt
		s.Replay.ExecutedTransactions = uint64(i)
		v = acc.Observe(&s, "fresh", s.SampledAt)
	}
	if v.Samples != 16 || len(v.History) != 15 || v.DurationSeconds != 15 || *v.Transactions != 15 {
		t.Fatalf("window not bounded: %+v", v)
	}
	v.History[0].Transactions = 999
	*v.Transactions = 999
	*v.WindowStart = time.Time{}
	again := acc.Observe(&s, "fresh", s.SampledAt)
	if again.History[0].Transactions != 1 || *again.Transactions != 15 || again.WindowStart.IsZero() {
		t.Fatalf("view aliases internal state: %+v", again)
	}
}

func throughputFixture(at time.Time) Snapshot {
	s := testSnapshot(at)
	s.Replay = &Replay{Attempt: 1, StartedAt: at.Add(-time.Second), SampledAt: at, Phase: "running"}
	return s
}

func TestThroughputCountsCumulativeWorkAndPreciseDeltas(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := throughputFixture(at)
	s.Replay.ExecutedTransactions = 1<<53 + 9
	s.Replay.ExecutedComputeUnits = 1<<53 + 100
	var acc Throughput
	first := acc.Observe(&s, "fresh", at)
	if first.State != "unavailable" || first.Samples != 1 || first.TransactionsPerSecond != nil {
		t.Fatalf("baseline: %+v", first)
	}
	// Several completions and a rewind occurred between polls. Only the cumulative
	// successful-work totals, not the last block or rewind count, determine rate.
	s.SampledAt = at.Add(2 * time.Second)
	s.Replay.SampledAt = s.SampledAt
	s.Replay.ExecutedTransactions += 7
	s.Replay.ExecutedComputeUnits += 11
	s.Replay.Rewinds = 1
	s.Replay.Completed = &CompletedBlock{Transactions: 2, ComputeUnits: 3}
	v := acc.Observe(&s, "fresh", s.SampledAt)
	if v.State != "available" || v.Samples != 2 || v.DurationSeconds != 2 || *v.Transactions != 7 || *v.ComputeUnits != 11 || *v.TransactionsPerSecond != 3.5 || *v.ComputeUnitsPerSecond != 5.5 {
		t.Fatalf("wrong cumulative rate: %+v", v)
	}
	if len(v.History) != 1 || v.History[0].Transactions != 7 || !v.WindowStart.Equal(at) || !v.WindowEnd.Equal(s.SampledAt) {
		t.Fatalf("wrong window: %+v", v)
	}
	encoded, err := json.Marshal(v)
	if err != nil || !strings.Contains(string(encoded), `"transactions":"7"`) {
		t.Fatalf("integer was not encoded as a decimal string: %s, %v", encoded, err)
	}
	// A measured skipped-slot interval with unchanged work is a real zero.
	s.SampledAt = s.SampledAt.Add(time.Second)
	s.Replay.SampledAt = s.SampledAt
	s.Replay.SkippedSlots++
	v = acc.Observe(&s, "fresh", s.SampledAt)
	if len(v.History) != 2 || v.History[1].Transactions != 0 || v.History[1].TransactionsPerSecond != 0 {
		t.Fatalf("not a measured zero: %+v", v)
	}
	duplicate := acc.Observe(&s, "fresh", s.SampledAt)
	if duplicate.Samples != v.Samples {
		t.Fatalf("duplicate added sample: %+v", duplicate)
	}
}

func TestPublicThroughputUsesReplayObservationTimes(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := throughputFixture(at)
	c := newCollector("/unused")
	defer c.client.CloseIdleConnections()
	c.public = true
	read := func() PublicThroughput {
		t.Helper()
		encoded, err := c.encode(&s, "fresh", s.SampledAt)
		if err != nil {
			t.Fatal(err)
		}
		var snapshot PublicSnapshot
		if err := json.Unmarshal(encoded.publicJSON, &snapshot); err != nil || snapshot.Throughput == nil {
			t.Fatalf("public throughput: %v", err)
		}
		return *snapshot.Throughput
	}
	read()
	s.SampledAt = at.Add(time.Second)
	if v := read(); v.State != "unavailable" || v.Samples != 1 {
		t.Fatalf("duplicate source became a public interval: %+v", v)
	}
	s.Replay.SampledAt = at.Add(2 * time.Second)
	s.SampledAt = at.Add(2500 * time.Millisecond)
	s.Replay.ExecutedTransactions = 8
	if v := read(); v.State != "available" || v.DurationSeconds != 2 || *v.TransactionsPerSecond != 4 || !v.WindowEnd.Equal(s.Replay.SampledAt) {
		t.Fatalf("public rate does not use source times: %+v", v)
	}
}

func TestThroughputResetsAndUnavailableCoverage(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name        string
		change      func(*Snapshot)
		state       string
		wantSamples int
	}{
		{"run", func(s *Snapshot) { s.RunID = "new" }, "fresh", 1},
		{"identity", func(s *Snapshot) { x := "identity"; s.IdentityKey = &x }, "fresh", 1},
		{"genesis", func(s *Snapshot) { x := "genesis"; s.GenesisHash = &x }, "fresh", 1},
		{"attempt", func(s *Snapshot) { s.Replay.Attempt++ }, "fresh", 1},
		{"attempt-start", func(s *Snapshot) { s.Replay.StartedAt = s.Replay.StartedAt.Add(time.Millisecond) }, "fresh", 1},
		{"counter-reset", func(s *Snapshot) { s.Replay.ExecutedTransactions = 0 }, "fresh", 1},
		{"compute-reset", func(s *Snapshot) { s.Replay.ExecutedComputeUnits = 0 }, "fresh", 1},
		{"saturated", func(s *Snapshot) { s.Replay.WorkCountersSaturated = true }, "fresh", 0},
		{"not-running", func(s *Snapshot) { s.Replay.Phase = "stopped" }, "fresh", 0},
		{"missing-replay", func(s *Snapshot) { s.Replay = nil }, "fresh", 0},
		{"stale-source", func(*Snapshot) {}, "stale", 0},
		{"feed-gap", func(s *Snapshot) { s.SampledAt = s.SampledAt.Add(4 * time.Second); s.Replay.SampledAt = s.SampledAt }, "fresh", 1},
		{"activity-gap", func(s *Snapshot) { s.SampledAt = s.SampledAt.Add(4 * time.Second) }, "fresh", 0},
		{"clock-reversal", func(s *Snapshot) {
			s.SampledAt = s.SampledAt.Add(-1500 * time.Millisecond)
			s.Replay.SampledAt = s.SampledAt
		}, "fresh", 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := throughputFixture(at)
			s.Replay.ExecutedTransactions = 10
			s.Replay.ExecutedComputeUnits = 10
			var acc Throughput
			acc.Observe(&s, "fresh", at)
			s.SampledAt = at.Add(time.Second)
			s.Replay.SampledAt = s.SampledAt
			test.change(&s)
			v := acc.Observe(&s, test.state, s.SampledAt)
			if v.State != "unavailable" || v.Samples != test.wantSamples || v.TransactionsPerSecond != nil {
				t.Fatalf("reset: %+v", v)
			}
		})
	}
	s := throughputFixture(at)
	var acc Throughput
	acc.Observe(&s, "fresh", at)
	s.Replay.ExecutedTransactions++
	if v := acc.Observe(&s, "fresh", at); v.Samples != 0 {
		t.Fatalf("changed duplicate accepted: %+v", v)
	}
	s.Replay.ExecutedTransactions = math.MaxUint64
	s.Replay.WorkCountersSaturated = true
	if v := acc.Observe(&s, "fresh", at); v.Samples != 0 {
		t.Fatalf("overflow accepted: %+v", v)
	}
	s = throughputFixture(at)
	acc.Observe(&s, "fresh", at)
	if v := acc.Observe(&s, "fresh", at.Add(4*time.Second)); v.Samples != 0 {
		t.Fatalf("unchanged feed retained coverage past replay gap: %+v", v)
	}
}
