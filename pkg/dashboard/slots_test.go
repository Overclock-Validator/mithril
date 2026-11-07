package dashboard

import (
	"math"
	"strings"
	"testing"
	"time"
)

func slotHistoryFixture(at time.Time) Snapshot {
	s := testSnapshot(at)
	s.Alpenglow = true
	end := uint64(math.MaxUint64)
	s.Replay = &Replay{StartedAt: at.Add(-time.Minute), SampledAt: at, DurableSlot: &end,
		SlotHistory: &SlotHistory{SampledAt: at, WindowStart: end - 31, WindowEnd: end, Outcomes: "p" + strings.Repeat("s", 30) + "?"}}
	return s
}

func TestPublicSlotHistoryBoundsPrecisionAndAge(t *testing.T) {
	at := time.Now().UTC()
	s := slotHistoryFixture(at)
	p, _ := publicValue(t, &s, "fresh", at)
	h := p.Replay.SlotHistory
	if h == nil || len(h.Slots) != 32 || h.Slots[0].Slot != math.MaxUint64-31 || h.Slots[31].Slot != math.MaxUint64 ||
		h.Slots[0].Outcome != "produced" || h.Slots[1].Outcome != "selected_fork_skipped" || h.Slots[31].Outcome != "unknown" ||
		!h.CoverageIncomplete || h.State != "fresh" || h.Provenance != "selected_and_folded" {
		t.Fatalf("bad bounded slot projection: %+v", h)
	}
	s.Replay.SlotHistory.Outcomes = strings.Repeat("p", 32)
	if h.Slots[31].Outcome != "unknown" {
		t.Fatal("projection aliases source")
	}
	if got := publicSlotHistory(&s, "fresh", at.Add(31*time.Second)); got == nil || got.State != "stale" {
		t.Fatal("stopped producer remained fresh")
	}
	if got := publicSlotHistory(&s, "stale", at); got == nil || got.State != "stale" {
		t.Fatal("stale source remained fresh")
	}
	p, _ = publicValue(t, &s, "unavailable", at)
	if p.Replay != nil {
		t.Fatal("unavailable source retained slot history")
	}
	s.Replay.SlotHistory.WindowStart, s.Replay.SlotHistory.WindowEnd, s.Replay.SlotHistory.Outcomes = 0, 0, "p"
	*s.Replay.DurableSlot = 0
	if got := publicSlotHistory(&s, "fresh", at); got == nil || len(got.Slots) != 1 || got.Slots[0].Slot != 0 {
		t.Fatal("known slot zero was discarded")
	}
}

func TestPublicSlotHistoryRejectsInvalidProducer(t *testing.T) {
	at := time.Now().UTC()
	cases := map[string]func(*Snapshot){
		"classic":       func(s *Snapshot) { s.Alpenglow = false },
		"missing":       func(s *Snapshot) { s.Replay.SlotHistory = nil },
		"not_durable":   func(s *Snapshot) { s.Replay.DurableSlot = nil },
		"future_slot":   func(s *Snapshot) { *s.Replay.DurableSlot-- },
		"reversed":      func(s *Snapshot) { s.Replay.SlotHistory.WindowEnd = 0 },
		"oversized":     func(s *Snapshot) { s.Replay.SlotHistory.WindowStart-- },
		"empty":         func(s *Snapshot) { s.Replay.SlotHistory.Outcomes = "" },
		"invalid_state": func(s *Snapshot) { s.Replay.SlotHistory.Outcomes = strings.Repeat("x", 32) },
		"no_sample":     func(s *Snapshot) { s.Replay.SlotHistory.SampledAt = time.Time{} },
		"future_sample": func(s *Snapshot) { s.Replay.SlotHistory.SampledAt = at.Add(2 * time.Second) },
		"prior_attempt": func(s *Snapshot) { s.Replay.StartedAt = at.Add(time.Second) },
		"prior_run":     func(s *Snapshot) { s.StartedAt = at },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			s := slotHistoryFixture(at)
			mutate(&s)
			if got := publicSlotHistory(&s, "fresh", at); got != nil {
				t.Fatalf("accepted invalid slot history: %+v", got)
			}
		})
	}
}
