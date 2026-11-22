package replay

import (
	"math"
	"testing"
	"time"
)

func TestProgressClearsForkAndAttemptState(t *testing.T) {
	var disabled *Progress
	disabled.begin(0, nil)
	disabled.phase("running")
	disabled.completed(CompletedBlock{}, 0, false)
	disabled.skipped(1, 0, false)
	disabled.rewound()
	disabled.restored(1, CertificateOutcomeSummary{})
	if _, ok := disabled.Snapshot(); ok {
		t.Fatal("disabled progress available")
	}
	p := &Progress{}
	p.begin(41, &ResumeState{ParentSlot: 40})
	if starting, _ := p.Snapshot(); starting.Frontier != 40 {
		t.Fatalf("start did not publish resume frontier: %+v", starting)
	}
	p.phase("running")
	p.completed(CompletedBlock{At: time.Now(), Slot: 41, Transactions: 3}, 40, true)
	first, _ := p.Snapshot()
	p.skipped(42, 40, true)
	p.settledOutcomes(40, CertificateOutcomeSummary{Pending: 2}, LeaderOutcomeSummary{}, SlotHistorySummary{},
		ChainActivitySummary{}, EpochOutcomeSummary{}, PaymentOutcomeSummary{}, RewardHealthSummary{})
	skipped, _ := p.Snapshot()
	p.rewound()
	rewound, _ := p.Snapshot()
	if !first.HasCompleted || first.Completed.Slot != 41 || rewound.HasCompleted || rewound.Rewinds != 1 || rewound.SkippedSlots != 1 || rewound.ExecutedBlocks != 1 {
		t.Fatalf("incorrect rewind: old=%+v next=%+v", first, rewound)
	}
	if first.Frontier != 41 || skipped.Frontier != 42 || skipped.Completed.Slot != 41 || rewound.Frontier != 0 {
		t.Fatalf("frontier not bound to replay observations: completed=%d skipped=%d rewinding=%d",
			first.Frontier, skipped.Frontier, rewound.Frontier)
	}
	if skipped.CertificateOutcomes.Pending != 2 || rewound.CertificateOutcomes.Pending != 0 {
		t.Fatal("rewind retained pending evidence from the old fork")
	}
	p.restored(40, CertificateOutcomeSummary{Pending: 1})
	if restored, _ := p.Snapshot(); restored.Frontier != 40 || restored.Rewinds != 1 || restored.HasCompleted || restored.CertificateOutcomes.Pending != 1 {
		t.Fatalf("rewind did not restore frontier independently of completion: %+v", restored)
	}
	p.begin(30, nil)
	next, _ := p.Snapshot()
	if next.Attempt != 2 || next.StartSlot != 30 || next.Frontier != 29 || next.HasCompleted || next.HasResumeParent || next.ExecutedBlocks != 0 || next.SkippedSlots != 0 || next.Rewinds != 0 || next.Phase != "starting" {
		t.Fatalf("attempt retained previous data: %+v", next)
	}
}

func TestProgressFoldPublishesFrontierWithoutNextBlock(t *testing.T) {
	p := &Progress{}
	p.begin(32, nil)
	p.completed(CompletedBlock{At: time.Now().Add(-time.Minute), Slot: 39}, 31, true)
	before, _ := p.Snapshot()
	for _, phase := range []string{"running", "halted"} {
		p.settledOutcomes(39, CertificateOutcomeSummary{}, LeaderOutcomeSummary{}, SlotHistorySummary{}, ChainActivitySummary{}, EpochOutcomeSummary{Through: 39}, PaymentOutcomeSummary{}, RewardHealthSummary{})
		if folded, _ := p.Snapshot(); !folded.UpdatedAt.After(before.UpdatedAt) {
			t.Fatal("fold retained old source timestamp")
		}
		p.phase(phase)
		s, _ := p.Snapshot()
		if !s.HasDurableSlot || s.DurableSlot != s.EpochOutcomes.Through || s.DurableSlot != 39 || s.ExecutedBlocks != 1 {
			t.Fatalf("fold requires a later block to publish: %+v", s)
		}
	}
	if before.DurableSlot != 31 || before.EpochOutcomes.Through != 0 {
		t.Fatal("fold mutated previous snapshot")
	}
	p.begin(40, nil)
	if s, _ := p.Snapshot(); s.HasDurableSlot || s.EpochOutcomes.Through != 0 {
		t.Fatal("new attempt retained folded observations")
	}
}

func TestProgressCountsCompletedLocalWork(t *testing.T) {
	p := &Progress{}
	p.begin(41, nil)
	p.completed(CompletedBlock{Slot: 41, Transactions: 3, ComputeUnits: 7}, 40, true)
	first, _ := p.Snapshot()
	p.completed(CompletedBlock{Slot: 42, Transactions: 5, ComputeUnits: 11}, 40, true)
	p.skipped(43, 40, true)
	p.phase("halted") // Failed work has no successful completion observation.
	p.rewound()
	s, _ := p.Snapshot()
	if s.ExecutedBlocks != 2 || s.ExecutedTransactions != 8 || s.ExecutedComputeUnits != 18 || s.WorkCountersSaturated {
		t.Fatalf("completed work lost or skip/failure added work: %+v", s)
	}
	p.completed(CompletedBlock{Slot: 41, Transactions: 3, ComputeUnits: 7}, 40, true)
	s, _ = p.Snapshot()
	if s.ExecutedBlocks != 3 || s.ExecutedTransactions != 11 || s.ExecutedComputeUnits != 25 {
		t.Fatalf("re-executed work not counted: %+v", s)
	}
	p.completed(CompletedBlock{Slot: 42}, 40, true)
	if empty, _ := p.Snapshot(); empty.ExecutedBlocks != 4 || empty.ExecutedTransactions != 11 || empty.ExecutedComputeUnits != 25 {
		t.Fatalf("empty completion changed work totals: %+v", empty)
	}
	if first.ExecutedBlocks != 1 || first.ExecutedTransactions != 3 || first.ExecutedComputeUnits != 7 {
		t.Fatalf("old snapshot changed: %+v", first)
	}
	s.ExecutedTransactions = 999
	if current, _ := p.Snapshot(); current.ExecutedTransactions != 11 {
		t.Fatalf("reader mutated published work: %+v", current)
	}
	p.begin(30, nil)
	if next, _ := p.Snapshot(); next.Attempt != 2 || next.ExecutedTransactions != 0 || next.ExecutedComputeUnits != 0 || next.WorkCountersSaturated {
		t.Fatalf("attempt retained work: %+v", next)
	}
}

func TestProgressWorkCountersSaturate(t *testing.T) {
	p := &Progress{}
	p.begin(1, nil)
	p.completed(CompletedBlock{Transactions: math.MaxUint64 - 1, ComputeUnits: math.MaxUint64 - 2}, 0, false)
	p.completed(CompletedBlock{Transactions: 1, ComputeUnits: 2}, 0, false)
	if s, _ := p.Snapshot(); s.ExecutedTransactions != math.MaxUint64 || s.ExecutedComputeUnits != math.MaxUint64 || s.WorkCountersSaturated {
		t.Fatalf("exact maximum incorrectly saturated: %+v", s)
	}
	p.completed(CompletedBlock{Transactions: 1}, 0, false)
	if s, _ := p.Snapshot(); s.ExecutedTransactions != math.MaxUint64 || s.ExecutedComputeUnits != math.MaxUint64 || !s.WorkCountersSaturated {
		t.Fatalf("transaction overflow wrapped: %+v", s)
	}
	p.rewound()
	p.completed(CompletedBlock{ComputeUnits: 1}, 0, false)
	if s, _ := p.Snapshot(); s.ExecutedTransactions != math.MaxUint64 || s.ExecutedComputeUnits != math.MaxUint64 || !s.WorkCountersSaturated {
		t.Fatalf("compute-unit overflow wrapped or saturation cleared: %+v", s)
	}
	p.begin(1, nil)
	if s, _ := p.Snapshot(); s.ExecutedTransactions != 0 || s.ExecutedComputeUnits != 0 || s.WorkCountersSaturated {
		t.Fatalf("attempt retained saturated counters: %+v", s)
	}
	p.completed(CompletedBlock{ComputeUnits: math.MaxUint64}, 0, false)
	p.completed(CompletedBlock{ComputeUnits: 1}, 0, false)
	if s, _ := p.Snapshot(); s.ExecutedTransactions != 0 || s.ExecutedComputeUnits != math.MaxUint64 || !s.WorkCountersSaturated {
		t.Fatalf("compute-unit overflow did not independently mark saturation: %+v", s)
	}
}

func TestProgressDistinguishesUnknownAndZeroDurableSlot(t *testing.T) {
	p := &Progress{}
	p.begin(1, nil)
	p.completed(CompletedBlock{At: time.Now(), Slot: 1}, 0, false)
	if s, _ := p.Snapshot(); s.HasDurableSlot {
		t.Fatal("missing durable bank became slot zero")
	}
	p.skipped(2, 0, true)
	if s, _ := p.Snapshot(); !s.HasDurableSlot || s.DurableSlot != 0 {
		t.Fatal("known durable slot zero became unavailable")
	}
}
