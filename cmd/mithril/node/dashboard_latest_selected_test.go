package node

import (
	"math"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

func TestLatestSelectedMeasurement(t *testing.T) {
	diagnostics := replayDetailSource(t, 0)
	s := replay.SelectedSlotSnapshot{RunID: diagnostics.RunID, Generation: diagnostics.Generation,
		Attempt: diagnostics.Attempt, Rewinds: diagnostics.Rewinds, StartedAt: diagnostics.StartedAt,
		ResetAt: diagnostics.ResetAt, UpdatedAt: replayDetailAt,
		Records: []replay.SelectedSlotRecord{{Gap: false, CertificateFoldEvidence: replay.CertificateFoldEvidence{
			Slot: 40, Bankhash: solana.Hash{1}, BlockID: solana.Hash{2}, HasBlockID: true},
			Facts:        replay.SelectedReplayFacts{ReplayResultAt: replayDetailAt.Add(time.Second), Epoch: 3},
			Transactions: sealevel.BankTransactionCounts{Total: 9, NonVote: 8, Failed: 2, NonVoteFailed: 1, ComputeUnits: 1234, Saturated: true}},
			{CertificateFoldEvidence: replay.CertificateFoldEvidence{Slot: 41}, Gap: true}}}
	value := latestSelectedMeasurement(s, diagnostics)
	if value == nil || value.Slot != 41 || !value.Gap || value.Facts != nil || value.Bankhash != nil || value.BlockID != nil {
		t.Fatal("newest gap was replaced by older facts or zero identifiers")
	}
	s.Records = s.Records[:1]
	value = latestSelectedMeasurement(s, diagnostics)
	if value == nil || value.Gap || value.Bankhash == nil || value.BlockID == nil || value.Facts == nil ||
		value.Facts.ExecutionLeader != nil || value.Facts.Transactions.Total != 9 || !value.Facts.Transactions.Saturated ||
		value.Facts.Transactions.ComputeUnits == nil || *value.Facts.Transactions.ComputeUnits != 1234 ||
		!value.ObservedAt.Equal(s.UpdatedAt) || !value.Facts.ReplayResultAt.Equal(s.Records[0].Facts.ReplayResultAt) {
		t.Fatal("known facts, independent dates or unknown leader were changed")
	}
	s.Records[0].Transactions.ComputeUnits = 0
	if *value.Facts.Transactions.ComputeUnits != 1234 || *latestSelectedMeasurement(s, diagnostics).Facts.Transactions.ComputeUnits != 0 {
		t.Fatal("compute units lost zero or alias the source")
	}
	s.Records[0].Facts.ExecutionLeader = solana.PublicKey{3}
	if value.Facts.ExecutionLeader != nil || latestSelectedMeasurement(s, diagnostics).Facts.ExecutionLeader == nil {
		t.Fatal("leader knownness or detached ownership was lost")
	}
	r := &s.Records[0]
	r.Facts.Economics = block.SelectedEconomics{FeesKnown: true, NativeCostKnown: true}
	r.Facts.Economics.FeeBurnKnown = true
	value = latestSelectedMeasurement(s, diagnostics)
	if value.Facts.Fees == nil || value.Facts.Fees.TotalLamports != 0 || value.Facts.Fees.BurnedLamports == nil || *value.Facts.Fees.BurnedLamports != 0 || value.Facts.LocalCost != nil {
		t.Fatal("known-zero fees or unavailable remote cost was lost")
	}
	r.Facts.Economics.FeeBurnLamports = 1
	if latestSelectedMeasurement(s, diagnostics).Facts.Fees.BurnedLamports != nil || *value.Facts.Fees.BurnedLamports != 0 {
		t.Fatal("invalid burn was displayed or detached value changed")
	}
	r.Facts.FromLocalProduction = true
	value = latestSelectedMeasurement(s, diagnostics)
	if value.Facts.LocalCost == nil || value.Facts.LocalCost.BlockCost != 0 {
		t.Fatal("known-zero local cost was lost")
	}
	r.Facts.Economics.Fees = block.SelectedFeeFacts{ExecutionLamports: math.MaxUint64, PriorityLamports: 1}
	r.Facts.Economics.NativeCost.BlockCost = 7
	if value.Facts.LocalCost.BlockCost != 0 || value.Facts.Fees.TotalLamports != 0 {
		t.Fatal("economic scalars alias their source")
	}
	value = latestSelectedMeasurement(s, diagnostics)
	if value.Facts.Fees != nil || value.Facts.LocalCost == nil || value.Facts.LocalCost.BlockCost != 7 {
		t.Fatal("invalid fees erased independent native cost or became zero")
	}
	for name, mutate := range map[string]func(*replay.SelectedSlotSnapshot){
		"run":        func(s *replay.SelectedSlotSnapshot) { s.RunID += "x" },
		"generation": func(s *replay.SelectedSlotSnapshot) { s.Generation++ },
		"attempt":    func(s *replay.SelectedSlotSnapshot) { s.Attempt++ },
		"rewind":     func(s *replay.SelectedSlotSnapshot) { s.Rewinds++ },
		"start":      func(s *replay.SelectedSlotSnapshot) { s.StartedAt = s.StartedAt.Add(time.Nanosecond) },
		"reset":      func(s *replay.SelectedSlotSnapshot) { s.ResetAt = s.ResetAt.Add(time.Nanosecond) },
		"empty":      func(s *replay.SelectedSlotSnapshot) { s.Records = nil },
		"oversize": func(s *replay.SelectedSlotSnapshot) {
			s.Records = make([]replay.SelectedSlotRecord, replay.SelectedSlotCapacity+1)
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := s
			mutate(&changed)
			if latestSelectedMeasurement(changed, diagnostics) != nil {
				t.Fatal("source binding or bound was ignored")
			}
		})
	}
}

func TestRecentSelectedMeasurement(t *testing.T) {
	d := replayDetailSource(t, 0)
	s := replay.SelectedSlotSnapshot{RunID: d.RunID, Generation: d.Generation, Attempt: d.Attempt, Rewinds: d.Rewinds,
		StartedAt: d.StartedAt, ResetAt: d.ResetAt, UpdatedAt: replayDetailAt,
		WindowEntries: 10, Records: make([]replay.SelectedSlotRecord, 10)}
	for i := range s.Records {
		s.Records[i] = replay.SelectedSlotRecord{CertificateFoldEvidence: replay.CertificateFoldEvidence{Slot: uint64(40 + i),
			Bankhash: solana.Hash{1}, BlockID: solana.Hash{2}, HasBlockID: true},
			Facts: replay.SelectedReplayFacts{ReplayResultAt: replayDetailAt.Add(-time.Duration(i) * time.Nanosecond), Epoch: 3,
				FromLocalProduction: true, Economics: block.SelectedEconomics{FeesKnown: true, NativeCostKnown: true}},
			Transactions: sealevel.BankTransactionCounts{Total: 1, NonVote: 1}}
	}
	s.Records[5].Gap, s.CoverageIncomplete = true, true
	v := recentSelectedMeasurement(s, d)
	if v == nil || len(v.Records) != 8 || v.Records[0].Slot != 42 || v.Records[7].Slot != 49 ||
		v.SourceRecords != 10 || v.WindowEntries != 10 || !v.CoverageIncomplete || !v.ObservedAt.Equal(s.UpdatedAt) {
		t.Fatal("recent subset, original window date or honest coverage lost")
	}
	if !v.Records[3].Gap || v.Records[3].Facts != nil {
		t.Fatal("gap was lost")
	}
	for _, mutate := range []func(*replay.SelectedSlotSnapshot){
		func(s *replay.SelectedSlotSnapshot) { s.RunID += "changed" },
		func(s *replay.SelectedSlotSnapshot) { s.Generation++ },
		func(s *replay.SelectedSlotSnapshot) { s.Attempt++ },
		func(s *replay.SelectedSlotSnapshot) { s.Rewinds++ },
		func(s *replay.SelectedSlotSnapshot) { s.ResetAt = s.ResetAt.Add(time.Second) },
		func(s *replay.SelectedSlotSnapshot) { s.StartedAt = s.StartedAt.Add(time.Second) },
		func(s *replay.SelectedSlotSnapshot) { s.WindowEntries++ },
		func(s *replay.SelectedSlotSnapshot) { s.OmittedFromWindow++ },
	} {
		bad := s
		mutate(&bad)
		if recentSelectedMeasurement(bad, d) != nil {
			t.Fatal("stale source/attempt/rewind or inconsistent coverage accepted")
		}
	}
}

func TestSelectedWindowMeasurement(t *testing.T) {
	d := replayDetailSource(t, 0)
	genesis := solana.Hash{9}.String()
	binding := dashboard.ReplayDetailBinding{RunID: d.RunID, NodeStartedAt: replayDetailAt,
		GenesisHash: &genesis, SourceGeneration: d.Generation, Attempt: d.Attempt, Rewinds: d.Rewinds,
		AttemptStartedAt: d.StartedAt, ResetAt: d.ResetAt}
	s := replay.SelectedSlotSnapshot{RunID: d.RunID, Generation: d.Generation, Attempt: d.Attempt, Rewinds: d.Rewinds,
		StartedAt: d.StartedAt, ResetAt: d.ResetAt, UpdatedAt: replayDetailAt,
		WindowEntries: 512, OmittedFromWindow: 256, Records: make([]replay.SelectedSlotRecord, 256)}
	for i := range s.Records {
		s.Records[i] = replay.SelectedSlotRecord{CertificateFoldEvidence: replay.CertificateFoldEvidence{Slot: uint64(i + 1),
			Bankhash: solana.Hash{1}, BlockID: solana.Hash{2}, HasBlockID: true},
			Facts:        replay.SelectedReplayFacts{ReplayResultAt: replayDetailAt, Epoch: 3},
			Transactions: sealevel.BankTransactionCounts{Total: 1, ComputeUnits: uint64(i)}}
	}
	s.Records[50].Gap = true
	w := selectedWindowMeasurement(s, d, binding, replayDetailAt)
	if w == nil || len(w.Records) != 256 || w.Records[0].Slot != 1 || w.Records[255].Slot != 256 ||
		w.WindowEntries != 512 || !w.CoverageIncomplete || !w.Records[50].Gap || w.Records[50].Facts != nil ||
		w.Records[255].Facts.Transactions.ComputeUnits == nil || *w.Records[255].Facts.Transactions.ComputeUnits != 255 {
		t.Fatal("selected window lost rows, exact values or gap coverage")
	}
	s.Records[255].Transactions.ComputeUnits = 0
	genesis = solana.Hash{7}.String()
	if *w.Records[255].Facts.Transactions.ComputeUnits != 255 || *w.Binding.GenesisHash == genesis {
		t.Fatal("selected window aliases its source")
	}
	for _, mutate := range []func(*replay.SelectedSlotSnapshot){
		func(s *replay.SelectedSlotSnapshot) { s.Rewinds++ },
		func(s *replay.SelectedSlotSnapshot) { s.WindowEntries-- },
		func(s *replay.SelectedSlotSnapshot) { s.OmittedFromWindow-- },
	} {
		changed := s
		mutate(&changed)
		if selectedWindowMeasurement(changed, d, binding, replayDetailAt) != nil {
			t.Fatal("changed source context or coverage accepted")
		}
	}
}
