package replay

import (
	"math"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func selectedProgress(t *testing.T) *Progress {
	t.Helper()
	p := &Progress{}
	require.NoError(t, p.EnableReplayDiagnostics("selected-test"))
	p.begin(1, nil)
	return p
}

func selectedFacts(t *testing.T, slot uint64, at time.Time) SelectedReplayFacts {
	t.Helper()
	schedule := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 4096}
	facts, ok := selectedReplayFacts(at, &block.Block{Slot: slot, Epoch: slot / 4096,
		Leader: solana.PublicKey{3}, FromLocalProduction: slot%2 == 0}, &schedule, CertificateCarrier{Slot: slot})
	require.True(t, ok)
	return facts
}

func attachSelectedFixture(t *testing.T, p *Progress, r *CertificateOutcomes, at time.Time) map[uint64]block.SelectedEconomics {
	t.Helper()
	p.enableSelectedSlots(r)
	want := make(map[uint64]block.SelectedEconomics)
	for _, pending := range r.pending {
		facts := selectedFacts(t, pending.carrier.Slot, at)
		switch pending.carrier.Slot % 4 {
		case 0:
			facts.Economics = block.SelectedEconomics{FeesKnown: true, NativeCostKnown: true}
		case 2:
			facts.Economics = block.SelectedEconomics{NativeCostKnown: true, NativeCost: block.SelectedNativeCost{BlockCost: 9, BlockCostLimit: 8}}
		case 3:
			facts.Economics = block.SelectedEconomics{FeesKnown: true, Fees: block.SelectedFeeFacts{ExecutionLamports: 10, PriorityLamports: 2, TotalLamports: 12}}
		}
		want[pending.carrier.Slot] = facts.Economics
		require.True(t, r.observeSelectedFacts(pending.carrier, facts))
	}
	return want
}

// These tests exercise real existing settlement/flush owners, not ReplayBlocks.
func TestSelectedSlotsExactFold(t *testing.T) {
	for _, tc := range []struct {
		name      string
		fail      uint64
		committed int
	}{{"success", 0, 4}, {"later_failure", 17, 2}, {"first_failure", 15, 0}} {
		t.Run(tc.name, func(t *testing.T) {
			fc := &fakeCommitter{durable: accounts.NewMemAccounts(), failOn: tc.fail}
			tail := asyncTestTail(fc, 15, 16, 17, 18)
			r, ids := settlementFixture(t, tail, 15, 16, 17, 18)
			p, replayedAt := selectedProgress(t), time.Now()
			wantEconomics := attachSelectedFixture(t, p, r, replayedAt)
			before := captureCertificateFlush(tail)
			through, ctx, err := tail.flush(18)
			if tc.fail == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			if through > 0 && ctx != nil {
				r.settleFlush(before, tail, ids, through, replayedAt.Add(time.Minute))
				p.publishSelectedSlots(r)
			}
			got, ok := p.SelectedSlotsSnapshot()
			require.Equal(t, tc.committed != 0, ok)
			require.Len(t, got.Records, tc.committed)
			require.Len(t, fc.committed, tc.committed)
			for i, row := range got.Records {
				require.False(t, row.Gap)
				require.Equal(t, fc.committed[i], row.Slot)
				require.Equal(t, ids[row.Slot], row.BlockID)
				require.Equal(t, solana.Hash(testHashBytes(byte(row.Slot))), row.Bankhash)
				require.Equal(t, replayedAt, row.Facts.ReplayResultAt, "fold redated the original clock")
				require.Equal(t, wantEconomics[row.Slot], row.Facts.Economics)
			}
		})
	}
	t.Run("async_only_after_apply", func(t *testing.T) {
		fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
		tail := asyncTestTail(fc, 15, 16, 17)
		r, ids := settlementFixture(t, tail, 15, 16, 17)
		p, at := selectedProgress(t), time.Now()
		wantEconomics := attachSelectedFixture(t, p, r, at)
		job, err := tail.buildFoldJob(17, false)
		require.NoError(t, err)
		require.NotNil(t, job)
		require.NoError(t, runFoldJob(fc, job))
		p.publishSelectedSlots(r)
		_, known := p.SelectedSlotsSnapshot()
		require.False(t, known, "a durable worker write is not an applied fold")
		require.NotNil(t, tail.applyFoldJob(job))
		r.settleFold(job.bankhashes, ids, job.slotCount, at.Add(time.Second))
		p.publishSelectedSlots(r)
		got, known := p.SelectedSlotsSnapshot()
		require.True(t, known)
		require.Len(t, got.Records, job.slotCount)
		for _, row := range got.Records {
			require.False(t, row.Gap)
			require.Equal(t, solana.Hash(job.bankhashes[row.Slot]), row.Bankhash)
			require.Equal(t, wantEconomics[row.Slot], row.Facts.Economics)
		}
	})
}

func TestSelectedSlotsEligibilityAndDisabledParity(t *testing.T) {
	at := time.Now()
	for _, tc := range []struct {
		name   string
		change func(*CertificateCarrier, *SelectedReplayFacts)
		known  bool
	}{
		{"known_zero", func(*CertificateCarrier, *SelectedReplayFacts) {}, true},
		{"saturated", func(c *CertificateCarrier, _ *SelectedReplayFacts) {
			c.Transactions = sealevel.BankTransactionCounts{Total: math.MaxUint64, ComputeUnits: math.MaxUint64, Saturated: true}
		}, true},
		{"zero_date", func(_ *CertificateCarrier, f *SelectedReplayFacts) { f.ReplayResultAt = time.Time{} }, false},
		{"zero_count", func(_ *CertificateCarrier, f *SelectedReplayFacts) { f.SlotCount = 0 }, false},
		{"overflow_range", func(_ *CertificateCarrier, f *SelectedReplayFacts) { f.FirstSlot, f.SlotCount = math.MaxUint64-1, 3 }, false},
		{"counts", func(c *CertificateCarrier, _ *SelectedReplayFacts) { c.Transactions.NonVote = 1 }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := selectedProgress(t)
			on, off := NewCertificateOutcomes(solana.PublicKey{}), NewCertificateOutcomes(solana.PublicKey{})
			p.enableSelectedSlots(on)
			c, facts := outcomeCarrier(15, 30, solana.PublicKey{}), selectedFacts(t, 15, at)
			tc.change(&c, &facts)
			require.True(t, on.Observe(c))
			require.True(t, off.Observe(c))
			require.Equal(t, tc.known, on.observeSelectedFacts(c, facts))
			require.False(t, off.observeSelectedFacts(c, facts))
			evidence := []CertificateFoldEvidence{outcomeEvidence(c)}
			on.Settle(evidence, at)
			off.Settle(evidence, at)
			require.Equal(t, off.Snapshot(), on.Snapshot(), "optional facts changed certificate classification")
			p.publishSelectedSlots(on)
			got, ok := p.SelectedSlotsSnapshot()
			require.True(t, ok)
			require.Len(t, got.Records, 1)
			require.Equal(t, !tc.known, got.Records[0].Gap)
			if tc.known {
				require.Equal(t, c.Transactions, got.Records[0].Transactions)
			}
		})
	}
	var absent *Progress
	absent.enableSelectedSlots(nil)
	absent.publishSelectedSlots(nil)
	_, ok := absent.SelectedSlotsSnapshot()
	require.False(t, ok)
	var disabled Progress
	disabled.begin(1, nil)
	r := NewCertificateOutcomes(solana.PublicKey{})
	disabled.enableSelectedSlots(r)
	require.Nil(t, r.selectedOwner)
	require.Zero(t, testing.AllocsPerRun(10, func() { r.observeSelectedFacts(CertificateCarrier{}, SelectedReplayFacts{}) }))
}

func TestSelectedSlotsDuplicateConflictAndMissingEvidence(t *testing.T) {
	p := selectedProgress(t)
	r := NewCertificateOutcomes(solana.PublicKey{})
	p.enableSelectedSlots(r)
	at := time.Now()
	a, b := outcomeCarrier(15, 20, solana.PublicKey{}), outcomeCarrier(16, 21, solana.PublicKey{})
	require.True(t, r.Observe(a))
	require.True(t, r.Observe(b))
	facts := selectedFacts(t, 15, at)
	require.True(t, r.observeSelectedFacts(a, facts))
	require.True(t, r.observeSelectedFacts(a, facts))
	changed := facts
	changed.ReplayResultAt = at.Add(time.Nanosecond)
	require.False(t, r.observeSelectedFacts(a, changed))
	require.False(t, r.observeSelectedFacts(a, facts), "a repeat revived conflicting facts")
	r.Settle([]CertificateFoldEvidence{outcomeEvidence(a), outcomeEvidence(b)}, at.Add(time.Second))
	p.publishSelectedSlots(r)
	got, ok := p.SelectedSlotsSnapshot()
	require.True(t, ok)
	require.Equal(t, uint64(1), got.FactConflicts)
	require.True(t, got.CoverageIncomplete)
	require.True(t, got.Records[0].Gap)
	require.True(t, got.Records[1].Gap, "missing source facts became zero-valued known facts")
	require.Zero(t, r.Snapshot().Gaps, "telemetry conflict became a certificate gap")
	// Missing and then contradictory identity evidence remains an explicit gap.
	bad := outcomeEvidence(b)
	bad.HasBlockID = false
	r.Settle([]CertificateFoldEvidence{bad}, at)
	p.publishSelectedSlots(r)
	again, _ := p.SelectedSlotsSnapshot()
	require.True(t, again.Records[1].Gap)
	require.Zero(t, again.Records[1].Facts)
}

func TestSelectedSlotsSourceGeometryAndCountCompleteness(t *testing.T) {
	at := time.Now()
	schedule := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128, Warmup: true, FirstNormalEpoch: 2, FirstNormalSlot: 96}
	for _, slot := range []uint64{0, 31, 32, 95, 96, 223, 224} {
		b := &block.Block{Slot: slot, Epoch: schedule.GetEpoch(slot), Transactions: []*solana.Transaction{{}}}
		carrier := CertificateCarrier{Slot: slot, Transactions: sealevel.BankTransactionCounts{Total: 1}}
		facts, ok := selectedReplayFacts(at, b, &schedule, carrier)
		require.True(t, ok)
		require.Equal(t, at, facts.ReplayResultAt)
		require.Equal(t, schedule.SlotsInEpoch(b.Epoch), facts.SlotCount)
		carrier.Transactions = sealevel.BankTransactionCounts{}
		_, ok = selectedReplayFacts(at, b, &schedule, carrier)
		require.False(t, ok, "valid marginal counts were partial for the actual block")
		carrier.Transactions = sealevel.BankTransactionCounts{Saturated: true}
		_, ok = selectedReplayFacts(at, b, &schedule, carrier)
		require.True(t, ok, "saturated partial totals must retain their explicit flag")
		b.Epoch++
		carrier.Transactions = sealevel.BankTransactionCounts{Total: 1}
		_, ok = selectedReplayFacts(at, b, &schedule, carrier)
		require.False(t, ok)
	}
	_, ok := selectedReplayFacts(at, nil, &schedule, CertificateCarrier{})
	require.False(t, ok)
	_, ok = selectedReplayFacts(at, &block.Block{}, nil, CertificateCarrier{})
	require.False(t, ok)
	one := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 1}
	last, ok := selectedReplayFacts(at, &block.Block{Slot: math.MaxUint64, Epoch: math.MaxUint64}, &one, CertificateCarrier{Slot: math.MaxUint64})
	require.True(t, ok)
	require.Equal(t, uint64(math.MaxUint64), last.FirstSlot)
	require.Equal(t, uint64(1), last.SlotCount)
}

func TestSelectedSlotsFoldedRepeatCannotRedate(t *testing.T) {
	p := selectedProgress(t)
	r := NewCertificateOutcomes(solana.PublicKey{})
	p.enableSelectedSlots(r)
	at := time.Now()
	c := outcomeCarrier(15, 20, solana.PublicKey{})
	require.True(t, r.Observe(c))
	require.True(t, r.observeSelectedFacts(c, selectedFacts(t, c.Slot, at)))
	r.Settle([]CertificateFoldEvidence{outcomeEvidence(c)}, at.Add(time.Second))
	p.publishSelectedSlots(r)
	before, ok := p.SelectedSlotsSnapshot()
	require.True(t, ok)
	require.True(t, r.Observe(c), "existing carrier duplicate behavior changed")
	require.False(t, r.observeSelectedFacts(c, selectedFacts(t, c.Slot, at.Add(time.Minute))))
	r.Settle([]CertificateFoldEvidence{outcomeEvidence(c)}, at.Add(2*time.Minute))
	p.publishSelectedSlots(r)
	after, ok := p.SelectedSlotsSnapshot()
	require.True(t, ok)
	require.Equal(t, before.Records, after.Records)
	require.Equal(t, at, after.Records[0].Facts.ReplayResultAt)
}

func TestSelectedSlotsRewindCopiesAndFences(t *testing.T) {
	fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
	tail := asyncTestTail(fc, 15, 16)
	r, ids := settlementFixture(t, tail, 15, 16)
	p, at := selectedProgress(t), time.Now()
	attachSelectedFixture(t, p, r, at)
	a := r.pending[certificateCarrierKey{15, ids[15]}].carrier
	r.Settle([]CertificateFoldEvidence{outcomeEvidence(a)}, at)
	p.publishSelectedSlots(r)
	first, ok := p.SelectedSlotsSnapshot()
	require.True(t, ok)
	first.Records[0].Facts.ExecutionLeader[0]++
	first.Records[0].Facts.Economics.Fees.TotalLamports++
	copy, _ := p.SelectedSlotsSnapshot()
	require.Equal(t, solana.PublicKey{3}, copy.Records[0].Facts.ExecutionLeader)
	require.Equal(t, uint64(12), copy.Records[0].Facts.Economics.Fees.TotalLamports)
	r.RewindFrom(16)
	p.rewound()
	_, ok = p.SelectedSlotsSnapshot()
	require.False(t, ok, "old rewind publication stayed current")
	require.Empty(t, r.pending)
	p.publishSelectedSlots(r)
	current, ok := p.SelectedSlotsSnapshot()
	require.True(t, ok)
	require.Equal(t, uint64(1), current.Rewinds)
	require.Equal(t, copy.Records, current.Records, "speculative rewind removed folded facts")
	require.Greater(t, current.Generation, copy.Generation)
	p.begin(1, nil)
	p.publishSelectedSlots(r)
	_, ok = p.SelectedSlotsSnapshot()
	require.False(t, ok, "retired attempt republished its old owner")
	require.Equal(t, at, copy.Records[0].Facts.ReplayResultAt)
}

func TestSelectedSlotsCapacityDrainBeforeCompleted(t *testing.T) {
	fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
	tail := newUnrootedTail(&fakeDurable{}, fc, 2, 2, "")
	for slot := uint64(1); slot <= 3; slot++ {
		tail.Add(slot, []*accounts.Account{testAccount(byte(slot), slot)}, testHashBytes(byte(slot)))
		tail.SetContext(slot, &state.ResumeContext{Slot: slot})
	}
	r, ids := settlementFixture(t, tail, 1, 2, 3)
	p, at := selectedProgress(t), time.Now()
	attachSelectedFixture(t, p, r, at)
	job, err := tail.buildFoldJob(2, false)
	require.NoError(t, err)
	require.NotNil(t, job)
	promoter := newAsyncPromoter(fc)
	t.Cleanup(promoter.stop)
	require.NoError(t, promoter.enqueue(job))
	stillOver := settleFoldsAtCapacity(tail, promoter, 3, func(res *foldResult) {
		require.NotNil(t, res)
		require.NoError(t, res.err)
		require.NotNil(t, tail.applyFoldJob(res.job))
		r.settleFold(res.job.bankhashes, ids, res.job.slotCount, at.Add(time.Second))
		p.publishSelectedSlots(r)
	}, nil)
	require.False(t, stillOver)
	s, _ := p.Snapshot()
	require.False(t, s.HasCompleted, "fixture must not call the later completion hook")
	selected, ok := p.SelectedSlotsSnapshot()
	require.True(t, ok)
	require.Len(t, selected.Records, 2)
	require.Equal(t, at, selected.Records[0].Facts.ReplayResultAt)
}

func TestSelectedSlotsBoundsAndSizes(t *testing.T) {
	p := selectedProgress(t)
	r := NewCertificateOutcomes(solana.PublicKey{})
	p.enableSelectedSlots(r)
	at := time.Now()
	var evidence []CertificateFoldEvidence
	for slot := uint64(1); slot <= certificatePendingLimit; slot++ {
		c := outcomeCarrier(slot, 30, solana.PublicKey{})
		require.True(t, r.Observe(c))
		facts := selectedFacts(t, slot, at)
		facts.Economics = block.SelectedEconomics{FeesKnown: true, Fees: block.SelectedFeeFacts{ExecutionLamports: slot, TotalLamports: slot}}
		require.True(t, r.observeSelectedFacts(c, facts))
		evidence = append(evidence, outcomeEvidence(c))
	}
	extra := outcomeCarrier(certificatePendingLimit+1, 30, solana.PublicKey{})
	require.False(t, r.Observe(extra))
	require.False(t, r.observeSelectedFacts(extra, selectedFacts(t, extra.Slot, at)))
	r.Settle(evidence, at.Add(time.Second))
	p.publishSelectedSlots(r)
	got, ok := p.SelectedSlotsSnapshot()
	require.True(t, ok)
	require.Equal(t, uint64(certificateWindowLimit), got.WindowEntries)
	require.Equal(t, uint64(certificateWindowLimit-SelectedSlotCapacity), got.OmittedFromWindow)
	require.Len(t, got.Records, SelectedSlotCapacity)
	require.Equal(t, uint64(certificatePendingLimit-SelectedSlotCapacity+1), got.Records[0].Slot)
	require.Equal(t, uint64(certificatePendingLimit), got.Records[SelectedSlotCapacity-1].Slot)
	for _, row := range got.Records {
		require.False(t, row.Gap)
		require.True(t, row.Facts.Economics.FeesKnown)
		require.Equal(t, row.Slot, row.Facts.Economics.Fees.TotalLamports)
	}
	factsBytes, rowBytes, viewBytes := unsafe.Sizeof(SelectedReplayFacts{}), unsafe.Sizeof(SelectedSlotRecord{}), unsafe.Sizeof(SelectedSlotSnapshot{})
	require.LessOrEqual(t, factsBytes, uintptr(160))
	require.LessOrEqual(t, rowBytes, uintptr(384))
	// Conservative representation accounting, not a measured RSS/heap maximum:
	// allow 32 bytes per optional allocation and twice 16 wrapper bytes per
	// retained/pending entry (including map bucket slack), plus view buffers.
	owner := uintptr(certificatePendingLimit+certificateWindowLimit) * (factsBytes + 32 + 32)
	view := uintptr(SelectedSlotCapacity)*rowBytes + viewBytes + 128
	retained, peak := owner+view, owner+3*view
	require.LessOrEqual(t, retained, uintptr(512<<10))
	require.LessOrEqual(t, peak, uintptr(1<<20))
	t.Logf("native facts=%d row=%d snapshot=%d; conservative added retained=%d peak=%d bytes (not RSS)", factsBytes, rowBytes, viewBytes, retained, peak)
	boundary := SelectedReplayFacts{ReplayResultAt: at, Epoch: math.MaxUint64, FirstSlot: math.MaxUint64, SlotCount: 1}
	require.True(t, boundary.valid(math.MaxUint64))
	boundary.SlotCount = 2
	require.False(t, boundary.valid(math.MaxUint64))
}

func TestSelectedSlotsConcurrentDetachedSnapshots(t *testing.T) {
	p := selectedProgress(t)
	r := NewCertificateOutcomes(solana.PublicKey{})
	p.enableSelectedSlots(r)
	stop := make(chan struct{})
	var reader sync.WaitGroup
	reader.Add(1)
	go func() {
		defer reader.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if snapshot, ok := p.SelectedSlotsSnapshot(); ok && len(snapshot.Records) != 0 {
				snapshot.Records[0].Facts.ExecutionLeader[0] = 99
				snapshot.Records[0].Facts.Economics.NativeCost.BlockCost = 99
			}
		}
	}()
	t.Cleanup(func() { close(stop); reader.Wait() })
	at := time.Now()
	for slot := uint64(10); slot < 50; slot++ {
		c := outcomeCarrier(slot, 30, solana.PublicKey{})
		require.True(t, r.Observe(c))
		require.True(t, r.observeSelectedFacts(c, selectedFacts(t, slot, at)))
		r.Settle([]CertificateFoldEvidence{outcomeEvidence(c)}, at)
		p.publishSelectedSlots(r)
	}
	view, ok := p.SelectedSlotsSnapshot()
	require.True(t, ok)
	for _, row := range view.Records {
		require.Equal(t, solana.PublicKey{3}, row.Facts.ExecutionLeader)
		require.Zero(t, row.Facts.Economics.NativeCost)
	}
}

func TestSelectedSlotsEconomicsIndependentKnownness(t *testing.T) {
	for _, tc := range []struct {
		name        string
		local       bool
		input, want block.SelectedEconomics
	}{
		{name: "unknown", local: true},
		{name: "known_zero", local: true, input: block.SelectedEconomics{FeesKnown: true, NativeCostKnown: true},
			want: block.SelectedEconomics{FeesKnown: true, NativeCostKnown: true}},
		{name: "full_width_and_above_limit", local: true,
			input: block.SelectedEconomics{FeesKnown: true, Fees: block.SelectedFeeFacts{ExecutionLamports: math.MaxUint64, TotalLamports: math.MaxUint64},
				NativeCostKnown: true, NativeCost: block.SelectedNativeCost{BlockCost: math.MaxUint64, BlockCostLimit: 1}},
			want: block.SelectedEconomics{FeesKnown: true, Fees: block.SelectedFeeFacts{ExecutionLamports: math.MaxUint64, TotalLamports: math.MaxUint64},
				NativeCostKnown: true, NativeCost: block.SelectedNativeCost{BlockCost: math.MaxUint64, BlockCostLimit: 1}}},
		{name: "invalid_fee_sum_keeps_cost", local: true,
			input: block.SelectedEconomics{FeesKnown: true, Fees: block.SelectedFeeFacts{ExecutionLamports: 1, TotalLamports: 2}, NativeCostKnown: true},
			want:  block.SelectedEconomics{NativeCostKnown: true}},
		{name: "fee_overflow_keeps_cost", local: true,
			input: block.SelectedEconomics{FeesKnown: true, Fees: block.SelectedFeeFacts{ExecutionLamports: math.MaxUint64, PriorityLamports: 1}, NativeCostKnown: true},
			want:  block.SelectedEconomics{NativeCostKnown: true}},
		{name: "unknown_ignores_values", local: true,
			input: block.SelectedEconomics{Fees: block.SelectedFeeFacts{TotalLamports: 99}, NativeCost: block.SelectedNativeCost{BlockCost: 99}}},
		{name: "remote_cost_unknown", input: block.SelectedEconomics{FeesKnown: true, NativeCostKnown: true, NativeCost: block.SelectedNativeCost{BlockCost: 1}},
			want: block.SelectedEconomics{FeesKnown: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, at := selectedProgress(t), time.Now()
			on, off := NewCertificateOutcomes(solana.PublicKey{}), NewCertificateOutcomes(solana.PublicKey{})
			p.enableSelectedSlots(on)
			carrier := outcomeCarrier(16, 30, solana.PublicKey{})
			facts := selectedFacts(t, carrier.Slot, at)
			facts.FromLocalProduction, facts.Economics = tc.local, tc.input
			require.True(t, on.Observe(carrier))
			require.True(t, off.Observe(carrier))
			require.True(t, on.observeSelectedFacts(carrier, facts))
			require.False(t, off.observeSelectedFacts(carrier, facts))
			require.True(t, on.observeSelectedFacts(carrier, facts), "identical invalid values normalize identically")
			facts.Economics.Fees.TotalLamports++
			evidence := []CertificateFoldEvidence{outcomeEvidence(carrier)}
			on.Settle(evidence, at)
			off.Settle(evidence, at)
			require.Equal(t, off.Snapshot(), on.Snapshot(), "economics must not alter certificate equality/classification")
			p.publishSelectedSlots(on)
			view, ok := p.SelectedSlotsSnapshot()
			require.True(t, ok)
			require.Len(t, view.Records, 1)
			require.False(t, view.Records[0].Gap, "unknown economics erased valid replay facts")
			require.Equal(t, tc.want, view.Records[0].Facts.Economics, "facts copy aliases caller state")
		})
	}
}

func TestSelectedSlotsEconomicsBindingAndConflict(t *testing.T) {
	for _, mode := range []string{"matching", "slot", "block_id", "bankhash", "saturated_count", "unsealed", "disabled"} {
		t.Run(mode, func(t *testing.T) {
			b := &block.Block{Slot: 16, FromLocalProduction: true, Transactions: []*solana.Transaction{{}},
				AlpenglowBlockID: [32]byte{3}, HasAlpenglowBlockID: true, ExpectedBankhash: [32]byte{4}, HasExpectedBankhash: true}
			carrier := CertificateCarrier{Slot: 16, BlockID: solana.Hash{3}, Bankhash: solana.Hash{4}, Transactions: sealevel.BankTransactionCounts{Total: 1}}
			if mode != "disabled" {
				b.EnableSelectedLocalEconomics()
				b.RecordSelectedFees(10, 2, 12)
				if mode != "unsealed" {
					require.True(t, b.SealSelectedLocalEconomics(16, b.ExpectedBankhash, 1, block.SelectedNativeCost{BlockCost: 9, BlockCostLimit: 8}))
				}
			}
			switch mode {
			case "slot":
				b.Slot, carrier.Slot = 17, 17
			case "block_id":
				carrier.BlockID = solana.Hash{9}
			case "bankhash":
				carrier.Bankhash = solana.Hash{9}
			case "saturated_count":
				carrier.Transactions = sealevel.BankTransactionCounts{Saturated: true}
			}
			at := time.Now()
			facts, ok := selectedReplayFacts(at, b, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}, carrier)
			require.True(t, ok)
			require.Equal(t, at, facts.ReplayResultAt)
			if mode != "matching" {
				require.Zero(t, facts.Economics, "mismatched/absent local binding became known")
				return
			}
			require.Equal(t, block.SelectedEconomics{FeesKnown: true, Fees: block.SelectedFeeFacts{ExecutionLamports: 10, PriorityLamports: 2, TotalLamports: 12},
				NativeCostKnown: true, NativeCost: block.SelectedNativeCost{BlockCost: 9, BlockCostLimit: 8}}, facts.Economics)
			p, owner := selectedProgress(t), NewCertificateOutcomes(solana.PublicKey{})
			p.enableSelectedSlots(owner)
			require.True(t, owner.Observe(carrier))
			require.True(t, owner.observeSelectedFacts(carrier, facts))
			changed := facts
			changed.Economics.NativeCost.BlockCost++
			require.False(t, owner.observeSelectedFacts(carrier, changed), "changed economics silently replaced the original facts")
			require.False(t, owner.observeSelectedFacts(carrier, facts))
			owner.Settle([]CertificateFoldEvidence{outcomeEvidence(carrier)}, at)
			p.publishSelectedSlots(owner)
			view, known := p.SelectedSlotsSnapshot()
			require.True(t, known)
			require.True(t, view.Records[0].Gap)
			require.Equal(t, uint64(1), view.FactConflicts)
			require.Zero(t, owner.Snapshot().Gaps, "economics conflict changed certificate classification")
		})
	}
}
