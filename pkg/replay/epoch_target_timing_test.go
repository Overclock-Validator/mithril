package replay

import (
	"math"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func epochTimingState(schedule sealevel.SysvarEpochSchedule, slot, ns uint64) *state.MithrilState {
	return &state.MithrilState{ManifestParentSlot: slot, ManifestParentBankhash: (solana.Hash{1}).String(), ManifestNsPerSlot: ns,
		ManifestEpochSchedule: &state.ManifestEpochScheduleSeed{SlotsPerEpoch: schedule.SlotsPerEpoch,
			LeaderScheduleSlotOffset: schedule.LeaderScheduleSlotOffset, Warmup: schedule.Warmup,
			FirstNormalEpoch: schedule.FirstNormalEpoch, FirstNormalSlot: schedule.FirstNormalSlot}}
}

func epochTimingBank(schedule sealevel.SysvarEpochSchedule, slot uint64, f *features.Features) (*sealevel.SlotCtx, CompletedBlock) {
	epoch, index := schedule.GetEpochAndSlotIndex(slot)
	completed := CompletedBlock{At: time.Date(2026, 11, 2, 3, 4, 5, 6, time.UTC), Slot: slot, Bankhash: solana.Hash{1},
		Epoch: epoch, SlotIndex: index, SlotsInEpoch: schedule.SlotsInEpoch(epoch), EpochSchedule: schedule}
	bank := &sealevel.SlotCtx{Slot: slot, Epoch: epoch, Features: f, FinalBankhash: append([]byte(nil), completed.Bankhash[:]...)}
	return bank, completed
}

func TestEpochTargetTimingDelayedBoundaries(t *testing.T) {
	schedule := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}
	for _, transition := range []struct {
		gate features.FeatureGate
		ns   uint64
	}{
		{features.ReduceSlotTimeTo350ms, 350_000_000},
		{features.ReduceSlotTimeTo300ms, 300_000_000},
		{features.ReduceSlotTimeTo250ms, 250_000_000},
		{features.ReduceSlotTimeTo200ms, 200_000_000},
	} {
		t.Run(transition.gate.Name, func(t *testing.T) {
			f := features.NewFeaturesDefault()
			seed := newEpochTargetSeed(epochTimingState(schedule, 10, 400_000_000))
			bank, completed := epochTimingBank(schedule, 31, f)
			require.Equal(t, EpochTargetTiming{400_000_000, 400_000_000}, seed.observe(bank, completed))
			f.EnableFeature(transition.gate, 32)
			for _, slot := range []uint64{32, 63, 64, 95, 96} {
				bank, completed = epochTimingBank(schedule, slot, f)
				current := uint64(400_000_000)
				if slot >= 64 {
					current = transition.ns
				}
				require.Equal(t, EpochTargetTiming{current, transition.ns}, seed.observe(bank, completed), "slot %d", slot)
			}
			// The represented epoch begins before this snapshot, but its target
			// is still known because changes apply only at epoch boundaries.
			seed = newEpochTargetSeed(epochTimingState(schedule, 50, 400_000_000))
			bank, completed = epochTimingBank(schedule, 51, f)
			require.Equal(t, EpochTargetTiming{400_000_000, transition.ns}, seed.observe(bank, completed))
			// A snapshot taken after the reduction already contains its target.
			seed = newEpochTargetSeed(epochTimingState(schedule, 70, transition.ns))
			bank, completed = epochTimingBank(schedule, 71, f)
			require.Equal(t, EpochTargetTiming{transition.ns, transition.ns}, seed.observe(bank, completed))
			seed = newEpochTargetSeed(epochTimingState(schedule, 70, 400_000_000))
			require.Equal(t, EpochTargetTiming{}, seed.observe(bank, completed), "contradictory effective snapshot")
			f = features.NewFeaturesDefault()
			f.EnableFeature(transition.gate, 0)
			seed = newEpochTargetSeed(epochTimingState(schedule, 0, 400_000_000))
			for _, slot := range []uint64{0, 31, 32} {
				bank, completed = epochTimingBank(schedule, slot, f)
				current := uint64(400_000_000)
				if slot >= 32 {
					current = transition.ns
				}
				require.Equal(t, EpochTargetTiming{current, transition.ns}, seed.observe(bank, completed), "genesis gate at slot %d", slot)
			}
		})
	}
}

func TestEpochTargetTimingSeedAndBankGuards(t *testing.T) {
	schedule := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}
	bank, completed := epochTimingBank(schedule, 51, features.NewFeaturesDefault())
	require.Equal(t, epochTargetSeed{}, newEpochTargetSeed(nil))
	for _, mutate := range []func(*state.MithrilState){
		func(s *state.MithrilState) { s.ManifestNsPerSlot = 0 },
		func(s *state.MithrilState) { s.ManifestParentBankhash = "" },
		func(s *state.MithrilState) { s.ManifestParentBankhash = "not a hash" },
		func(s *state.MithrilState) { s.ManifestParentBankhash = (solana.Hash{}).String() },
		func(s *state.MithrilState) { s.ManifestParentBankhash = "1" + s.ManifestParentBankhash },
		func(s *state.MithrilState) { s.ManifestEpochSchedule = nil },
		func(s *state.MithrilState) { s.ManifestEpochSchedule.SlotsPerEpoch = 0 },
		func(s *state.MithrilState) { s.ManifestEpochSchedule.FirstNormalEpoch = 1 },
		func(s *state.MithrilState) {
			s.ManifestEpochSchedule.Warmup = true
			s.ManifestEpochSchedule.SlotsPerEpoch = 64
		},
	} {
		s := epochTimingState(schedule, 50, 400_000_000)
		mutate(s)
		require.Equal(t, EpochTargetTiming{}, newEpochTargetSeed(s).observe(bank, completed))
	}
	seed := newEpochTargetSeed(epochTimingState(schedule, 50, 400_000_000))
	require.Equal(t, EpochTargetTiming{}, seed.observe(nil, completed))
	for _, mutate := range []func(*sealevel.SlotCtx, *CompletedBlock){
		func(b *sealevel.SlotCtx, c *CompletedBlock) { b.Features = nil },
		func(b *sealevel.SlotCtx, c *CompletedBlock) { var f features.Features; b.Features = &f },
		func(b *sealevel.SlotCtx, c *CompletedBlock) { b.Slot++ },
		func(b *sealevel.SlotCtx, c *CompletedBlock) { b.Epoch++ },
		func(b *sealevel.SlotCtx, c *CompletedBlock) { b.FinalBankhash = nil },
		func(b *sealevel.SlotCtx, c *CompletedBlock) { b.FinalBankhash = append(b.FinalBankhash, 0) },
		func(b *sealevel.SlotCtx, c *CompletedBlock) { b.FinalBankhash[0]++ },
		func(b *sealevel.SlotCtx, c *CompletedBlock) { c.Bankhash = solana.Hash{} },
		func(b *sealevel.SlotCtx, c *CompletedBlock) { c.Epoch++ },
		func(b *sealevel.SlotCtx, c *CompletedBlock) { c.SlotIndex++ },
		func(b *sealevel.SlotCtx, c *CompletedBlock) { c.SlotsInEpoch++ },
		func(b *sealevel.SlotCtx, c *CompletedBlock) { c.EpochSchedule.LeaderScheduleSlotOffset++ },
		func(b *sealevel.SlotCtx, c *CompletedBlock) { c.EpochSchedule.SlotsPerEpoch = 0 },
		func(b *sealevel.SlotCtx, c *CompletedBlock) {
			b.Features.EnableFeature(features.ReduceSlotTimeTo200ms, c.Slot+1)
		},
	} {
		b, c := epochTimingBank(schedule, 51, features.NewFeaturesDefault())
		mutate(b, &c)
		require.Equal(t, EpochTargetTiming{}, seed.observe(b, c))
	}
	bank, completed = epochTimingBank(schedule, 49, features.NewFeaturesDefault())
	require.Equal(t, EpochTargetTiming{}, seed.observe(bank, completed))
	bank, completed = epochTimingBank(schedule, 50, features.NewFeaturesDefault())
	require.Equal(t, EpochTargetTiming{400_000_000, 400_000_000}, seed.observe(bank, completed))
	completed.Bankhash[0]++
	bank.FinalBankhash = append([]byte(nil), completed.Bankhash[:]...)
	require.Equal(t, EpochTargetTiming{}, seed.observe(bank, completed), "same slot must have the snapshot hash")
}

func TestEpochTargetTimingCheckedOverflow(t *testing.T) {
	for _, schedule := range []sealevel.SysvarEpochSchedule{
		{SlotsPerEpoch: 1}, {SlotsPerEpoch: 32}, {SlotsPerEpoch: 1 << 63},
		{SlotsPerEpoch: 128, Warmup: true, FirstNormalEpoch: 2, FirstNormalSlot: 96},
	} {
		seed := newEpochTargetSeed(epochTimingState(schedule, 0, math.MaxUint64))
		f := features.NewFeaturesDefault()
		bank, completed := epochTimingBank(schedule, math.MaxUint64, f)
		require.Equal(t, EpochTargetTiming{CurrentNanoseconds: math.MaxUint64}, seed.observe(bank, completed))
		f.EnableFeature(features.ReduceSlotTimeTo200ms, math.MaxUint64)
		require.Equal(t, EpochTargetTiming{}, seed.observe(bank, completed), "unrepresentable feature boundary")
	}
}

func TestEpochTargetTimingImmutableAttemptAndFork(t *testing.T) {
	schedule := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}
	s := epochTimingState(schedule, 10, 400_000_000)
	seed := newEpochTargetSeed(s)
	s.ManifestNsPerSlot = 1
	s.ManifestParentSlot = 1000
	s.ManifestParentBankhash = "invalid"
	s.ManifestEpochSchedule.SlotsPerEpoch = 1
	f := features.NewFeaturesDefault()
	f.EnableFeature(features.ReduceSlotTimeTo200ms, 32)
	before := f.Clone()
	bank, completed := epochTimingBank(schedule, 64, f)
	completed.EpochTargetTiming = seed.observe(bank, completed)
	require.Equal(t, EpochTargetTiming{200_000_000, 200_000_000}, completed.EpochTargetTiming)
	require.Equal(t, before, f, "observation changed source features")
	p := &Progress{}
	p.begin(11, nil)
	p.completed(completed, 0, false)
	snapshot, ok := p.Snapshot()
	require.True(t, ok)
	snapshot.Completed.EpochTargetTiming.CurrentNanoseconds = 1
	f.DisableFeature(features.ReduceSlotTimeTo200ms)
	p.skipped(0, false)
	p.phase("halted")
	snapshot, _ = p.Snapshot()
	require.Equal(t, completed.EpochTargetTiming, snapshot.Completed.EpochTargetTiming)
	require.Equal(t, completed.At, snapshot.Completed.At)
	p.rewound()
	snapshot, _ = p.Snapshot()
	require.False(t, snapshot.HasCompleted)
	require.Equal(t, EpochTargetTiming{}, snapshot.Completed.EpochTargetTiming)
	// A fork without the earlier gate returns to the seed instead of keeping
	// a historical minimum. Voter mode/identity do not enter the calculation.
	for _, gate := range []features.FeatureGate{features.Alpenglow, features.AlpenglowDevContext} {
		f.EnableFeature(gate, 0)
	}
	completed.EpochTargetTiming = seed.observe(bank, completed)
	require.Equal(t, EpochTargetTiming{400_000_000, 400_000_000}, completed.EpochTargetTiming)
	p.completed(completed, 0, false)
	p.begin(65, nil)
	snapshot, _ = p.Snapshot()
	require.Equal(t, EpochTargetTiming{}, snapshot.Completed.EpochTargetTiming)
	require.False(t, snapshot.HasCompleted)
}
