package replay

import (
	"encoding/binary"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
)

func initialAnchorSource(t *testing.T) alpenglowStartupTestReader {
	t.Helper()
	schedule := make([]byte, 40)
	binary.LittleEndian.PutUint64(schedule, 432000)
	binary.LittleEndian.PutUint64(schedule[8:], 432000)
	recent := sealevel.SysvarRecentBlockhashes{{Blockhash: [32]byte{1}}}
	hashes := sealevel.SysvarSlotHashes{{Slot: 6, Hash: [32]byte{2}}}
	data := map[solana.PublicKey][]byte{
		sealevel.SysvarClockAddr:             (&sealevel.SysvarClock{Slot: 7, UnixTimestamp: 70}).MustMarshal(),
		sealevel.SysvarRentAddr:              (&sealevel.SysvarRent{}).MustMarshal(),
		sealevel.SysvarEpochScheduleAddr:     schedule,
		sealevel.SysvarStakeHistoryAddr:      make([]byte, 8),
		sealevel.SysvarRecentBlockHashesAddr: recent.MustMarshal(),
		sealevel.SysvarSlotHashesAddr:        hashes.MustMarshal(),
		sealevel.SysvarEpochRewardsAddr:      encodeEpochRewardsForTest(t, sealevel.SysvarEpochRewards{DistributedRewards: 100}),
	}
	source := alpenglowStartupTestReader{accounts: make(map[solana.PublicKey]*accounts.Account)}
	for key, value := range data {
		source.accounts[key] = &accounts.Account{Key: key, Lamports: 1, Data: value}
	}
	return source
}

func TestInitialForkAnchorIgnoresStaleGlobalsAndPreservesResumeOverrides(t *testing.T) {
	old := sealevel.SysvarCache
	t.Cleanup(func() { sealevel.SysvarCache = old })
	sealevel.SysvarCache.Clock.Sysvar = &sealevel.SysvarClock{Slot: 99, UnixTimestamp: 9900}
	sealevel.SysvarCache.EpochRewards.Sysvar = &sealevel.SysvarEpochRewards{DistributedRewards: 999}
	source := initialAnchorSource(t)
	recent := sealevel.SysvarRecentBlockhashes{{Blockhash: [32]byte{5}}}
	hashes := sealevel.SysvarSlotHashes{{Slot: 6, Hash: [32]byte{6}}}
	resume := &ResumeState{ParentSlot: 7, Clock: (&sealevel.SysvarClock{Slot: 7, UnixTimestamp: 71}).MustMarshal(), RecentBlockhashes: &recent, SlotHashes: &hashes}
	sysvars, err := initialParentBankSysvars(source, 7, resume, nil)
	require.NoError(t, err)
	clock, _ := sysvars.Clock()
	require.Equal(t, int64(71), clock.UnixTimestamp)
	gotRecent, _ := sysvars.RecentBlockhashes()
	require.Equal(t, recent, gotRecent)
	gotHashes, _ := sysvars.SlotHashes()
	require.Equal(t, hashes, gotHashes)
	epochRewards, _ := sysvars.EpochRewards()
	require.Equal(t, uint64(100), epochRewards.DistributedRewards)
	// Resume override never mutates a borrowed durable template.
	require.Equal(t, (&sealevel.SysvarClock{Slot: 7, UnixTimestamp: 70}).MustMarshal(), source.accounts[sealevel.SysvarClockAddr].Data)
}

func TestInitialForkAnchorAllowsSwitchBeforeFirstFold(t *testing.T) {
	resetVoteStakeDirty()
	t.Cleanup(resetVoteStakeDirty)
	source := initialAnchorSource(t)
	tail := newUnrootedTail(&fakeDurable{}, &fakeCommitter{durable: accounts.NewMemAccounts()}, 512, 1, "")
	block := &b.Block{Slot: 8, ParentSlot: 7, BlockHeight: 6, ParentBankhash: [32]byte{7},
		AcctsLtHash: new(lthash.LtHash).InitWithHash(make([]byte, 2048)), PrevFeeRateGovernor: &sealevel.FeeRateGovernor{LamportsPerSignature: 5000}}
	persisted := &state.MithrilState{ManifestCapitalization: 123, ManifestRecentBlockhashes: []state.BlockhashEntry{{Blockhash: base58.Encode(make([]byte, 32))}}}
	childFeatures := features.NewFeaturesDefault()
	childFeatures.EnableFeature(features.EnablePartitionedEpochReward, 8)
	forkState := &replayForkState{epoch: 0, ctx: ReplayCtx{Capitalization: 456, CurrentFeatures: childFeatures}, rewardsEnabled: true}
	require.NoError(t, seedInitialRootAnchor(tail, source, block, nil, persisted, forkState))
	require.Equal(t, uint64(5), tail.rootContext.BlockHeight)
	require.Equal(t, uint64(123), tail.rootContext.Capitalization)
	require.Equal(t, uint64(123), tail.rootForkState.ctx.Capitalization)
	require.Equal(t, uint64(456), forkState.ctx.Capitalization, "parent capture cannot mutate the child working context")
	require.False(t, tail.rootForkState.ctx.CurrentFeatures.IsActive(features.EnablePartitionedEpochReward))
	require.False(t, tail.rootForkState.rewardsEnabled)
	require.True(t, childFeatures.IsActive(features.EnablePartitionedEpochReward), "capturing parent cannot modify child features")
	tail.Add(8, []*accounts.Account{testAccount(1, 8)}, testHashBytes(8))
	tail.SetContext(8, forkTestContext(8), testUnwindBankSysvars(t, 8, 800))
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, LeaderScheduleSlotOffset: 432000}
	rs, bankSysvars, reason := tryInLoopUnwind(&CertifiedSwitch{Slot: 8}, tail, &state.MithrilState{LastRootedSlot: 7}, schedule, 0, nil)
	require.Empty(t, reason)
	require.Equal(t, uint64(7), rs.ParentSlot)
	require.Equal(t, uint64(7), bankSysvars.Slot())
	require.Equal(t, uint64(5), rs.ParentBlockHeight)
	require.Equal(t, uint64(123), rs.Capitalization)
	clock, _ := bankSysvars.Clock()
	require.Equal(t, int64(70), clock.UnixTimestamp)
}

func TestInitialForkAnchorUsesCheckpointCapitalizationBeforeMigration(t *testing.T) {
	source := initialAnchorSource(t)
	tail := newUnrootedTail(&fakeDurable{}, &fakeCommitter{durable: accounts.NewMemAccounts()}, 512, 1, "")
	block := &b.Block{Slot: 8, ParentSlot: 7, BlockHeight: 6, ParentBankhash: [32]byte{7},
		AcctsLtHash: new(lthash.LtHash).InitWithHash(make([]byte, 2048)), PrevFeeRateGovernor: &sealevel.FeeRateGovernor{}}
	persisted := &state.MithrilState{ManifestCapitalization: 999}
	resume := &ResumeState{ParentSlot: 7, Capitalization: 123}
	working := &replayForkState{ctx: ReplayCtx{Capitalization: 456}}
	require.NoError(t, seedInitialRootAnchor(tail, source, block, resume, persisted, working))
	require.Equal(t, uint64(123), tail.rootContext.Capitalization)
	var restored ReplayCtx
	tail.rootForkState.restore(&restored)
	require.Equal(t, uint64(123), restored.Capitalization)
	require.Equal(t, uint64(456), working.ctx.Capitalization)
}
