package replay

import (
	"math"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/blockhistory"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestFreshBlockHistoryPreservesSnapshotParentHash(t *testing.T) {
	const epoch, parentSlot = uint64(100_000), uint64(800_000)
	vote := solana.PublicKey{249}
	db := epochBoundaryStakeDB(t, parentSlot, []sealevel.Delegation{{
		VoterPubkey: vote, StakeLamports: 1,
		ActivationEpoch: math.MaxUint64, DeactivationEpoch: math.MaxUint64,
	}})
	require.False(t, global.HasEpochStakes(epoch))
	global.PutEpochStakes(epoch, map[solana.PublicKey]uint64{vote: 1}, map[solana.PublicKey]*epochstakes.VoteAccount{vote: {NodePubkey: vote}}, 1)
	t.Cleanup(func() { global.ClearEpochStakes(epoch); global.SetLeaderScheduleForEpoch(epoch, nil) })
	previousManage := global.ManageLeaderSchedule()
	global.SetManageLeaderSchedule(true)
	t.Cleanup(func() { global.SetManageLeaderSchedule(previousManage) })
	parentHash := solana.Hash{77}
	firstHash := solana.Hash{88}
	manifestState := &state.MithrilState{
		ManifestParentSlot:        parentSlot,
		ManifestParentBankhash:    (solana.Hash{66}).String(),
		ManifestEvictedBlockhash:  (solana.Hash{55}).String(),
		ManifestFeeRateGovernor:   &state.ManifestFeeRateGovernorSeed{},
		ManifestRecentBlockhashes: []state.BlockhashEntry{{Blockhash: parentHash.String()}},
	}
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 8, LeaderScheduleSlotOffset: 8}
	// The first executed block can follow skipped slots; its parent remains the snapshot.
	first := turbine.BlockFromEntries(parentSlot+4, parentSlot, []turbine.Entry{{Hash: firstHash}})
	first.Epoch = epoch
	require.NoError(t, configureInitialBlock(db, first, manifestState, schedule))
	second := &b.Block{Slot: parentSlot + 5, Epoch: epoch, Blockhash: solana.Hash{99}}
	require.NoError(t, configureBlock(second, &sealevel.SlotCtx{Slot: first.Slot, Epoch: epoch, Blockhash: first.Blockhash}, schedule))
	require.Equal(t, [32]byte(firstHash), second.LastBlockhash)
	dir := t.TempDir()
	store, err := blockhistory.Open(dir, 100)
	require.NoError(t, err)
	require.NoError(t, store.RecordBlock(blockForHistory(first, parentHash)))
	require.NoError(t, store.RecordBlock(blockForHistory(second, firstHash)))
	require.Zero(t, first.LastBlockhash, "history must not alter the execution block's nonce context")
	require.NoError(t, store.Prepare(second.Slot))
	require.NoError(t, store.SetRooted(second.Slot))
	reopened, err := blockhistory.Open(dir, 100)
	require.NoError(t, err)
	require.NoError(t, reopened.SetRooted(second.Slot))
	firstAvailable, err := reopened.FirstAvailableBlock()
	require.NoError(t, err)
	require.Equal(t, first.Slot, firstAvailable)
	secondRecord, err := reopened.Get(second.Slot)
	require.NoError(t, err)
	require.Equal(t, firstHash.String(), secondRecord.PreviousBlockhash)
	firstRecord, err := reopened.Get(firstAvailable)
	require.NoError(t, err)
	require.Equal(t, parentHash.String(), firstRecord.PreviousBlockhash,
		"fresh snapshot parent hash must survive first replay block recording, persistence and restart")
}

func TestBlockHistoryUnknownParentRemainsUnavailable(t *testing.T) {
	const parentSlot = uint64(100)
	first := &b.Block{Slot: parentSlot + 1, ParentSlot: parentSlot, Blockhash: solana.Hash{1}}
	second := &b.Block{Slot: parentSlot + 2, ParentSlot: first.Slot, Blockhash: solana.Hash{2}, LastBlockhash: first.Blockhash}
	store, err := blockhistory.Open(t.TempDir(), 100)
	require.NoError(t, err)
	require.Nil(t, blockForHistory(first, solana.Hash{}))
	require.NoError(t, store.RecordBlock(blockForHistory(second, solana.Hash{})))
	require.NoError(t, store.Prepare(second.Slot))
	require.NoError(t, store.SetRooted(second.Slot))
	_, err = store.Get(first.Slot)
	require.ErrorIs(t, err, blockhistory.ErrNotAvailable)
	firstAvailable, err := store.FirstAvailableBlock()
	require.NoError(t, err)
	require.Equal(t, second.Slot, firstAvailable)
	record, err := store.Get(second.Slot)
	require.NoError(t, err)
	require.Equal(t, solana.Hash(first.Blockhash).String(), record.PreviousBlockhash)
	require.Zero(t, first.LastBlockhash)
	require.Equal(t, first.Blockhash, second.LastBlockhash)

	knownParent := solana.Hash{3}
	second.LastBlockhash = knownParent
	require.Same(t, second, blockForHistory(second, first.Blockhash), "an existing parent hash must not be replaced")
	require.Equal(t, [32]byte(knownParent), second.LastBlockhash)
	genesis := &b.Block{}
	require.Same(t, genesis, blockForHistory(genesis, solana.Hash{}))
	childOfGenesis := &b.Block{Slot: 1, ParentSlot: 0}
	require.Nil(t, blockForHistory(childOfGenesis, solana.Hash{}), "a genesis child still requires a known parent hash")
	historyChild := blockForHistory(childOfGenesis, knownParent)
	require.NotNil(t, historyChild)
	require.Equal(t, [32]byte(knownParent), historyChild.LastBlockhash)
	require.Zero(t, childOfGenesis.LastBlockhash)
}
