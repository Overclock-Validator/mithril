package blockprod

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func coherentTestParentContext(parentSlot uint64, bankhash solana.Hash) ParentContext {
	blockID, hasBlockID := global.AlpenglowBlockID(parentSlot)
	chainedRoot, hasChainedRoot := global.AlpenglowChainedMerkleRoot(parentSlot)
	return ParentContext{
		ReplayGeneration:           1,
		ParentSlot:                 parentSlot,
		ParentBankhash:             bankhash,
		ParentBlockID:              blockID,
		HasParentBlockID:           hasBlockID,
		ParentChainedMerkleRoot:    chainedRoot,
		HasParentChainedMerkleRoot: hasChainedRoot,
		PrevFeeGovernor: &sealevel.FeeRateGovernor{
			TargetLamportsPerSignature: 10_000,
			TargetSignaturesPerSlot:    20_000,
			MinLamportsPerSignature:    5_000,
			MaxLamportsPerSignature:    100_000,
			LamportsPerSignature:       5_000,
			PrevLamportsPerSignature:   5_000,
		},
		TransactionStatuses: completeTestTransactionStatuses(),
	}
}

func TestLeaderStartPinsIdentityFromReplaySnapshot(t *testing.T) {
	global.ResetAlpenglowChainMetadata()
	t.Cleanup(global.ResetAlpenglowChainMetadata)
	const slot = uint64(82)
	const parentSlot = slot - 1
	originalID := solana.Hash{1}
	originalRoot := solana.Hash{2}
	global.SetReplayFrontier(parentSlot)
	global.SetAlpenglowBlockID(parentSlot, originalID)
	global.SetAlpenglowChainedMerkleRoot(parentSlot, originalRoot)
	parent := coherentTestParentContext(parentSlot, solana.Hash{3})

	// Process-wide metadata may already describe another fork. The atomic replay
	// snapshot remains the sole identity source for this producer bank.
	global.SetAlpenglowBlockID(parentSlot, solana.Hash{9})
	global.SetAlpenglowChainedMerkleRoot(parentSlot, solana.Hash{8})
	loop := NewLeaderLoop(LeaderLoopConfig{
		Identity:    identityHolder(txfixture.PayerPrivateKey()),
		Broadcaster: &captureBroadcaster{},
		ParentContext: func(uint64) ParentContext {
			return parent
		},
	})

	require.NoError(t, loop.startSlotLocked(slot))
	require.Equal(t, originalID, loop.activeParentID)
	require.Equal(t, originalRoot, loop.activeParentChainedRoot)
	require.Equal(t, parent.ReplayGeneration, loop.activeParentGeneration)
}

func TestLeaderStartRejectsGenerationSwitchWhileOpening(t *testing.T) {
	global.ResetAlpenglowChainMetadata()
	t.Cleanup(global.ResetAlpenglowChainMetadata)
	const slot = uint64(86)
	const parentSlot = slot - 1
	global.SetReplayFrontier(parentSlot)
	global.SetAlpenglowBlockID(parentSlot, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(parentSlot, solana.Hash{2})
	parent := coherentTestParentContext(parentSlot, solana.Hash{3})
	reads := 0
	broadcaster := &captureBroadcaster{}
	loop := NewLeaderLoop(LeaderLoopConfig{
		Identity:    identityHolder(txfixture.PayerPrivateKey()),
		Broadcaster: broadcaster,
		ParentContext: func(uint64) ParentContext {
			reads++
			current := parent
			if reads > 1 {
				current.ReplayGeneration++
			}
			return current
		},
	})

	err := loop.startSlotLocked(slot)
	require.ErrorIs(t, err, errParentNotReady)
	require.ErrorContains(t, err, "replay parent changed while opening")
	require.Nil(t, loop.activeBank)
	require.Zero(t, broadcaster.count(), "an incoherent parent must be rejected before broadcasting the header")
}

func TestLeaderRestartsOnGenerationOnlyForkSwitch(t *testing.T) {
	global.ResetAlpenglowChainMetadata()
	t.Cleanup(global.ResetAlpenglowChainMetadata)
	const slot = uint64(90)
	const parentSlot = slot - 1
	global.SetReplayFrontier(parentSlot)
	global.SetAlpenglowBlockID(parentSlot, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(parentSlot, solana.Hash{2})
	parent := coherentTestParentContext(parentSlot, solana.Hash{3})
	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  NewController(),
		Identity:    identityHolder(txfixture.PayerPrivateKey()),
		Broadcaster: &captureBroadcaster{},
		CurrentSlot: func() uint64 { return slot },
		LeaderForSlot: func(candidate uint64) (solana.PublicKey, bool) {
			return txfixture.PayerPubkey(), candidate == slot
		},
		ParentContext: func(uint64) ParentContext { return parent },
	})

	loop.tick()
	first := loop.activeBank
	require.NotNil(t, first)
	parent.ReplayGeneration++ // same slot/hash/identity, different replay fork generation
	loop.tick()
	require.NotNil(t, loop.activeBank)
	require.NotSame(t, first, loop.activeBank)
	require.Equal(t, parent.ReplayGeneration, loop.activeParentGeneration)
}

func TestLeaderSuppressesFooterIfGenerationSwitchesDuringCommit(t *testing.T) {
	global.ResetAlpenglowChainMetadata()
	t.Cleanup(global.ResetAlpenglowChainMetadata)
	const slot = uint64(94)
	const parentSlot = slot - 1
	global.SetReplayFrontier(parentSlot)
	global.SetAlpenglowBlockID(parentSlot, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(parentSlot, solana.Hash{2})
	parent := coherentTestParentContext(parentSlot, solana.Hash{3})
	parent.PrevFeeGovernor = &sealevel.FeeRateGovernor{
		PrevLamportsPerSignature: 5000,
		LamportsPerSignature:     5000,
	}
	broadcaster := &captureBroadcaster{}
	identity := txfixture.PayerPrivateKey()
	session := turbine.NewBroadcastSession(turbine.BroadcastSessionConfig{
		Leader:                  identity,
		Slot:                    slot,
		ParentSlot:              parentSlot,
		ParentBlockID:           parent.ParentBlockID,
		ParentChainedMerkleRoot: parent.ParentChainedMerkleRoot,
		Broadcaster:             broadcaster,
	})
	slotCtx := &sealevel.SlotCtx{
		Slot:            slot,
		ParentSlot:      parentSlot,
		Features:        features.NewFeaturesDefault(),
		FeeRateGovernor: parent.PrevFeeGovernor,
	}
	sink := NewShredSink(session)
	bank := NewWorkingBank(BankConfig{
		SlotCtx: slotCtx,
		Slot:    slot,
		Leader:  identity.PublicKey(),
		Sink:    sink,
	})
	blockDelivered := false
	loop := NewLeaderLoop(LeaderLoopConfig{
		Identity:      identityHolder(identity),
		AccountsDb:    &accountsdb.AccountsDb{},
		EpochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 1_000},
		ParentContext: func(uint64) ParentContext { return parent },
		OnBlock:       func(*block.Block) { blockDelivered = true },
	})
	loop.activeSlot = slot
	loop.activeBank = bank
	loop.activeSess = session
	loop.activeSink = sink
	loop.parentCtx = parent
	loop.activeParentID = parent.ParentBlockID
	loop.activeParentChainedRoot = parent.ParentChainedMerkleRoot
	loop.activeParentGeneration = parent.ReplayGeneration
	commitCalled := false
	loop.commitLeaderSlot = func(in replay.CommitLeaderInput) (*sealevel.SlotCtx, error) {
		commitCalled = true
		require.Equal(t, parent.ParentBlockID, solana.Hash(in.Block.AlpenglowParentBlockID))
		in.SlotCtx.FinalBankhash = make([]byte, 32)
		parent.ReplayGeneration++
		return in.SlotCtx, nil
	}

	loop.finishActiveSlotLocked()
	require.True(t, commitCalled)
	assert.Nil(t, loop.activeBank)
	assert.Zero(t, broadcaster.count(), "no footer or ending tick may be emitted for the stale parent generation")
	assert.False(t, blockDelivered)
}
