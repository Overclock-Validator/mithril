package blockprod

import (
	"errors"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildLeaderBlockCarriesSelectedSkippedParent(t *testing.T) {
	parentID := solana.Hash{9}
	bank := NewWorkingBank(BankConfig{
		Slot:    212,
		SlotCtx: &sealevel.SlotCtx{Slot: 212, ParentSlot: 208},
	})
	block := BuildLeaderBlock(LeaderBlockInput{
		Bank: bank,
		EpochSchedule: &sealevel.SysvarEpochSchedule{
			SlotsPerEpoch: 32, FirstNormalEpoch: 0, FirstNormalSlot: 0,
		},
		PrevFeeGovernor: &sealevel.FeeRateGovernor{TargetLamportsPerSignature: 5000},
		ParentBlockID:   parentID,
	})
	if block.ParentSlot != 208 || block.SourceParentSlot != 208 {
		t.Fatalf("leader parent slots = parent %d source %d, want 208", block.ParentSlot, block.SourceParentSlot)
	}
	if !block.HasAlpenglowParentBlockID || solana.Hash(block.AlpenglowParentBlockID) != parentID {
		t.Fatalf("leader parent block id = %v present=%v", block.AlpenglowParentBlockID, block.HasAlpenglowParentBlockID)
	}
}

func TestLeaderPreparationFailureDoesNotCompleteBroadcast(t *testing.T) {
	bc := &captureBroadcaster{}
	identity := txfixture.PayerPrivateKey()
	session := turbine.NewBroadcastSession(turbine.BroadcastSessionConfig{
		Leader: identity, Slot: 212, ParentSlot: 208,
		ParentBlockID: solana.Hash{1}, ParentChainedMerkleRoot: solana.Hash{2},
		Broadcaster: bc,
	})
	if err := session.BroadcastHeader(solana.Hash{1}); err != nil {
		t.Fatal(err)
	}
	before := bc.count()
	var fatalErr error
	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:    NewController(),
		AccountsDb:    &accountsdb.AccountsDb{},
		EpochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32},
		PrepareCommit: func(replay.CommitLeaderInput) (*replay.PreparedLeaderCommit, error) {
			return nil, errors.New("injected preparation failure")
		},
		OnFatal: func(err error) { fatalErr = err },
	})
	loop.activeSlot = 212
	loop.activeParentID = solana.Hash{1}
	loop.parentCtx = ParentContext{PrevFeeGovernor: &sealevel.FeeRateGovernor{TargetLamportsPerSignature: 5000}}
	loop.activeSess = session
	loop.activeBank = NewWorkingBank(BankConfig{
		Slot: 212, SlotCtx: &sealevel.SlotCtx{Slot: 212, ParentSlot: 208},
	})
	loop.finishActiveSlotLocked()

	if got := bc.count(); got != before {
		t.Fatalf("preparation failure broadcast %d additional packets", got-before)
	}
	if !loop.halted || fatalErr == nil || loop.activeBank != nil {
		t.Fatalf("leader halt state: halted=%v fatal=%v active=%v", loop.halted, fatalErr, loop.activeBank)
	}
}

func TestLeaderWindowUsesVerifiedParentReady(t *testing.T) {
	const leaderSlot = uint64(212)
	parent := alpenglow.BlockID{Slot: 208, Hash: solana.Hash{1}}
	global.SetSlot(parent.Slot)
	global.SetAlpenglowBlockID(parent.Slot, parent.Hash)
	global.SetAlpenglowChainedMerkleRoot(parent.Slot, solana.Hash{2})

	loop := NewLeaderLoop(LeaderLoopConfig{
		ProductionParent: func(slot uint64) alpenglow.BlockProductionParent {
			if slot != leaderSlot {
				t.Fatalf("production parent requested for slot %d", slot)
			}
			return alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentReady, Parent: parent}
		},
	})

	ready, err := loop.pocLeaderSlotReplayReady(leaderSlot)
	if err != nil || !ready {
		t.Fatalf("verified ParentReady did not open leader window: ready=%v err=%v", ready, err)
	}
}

func TestLeaderWindowRejectsUnexecutedParentReadyFork(t *testing.T) {
	const leaderSlot = uint64(216)
	selected := alpenglow.BlockID{Slot: 212, Hash: solana.Hash{3}}
	global.SetSlot(selected.Slot)
	global.SetAlpenglowBlockID(selected.Slot, solana.Hash{4})
	global.SetAlpenglowChainedMerkleRoot(selected.Slot, solana.Hash{5})

	loop := NewLeaderLoop(LeaderLoopConfig{
		ProductionParent: func(uint64) alpenglow.BlockProductionParent {
			return alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentReady, Parent: selected}
		},
	})

	ready, err := loop.pocLeaderSlotReplayReady(leaderSlot)
	if ready || err == nil {
		t.Fatalf("leader window opened on a selected fork Mithril has not executed: ready=%v err=%v", ready, err)
	}
}

func TestLeaderWindowRejectsUnpreparedEpochBoundary(t *testing.T) {
	const leaderSlot = uint64(32)
	parent := alpenglow.BlockID{Slot: 31, Hash: solana.Hash{6}}
	global.SetSlot(parent.Slot)
	global.SetAlpenglowBlockID(parent.Slot, parent.Hash)
	global.SetAlpenglowChainedMerkleRoot(parent.Slot, solana.Hash{7})

	loop := NewLeaderLoop(LeaderLoopConfig{
		EpochSchedule: &sealevel.SysvarEpochSchedule{
			SlotsPerEpoch:    32,
			FirstNormalEpoch: 0,
			FirstNormalSlot:  0,
		},
		ProductionParent: func(uint64) alpenglow.BlockProductionParent {
			return alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentReady, Parent: parent}
		},
	})

	ready, err := loop.pocLeaderSlotReplayReady(leaderSlot)
	if ready || err == nil {
		t.Fatalf("leader production crossed an unprepared epoch boundary: ready=%v err=%v", ready, err)
	}
}

func TestLeaderLoopWaitsForParentBlockID(t *testing.T) {
	bc := &captureBroadcaster{}
	controller := NewController()
	leader := txfixture.PayerPubkey()

	const slot = uint64(210)
	const parentSlot = slot - 1
	var wallSlot uint64 = slot
	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  controller,
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: bc,
		CurrentSlot: func() uint64 { return wallSlot },
		LeaderForSlot: func(s uint64) (solana.PublicKey, bool) {
			if s == slot {
				return leader, true
			}
			return solana.PublicKey{}, false
		},
		ParentContext: func(uint64, uint64) ParentContext {
			return ParentContext{ParentBankhash: solana.Hash{9}}
		},
		ParentBlockID: func(uint64) (solana.Hash, bool) {
			return global.AlpenglowBlockID(parentSlot)
		},
		BankHash:     DefaultBankHash,
		PollInterval: 5 * time.Millisecond,
	})

	global.SetSlot(parentSlot)
	stop := make(chan struct{})
	go loop.Run(stop)
	time.Sleep(25 * time.Millisecond)
	assert.Nil(t, controller.WorkingBank())
	assert.Equal(t, 0, bc.count())

	global.SetAlpenglowBlockID(parentSlot, solana.Hash{7})
	global.SetAlpenglowChainedMerkleRoot(parentSlot, solana.Hash{8})
	time.Sleep(25 * time.Millisecond)
	require.NotNil(t, controller.WorkingBank())
	assert.Greater(t, bc.count(), 0)

	close(stop)
}
