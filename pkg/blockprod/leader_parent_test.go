package blockprod

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLeaderLoopWaitsForParentBlockID(t *testing.T) {
	global.ResetAlpenglowChainMetadata()
	bc := &captureBroadcaster{}
	controller := NewController()
	leader := txfixture.PayerPubkey()

	const slot = uint64(210)
	const parentSlot = slot - 1
	var wallSlot atomic.Uint64
	wallSlot.Store(slot)
	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  controller,
		Identity:    identityHolder(txfixture.PayerPrivateKey()),
		Broadcaster: bc,
		CurrentSlot: wallSlot.Load,
		LeaderForSlot: func(s uint64) (solana.PublicKey, bool) {
			if s == slot {
				return leader, true
			}
			return solana.PublicKey{}, false
		},
		ParentContext: func(uint64) ParentContext {
			return coherentTestParentContext(parentSlot, solana.Hash{9})
		},
		ParentBlockID: func(uint64) (solana.Hash, bool) {
			return global.AlpenglowBlockID(parentSlot)
		},
		PollInterval: 5 * time.Millisecond,
	})

	global.SetReplayFrontier(parentSlot)
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		loop.Run(stop)
	}()
	t.Cleanup(func() {
		close(stop)
		<-done
	})
	time.Sleep(25 * time.Millisecond)
	assert.Nil(t, controller.WorkingBank())
	assert.Equal(t, 0, bc.count())

	global.SetAlpenglowBlockID(parentSlot, solana.Hash{7})
	global.SetAlpenglowChainedMerkleRoot(parentSlot, solana.Hash{8})
	require.Eventually(t, func() bool { return controller.WorkingBank() != nil }, time.Second, 5*time.Millisecond)
	assert.Greater(t, bc.count(), 0)
}

func TestLeaderProductionFailsClosedAtEpochTransition(t *testing.T) {
	global.SetReplayFrontier(9)
	loop := NewLeaderLoop(LeaderLoopConfig{
		Identity:      identityHolder(txfixture.PayerPrivateKey()),
		EpochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 10},
		ParentContext: func(uint64) ParentContext {
			return ParentContext{ParentSlot: 9, ParentBankhash: solana.Hash{1}}
		},
	})

	err := loop.startSlotLocked(10)
	require.ErrorIs(t, err, errEpochTransitionProductionUnsupported)
	require.Nil(t, loop.activeBank)
}

func TestLeaderProductionFailsClosedDuringPartitionedRewards(t *testing.T) {
	global.SetReplayFrontier(8)
	loop := NewLeaderLoop(LeaderLoopConfig{
		Identity:      identityHolder(txfixture.PayerPrivateKey()),
		EpochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 10},
		ParentContext: func(uint64) ParentContext {
			return ParentContext{ParentSlot: 8, ParentBankhash: solana.Hash{1}, EpochRewardsActive: true}
		},
	})

	err := loop.startSlotLocked(9)
	require.ErrorIs(t, err, errEpochRewardsProductionUnsupported)
	require.Nil(t, loop.activeBank)
}

func TestLeaderProductionRequiresCompleteTransactionStatuses(t *testing.T) {
	incomplete, err := replay.NewTransactionStatusCacheFromSnapshot(nil)
	require.NoError(t, err)
	tests := []struct {
		name     string
		statuses *replay.TransactionStatusView
	}{
		{name: "missing"},
		{name: "incomplete", statuses: incomplete.View()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			global.SetReplayFrontier(8)
			loop := NewLeaderLoop(LeaderLoopConfig{
				Identity: identityHolder(txfixture.PayerPrivateKey()),
				ParentContext: func(uint64) ParentContext {
					return ParentContext{
						ParentSlot:          8,
						ParentBankhash:      solana.Hash{1},
						TransactionStatuses: tt.statuses,
					}
				},
			})

			err := loop.startSlotLocked(9)
			require.ErrorIs(t, err, errParentNotReady)
			require.ErrorContains(t, err, "complete transaction-status view missing")
			require.Nil(t, loop.activeBank)
		})
	}
}
