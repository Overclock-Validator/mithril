package blockprod

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPOCLeaderSlotReplayReadyOtherLeaderParent(t *testing.T) {
	self := txfixture.PayerPubkey()
	other := solana.NewWallet().PublicKey()

	loop := &LeaderLoop{
		identity: txfixture.PayerPrivateKey(),
		leaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			switch slot {
			case 44:
				return other, true
			case 45:
				return self, true
			default:
				return solana.PublicKey{}, false
			}
		},
	}

	global.SetSlot(43)
	ready, err := loop.pocLeaderSlotReplayReady(45)
	assert.False(t, ready)
	require.ErrorIs(t, err, errParentNotReady)

	global.SetSlot(44)
	global.SetAlpenglowBlockID(44, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(44, solana.Hash{2})
	ready, err = loop.pocLeaderSlotReplayReady(45)
	assert.True(t, ready)
	assert.NoError(t, err)
}

func TestPOCLeaderSlotReplayReadyOwnParentAllowsLocalFinish(t *testing.T) {
	self := txfixture.PayerPubkey()

	loop := &LeaderLoop{
		identity:            txfixture.PayerPrivateKey(),
		finishedLeaderSlots: map[uint64]struct{}{42: {}},
		leaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			if slot == 42 || slot == 43 {
				return self, true
			}
			return solana.PublicKey{}, false
		},
	}

	global.SetAlpenglowBlockID(42, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(42, solana.Hash{2})
	global.SetSlot(40)
	ready, err := loop.pocLeaderSlotReplayReady(43)
	assert.True(t, ready)
	assert.NoError(t, err)
}

func TestLeaderLoopDoesNotAttemptLeaderUntilOtherParentReplayed(t *testing.T) {
	self := txfixture.PayerPubkey()
	other := solana.NewWallet().PublicKey()
	var wallSlot uint64 = 50

	global.SetSlot(43)
	global.SetAlpenglowBlockID(43, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(43, solana.Hash{2})

	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  NewController(),
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: &captureBroadcaster{},
		CurrentSlot: func() uint64 { return wallSlot },
		LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
			switch slot {
			case 44:
				return other, true
			case 45:
				return self, true
			default:
				return solana.PublicKey{}, false
			}
		},
		ParentContext: func(uint64, uint64) ParentContext {
			return ParentContext{ParentBankhash: solana.Hash{2}}
		},
		ParentBlockID: func(slot uint64) (solana.Hash, bool) {
			return global.AlpenglowBlockID(slot - 1)
		},
		BankHash: DefaultBankHash,
	})

	loop.tick()
	assert.Nil(t, loop.activeBank)

	global.SetSlot(44)
	global.SetAlpenglowBlockID(44, solana.Hash{3})
	global.SetAlpenglowChainedMerkleRoot(44, solana.Hash{4})
	loop.tick()
	assert.True(t, loop.isLeaderSlotFinished(45))
}
