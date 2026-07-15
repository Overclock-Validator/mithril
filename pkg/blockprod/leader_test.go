package blockprod

import (
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type captureBroadcaster struct {
	mu      sync.Mutex
	packets [][]byte
}

func (c *captureBroadcaster) Broadcast(packets [][]byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, pkt := range packets {
		c.packets = append(c.packets, append([]byte(nil), pkt...))
	}
	return nil
}

func (c *captureBroadcaster) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.packets)
}

func TestLeaderLoopActivatesAndFinishesSlot(t *testing.T) {
	bc := &captureBroadcaster{}
	controller := NewController()
	leader := txfixture.PayerPubkey()

	var slot uint64 = 42
	global.SetSlot(41)
	global.SetAlpenglowBlockID(41, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(41, solana.Hash{2})
	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  controller,
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: bc,
		CurrentSlot: func() uint64 { return slot },
		LeaderForSlot: func(s uint64) (solana.PublicKey, bool) {
			if s == 42 {
				return leader, true
			}
			return solana.PublicKey{}, false
		},
		ParentContext: func(uint64, uint64) ParentContext {
			return ParentContext{ParentBankhash: solana.Hash{1}}
		},
		ParentBlockID: func(slot uint64) (solana.Hash, bool) {
			return global.AlpenglowBlockID(slot - 1)
		},
		BankHash:     DefaultBankHash,
		PollInterval: 5 * time.Millisecond,
	})

	stop := make(chan struct{})
	go loop.Run(stop)
	time.Sleep(25 * time.Millisecond)
	require.NotNil(t, controller.WorkingBank())
	assert.Greater(t, bc.count(), 0)

	slot = 43
	time.Sleep(25 * time.Millisecond)
	assert.Nil(t, controller.WorkingBank())
	close(stop)
}

func TestLeaderLoopProducesMissedLeaderSlotAfterWallClockPasses(t *testing.T) {
	bc := &captureBroadcaster{}
	controller := NewController()
	leader := txfixture.PayerPubkey()

	var wallSlot uint64 = 50
	global.SetSlot(44)
	global.SetAlpenglowBlockID(44, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(44, solana.Hash{2})
	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  controller,
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: bc,
		CurrentSlot: func() uint64 { return wallSlot },
		LeaderForSlot: func(s uint64) (solana.PublicKey, bool) {
			if s >= 45 && s <= 47 {
				return leader, true
			}
			return solana.PublicKey{}, false
		},
		ParentContext: func(uint64, uint64) ParentContext {
			return ParentContext{ParentBankhash: solana.Hash{1}}
		},
		ParentBlockID: func(uint64) (solana.Hash, bool) { return solana.Hash{1}, true },
		BankHash:      DefaultBankHash,
		PollInterval:  5 * time.Millisecond,
	})

	stop := make(chan struct{})
	go loop.Run(stop)
	time.Sleep(15 * time.Millisecond)
	assert.True(t, loop.isLeaderSlotFinished(45) || controller.WorkingBank() != nil)

	wallSlot = 51
	time.Sleep(50 * time.Millisecond)
	assert.Nil(t, controller.WorkingBank())
	assert.True(t, loop.isLeaderSlotFinished(45))
	assert.True(t, loop.isLeaderSlotFinished(46))
	assert.True(t, loop.isLeaderSlotFinished(47))

	close(stop)
}
