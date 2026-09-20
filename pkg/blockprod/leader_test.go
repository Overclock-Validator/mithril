package blockprod

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
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

	var slot atomic.Uint64
	slot.Store(42)
	global.SetReplayFrontier(41)
	global.SetAlpenglowBlockID(41, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(41, solana.Hash{2})
	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  controller,
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: bc,
		CurrentSlot: slot.Load,
		LeaderForSlot: func(s uint64) (solana.PublicKey, bool) {
			if s == 42 {
				return leader, true
			}
			return solana.PublicKey{}, false
		},
		ParentContext: func(uint64) ParentContext {
			return coherentTestParentContext(41, solana.Hash{1})
		},
		ParentBlockID: func(uint64) (solana.Hash, bool) { return solana.Hash{1}, true },
		PollInterval:  5 * time.Millisecond,
	})

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
	require.Eventually(t, func() bool { return controller.WorkingBank() != nil }, time.Second, 5*time.Millisecond)
	assert.Greater(t, bc.count(), 0)

	slot.Store(43)
	require.Eventually(t, func() bool { return controller.WorkingBank() == nil }, time.Second, 5*time.Millisecond)
}

func TestLeaderLoopStoresGeneratedHeader(t *testing.T) {
	spool, err := turbine.OpenShredSpool(t.TempDir(), 0)
	require.NoError(t, err)
	t.Cleanup(spool.Close)
	global.SetReplayFrontier(41)
	global.SetAlpenglowBlockID(41, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(41, solana.Hash{2})
	capture := &captureBroadcaster{}
	loop := NewLeaderLoop(LeaderLoopConfig{
		Identity: txfixture.PayerPrivateKey(), Broadcaster: capture, ShredSpool: spool,
		ParentContext: func(uint64) ParentContext {
			return coherentTestParentContext(41, solana.Hash{1})
		},
	})
	require.NoError(t, loop.startSlotLocked(42))
	t.Cleanup(loop.abortActiveSlotLocked)
	stored, err := spool.ReadSlot(42)
	require.NoError(t, err)
	require.NotEmpty(t, stored)
	require.ElementsMatch(t, capture.packets, stored)
}

func TestLeaderLoopDoesNotBackfillMissedSlotsAheadOfReplay(t *testing.T) {
	bc := &captureBroadcaster{}
	controller := NewController()
	leader := txfixture.PayerPubkey()

	var wallSlot atomic.Uint64
	wallSlot.Store(50)
	global.SetReplayFrontier(44)
	global.SetAlpenglowBlockID(44, solana.Hash{1})
	global.SetAlpenglowChainedMerkleRoot(44, solana.Hash{2})
	loop := NewLeaderLoop(LeaderLoopConfig{
		Controller:  controller,
		Identity:    txfixture.PayerPrivateKey(),
		Broadcaster: bc,
		CurrentSlot: wallSlot.Load,
		LeaderForSlot: func(s uint64) (solana.PublicKey, bool) {
			if s >= 45 && s <= 47 {
				return leader, true
			}
			return solana.PublicKey{}, false
		},
		ParentContext: func(uint64) ParentContext {
			return coherentTestParentContext(44, solana.Hash{1})
		},
		ParentBlockID: func(uint64) (solana.Hash, bool) { return solana.Hash{1}, true },
		PollInterval:  5 * time.Millisecond,
	})

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
	time.Sleep(15 * time.Millisecond)
	assert.Nil(t, controller.WorkingBank())
	assert.False(t, loop.isLeaderSlotFinished(45))

	wallSlot.Store(51)
	time.Sleep(50 * time.Millisecond)
	assert.Nil(t, controller.WorkingBank())
	assert.False(t, loop.isLeaderSlotFinished(45))
	assert.False(t, loop.isLeaderSlotFinished(46))
	assert.False(t, loop.isLeaderSlotFinished(47))
}
