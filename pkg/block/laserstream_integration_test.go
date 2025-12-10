package block

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/rpcpool/yellowstone-grpc/examples/golang/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MockRpcClientForIntegration struct {
	callCount      int32
	failUntilCall  int32
	leaderToReturn solana.PublicKey
	callTimestamps []time.Time
	mu             sync.Mutex
}

func NewMockRpcClientForIntegration(failUntilCall int32, leader solana.PublicKey) *MockRpcClientForIntegration {
	return &MockRpcClientForIntegration{
		failUntilCall:  failUntilCall,
		leaderToReturn: leader,
		callTimestamps: make([]time.Time, 0),
	}
}

func (m *MockRpcClientForIntegration) GetLeaderForSlot(slot uint64) (solana.PublicKey, error) {
	m.mu.Lock()
	m.callTimestamps = append(m.callTimestamps, time.Now())
	m.mu.Unlock()

	callNum := atomic.AddInt32(&m.callCount, 1)
	if callNum <= m.failUntilCall {
		return solana.PublicKey{}, fmt.Errorf("simulated RPC failure (attempt %d)", callNum)
	}
	return m.leaderToReturn, nil
}

func (m *MockRpcClientForIntegration) GetCallCount() int32 {
	return atomic.LoadInt32(&m.callCount)
}

func (m *MockRpcClientForIntegration) GetCallTimestamps() []time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	timestamps := make([]time.Time, len(m.callTimestamps))
	copy(timestamps, m.callTimestamps)
	return timestamps
}

func TestIntegration_RetrySucceedsAfterTransientFailure(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testLeader := solana.MustPublicKeyFromBase58("HLwR8nYj9tLdLvXEYQfCDh38AGMYCjHYWvNT9Dwc9Ky")
	mock := NewMockRpcClientForIntegration(2, testLeader)

	lsBlock := &proto.SubscribeUpdateBlock{
		Slot:        100,
		Blockhash:   "EhYXq3bK8yWAPsVKLy53ysrHP1RPADvg6oDEXn7dgKda",
		BlockHeight: &proto.BlockHeight{BlockHeight: 200},
		BlockTime: &proto.UnixTimestamp{
			Timestamp: 1000,
		},
		ParentBlockhash: "EhYXq3bK8yWAPsVKLy53ysrHP1RPADvg6oDEXn7dgKda",
		Rewards: &proto.Rewards{
			Rewards: []*proto.Reward{},
		},
	}

	start := time.Now()
	block := fromLaserStreamWithMock(lsBlock, mock)
	elapsed := time.Since(start)

	require.NotNil(t, block)
	require.NotNil(t, block.BlockReward)
	assert.Equal(t, testLeader, block.BlockReward.Leader)

	assert.Equal(t, int32(3), mock.GetCallCount())
	assert.GreaterOrEqual(t, elapsed, 300*time.Millisecond)

	t.Logf("✓ Retry succeeded after 2 transient failures in %v", elapsed)
}

func TestIntegration_ExponentialBackoffTiming(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testLeader := solana.MustPublicKeyFromBase58("HLwR8nYj9tLdLvXEYQfCDh38AGMYCjHYWvNT9Dwc9Ky")
	mock := NewMockRpcClientForIntegration(4, testLeader)

	lsBlock := &proto.SubscribeUpdateBlock{
		Slot:        100,
		Blockhash:   "EhYXq3bK8yWAPsVKLy53ysrHP1RPADvg6oDEXn7dgKda",
		BlockHeight: &proto.BlockHeight{BlockHeight: 200},
		BlockTime: &proto.UnixTimestamp{
			Timestamp: 1000,
		},
		ParentBlockhash: "EhYXq3bK8yWAPsVKLy53ysrHP1RPADvg6oDEXn7dgKda",
		Rewards: &proto.Rewards{
			Rewards: []*proto.Reward{},
		},
	}

	start := time.Now()
	block := fromLaserStreamWithMock(lsBlock, mock)
	elapsed := time.Since(start)

	require.NotNil(t, block.BlockReward)
	assert.Equal(t, testLeader, block.BlockReward.Leader)
	assert.Equal(t, int32(5), mock.GetCallCount())

	expectedMinDelay := 1000 * time.Millisecond
	expectedMaxDelay := 1100 * time.Millisecond
	assert.GreaterOrEqual(t, elapsed, expectedMinDelay)
	assert.Less(t, elapsed, expectedMaxDelay)

	timestamps := mock.GetCallTimestamps()
	require.Equal(t, 5, len(timestamps))

	for i := 1; i < len(timestamps); i++ {
		actualDelay := timestamps[i].Sub(timestamps[i-1])
		expectedDelay := time.Duration(i*100) * time.Millisecond
		assert.Greater(t, actualDelay, expectedDelay-50*time.Millisecond)
	}

	t.Logf("✓ Exponential backoff verified: %d calls in %v", mock.GetCallCount(), elapsed)
}

func TestIntegration_PanicAfterAllRetriesExhausted(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testLeader := solana.MustPublicKeyFromBase58("HLwR8nYj9tLdLvXEYQfCDh38AGMYCjHYWvNT9Dwc9Ky")
	mock := NewMockRpcClientForIntegration(999, testLeader)

	lsBlock := &proto.SubscribeUpdateBlock{
		Slot:        100,
		Blockhash:   "EhYXq3bK8yWAPsVKLy53ysrHP1RPADvg6oDEXn7dgKda",
		BlockHeight: &proto.BlockHeight{BlockHeight: 200},
		BlockTime: &proto.UnixTimestamp{
			Timestamp: 1000,
		},
		ParentBlockhash: "EhYXq3bK8yWAPsVKLy53ysrHP1RPADvg6oDEXn7dgKda",
		Rewards: &proto.Rewards{
			Rewards: []*proto.Reward{},
		},
	}

	defer func() {
		if r := recover(); r != nil {
			panicMsg := r.(string)
			assert.Contains(t, panicMsg, "unable to get blockreward for slot 100")
			assert.Contains(t, panicMsg, "after 10 attempts")
			assert.Contains(t, panicMsg, "simulated RPC failure")
			assert.Equal(t, int32(10), mock.GetCallCount())
			t.Logf("✓ Panic correctly triggered after 10 failed attempts")
			return
		}
		t.Fatal("Expected panic but none occurred")
	}()

	fromLaserStreamWithMock(lsBlock, mock)
}

func TestIntegration_SuccessOnFirstAttempt(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	testLeader := solana.MustPublicKeyFromBase58("HLwR8nYj9tLdLvXEYQfCDh38AGMYCjHYWvNT9Dwc9Ky")
	mock := NewMockRpcClientForIntegration(0, testLeader)

	lsBlock := &proto.SubscribeUpdateBlock{
		Slot:        100,
		Blockhash:   "EhYXq3bK8yWAPsVKLy53ysrHP1RPADvg6oDEXn7dgKda",
		BlockHeight: &proto.BlockHeight{BlockHeight: 200},
		BlockTime: &proto.UnixTimestamp{
			Timestamp: 1000,
		},
		ParentBlockhash: "EhYXq3bK8yWAPsVKLy53ysrHP1RPADvg6oDEXn7dgKda",
		Rewards: &proto.Rewards{
			Rewards: []*proto.Reward{},
		},
	}

	start := time.Now()
	block := fromLaserStreamWithMock(lsBlock, mock)
	elapsed := time.Since(start)

	require.NotNil(t, block.BlockReward)
	assert.Equal(t, testLeader, block.BlockReward.Leader)
	assert.Equal(t, int32(1), mock.GetCallCount())
	assert.Less(t, elapsed, 100*time.Millisecond)

	t.Logf("✓ Success on first attempt: 1 call in %v", elapsed)
}

func fromLaserStreamWithMock(lsBlock *proto.SubscribeUpdateBlock, mock *MockRpcClientForIntegration) *Block {
	block := &Block{}
	block.Slot = lsBlock.GetSlot()
	block.BlockHeight = lsBlock.BlockHeight.BlockHeight
	block.UnixTimestamp = lsBlock.BlockTime.Timestamp
	block.Blockhash = solana.MustHashFromBase58(lsBlock.Blockhash)
	block.LastBlockhash = solana.MustHashFromBase58(lsBlock.ParentBlockhash)

	blockReward := blockRewardRewards(block.Rewards)
	if blockReward != nil {
		block.BlockReward = &BlockRewardsInfo{Leader: blockReward.Pubkey}
	} else {
		if mock != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			result, err := RetryWithExponentialBackoff(ctx, maxRetriesGetLeaderForSlot, func(retryCtx context.Context) (interface{}, error) {
				return mock.GetLeaderForSlot(lsBlock.Slot)
			})

			if err != nil {
				panic(fmt.Sprintf("unable to get blockreward for slot %d after %d attempts: %v",
					lsBlock.Slot, maxRetriesGetLeaderForSlot, err))
			}

			leaderForSlot := result.(solana.PublicKey)
			block.BlockReward = &BlockRewardsInfo{Leader: leaderForSlot}
		}
	}

	return block
}
