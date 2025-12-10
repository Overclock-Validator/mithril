package block

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/rpcpool/yellowstone-grpc/examples/golang/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockRpcClient struct {
	callCount      int32
	failUntilCall  int32
	leaderToReturn solana.PublicKey
}

func newMockRpcClient(failUntilCall int32, leader solana.PublicKey) *mockRpcClient {
	return &mockRpcClient{
		failUntilCall:  failUntilCall,
		leaderToReturn: leader,
	}
}

func (m *mockRpcClient) GetLeaderForSlot(slot uint64) (solana.PublicKey, error) {
	callNum := atomic.AddInt32(&m.callCount, 1)
	if callNum <= m.failUntilCall {
		return solana.PublicKey{}, fmt.Errorf("mock RPC failure (call %d)", callNum)
	}
	return m.leaderToReturn, nil
}

func (m *mockRpcClient) GetCallCount() int32 {
	return atomic.LoadInt32(&m.callCount)
}

func TestFromLaserStream_BlockRewardFromRewards(t *testing.T) {
	testLeader := solana.MustPublicKeyFromBase58("HLwR8nYj9tLdLvXEYQfCDh38AGMYCjHYWvNT9Dwc9Ky")

	lsBlock := &proto.SubscribeUpdateBlock{
		Slot:        100,
		Blockhash:   "EhYXq3bK8yWAPsVKLy53ysrHP1RPADvg6oDEXn7dgKda",
		BlockHeight: &proto.BlockHeight{BlockHeight: 200},
		BlockTime: &proto.UnixTimestamp{
			Timestamp: 1000,
		},
		ParentBlockhash: "EhYXq3bK8yWAPsVKLy53ysrHP1RPADvg6oDEXn7dgKda",
		Rewards: &proto.Rewards{
			Rewards: []*proto.Reward{
				{
					Pubkey:      testLeader.String(),
					Lamports:    5000,
					PostBalance: 10000,
					RewardType:  proto.RewardType_Fee,
				},
			},
		},
	}

	block := FromLaserStream(lsBlock, nil)

	require.NotNil(t, block.BlockReward)
	assert.Equal(t, testLeader, block.BlockReward.Leader)
	assert.Equal(t, uint64(5000), block.BlockReward.Lamports)
	assert.Equal(t, uint64(10000), block.BlockReward.PostBalance)
}

func TestFromLaserStream_NoRpcClient(t *testing.T) {
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

	block := FromLaserStream(lsBlock, nil)

	require.NotNil(t, block)
	assert.Nil(t, block.BlockReward)
	assert.Equal(t, uint64(100), block.Slot)
}

func TestFromLaserStream_RetrySucceeds(t *testing.T) {
	testLeader := solana.MustPublicKeyFromBase58("HLwR8nYj9tLdLvXEYQfCDh38AGMYCjHYWvNT9Dwc9Ky")
	mock := newMockRpcClient(2, testLeader)

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

	block := FromLaserStream(lsBlock, mock)
	require.NotNil(t, block)
	require.NotNil(t, block.BlockReward)

	assert.Equal(t, testLeader, block.BlockReward.Leader)
	assert.Equal(t, int32(3), mock.GetCallCount())
}

func TestFromLaserStream_RetryPanicsAfterMaxAttempts(t *testing.T) {
	testLeader := solana.MustPublicKeyFromBase58("HLwR8nYj9tLdLvXEYQfCDh38AGMYCjHYWvNT9Dwc9Ky")
	mock := newMockRpcClient(999, testLeader)

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
			assert.Equal(t, int32(10), mock.GetCallCount())
		} else {
			t.Fatal("expected panic but none occurred")
		}
	}()

	FromLaserStream(lsBlock, mock)
}

func TestFromLaserStream_TransactionsParsing(t *testing.T) {
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

	block := FromLaserStream(lsBlock, nil)

	assert.Equal(t, uint64(100), block.Slot)
	assert.Equal(t, uint64(200), block.BlockHeight)
	assert.Equal(t, int64(1000), block.UnixTimestamp)
	assert.Len(t, block.Transactions, 0)
}

func TestFromLaserStream_RewardPartitions(t *testing.T) {
	numPartitions := uint64(4)

	lsBlock := &proto.SubscribeUpdateBlock{
		Slot:        100,
		Blockhash:   "EhYXq3bK8yWAPsVKLy53ysrHP1RPADvg6oDEXn7dgKda",
		BlockHeight: &proto.BlockHeight{BlockHeight: 200},
		BlockTime: &proto.UnixTimestamp{
			Timestamp: 1000,
		},
		ParentBlockhash: "EhYXq3bK8yWAPsVKLy53ysrHP1RPADvg6oDEXn7dgKda",
		Rewards: &proto.Rewards{
			Rewards:       []*proto.Reward{},
			NumPartitions: &proto.NumPartitions{NumPartitions: numPartitions},
		},
	}

	block := FromLaserStream(lsBlock, nil)

	assert.Equal(t, numPartitions, block.NumRewardPartitions)
}

func TestFromLaserStream_RewardPartitions_Nil(t *testing.T) {
	lsBlock := &proto.SubscribeUpdateBlock{
		Slot:        100,
		Blockhash:   "EhYXq3bK8yWAPsVKLy53ysrHP1RPADvg6oDEXn7dgKda",
		BlockHeight: &proto.BlockHeight{BlockHeight: 200},
		BlockTime: &proto.UnixTimestamp{
			Timestamp: 1000,
		},
		ParentBlockhash: "EhYXq3bK8yWAPsVKLy53ysrHP1RPADvg6oDEXn7dgKda",
		Rewards: &proto.Rewards{
			Rewards:       []*proto.Reward{},
			NumPartitions: nil,
		},
	}

	block := FromLaserStream(lsBlock, nil)

	assert.Equal(t, uint64(^uint64(0)), block.NumRewardPartitions)
}
