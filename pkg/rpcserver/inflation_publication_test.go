package rpcserver

import (
	"context"
	"fmt"
	"sync"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func TestInflationRewardVerifiedPublicationBeforeDurableFold(t *testing.T) {
	server := &RpcServer{epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128}}
	require.NoError(t, server.EnableEpochRewards(t.TempDir(), 512, 255))
	address := solana.PublicKey{1}
	block := &b.Block{Slot: 256, ParentSlot: 255, Epoch: 2, Rewards: []rpc.BlockReward{
		{Pubkey: address, Lamports: 7, PostBalance: 107, RewardType: rpc.RewardTypeVoting},
	}}
	require.NoError(t, server.RecordEpochRewards(block))
	server.RecordEpochRewardStatus(block, rewardCoverageBank(t, 256, false), true)
	params := rawParams(t, []any{[]string{address.String()}, map[string]any{"minContextSlot": 256}})
	_, err := server.GetInflationReward(context.Background(), params)
	require.Error(t, err, "execution alone is not verified publication")
	server.PublishEpochRewards(256)
	result, err := server.GetInflationReward(context.Background(), params)
	require.NoError(t, err, "a verified canonical reward must not wait for a durable fold")
	require.Len(t, result, 1)
	require.Equal(t, uint64(1), result[0].Epoch)
	require.Equal(t, uint64(7), result[0].Amount)
	require.Equal(t, uint64(255), server.epochRewards.RootedSlot(), "read publication must not advance the recovery checkpoint")
}

func TestInflationRewardPublicationPinsWholeRequest(t *testing.T) {
	server := &RpcServer{epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128}}
	require.NoError(t, server.EnableEpochRewards(t.TempDir(), 100000, 0))
	addresses := make([]string, maxInflationRewardAddresses)
	for i := range addresses {
		addresses[i] = solana.PublicKey{byte(i + 1)}.String()
	}
	params := rawParams(t, []any{addresses})
	publish := func(epoch uint64) error {
		block := &b.Block{Slot: epoch * 128, ParentSlot: epoch*128 - 1, Epoch: epoch}
		for _, address := range addresses {
			block.Rewards = append(block.Rewards, rpc.BlockReward{Pubkey: solana.MustPublicKeyFromBase58(address),
				Lamports: int64(epoch), PostBalance: 100 + epoch, RewardType: rpc.RewardTypeVoting})
		}
		if err := server.RecordEpochRewards(block); err != nil {
			return err
		}
		server.PublishEpochRewards(block.Slot)
		return nil
	}
	require.NoError(t, publish(1))
	var wg sync.WaitGroup
	errors := make(chan error, 5)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for epoch := uint64(2); epoch <= 128; epoch++ {
			if err := publish(epoch); err != nil {
				errors <- err
				return
			}
		}
	}()
	for reader := 0; reader < 4; reader++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 128; i++ {
				result, err := server.GetInflationReward(context.Background(), params)
				if err != nil {
					errors <- err
					return
				}
				for _, record := range result {
					if record == nil || record.Epoch != result[0].Epoch || record.Amount != record.Epoch+1 || record.EffectiveSlot != (record.Epoch+1)*128 {
						errors <- fmt.Errorf("mixed reward view: %+v", result)
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errors)
	for err := range errors {
		require.NoError(t, err)
	}
}
