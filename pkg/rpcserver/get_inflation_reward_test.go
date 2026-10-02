package rpcserver

import (
	"context"
	"encoding/json"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/epochrewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func TestGetInflationRewardCommitmentAndDefaults(t *testing.T) {
	store, err := epochrewards.Open(t.TempDir(), 512)
	require.NoError(t, err)
	server := &RpcServer{
		epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128},
		slotCtx:       &sealevel.SlotCtx{Slot: 260},
		epochRewards:  store,
	}
	address := solana.NewWallet().PublicKey()
	missing := solana.NewWallet().PublicKey()
	require.NoError(t, server.RecordEpochRewards(&b.Block{
		Slot: 256, Epoch: 2,
		Rewards: []rpc.BlockReward{{
			Pubkey: address, Lamports: 42, PostBalance: 1_042, RewardType: rpc.RewardTypeVoting,
		}},
	}))

	confirmed, err := server.GetInflationReward(context.Background(), rawParams(t, []any{
		[]string{address.String(), missing.String()}, map[string]any{"commitment": "confirmed"},
	}))
	require.NoError(t, err)
	require.Len(t, confirmed, 2)
	require.Nil(t, confirmed[0], "executed rewards are not confirmed until their state is rooted")
	require.Nil(t, confirmed[1])

	finalized, err := server.GetInflationReward(context.Background(), rawParams(t, []any{
		[]string{address.String()}, map[string]any{"commitment": "finalized", "epoch": 1},
	}))
	require.NoError(t, err)
	require.Nil(t, finalized[0])
	_, err = server.GetInflationReward(context.Background(), rawParams(t, []any{
		[]string{address.String()}, map[string]any{"commitment": "finalized", "epoch": 1, "minContextSlot": 1},
	}))
	var contextErr *MinContextSlotNotReachedError
	require.ErrorAs(t, err, &contextErr)
	require.Zero(t, contextErr.ContextSlot)

	require.NoError(t, server.PrepareEpochRewards(256))
	require.NoError(t, server.SetRootedEpochRewardsSlot(256))
	finalized, err = server.GetInflationReward(context.Background(), rawParams(t, []any{
		[]string{address.String()}, map[string]any{"epoch": 1},
	}))
	require.NoError(t, err)
	require.Equal(t, &InflationRewardResp{Epoch: 1, EffectiveSlot: 256, Amount: 42, PostBalance: 1_042}, finalized[0])
	confirmed, err = server.GetInflationReward(context.Background(), rawParams(t, []any{
		[]string{address.String()}, map[string]any{"commitment": "confirmed"},
	}))
	require.NoError(t, err)
	require.Equal(t, finalized, confirmed)
	_, err = server.GetInflationReward(context.Background(), rawParams(t, []any{
		[]string{address.String()}, map[string]any{"commitment": "confirmed", "minContextSlot": 260},
	}))
	require.ErrorAs(t, err, &contextErr)
	require.Equal(t, uint64(256), contextErr.ContextSlot)
}

func TestGetInflationRewardRejectsInvalidRequests(t *testing.T) {
	store, err := epochrewards.Open(t.TempDir(), 512)
	require.NoError(t, err)
	server := &RpcServer{
		epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128},
		slotCtx:       &sealevel.SlotCtx{Slot: 260},
		epochRewards:  store,
	}
	address := solana.NewWallet().PublicKey().String()

	tests := []struct {
		name   string
		params []any
		typeOf any
	}{
		{name: "null addresses", params: []any{nil}, typeOf: &InvalidParamsError{}},
		{name: "invalid address", params: []any{[]string{"not-a-key"}}, typeOf: &InvalidParamsError{}},
		{name: "processed commitment", params: []any{[]string{address}, map[string]any{"commitment": "processed"}}, typeOf: &InvalidParamsError{}},
		{name: "minimum context", params: []any{[]string{address}, map[string]any{"minContextSlot": 261}}, typeOf: &MinContextSlotNotReachedError{}},
		{name: "too many addresses", params: []any{[]string{address, address, address, address, address, address}}, typeOf: &InvalidParamsError{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := server.GetInflationReward(context.Background(), rawParams(t, test.params))
			require.Error(t, err)
			switch test.typeOf.(type) {
			case *InvalidParamsError:
				require.IsType(t, &InvalidParamsError{}, err)
			case *MinContextSlotNotReachedError:
				require.IsType(t, &MinContextSlotNotReachedError{}, err)
			}
		})
	}
}

func TestGetInflationRewardAfterSnapshotBeforeReplay(t *testing.T) {
	store, err := epochrewards.Open(t.TempDir(), 512)
	require.NoError(t, err)
	server := &RpcServer{
		epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128},
		epochRewards:  store,
	}
	address := solana.NewWallet().PublicKey()
	require.NoError(t, server.RecordEpochRewards(&b.Block{
		Slot: 256, Epoch: 2,
		Rewards: []rpc.BlockReward{{
			Pubkey: address, Lamports: 42, PostBalance: 1_042, RewardType: rpc.RewardTypeVoting,
		}},
	}))
	require.NoError(t, server.PrepareEpochRewards(256))
	require.NoError(t, server.SetRootedEpochRewardsSlot(256))

	result, err := server.GetInflationReward(context.Background(), rawParams(t, []any{
		[]string{address.String()}, map[string]any{"epoch": 1, "minContextSlot": 256},
	}))
	require.NoError(t, err)
	require.Equal(t, &InflationRewardResp{Epoch: 1, EffectiveSlot: 256, Amount: 42, PostBalance: 1_042}, result[0])
}

func TestInflationRewardZeroCommissionJSON(t *testing.T) {
	encoded, err := json.Marshal(InflationRewardResp{Epoch: 1, EffectiveSlot: 256, Amount: 0, PostBalance: 1_000})
	require.NoError(t, err)
	require.JSONEq(t, `{"epoch":1,"effectiveSlot":256,"amount":0,"postBalance":1000,"commission":null}`, string(encoded))
}

func rawParams(t *testing.T, value any) jsonrpc.RawParams {
	t.Helper()
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	return jsonrpc.RawParams(encoded)
}
