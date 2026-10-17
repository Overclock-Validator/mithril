package rpcserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/epochrewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
	bin "github.com/gagliardetto/binary"
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
	require.Error(t, err, "executed rewards do not establish complete rooted history")
	require.Nil(t, confirmed)

	finalized, err := server.GetInflationReward(context.Background(), rawParams(t, []any{
		[]string{address.String()}, map[string]any{"commitment": "finalized", "epoch": 1},
	}))
	require.Error(t, err)
	require.Nil(t, finalized)
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
	tooMany := make([]string, 33)
	for index := range tooMany {
		tooMany[index] = address
	}

	tests := []struct {
		name   string
		params []any
		typeOf any
	}{
		{name: "null addresses", params: []any{nil}, typeOf: &InvalidParamsError{}},
		{name: "invalid address", params: []any{[]string{"not-a-key"}}, typeOf: &InvalidParamsError{}},
		{name: "processed commitment", params: []any{[]string{address}, map[string]any{"commitment": "processed"}}, typeOf: &InvalidParamsError{}},
		{name: "minimum context", params: []any{[]string{address}, map[string]any{"minContextSlot": 261}}, typeOf: &MinContextSlotNotReachedError{}},
		{name: "too many addresses", params: []any{tooMany}, typeOf: &InvalidParamsError{}},
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

func TestInflationRewardNullCommissionJSON(t *testing.T) {
	encoded, err := json.Marshal(InflationRewardResp{Epoch: 1, EffectiveSlot: 256, Amount: 0, PostBalance: 1_000})
	require.NoError(t, err)
	require.JSONEq(t, `{"epoch":1,"effectiveSlot":256,"amount":0,"postBalance":1000,"commission":null}`, string(encoded))
}

func TestGetInflationRewardAddressLimitCompatibility(t *testing.T) {
	store, err := epochrewards.Open(t.TempDir(), 512)
	require.NoError(t, err)
	server := &RpcServer{epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128}, epochRewards: store}
	address := solana.PublicKey{1}.String()
	require.NoError(t, server.RecordEpochRewards(&b.Block{Slot: 256, ParentSlot: 255, Epoch: 2}))
	bank, err := sealevel.NewBankSysvars(256)
	require.NoError(t, err)
	server.RecordEpochRewardStatus(&b.Block{Slot: 256, ParentSlot: 255, Epoch: 2}, bank, false)
	require.NoError(t, server.PrepareEpochRewards(256))
	require.NoError(t, server.SetRootedEpochRewardsSlot(256))
	for _, count := range []int{6, 32, 33} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			addresses := make([]string, count)
			for index := range addresses {
				addresses[index] = address
			}
			result, err := server.GetInflationReward(context.Background(), rawParams(t, []any{addresses}))
			if count > 32 {
				require.IsType(t, &InvalidParamsError{}, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, result, count)
		})
	}
}

func TestGetInflationRewardUnavailableEpochs(t *testing.T) {
	store, err := epochrewards.Open(t.TempDir(), 128)
	require.NoError(t, err)
	server := &RpcServer{epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128}, epochRewards: store}
	address := solana.PublicKey{1}
	require.NoError(t, server.RecordEpochRewards(&b.Block{Slot: 256, ParentSlot: 255, Epoch: 2,
		Rewards: []rpc.BlockReward{{Pubkey: address, Lamports: 7, PostBalance: 107, RewardType: rpc.RewardTypeVoting}},
	}))
	require.NoError(t, server.PrepareEpochRewards(256))
	require.NoError(t, server.SetRootedEpochRewardsSlot(256))
	t.Run("future", func(t *testing.T) {
		_, err := server.GetInflationReward(context.Background(), rawParams(t, []any{[]string{address.String()}, map[string]any{"epoch": 3}}))
		require.Error(t, err, "an unavailable reward epoch must not look like an account with no reward")
	})
	t.Run("pruned", func(t *testing.T) {
		require.NoError(t, server.SetRootedEpochRewardsSlot(512))
		_, err := server.GetInflationReward(context.Background(), rawParams(t, []any{[]string{address.String()}, map[string]any{"epoch": 1}}))
		require.Error(t, err, "pruned reward history must not look like an account with no reward")
	})
}

func TestEpochRewardAvailabilityHTTP(t *testing.T) {
	server := NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128}, solana.Hash{1})
	t.Cleanup(func() { require.NoError(t, server.listener.Close()) })
	dir := t.TempDir()
	require.NoError(t, server.EnableEpochRewards(dir, 512, 0))
	httpServer := httptest.NewServer(server)
	t.Cleanup(httpServer.Close)
	client := &http.Client{Timeout: 5 * time.Second}
	address, missing := solana.PublicKey{1}, solana.PublicKey{2}
	call := func(method string, params []any) (json.RawMessage, *jsonrpc.JSONRPCError) {
		t.Helper()
		body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
		require.NoError(t, err)
		response, err := client.Post(httpServer.URL, "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		defer response.Body.Close()
		var reply struct {
			Result json.RawMessage       `json:"result"`
			Error  *jsonrpc.JSONRPCError `json:"error"`
		}
		require.NoError(t, json.NewDecoder(response.Body).Decode(&reply))
		return reply.Result, reply.Error
	}
	block := &b.Block{Slot: 256, ParentSlot: 255, Epoch: 2, BlockHeight: 100, Rewards: []rpc.BlockReward{
		{Pubkey: address, Lamports: 7, PostBalance: 107, RewardType: rpc.RewardTypeVoting},
	}}
	require.NoError(t, server.RecordEpochRewards(block))
	server.RecordEpochRewardStatus(block, rewardDistributionBank(t, 256, sealevel.SysvarEpochRewards{
		Active: true, DistributionStartingBlockHeight: 101, NumPartitions: 1,
	}), true)
	require.NoError(t, server.PrepareEpochRewards(256))
	require.NoError(t, server.SetRootedEpochRewardsSlot(256))
	_, rpcErr := call("getInflationReward", []any{[]string{missing.String()}, map[string]any{"epoch": 1}})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(-32017), rpcErr.Code)
	require.Equal(t, "Epoch rewards period still active at slot 256", rpcErr.Message)
	require.Equal(t, map[string]any{"slot": float64(256), "currentBlockHeight": float64(100), "rewardsCompleteBlockHeight": float64(102)}, rpcErr.Data)
	_, rpcErr = call("getInflationReward", []any{[]string{address.String(), missing.String()}, map[string]any{"epoch": 1}})
	require.NotNil(t, rpcErr, "a retained legacy record does not certify a missing address")
	result, rpcErr := call("getInflationReward", []any{[]string{address.String()}, map[string]any{"epoch": 1}})
	require.Nil(t, rpcErr, "known committed entries remain readable before complete coverage")
	require.Contains(t, string(result), `"amount":7`)

	completed := &b.Block{Slot: 257, ParentSlot: 256, Epoch: 2}
	require.NoError(t, server.RecordEpochRewards(completed))
	server.RecordEpochRewardStatus(completed, rewardCoverageBank(t, 257, false), true)
	require.NoError(t, server.PrepareEpochRewards(257))
	require.False(t, server.epochRewards.EpochAvailable(1))
	require.NoError(t, server.SetRootedEpochRewardsSlot(257))
	for _, commitment := range []string{"confirmed", "finalized"} {
		result, rpcErr = call("getInflationReward", []any{[]string{address.String(), missing.String()}, map[string]any{"commitment": commitment}})
		require.Nil(t, rpcErr)
		var rewards []*InflationRewardResp
		require.NoError(t, json.Unmarshal(result, &rewards))
		require.Len(t, rewards, 2)
		require.Equal(t, uint64(7), rewards[0].Amount)
		require.Nil(t, rewards[1])
	}
	require.NoError(t, server.EnableEpochRewards(dir, 512, 257))
	require.True(t, server.epochRewards.EpochAvailable(1))
	_, rpcErr = call("getInflationReward", []any{[]string{missing.String()}, map[string]any{"minContextSlot": 258}})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(-32016), rpcErr.Code)
	_, rpcErr = call("recordEpochRewardStatus", []any{})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(-32601), rpcErr.Code)
	require.NoError(t, server.SetRootedEpochRewardsSlot(1024))
	_, rpcErr = call("getInflationReward", []any{[]string{missing.String()}, map[string]any{"epoch": 1}})
	require.NotNil(t, rpcErr)
	require.Equal(t, jsonrpc.ErrorCode(-32001), rpcErr.Code)
}

func TestEpochRewardStatusRequiresObservedBoundaryAndSysvar(t *testing.T) {
	store, err := epochrewards.Open(t.TempDir(), 512)
	require.NoError(t, err)
	server := &RpcServer{epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128}, epochRewards: store}
	server.RecordEpochRewardStatus(&b.Block{Slot: 300, ParentSlot: 299, Epoch: 2}, rewardCoverageBank(t, 300, false), true)
	require.NoError(t, store.Prepare(300))
	require.NoError(t, store.SetRooted(300))
	require.False(t, store.EpochAvailable(1), "a mid-epoch snapshot cannot establish missing reward history")
	boundary := &b.Block{Slot: 384, ParentSlot: 383, Epoch: 3}
	server.RecordEpochRewardStatus(boundary, nil, true)
	missingSysvar, err := sealevel.NewBankSysvars(385)
	require.NoError(t, err)
	server.RecordEpochRewardStatus(&b.Block{Slot: 385, ParentSlot: 384, Epoch: 3}, missingSysvar, true)
	server.RecordEpochRewardStatus(&b.Block{Slot: 386, ParentSlot: 385, Epoch: 3}, rewardCoverageBank(t, 387, false), true)
	require.NoError(t, store.Prepare(386))
	require.NoError(t, store.SetRooted(386))
	require.False(t, store.EpochAvailable(2), "missing or wrong-bank sysvars cannot complete an epoch")
}

func rewardCoverageBank(t *testing.T, slot uint64, active bool) *sealevel.BankSysvars {
	return rewardDistributionBank(t, slot, sealevel.SysvarEpochRewards{Active: active})
}

func rewardDistributionBank(t *testing.T, slot uint64, value sealevel.SysvarEpochRewards) *sealevel.BankSysvars {
	t.Helper()
	var encoded bytes.Buffer
	require.NoError(t, value.MarshalWithEncoder(bin.NewBinEncoder(&encoded)))
	bank, err := sealevel.NewBankSysvars(slot, &accounts.Account{
		Key: sealevel.SysvarEpochRewardsAddr, Lamports: 1, Owner: addresses.SysvarOwnerAddr, Data: encoded.Bytes(),
	})
	require.NoError(t, err)
	return bank
}

func rawParams(t *testing.T, value any) jsonrpc.RawParams {
	t.Helper()
	encoded, err := json.Marshal(value)
	require.NoError(t, err)
	return jsonrpc.RawParams(encoded)
}
