package rpcserver

import (
	"context"
	"encoding/json"
	"math"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestInflationRewardUnavailableReasons(t *testing.T) {
	for _, tc := range []struct {
		name  string
		root  uint64
		epoch uint64
		code  jsonrpc.ErrorCode
	}{
		{"distribution", 256, 1, -32017},
		{"pruned", 1024, 1, -32001},
		{"future", 256, 3, -32004},
		{"maximum epoch", 256, math.MaxUint64, -32004},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := &RpcServer{epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128}}
			require.NoError(t, server.EnableEpochRewards(t.TempDir(), 512, 255))
			block := &b.Block{Slot: 256, ParentSlot: 255, Epoch: 2, BlockHeight: 100}
			server.RecordEpochRewardStatus(block, rewardDistributionBank(t, 256, sealevel.SysvarEpochRewards{
				Active: true, DistributionStartingBlockHeight: 101, NumPartitions: 3,
			}), true)
			require.NoError(t, server.PrepareEpochRewards(256))
			require.NoError(t, server.SetRootedEpochRewardsSlot(tc.root))
			_, err := server.GetInflationReward(context.Background(), rawParams(t, []any{[]string{solana.PublicKey{2}.String()}, map[string]any{"epoch": tc.epoch}}))
			require.Error(t, err)
			converter, ok := err.(interface {
				ToJSONRPCError() (jsonrpc.JSONRPCError, error)
			})
			require.True(t, ok)
			wire, err := converter.ToJSONRPCError()
			require.NoError(t, err)
			require.Equal(t, tc.code, wire.Code)
			if tc.code == -32017 {
				data, err := json.Marshal(wire.Data)
				require.NoError(t, err)
				require.JSONEq(t, `{"slot":256,"currentBlockHeight":100,"rewardsCompleteBlockHeight":104}`, string(data))
			}
		})
	}
}

func TestInflationRewardErrorCodec(t *testing.T) {
	for _, original := range []jsonrpc.RPCErrorCodec{
		&EpochRewardHistoryPrunedError{Message: "Reward history for slot 256 has been cleaned up"},
		&EpochRewardsPeriodActiveError{Slot: 256, CurrentBlockHeight: 100, RewardsCompleteBlockHeight: 104},
	} {
		wire, err := original.ToJSONRPCError()
		require.NoError(t, err)
		encoded, err := json.Marshal(wire)
		require.NoError(t, err)
		var decoded jsonrpc.JSONRPCError
		require.NoError(t, json.Unmarshal(encoded, &decoded))
		var restored jsonrpc.RPCErrorCodec
		if wire.Code == rpcCodeBlockCleanedUp {
			restored = &EpochRewardHistoryPrunedError{}
			require.Nil(t, decoded.Data)
		} else {
			restored = &EpochRewardsPeriodActiveError{}
		}
		require.NoError(t, restored.FromJSONRPCError(decoded))
		require.Equal(t, original, restored)
		decoded.Code = 1
		require.Error(t, restored.FromJSONRPCError(decoded))
	}
}

func TestInflationRewardPartitionProgress(t *testing.T) {
	server := &RpcServer{epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128}}
	dir := t.TempDir()
	require.NoError(t, server.EnableEpochRewards(dir, 512, 255))
	var first, last solana.PublicKey
	for i := 1; first.IsZero() || last.IsZero(); i++ {
		address := solana.PublicKey{byte(i)}
		switch rewards.CalculateRewardPartitionForPubkey(address, [32]byte{}, 3) {
		case 0:
			first = address
		case 2:
			last = address
		}
	}
	capture := func(slot, parent, height uint64, active bool) {
		t.Helper()
		block := &b.Block{Slot: slot, ParentSlot: parent, Epoch: 2, BlockHeight: height}
		server.RecordEpochRewardStatus(block, rewardDistributionBank(t, slot, sealevel.SysvarEpochRewards{
			Active: active, DistributionStartingBlockHeight: 101, NumPartitions: 3,
		}), true)
	}
	query := func(addresses ...string) ([]*InflationRewardResp, error) {
		return server.GetInflationReward(context.Background(), rawParams(t, []any{addresses, map[string]any{"epoch": 1}}))
	}
	capture(256, 255, 100, true)
	capture(260, 256, 101, true) // Skipped slots do not advance reward partitions.
	capture(263, 260, 102, true)
	server.PublishEpochRewards(256)
	_, err := query(first.String())
	require.IsType(t, &EpochRewardsPeriodActiveError{}, err, "later executed banks are not yet verified")
	server.PublishEpochRewards(260)
	result, err := query(first.String())
	require.NoError(t, err)
	require.Equal(t, []*InflationRewardResp{nil}, result, "absence in an already processed partition is known")
	_, err = query(first.String(), last.String())
	require.IsType(t, &EpochRewardsPeriodActiveError{}, err, "one incomplete partition fails the whole request")
	require.Equal(t, uint64(101), err.(*EpochRewardsPeriodActiveError).CurrentBlockHeight)
	require.NoError(t, server.PrepareEpochRewards(260))
	require.NoError(t, server.SetRootedEpochRewardsSlot(260))
	require.NoError(t, server.EnableEpochRewards(dir, 512, 260))
	result, err = query(first.String())
	require.NoError(t, err)
	require.Nil(t, result[0], "durable progress survives reopening")
	_, err = query(last.String())
	require.IsType(t, &EpochRewardsPeriodActiveError{}, err)
	capture(265, 260, 103, false)
	server.PublishEpochRewards(265)
	result, err = query(last.String())
	require.NoError(t, err)
	require.Nil(t, result[0])
	require.NoError(t, server.RewindEpochRewards(265))
	_, err = query(last.String())
	require.IsType(t, &EpochRewardsPeriodActiveError{}, err, "rewind restores committed progress")
}

func TestInflationRewardEmptyRequestRequiresHistory(t *testing.T) {
	server := &RpcServer{epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128}}
	require.NoError(t, server.EnableEpochRewards(t.TempDir(), 512, 255))
	_, err := server.GetInflationReward(context.Background(), rawParams(t, []any{[]string{}, map[string]any{"epoch": uint64(math.MaxUint64)}}))
	require.IsType(t, &EpochRewardHistoryUnavailableError{}, err)
}

func TestInflationRewardProgressRequiresCurrentBank(t *testing.T) {
	server := &RpcServer{epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128}}
	require.NoError(t, server.EnableEpochRewards(t.TempDir(), 512, 255))
	server.RecordEpochRewardStatus(&b.Block{Slot: 256, ParentSlot: 255, Epoch: 2, BlockHeight: 100},
		rewardDistributionBank(t, 256, sealevel.SysvarEpochRewards{Active: true, DistributionStartingBlockHeight: 101, NumPartitions: 3}), true)
	server.PublishEpochRewards(256)
	server.RecordEpochRewardStatus(&b.Block{Slot: 257, ParentSlot: 256, Epoch: 2, BlockHeight: 101}, rewardCoverageBank(t, 258, true), true)
	server.PublishEpochRewards(257)
	_, err := server.GetInflationReward(context.Background(), rawParams(t, []any{[]string{solana.PublicKey{1}.String()}}))
	require.IsType(t, &EpochRewardHistoryUnavailableError{}, err, "older progress cannot be paired with a newer published context")
}

func TestInflationRewardSkippedBoundaryRemainsReadable(t *testing.T) {
	server := &RpcServer{epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128}}
	require.NoError(t, server.EnableEpochRewards(t.TempDir(), 100, 255))
	server.RecordEpochRewardStatus(&b.Block{Slot: 260, ParentSlot: 255, Epoch: 2}, rewardCoverageBank(t, 260, false), false)
	require.NoError(t, server.PrepareEpochRewards(260))
	require.NoError(t, server.SetRootedEpochRewardsSlot(357))
	result, err := server.GetInflationReward(context.Background(), rawParams(t, []any{[]string{solana.PublicKey{1}.String()}, map[string]any{"epoch": 1}}))
	require.NoError(t, err, "an observed retained boundary is stronger evidence than the nominal skipped start slot")
	require.Nil(t, result[0])
}
