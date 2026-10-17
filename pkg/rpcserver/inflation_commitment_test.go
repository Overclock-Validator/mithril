package rpcserver

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func TestInflationRewardHTTPUsesDurableEpoch(t *testing.T) {
	server := NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128}, solana.Hash{1})
	t.Cleanup(func() { require.NoError(t, server.listener.Close()) })
	require.NoError(t, server.EnableEpochRewards(t.TempDir(), 512, 0))
	address := solana.PublicKey{1}
	reward := func(slot, epoch, amount uint64) *b.Block {
		return &b.Block{Slot: slot, Epoch: epoch, Rewards: []rpc.BlockReward{
			{Pubkey: address, Lamports: int64(amount), PostBalance: 100 + amount, RewardType: rpc.RewardTypeVoting},
		}}
	}
	require.NoError(t, server.RecordEpochRewards(reward(128, 1, 5)))
	require.NoError(t, server.PrepareEpochRewards(255))
	require.NoError(t, server.SetRootedEpochRewardsSlot(255))
	// An executed slot alone does not establish a certified or committed bank.
	server.SetSlotCtx(&sealevel.SlotCtx{Slot: 260})
	require.NoError(t, server.RecordEpochRewards(reward(256, 2, 7)))
	endpoint := httptest.NewServer(server)
	t.Cleanup(endpoint.Close)
	client := &http.Client{Timeout: 3 * time.Second}
	call := func(config map[string]any) ([]*InflationRewardResp, *jsonrpc.JSONRPCError) {
		t.Helper()
		body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": "getInflationReward",
			"params": []any{[]string{address.String()}, config}})
		require.NoError(t, err)
		response, err := client.Post(endpoint.URL, "application/json", bytes.NewReader(body))
		require.NoError(t, err)
		defer response.Body.Close()
		var reply struct {
			Result []*InflationRewardResp `json:"result"`
			Error  *jsonrpc.JSONRPCError  `json:"error"`
		}
		require.NoError(t, json.NewDecoder(response.Body).Decode(&reply))
		return reply.Result, reply.Error
	}
	for _, phase := range []string{"pending", "prepared"} {
		if phase == "prepared" {
			require.NoError(t, server.PrepareEpochRewards(256))
		}
		for _, commitment := range []string{"confirmed", "finalized"} {
			result, err := call(map[string]any{"commitment": commitment})
			require.Nil(t, err, phase)
			require.Len(t, result, 1)
			require.Equal(t, uint64(0), result[0].Epoch)
			require.Equal(t, uint64(5), result[0].Amount)
			_, err = call(map[string]any{"commitment": commitment, "epoch": 1})
			require.NotNil(t, err)
			require.Equal(t, jsonrpc.ErrorCode(-32004), err.Code)
			_, err = call(map[string]any{"commitment": commitment, "minContextSlot": 256})
			require.NotNil(t, err)
			require.Equal(t, jsonrpc.ErrorCode(-32016), err.Code)
			require.Equal(t, map[string]any{"contextSlot": float64(255)}, err.Data)
		}
	}
	// Replay's verified publication can make both commitments readable before
	// an account checkpoint. Pending capture and prepared files above did not.
	server.PublishEpochRewards(256)
	for _, commitment := range []string{"confirmed", "finalized"} {
		result, err := call(map[string]any{"commitment": commitment, "minContextSlot": 256})
		require.Nil(t, err)
		require.Equal(t, uint64(1), result[0].Epoch)
		require.Equal(t, uint64(7), result[0].Amount)
	}
	require.Equal(t, uint64(255), server.epochRewards.RootedSlot())
	server.SetHistoryRecoveryPending(true)
	_, rpcErr := call(map[string]any{"minContextSlot": 256})
	require.NotNil(t, rpcErr)
	server.PublishEpochRewards(256)
	server.SetHistoryRecoveryPending(false)
	_, rpcErr = call(map[string]any{"minContextSlot": 256})
	require.NotNil(t, rpcErr, "publication during recovery must not survive reopening the gate")
	require.Equal(t, jsonrpc.ErrorCode(-32016), rpcErr.Code)
	require.NoError(t, server.SetRootedEpochRewardsSlot(256))
	for _, commitment := range []string{"confirmed", "finalized"} {
		result, err := call(map[string]any{"commitment": commitment, "minContextSlot": 256})
		require.Nil(t, err)
		require.Len(t, result, 1)
		require.Equal(t, uint64(1), result[0].Epoch)
		require.Equal(t, uint64(7), result[0].Amount)
	}
}
