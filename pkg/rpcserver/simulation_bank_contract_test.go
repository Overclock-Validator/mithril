package rpcserver

import (
	"encoding/json"
	"math"
	"net/http/httptest"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
)

func TestSimulationReplacementBlockhashBankContext(t *testing.T) {
	server := transactionOptionsServer(t)
	oldHash, oldHeight := global.LatestBlockHash(), global.BlockHeight()
	t.Cleanup(func() {
		global.SetLatestBlockHash(oldHash)
		global.SetBlockHeight(oldHeight)
		replay.ResetChainTip()
	})
	metadata := simulationBankFixtureAt(t, 150, solana.Hash{2}, 6000)
	metadata.BlockHeight = 90
	bank, err := newSimulationBankContext(metadata, server.acctsDb, server.acctsDb)
	require.NoError(t, err)
	bank.UnrootedRead = nil
	server.SetSlotCtx(bank)
	_, wire := testLegacyTransaction(t)
	params := mustRawParams(t, []any{base58.Encode(wire), map[string]any{"commitment": "processed", "replaceRecentBlockhash": true}})

	for _, tc := range []struct {
		name     string
		progress uint64
		hash     [32]byte
	}{
		{"different_hash", 50, [32]byte{9}},
		{"same_hash_different_height", 900, bank.Blockhash},
	} {
		t.Run(tc.name, func(t *testing.T) {
			global.SetSlot(tc.progress)
			global.SetLatestBlockHash(tc.hash)
			global.SetBlockHeight(800)
			replay.UpdateChainTipFromSlotCtxWithBankMetadata(bank, bank.Features, nil, replay.ChainTipIdentity{}, replay.ChainTipBankMetadata{BlockHeight: 90})
			response, err := server.SimulateTransaction(t.Context(), params)
			require.NoError(t, err)
			require.Equal(t, bank.Slot, response.Context.Slot)
			require.NotNil(t, response.Value.ReplacementBlockhash)
			require.Equal(t, solana.Hash(bank.Blockhash).String(), response.Value.ReplacementBlockhash.Blockhash)
			require.Equal(t, uint64(240), response.Value.ReplacementBlockhash.LastValidBlockHeight)
		})
	}

	t.Run("HTTP", func(t *testing.T) {
		rpc := NewRpcServer(server.acctsDb, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 512}, solana.Hash{1})
		require.NoError(t, rpc.listener.Close())
		rpc.SetSlotCtx(bank)
		root := server.rootedBank.Load()
		rpc.PublishRootedBankState(root.Slot, root.BlockHeight, root.TransactionCount)
		rpc.SetRootedExecutionBank(root.executionBank)
		endpoint := httptest.NewServer(rpc)
		t.Cleanup(endpoint.Close)
		body := getBlockHistoryHTTPCall(t, endpoint, "simulateTransaction", string(params))
		require.NotContains(t, body, "error")
		var result SimulateTransactionResp
		require.NoError(t, json.Unmarshal(body["result"], &result))
		require.Equal(t, bank.Slot, result.Context.Slot)
		require.Empty(t, result.Context.ApiVersion)
		require.Equal(t, &ReplacementBlockhashPayload{Blockhash: solana.Hash(bank.Blockhash).String(), LastValidBlockHeight: 240}, result.Value.ReplacementBlockhash)
	})

	replay.ResetChainTip()
	_, err = server.SimulateTransaction(t.Context(), params)
	require.Error(t, err, "missing bank metadata must not fall back to unrelated process globals")

	other, err := newSimulationBankContext(simulationBankFixtureAt(t, bank.Slot, solana.Hash(bank.Blockhash), 6000), server.acctsDb, server.acctsDb)
	require.NoError(t, err)
	other.UnrootedRead = nil
	replay.UpdateChainTipFromSlotCtxWithBankMetadata(other, bank.Features, nil, replay.ChainTipIdentity{}, replay.ChainTipBankMetadata{BlockHeight: 90})
	_, err = server.SimulateTransaction(t.Context(), params)
	require.Error(t, err, "the same slot number does not establish the same bank generation")

	replay.UpdateChainTipFromSlotCtxWithBankMetadata(bank, bank.Features, nil, replay.ChainTipIdentity{}, replay.ChainTipBankMetadata{BlockHeight: math.MaxUint64})
	_, err = server.SimulateTransaction(t.Context(), params)
	require.Error(t, err, "an overflowing expiry height must be rejected")
}
