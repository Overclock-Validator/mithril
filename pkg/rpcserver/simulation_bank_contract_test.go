package rpcserver

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
	bank := server.slotCtx
	bank.Blockhash = [32]byte{2}
	sysvars, err := sealevel.NewBankSysvars(bank.Slot)
	require.NoError(t, err)
	require.NoError(t, bank.PublishBankSysvars(sysvars))
	_, wire := testLegacyTransaction(t)
	params := mustRawParams(t, []any{base58.Encode(wire), map[string]any{"replaceRecentBlockhash": true}})

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
		rpc := NewRpcServer(nil, 0, &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 512}, solana.Hash{1})
		require.NoError(t, rpc.listener.Close())
		rpc.SetSlotCtx(bank)
		endpoint := httptest.NewServer(rpc)
		t.Cleanup(endpoint.Close)
		client := endpoint.Client()
		client.Timeout = 5 * time.Second
		request := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"simulateTransaction","params":%s}`, params)
		response, err := client.Post(endpoint.URL, "application/json", strings.NewReader(request))
		require.NoError(t, err)
		defer response.Body.Close()
		var reply struct {
			Result *SimulateTransactionResp `json:"result"`
			Error  json.RawMessage          `json:"error"`
		}
		require.NoError(t, json.NewDecoder(response.Body).Decode(&reply))
		require.Empty(t, reply.Error)
		require.NotNil(t, reply.Result)
		require.Equal(t, bank.Slot, reply.Result.Context.Slot)
		require.Equal(t, &ReplacementBlockhashPayload{Blockhash: solana.Hash(bank.Blockhash).String(), LastValidBlockHeight: 240}, reply.Result.Value.ReplacementBlockhash)
	})

	replay.ResetChainTip()
	_, err = server.SimulateTransaction(t.Context(), params)
	require.Error(t, err, "missing bank metadata must not fall back to unrelated process globals")

	other := &sealevel.SlotCtx{Slot: bank.Slot, Blockhash: bank.Blockhash}
	otherSysvars, err := sealevel.NewBankSysvars(bank.Slot)
	require.NoError(t, err)
	require.NoError(t, other.PublishBankSysvars(otherSysvars))
	replay.UpdateChainTipFromSlotCtxWithBankMetadata(other, bank.Features, nil, replay.ChainTipIdentity{}, replay.ChainTipBankMetadata{BlockHeight: 90})
	_, err = server.SimulateTransaction(t.Context(), params)
	require.Error(t, err, "the same slot number does not establish the same bank generation")

	replay.UpdateChainTipFromSlotCtxWithBankMetadata(bank, bank.Features, nil, replay.ChainTipIdentity{}, replay.ChainTipBankMetadata{BlockHeight: math.MaxUint64})
	_, err = server.SimulateTransaction(t.Context(), params)
	require.Error(t, err, "an overflowing expiry height must be rejected")
}
