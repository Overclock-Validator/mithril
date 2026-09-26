package rpcserver

import (
	"encoding/json"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/blockhistory"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func TestBlockHistoryRPCServesExporterContract(t *testing.T) {
	store, err := blockhistory.Open(t.TempDir(), 100)
	require.NoError(t, err)
	leader := solana.PublicKey{1}
	require.NoError(t, store.RecordSkipped(40))
	require.NoError(t, store.RecordBlock(&b.Block{
		Slot:          41,
		ParentSlot:    39,
		BlockHeight:   30,
		Blockhash:     solana.Hash{2},
		LastBlockhash: solana.Hash{3},
		Rewards: []rpc.BlockReward{{
			Pubkey: leader, Lamports: 25, PostBalance: 1_025, RewardType: rpc.RewardTypeFee,
		}},
	}))
	require.NoError(t, store.Prepare(41))
	require.NoError(t, store.SetRooted(41))
	server := &RpcServer{blockHistory: store}

	minimum, err := server.MinimumLedgerSlot(t.Context(), jsonrpc.RawParams(`[]`))
	require.NoError(t, err)
	require.Equal(t, uint64(40), minimum)
	first, err := server.GetFirstAvailableBlock(t.Context(), jsonrpc.RawParams(`[]`))
	require.NoError(t, err)
	require.Equal(t, uint64(41), first)
	for _, params := range []jsonrpc.RawParams{nil, jsonrpc.RawParams(`null`)} {
		minimum, err = server.MinimumLedgerSlot(t.Context(), params)
		require.NoError(t, err)
		require.Equal(t, uint64(40), minimum)
		first, err = server.GetFirstAvailableBlock(t.Context(), params)
		require.NoError(t, err)
		require.Equal(t, uint64(41), first)
	}

	response, err := server.GetBlock(t.Context(), jsonrpc.RawParams(`[41,{"commitment":"confirmed","encoding":"json","transactionDetails":"none","rewards":true,"maxSupportedTransactionVersion":0}]`))
	require.NoError(t, err)
	require.Equal(t, uint64(30), response.BlockHeight)
	require.Equal(t, int64(25), response.Rewards[0].Lamports)
	require.Equal(t, leader, response.Rewards[0].Pubkey)
	encoded, err := json.Marshal(response)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"commission":null`)

	_, err = server.GetBlock(t.Context(), jsonrpc.RawParams(`[40,{"transactionDetails":"none"}]`))
	var skipped *SlotSkippedError
	require.ErrorAs(t, err, &skipped)
	require.Equal(t, uint64(40), skipped.Slot)

	_, err = server.GetBlock(t.Context(), jsonrpc.RawParams(`[42,{"transactionDetails":"none"}]`))
	var unavailable *BlockNotAvailableError
	require.ErrorAs(t, err, &unavailable)
	require.Equal(t, uint64(42), unavailable.Slot)
}

func TestGetBlockRejectsUnretainedTransactionDetails(t *testing.T) {
	store, err := blockhistory.Open(t.TempDir(), 100)
	require.NoError(t, err)
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 1}))
	require.NoError(t, store.Prepare(1))
	require.NoError(t, store.SetRooted(1))
	server := &RpcServer{blockHistory: store}
	_, err = server.GetBlock(t.Context(), jsonrpc.RawParams(`[1,{"transactionDetails":"full"}]`))
	var invalid *InvalidParamsError
	require.ErrorAs(t, err, &invalid)
	require.Contains(t, invalid.Error(), "transactions are not retained")
	_, err = server.GetBlock(t.Context(), jsonrpc.RawParams(`[2,{"transactionDetails":"full"}]`))
	require.ErrorAs(t, err, &invalid)
}
