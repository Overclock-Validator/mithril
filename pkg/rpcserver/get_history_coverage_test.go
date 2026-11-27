package rpcserver

import (
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/transactionhistory"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/stretchr/testify/require"
)

func TestGetHistoryCoverage(t *testing.T) {
	bare := &RpcServer{}
	got, err := bare.GetHistoryCoverage(t.Context(), jsonrpc.RawParams(nil))
	require.NoError(t, err)
	require.Equal(t, HistoryCoverageResp{}, got, "a node serving no history says so")

	server, block := transactionHistoryReadFixture(t)
	got, err = server.GetHistoryCoverage(t.Context(), jsonrpc.RawParams(nil))
	require.NoError(t, err)
	require.Equal(t, uint64(20), got.Blocks.RootedSlot)
	require.Zero(t, got.Transactions.Blocks, "nothing is recorded yet, so nothing is judged missing")
	require.Zero(t, got.Transactions.MissingBlocks)
	require.True(t, got.Transactions.Recording)

	// Block 20 is recorded. Block 30 is rooted but its transactions never were.
	require.NoError(t, server.transactionHistory.WriteBlock(block))
	later := block
	later.ID = transactionhistory.BlockID{Slot: 30, Blockhash: [32]byte{30}}
	later.ParentSlot, later.PreviousBlockhash = 20, block.ID.Blockhash
	publishTransactionHistorySummary(t, server, later)
	got, err = server.GetHistoryCoverage(t.Context(), jsonrpc.RawParams(nil))
	require.NoError(t, err)
	require.Equal(t, uint64(30), got.Blocks.RootedSlot)
	require.Equal(t, uint64(1), got.Transactions.Blocks)
	require.Equal(t, uint64(20), got.Transactions.FirstSlot)
	require.Equal(t, uint64(20), got.Transactions.CheckedFromSlot)
	require.Equal(t, uint64(30), got.Transactions.CheckedThroughSlot)
	require.Equal(t, uint64(1), got.Transactions.MissingBlocks)
	require.Equal(t, []uint64{30}, got.Transactions.MissingSlots, "skipped slots between are not holes")

	// A block recorded under another blockhash for a rooted slot is still a hole.
	fork := later
	fork.ID.Blockhash = [32]byte{31}
	require.NoError(t, server.transactionHistory.WriteBlock(fork))
	got, err = server.GetHistoryCoverage(t.Context(), jsonrpc.RawParams(nil))
	require.NoError(t, err)
	require.Equal(t, []uint64{30}, got.Transactions.MissingSlots)
	require.NoError(t, server.transactionHistory.WriteBlock(later))
	got, err = server.GetHistoryCoverage(t.Context(), jsonrpc.RawParams(nil))
	require.NoError(t, err)
	require.Empty(t, got.Transactions.MissingSlots)
	require.Zero(t, got.Transactions.MissingBlocks)

	server.SetHistoryRecorderStatus(func() (bool, string) { return false, "held history write failed" })
	got, err = server.GetHistoryCoverage(t.Context(), jsonrpc.RawParams(nil))
	require.NoError(t, err)
	require.False(t, got.Transactions.Recording)
	require.Equal(t, "held history write failed", got.Transactions.StoppedReason)

	_, err = server.GetHistoryCoverage(t.Context(), jsonrpc.RawParams(`[1]`))
	require.Error(t, err)

	// With a watch set, recorded blocks hold a subset: coverage says so, and
	// getBlock will not present one as a whole block.
	_, err = server.GetBlock(t.Context(), jsonrpc.RawParams(`[20]`))
	require.NotContains(t, fmt.Sprint(err), "watched address")
	server.SetHistoryWatch(2)
	got, err = server.GetHistoryCoverage(t.Context(), jsonrpc.RawParams(nil))
	require.NoError(t, err)
	require.Equal(t, int64(2), got.Transactions.WatchedAddresses)
	_, err = server.GetBlock(t.Context(), jsonrpc.RawParams(`[20]`))
	require.ErrorContains(t, err, "watched address")
	_, err = server.GetBlock(t.Context(), jsonrpc.RawParams(`[20, {"transactionDetails": "none"}]`))
	require.NotContains(t, fmt.Sprint(err), "watched address", "block summaries are still served")
}
