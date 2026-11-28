package rpcserver

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
	"github.com/stretchr/testify/require"
)

func TestGetEpochInfoUsesRootedBankForFinalized(t *testing.T) {
	oldSlot, oldEpoch := global.Slot(), global.Epoch()
	oldHeight, oldTransactions := global.BlockHeight(), global.ExactTransactionCount()
	t.Cleanup(func() {
		global.SetSlot(oldSlot)
		global.SetEpoch(oldEpoch)
		global.SetBlockHeight(oldHeight)
		global.SetTransactionCount(oldTransactions)
	})
	global.SetSlot(432250)
	global.SetEpoch(1)
	global.SetBlockHeight(432000)
	global.SetTransactionCount(new(uint64(2000)))

	server := &RpcServer{epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, LeaderScheduleSlotOffset: 432000}}
	server.SetRootedBankState(432123, 431900, 1900)
	got, err := server.GetEpochInfo(t.Context(), jsonrpc.RawParams(`[{"commitment":"finalized"}]`))
	require.NoError(t, err)
	require.Equal(t, uint64(432123), got.AbsoluteSlot)
	require.Equal(t, uint64(431900), got.BlockHeight)
	require.Equal(t, new(uint64(1900)), got.TransactionCount)
}

func TestEpochInfoOwnsRootedCount(t *testing.T) {
	server := &RpcServer{epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}}
	count := uint64(7)
	server.PublishRootedBankState(20, 19, &count)
	count++
	got, err := server.GetEpochInfo(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, new(uint64(7)), got.TransactionCount)
	*got.TransactionCount = 9
	again, err := server.GetEpochInfo(t.Context(), nil)
	require.NoError(t, err)
	require.Equal(t, new(uint64(7)), again.TransactionCount)
}

func TestEpochInfoUsesPublishedProcessedBank(t *testing.T) {
	oldHeight, oldCount := global.BlockHeight(), global.ExactTransactionCount()
	t.Cleanup(func() { global.SetBlockHeight(oldHeight); global.SetTransactionCount(oldCount) })
	server := &RpcServer{epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}}
	global.SetBlockHeight(7)
	global.SetTransactionCount(new(uint64(0)))
	server.SetSlotCtx(&sealevel.SlotCtx{Slot: 10})
	global.SetBlockHeight(8)
	global.SetTransactionCount(nil)
	config := jsonrpc.RawParams(`[{"commitment":"processed","minContextSlot":10}]`)
	got, err := server.GetEpochInfo(t.Context(), config)
	require.NoError(t, err)
	require.Equal(t, uint64(10), got.AbsoluteSlot)
	require.Equal(t, uint64(7), got.BlockHeight)
	require.Equal(t, new(uint64(0)), got.TransactionCount)
	_, err = server.GetEpochInfo(t.Context(), jsonrpc.RawParams(`[{"commitment":"processed","minContextSlot":11}]`))
	require.ErrorAs(t, err, new(*MinContextSlotNotReachedError))
	server.InvalidateHealth()
	_, err = server.GetEpochInfo(t.Context(), config)
	require.Error(t, err)
	server.SetSlotCtx(&sealevel.SlotCtx{Slot: 11})
	got, err = server.GetEpochInfo(t.Context(), config)
	require.NoError(t, err)
	require.Nil(t, got.TransactionCount)
}
