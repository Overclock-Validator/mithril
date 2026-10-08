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
	oldHeight, oldTransactions := global.BlockHeight(), global.TransactionCount()
	t.Cleanup(func() {
		global.SetSlot(oldSlot)
		global.SetEpoch(oldEpoch)
		global.SetBlockHeight(oldHeight)
		global.SetTransactionCount(oldTransactions)
	})
	global.SetSlot(432250)
	global.SetEpoch(1)
	global.SetBlockHeight(432000)
	global.SetTransactionCount(2000)

	server := &RpcServer{epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, LeaderScheduleSlotOffset: 432000}}
	server.SetRootedBankState(432123, 431900, 1900)
	got, err := server.GetEpochInfo(t.Context(), jsonrpc.RawParams(`[{"commitment":"finalized"}]`))
	require.NoError(t, err)
	require.Equal(t, uint64(432123), got.AbsoluteSlot)
	require.Equal(t, uint64(431900), got.BlockHeight)
	require.Equal(t, uint64(1900), got.TransactionCount)
}
