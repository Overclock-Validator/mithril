package rpcserver

import (
	"errors"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/leaderschedule"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestGetBlockProductionUsesRootedSlotHistory(t *testing.T) {
	const rootedSlot = uint64(13)
	firstLeader := solana.PublicKey{1}
	secondLeader := solana.PublicKey{2}
	global.SetLeaderScheduleForEpoch(0, leaderschedule.NewLeaderScheduleFromKeyedSlots(
		map[solana.PublicKey][]uint64{
			firstLeader:  {10, 11},
			secondLeader: {12, 13},
		},
		0,
	))
	t.Cleanup(func() { global.SetLeaderScheduleForEpoch(0, nil) })

	history := sealevel.SysvarSlotHistory{
		Bits: sealevel.SlotHistoryBitvec{
			Bits: sealevel.SlotHistoryInner{BlocksLen: 1, Blocks: []uint64{(1 << 10) | (1 << 12) | (1 << 13)}},
			Len:  64,
		},
		NextSlot: rootedSlot + 1,
	}
	db := newRPCAccountsDB(t)
	_, err := db.CommitBatch([]accounts.SlotDelta{{
		Slot: rootedSlot,
		Delta: []*accounts.Account{{
			Key:      sealevel.SysvarSlotHistoryAddr,
			Lamports: 1,
			Data:     history.MustMarshal(),
		}},
	}}, rootedSlot, nil, nil)
	require.NoError(t, err)

	server := &RpcServer{
		acctsDb:       db,
		epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32},
	}
	server.SetRootedBankState(rootedSlot, 12, 0)
	got, err := server.GetBlockProduction(t.Context(), mustRawParams(t, []interface{}{
		map[string]interface{}{
			"commitment": "finalized",
			"range": map[string]interface{}{
				"firstSlot": float64(10),
				"lastSlot":  float64(rootedSlot),
			},
		},
	}))
	require.NoError(t, err)
	require.Equal(t, rootedSlot, got.Context.Slot)
	require.Equal(t, BlockProductionRange{FirstSlot: 10, LastSlot: 13}, got.Value.Range)
	require.Equal(t, [2]uint64{2, 1}, got.Value.ByIdentity[firstLeader.String()])
	require.Equal(t, [2]uint64{2, 2}, got.Value.ByIdentity[secondLeader.String()])
}

func TestGetBlockProductionRejectsInvalidRange(t *testing.T) {
	server := &RpcServer{
		acctsDb:       newRPCAccountsDB(t),
		epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32},
	}
	server.SetRootedBankState(13, 12, 0)
	for _, params := range [][]interface{}{
		{true},
		{map[string]interface{}{"commitment": "unknown"}},
		{map[string]interface{}{"identity": "invalid"}},
		{map[string]interface{}{"range": true}},
		{map[string]interface{}{"range": map[string]interface{}{}}},
		{map[string]interface{}{"range": map[string]interface{}{"firstSlot": 1.5}}},
		{map[string]interface{}{}, map[string]interface{}{}},
	} {
		_, err := server.GetBlockProduction(t.Context(), mustRawParams(t, params))
		var invalid *InvalidParamsError
		require.True(t, errors.As(err, &invalid), "params: %#v; error: %v", params, err)
	}
}
