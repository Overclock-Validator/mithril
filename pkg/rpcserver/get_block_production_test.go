package rpcserver

import (
	"context"
	"errors"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/leaderschedule"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type scanCheckContext struct {
	context.Context
	check func()
}

func (c scanCheckContext) Err() error {
	c.check()
	return c.Context.Err()
}

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
	require.Empty(t, got.Context.APIVersion)
	require.Equal(t, BlockProductionRange{FirstSlot: 10, LastSlot: 13}, got.Value.Range)
	require.Equal(t, [2]uint64{2, 1}, got.Value.ByIdentity[firstLeader.String()])
	require.Equal(t, [2]uint64{2, 2}, got.Value.ByIdentity[secondLeader.String()])

	// Cancel after the work needed for a one-slot query, so the test exercises
	// the longer scan rather than request admission or the initial account read.
	checks := 0
	run := func(ctx context.Context, last uint64) error {
		_, err := server.GetBlockProduction(ctx, mustRawParams(t, []any{
			map[string]any{"range": map[string]any{"firstSlot": 10, "lastSlot": last}},
		}))
		return err
	}
	require.NoError(t, run(scanCheckContext{t.Context(), func() { checks++ }}, 10))
	oneSlotChecks := checks
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	err = run(scanCheckContext{ctx, func() {
		if checks--; checks < 0 {
			cancel()
		}
	}}, rootedSlot)
	require.ErrorIs(t, err, context.Canceled)

	// Replace the schedules after the first slot lookup of the longer scan.
	checks = 0
	swapped, err := server.GetBlockProduction(scanCheckContext{t.Context(), func() {
		checks++
		if checks == oneSlotChecks+1 {
			global.SetLeaderScheduleForEpoch(0, nil)
		}
	}}, mustRawParams(t, []any{map[string]any{"range": map[string]any{"firstSlot": 10}}}))
	require.NoError(t, err)
	require.Equal(t, got.Value, swapped.Value)
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
