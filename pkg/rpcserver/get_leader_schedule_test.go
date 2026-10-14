package rpcserver

import (
	"errors"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/leaderschedule"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestGetLeaderScheduleReturnsRelativeEpochSlots(t *testing.T) {
	epochSchedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 4}
	firstSlot := epochSchedule.FirstSlotInEpoch(3)
	firstLeader := solana.PublicKey{1}
	secondLeader := solana.PublicKey{2}
	global.SetLeaderScheduleForEpoch(3, leaderschedule.NewLeaderScheduleFromKeyedSlots(
		map[solana.PublicKey][]uint64{
			firstLeader:  {0, 2},
			secondLeader: {1, 3},
		},
		firstSlot,
	))
	t.Cleanup(func() { global.SetLeaderScheduleForEpoch(3, nil) })

	server := &RpcServer{epochSchedule: epochSchedule}
	server.SetRootedBankState(firstSlot+2, firstSlot+2, 0)
	got, err := server.GetLeaderSchedule(t.Context(), mustRawParams(t, []interface{}{
		float64(firstSlot + 1),
		map[string]interface{}{"commitment": "confirmed"},
	}))
	require.NoError(t, err)
	require.Equal(t, []uint64{0, 2}, got[firstLeader.String()])
	require.Equal(t, []uint64{1, 3}, got[secondLeader.String()])

	filtered, err := server.GetLeaderSchedule(t.Context(), mustRawParams(t, []interface{}{
		float64(firstSlot),
		map[string]interface{}{"identity": secondLeader.String()},
	}))
	require.NoError(t, err)
	require.Equal(t, map[string][]uint64{secondLeader.String(): {1, 3}}, filtered)

	filtered, err = server.GetLeaderSchedule(t.Context(), mustRawParams(t, []interface{}{
		map[string]interface{}{"commitment": "finalized", "identity": secondLeader.String()},
	}))
	require.NoError(t, err)
	require.Equal(t, map[string][]uint64{secondLeader.String(): {1, 3}}, filtered)

	missing, err := server.GetLeaderSchedule(t.Context(), mustRawParams(t, []interface{}{
		float64(firstSlot), map[string]interface{}{"identity": solana.PublicKey{9}.String()},
	}))
	require.NoError(t, err)
	require.Empty(t, missing)
}

func TestGetLeaderScheduleReturnsNullWhenEpochScheduleIsUnavailable(t *testing.T) {
	server := &RpcServer{epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 4}}
	server.SetRootedBankState(40_002, 1, 0)

	got, err := server.GetLeaderSchedule(t.Context(), mustRawParams(t, []interface{}{float64(40_002)}))
	require.NoError(t, err)
	require.Nil(t, got)
}

func TestGetLeaderScheduleRejectsInvalidParams(t *testing.T) {
	server := &RpcServer{epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 4}}
	server.SetRootedBankState(1, 1, 0)
	for _, params := range [][]interface{}{
		{-1.0},
		{1.5},
		{float64(1), true},
		{float64(1), map[string]interface{}{"commitment": "unknown"}},
		{float64(1), map[string]interface{}{"identity": "invalid"}},
		{float64(1), map[string]interface{}{"keyByVoteAccount": true}},
		{float64(1), nil, nil},
	} {
		_, err := server.GetLeaderSchedule(t.Context(), mustRawParams(t, params))
		var invalid *InvalidParamsError
		require.True(t, errors.As(err, &invalid), "params: %#v; error: %v", params, err)
	}
}
