package rpcserver

import (
	"errors"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
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

func TestGetLeaderScheduleKeysBySelectedVoteAccount(t *testing.T) {
	epochSchedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 32}
	const epoch = 13
	firstSlot := epochSchedule.FirstSlotInEpoch(epoch)
	node := solana.PublicKey{31}
	other := solana.PublicKey{32}
	votes := map[solana.PublicKey]*epochstakes.VoteAccount{
		{41}: {NodePubkey: node},
		{42}: {NodePubkey: node},
		{43}: {NodePubkey: other},
	}
	schedule := leaderschedule.New(votes, map[solana.PublicKey]uint64{{41}: 100, {42}: 100, {43}: 100}, epochSchedule, epoch, 32, 1)
	global.SetLeaderScheduleForEpoch(epoch, schedule)
	t.Cleanup(func() { global.SetLeaderScheduleForEpoch(epoch, nil) })
	server := &RpcServer{epochSchedule: epochSchedule}
	server.SetRootedBankState(firstSlot, firstSlot, 0)
	for _, identity := range []string{"", node.String(), other.String()} {
		config := map[string]interface{}{"keyByVoteAccount": true}
		if identity != "" {
			config["identity"] = identity
		}
		want := make(map[string][]uint64)
		for index := uint64(0); index < 32; index++ {
			leader, vote, ok := schedule.LeaderForSlotWithVoteAccount(firstSlot + index)
			require.True(t, ok)
			if identity == "" || leader.String() == identity {
				want[vote.String()] = append(want[vote.String()], index)
			}
		}
		got, err := server.GetLeaderSchedule(t.Context(), mustRawParams(t, []interface{}{nil, config}))
		require.NoError(t, err)
		require.Equal(t, want, got)
		if identity == node.String() {
			require.Len(t, got, 2, "two vote accounts sharing an identity must stay distinct")
		}
	}
	global.SetLeaderScheduleForEpoch(epoch, leaderschedule.NewLeaderScheduleFromKeyedSlots(map[solana.PublicKey][]uint64{node: {0}}, firstSlot))
	got, err := server.GetLeaderSchedule(t.Context(), mustRawParams(t, []interface{}{nil, map[string]interface{}{"keyByVoteAccount": true}}))
	require.ErrorContains(t, err, "vote-account leader schedule unavailable")
	require.Nil(t, got, "node-keyed compatibility schedules must not invent vote accounts")
	global.SetLeaderScheduleForEpoch(epoch, nil)
	got, err = server.GetLeaderSchedule(t.Context(), mustRawParams(t, []interface{}{nil, map[string]interface{}{"keyByVoteAccount": true}}))
	require.NoError(t, err)
	require.Nil(t, got, "missing epochs keep the standard null result")
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
		{float64(1), map[string]interface{}{"keyByVoteAccount": "true"}},
		{float64(1), nil, nil},
	} {
		_, err := server.GetLeaderSchedule(t.Context(), mustRawParams(t, params))
		var invalid *InvalidParamsError
		require.True(t, errors.As(err, &invalid), "params: %#v; error: %v", params, err)
	}
}
