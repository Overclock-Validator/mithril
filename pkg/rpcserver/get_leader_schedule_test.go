package rpcserver

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/leaderschedule"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/filecoin-project/go-jsonrpc"
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

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	checks := 0
	_, err = server.GetLeaderSchedule(scanCheckContext{ctx, func() {
		checks++
		if checks == 3 {
			cancel()
		}
	}}, nil)
	require.ErrorIs(t, err, context.Canceled)

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

	id := solana.PublicKey{9}.String()
	service := jsonrpc.NewServer(jsonrpc.WithServerErrors(rpcErrorRegistry()))
	service.Register("MithrilRpc", rpcHandler{server})
	body := fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":"MithrilRpc.GetLeaderSchedule","params":[%d,{"identity":%q}]}`, firstSlot, id)
	w := httptest.NewRecorder()
	service.ServeHTTP(w, httptest.NewRequest("POST", "/", strings.NewReader(body)))
	require.JSONEq(t, fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"error":{"code":-32022,"message":"Node %s was not in the leader schedule for specified epoch"}}`, id), w.Body.String())

	checks = 0
	swapped, err := server.GetLeaderSchedule(scanCheckContext{t.Context(), func() {
		checks++
		if checks == 3 {
			global.SetLeaderScheduleForEpoch(3, nil)
		}
	}}, nil)
	require.NoError(t, err)
	require.Equal(t, got, swapped, "a replacement during a scan cannot change its remaining slots")
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
	missing := solana.PublicKey{9}.String()
	params := mustRawParams(t, []interface{}{nil, map[string]interface{}{"keyByVoteAccount": true, "identity": missing}})
	got, err := server.GetLeaderSchedule(t.Context(), params)
	require.EqualError(t, err, "Node "+missing+" was not in the leader schedule for specified epoch")
	require.Nil(t, got)
	global.SetLeaderScheduleForEpoch(epoch, leaderschedule.NewLeaderScheduleFromKeyedSlots(map[solana.PublicKey][]uint64{node: {0}}, firstSlot))
	got, err = server.GetLeaderSchedule(t.Context(), params)
	require.ErrorContains(t, err, "vote-account leader schedule unavailable")
	require.Nil(t, got, "node-keyed compatibility schedules must not invent vote accounts")
	global.SetLeaderScheduleForEpoch(epoch, nil)
	got, err = server.GetLeaderSchedule(t.Context(), params)
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
