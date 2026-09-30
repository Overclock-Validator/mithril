package rpcserver

import (
	"reflect"
	"runtime"
	"testing"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func TestHistoryMutationsWaitForReaders(t *testing.T) {
	var disabled *RpcServer
	disabled.SetHistoryRecoveryPending(true)
	disabled.SetHistoryRecoveryPending(false)

	_, registered := reflect.TypeOf(rpcHandler{}).MethodByName("SetHistoryRecoveryPending")
	require.False(t, registered, "the internal recovery gate must not be an RPC method")

	tests := []struct {
		name   string
		mutate func(*RpcServer) error
	}{
		{name: "recovery", mutate: func(server *RpcServer) error {
			server.SetHistoryRecoveryPending(true)
			return nil
		}},
		{name: "prepare", mutate: func(server *RpcServer) error { return server.PrepareEpochRewards(3) }},
		{name: "publish", mutate: func(server *RpcServer) error { return server.SetRootedEpochRewardsSlot(2) }},
		{name: "rewind", mutate: func(server *RpcServer) error { return server.RewindEpochRewards(2) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &RpcServer{}
			require.NoError(t, server.EnableEpochRewards(t.TempDir(), 100, 0))
			require.NoError(t, server.RecordEpochRewards(&b.Block{Slot: 2, Epoch: 1, Rewards: []rpc.BlockReward{{
				Pubkey: solana.PublicKey{1}, Lamports: 1, PostBalance: 2, RewardType: rpc.RewardTypeVoting,
			}}}))
			require.NoError(t, server.PrepareEpochRewards(2))
			server.historyRecoveryMu.RLock()
			readerHeld := true
			defer func() {
				if readerHeld {
					server.historyRecoveryMu.RUnlock()
				}
			}()
			done := make(chan error, 1)
			go func() { done <- test.mutate(server) }()

			// Observe the waiting writer without releasing the in-flight reader.
			timeout := time.After(5 * time.Second)
			for server.historyRecoveryMu.TryRLock() {
				server.historyRecoveryMu.RUnlock()
				select {
				case err := <-done:
					require.NoError(t, err)
					t.Fatal("history mutation finished before the in-flight reader")
				case <-timeout:
					t.Fatal("history mutation did not wait for the in-flight reader")
				default:
					runtime.Gosched()
				}
			}
			require.Zero(t, server.epochRewards.RootedSlot())
			select {
			case <-done:
				t.Fatal("history mutation finished before the in-flight reader")
			default:
			}
			server.historyRecoveryMu.RUnlock()
			readerHeld = false
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-timeout:
				t.Fatal("history mutation remained blocked after the reader finished")
			}
			require.Equal(t, test.name == "recovery", server.historyRecoveryPending)
			server.SetHistoryRecoveryPending(false)
			require.False(t, server.historyRecoveryPending)
		})
	}
}

func TestEpochRewardHistoryRecoveryGate(t *testing.T) {
	server := &RpcServer{epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 2}}
	require.NoError(t, server.EnableEpochRewards(t.TempDir(), 100, 0))
	address := solana.PublicKey{1}
	require.NoError(t, server.RecordEpochRewards(&b.Block{
		Slot: 2, Epoch: 1,
		Rewards: []rpc.BlockReward{{
			Pubkey: address, Lamports: 1, PostBalance: 2, RewardType: rpc.RewardTypeVoting,
		}},
	}))
	require.NoError(t, server.PrepareEpochRewards(2))
	require.NoError(t, server.SetRootedEpochRewardsSlot(2))
	params := rawParams(t, []any{[]string{address.String()}, map[string]any{"epoch": 0}})

	for _, pending := range []bool{false, true, false} {
		server.SetHistoryRecoveryPending(pending)
		rewards, err := server.GetInflationReward(t.Context(), params)
		if pending {
			require.EqualError(t, err, "RPC history is unavailable during account recovery")
			require.Nil(t, rewards)
			continue
		}
		require.NoError(t, err)
		require.Equal(t, []*InflationRewardResp{{Epoch: 0, EffectiveSlot: 2, Amount: 1, PostBalance: 2}}, rewards)
	}
}
