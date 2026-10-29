package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/epochrewards"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/stretchr/testify/require"
)

func TestEpochRewardsSurvivePartialForcedFlush(t *testing.T) {
	for _, publishEachCommit := range []bool{false, true} {
		for _, restart := range []bool{false, true} {
			name := "running"
			if publishEachCommit {
				name += "_publish_each_commit"
			}
			if restart {
				name += "_restart"
			}
			t.Run(name, func(t *testing.T) {
				dir := t.TempDir()
				store, err := epochrewards.Open(dir, 100)
				require.NoError(t, err)
				require.NoError(t, store.SetRooted(4))
				key := solana.PublicKey{5}
				require.NoError(t, store.RecordBlock(&b.Block{Slot: 5, Epoch: 1, Rewards: []rpc.BlockReward{{Pubkey: key, Lamports: 7, PostBalance: 107, RewardType: rpc.RewardTypeStaking}}}))
				committer := &fakeCommitter{durable: accounts.NewMemAccounts(), failOn: 7}
				tail := asyncTestTail(committer, 5, 6, 7, 8)
				statuses := NewTransactionStatusCache()
				checkpointDir := t.TempDir()
				var prepared []uint64
				require.NoError(t, tail.SetTransactionStatusCheckpointHooks(TransactionStatusCheckpointHooks{
					Capture: statuses.CaptureSnapshotThrough,
					AfterCommit: func(ref *state.TransactionStatusCheckpointRef) error {
						if publishEachCommit {
							return store.SetRooted(ref.Root)
						}
						return nil
					},
					Install: func(through uint64, payload []byte) (*state.TransactionStatusCheckpointRef, error) {
						ref, err := PrepareTransactionStatusCheckpoint(checkpointDir, through, payload)
						if err != nil {
							return nil, err
						}
						prepared = append(prepared, through)
						return ref, store.Prepare(through)
					},
				}))
				through, rooted, err := tail.flush(8)
				require.ErrorContains(t, err, "commit boom at slot 7")
				require.Equal(t, []uint64{6, 8}, prepared)
				require.Equal(t, []uint64{6}, committer.throughs)
				require.Equal(t, uint64(6), through)
				require.Equal(t, uint64(6), rooted.Slot)
				if restart {
					store, err = epochrewards.Open(dir, 100)
					require.NoError(t, err)
				}
				require.NoError(t, store.SetRooted(through))
				got, ok := store.Get(0, key.String(), false)
				require.True(t, ok, "first fold committed reward before second fold failed")
				require.Equal(t, uint64(7), got.Amount)
			})
		}
	}
}
