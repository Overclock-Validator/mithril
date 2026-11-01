package replay

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/blockhistory"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestBlockHistorySurvivesPartialForcedFlush(t *testing.T) {
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
				store, err := blockhistory.Open(dir, 100)
				require.NoError(t, err)
				require.NoError(t, store.SetRooted(4))
				for _, slot := range []uint64{5, 6, 7, 8} {
					require.NoError(t, store.RecordBlock(&b.Block{Slot: slot, ParentSlot: slot - 1, Blockhash: solana.Hash{byte(slot)}, LastBlockhash: solana.Hash{byte(slot - 1)}}))
				}
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
					store, err = blockhistory.Open(dir, 100)
					require.NoError(t, err)
				}
				require.NoError(t, store.SetRooted(through))
				got, err := store.Get(5)
				require.NoError(t, err, "first fold committed slot 5 before second fold failed")
				require.Equal(t, solana.Hash{5}.String(), got.Blockhash)
			})
		}
	}
}
