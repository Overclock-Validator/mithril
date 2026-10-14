package blockhistory

import (
	"os"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestStoreRestoresCommittedRecordAfterOrphanReadRetry(t *testing.T) {
	store, err := Open(t.TempDir(), 100)
	require.NoError(t, err)
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 5, Blockhash: solana.Hash{5}}))
	require.NoError(t, store.Prepare(6))
	require.NoError(t, store.RecordBlock(&b.Block{Slot: 5, Blockhash: solana.Hash{55}}))
	require.NoError(t, store.Prepare(8))

	committed := store.batchPath(6)
	require.NoError(t, os.Rename(committed, committed+".saved"))
	require.Error(t, store.SetRooted(6))
	require.FileExists(t, store.batchPath(8), "a failed recovery must leave the orphan available for retry")
	_, err = store.Get(5)
	require.ErrorIs(t, err, ErrNotAvailable)
	require.NoError(t, os.Rename(committed+".saved", committed))
	require.NoError(t, store.SetRooted(6))
	record, err := store.Get(5)
	require.NoError(t, err)
	require.Equal(t, solana.Hash{5}.String(), record.Blockhash)
}
