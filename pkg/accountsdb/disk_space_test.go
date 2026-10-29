package accountsdb

import (
	"os"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/stretchr/testify/require"
)

func TestDiskBudgetBoundaryAndOverflow(t *testing.T) {
	require.NoError(t, checkDiskBudget(100, 60, 40))
	require.Error(t, checkDiskBudget(99, 60, 40))
	require.Error(t, checkDiskBudget(39, 0, 40))
	require.Error(t, checkDiskBudget(100, ^uint64(0), 40))
}

func TestLowDiskRejectsFoldBeforeAnyStoreMutation(t *testing.T) {
	db, dir := newFoldTestDb(t)
	defer func() { db.CloseDb() }()
	commitTestBatch(t, db, 110, foldAcct(1, 10, []byte("durable")))
	before, err := os.ReadDir(db.AcctsDir)
	require.NoError(t, err)
	index := snapshotIndex(t, db)
	id := db.LargestFileId.Load()
	// Larger than any possible available filesystem capacity.
	db.MinFreeBytes = ^uint64(0)
	_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: 120, Delta: []*accounts.Account{foldAcct(1, 11, []byte("rejected"))}}}, 120, nil, nil)
	require.ErrorContains(t, err, "insufficient disk space")
	require.Equal(t, id, db.LargestFileId.Load())
	require.Equal(t, index, snapshotIndex(t, db))
	after, err := os.ReadDir(db.AcctsDir)
	require.NoError(t, err)
	require.Len(t, after, len(before))
	db = reopenFoldTestDb(t, db, dir)
	recovered, err := db.RecoverFoldState()
	require.NoError(t, err)
	require.Equal(t, uint64(110), recovered.DurableThrough)
	require.Empty(t, recovered.OrphansRemoved)
	db.MinFreeBytes = 1
	commitTestBatch(t, db, 120, foldAcct(1, 11, []byte("resumed")))
}
