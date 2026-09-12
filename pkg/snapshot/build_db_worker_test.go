package snapshot

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestSnapshotWorkerParseFailurePropagatesAfterDrain(t *testing.T) {
	oldCopyingWorkers := SnapshotAppendVecCopyingWorkers
	oldBuilderWorkers := SnapshotIndexEntryBuilderWorkers
	oldCommitterWorkers := SnapshotIndexEntryCommitterWorkers
	SnapshotAppendVecCopyingWorkers = 1
	SnapshotIndexEntryBuilderWorkers = 1
	SnapshotIndexEntryCommitterWorkers = 1
	t.Cleanup(func() {
		SnapshotAppendVecCopyingWorkers = oldCopyingWorkers
		SnapshotIndexEntryBuilderWorkers = oldBuilderWorkers
		SnapshotIndexEntryCommitterWorkers = oldCommitterWorkers
	})

	accountsDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(accountsDir, "accounts"), 0o755))
	shardLogger := NewShardLogger(1, t.TempDir())
	t.Cleanup(func() {
		require.NoError(t, shardLogger.Close(context.Background()))
	})

	var key solana.PublicKey
	key[0] = 1
	var encoded bytes.Buffer
	require.NoError(t, (&accountsdb.AppendVecAccount{
		DataLen:  4,
		Pubkey:   key,
		Lamports: 1,
		Data:     []byte{1, 2, 3, 4},
	}).Marshal(&encoded))
	// Remove the four alignment bytes and the final account-data byte.
	malformed := encoded.Bytes()[:encoded.Len()-5]

	const slot, fileID = uint64(20), uint64(21)
	manifest := &SnapshotManifest{AccountsDb: &AccountsDbFields{}}
	manifest.AccountsDb.Storages = map[uint64]SlotAcctVecs{
		slot: {Slot: slot, AcctVecs: []AcctVec{{Id: fileID, FileSize: uint64(len(malformed))}}},
	}
	wg := &sync.WaitGroup{}
	pools, err := initWorkerPools(
		wg,
		shardLogger,
		manifest,
		nil,
		accountsDir,
		&atomic.Uint64{},
		&stakeIndexCollector{},
	)
	require.NoError(t, err)
	t.Cleanup(pools.Release)

	err = invokeSnapshotTask(wg, pools.appendVecCopying, appendVecCopyingTask{
		Filename:  "accounts/20.21",
		TarBuffer: bytes.NewBuffer(malformed),
	})
	require.NoError(t, err)
	require.ErrorContains(t, waitForSnapshotWorkers(wg, pools), "truncated appendvec account data")
}
