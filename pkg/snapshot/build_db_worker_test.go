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
	"github.com/Overclock-Validator/mithril/pkg/addresses"
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
		&snapshotAccountCollector{},
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

func TestSnapshotWorkersSupportArchiveSizedAppendVecs(t *testing.T) {
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

	const slot, fileID = uint64(20), uint64(21)
	var encoded bytes.Buffer
	var keys [2]solana.PublicKey
	for i := range keys {
		keys[i][0] = byte(i + 1)
		require.NoError(t, (&accountsdb.AppendVecAccount{
			Pubkey: keys[i], Lamports: 1,
			Owner:   solana.PublicKeyFromBytes(addresses.StakeProgramAddr[:]),
			DataLen: 4, Data: []byte{1, 2, 3, 4},
		}).Marshal(&encoded))
	}
	data := encoded.Bytes()
	oneAccountSize := uint64(len(data) / 2)
	makeManifest := func(storages map[uint64]SlotAcctVecs) *SnapshotManifest {
		return &SnapshotManifest{AccountsDb: &AccountsDbFields{Storages: storages}}
	}
	empty := makeManifest(map[uint64]SlotAcctVecs{})
	legacy := makeManifest(map[uint64]SlotAcctVecs{
		slot: {Slot: slot, AcctVecs: []AcctVec{{Id: fileID, FileSize: oneAccountSize}}},
	})
	missing := makeManifest(map[uint64]SlotAcctVecs{
		slot: {Slot: slot, AcctVecs: []AcctVec{{Id: fileID + 1, FileSize: oneAccountSize}}},
	})

	tests := []struct {
		name        string
		full        *SnapshotManifest
		incremental *SnapshotManifest
		fromIncr    bool
		data        []byte
		wantEntries int
		wantErr     string
	}{
		{name: "full empty storage map", full: empty, data: data, wantEntries: 2},
		{name: "incremental empty storage map overrides full size", full: legacy, incremental: empty, fromIncr: true, data: data, wantEntries: 2},
		{name: "legacy full retains manifest size", full: legacy, data: data, wantEntries: 1},
		{name: "legacy incremental retains manifest size", full: empty, incremental: legacy, fromIncr: true, data: data, wantEntries: 1},
		{name: "legacy incremental retains full fallback", full: legacy, incremental: missing, fromIncr: true, data: data, wantEntries: 1},
		{name: "missing full entry still fails", full: missing, data: data, wantErr: "manifest has no file size"},
		{name: "missing incremental entry still fails", full: empty, incremental: missing, fromIncr: true, data: data, wantErr: "manifest has no file size"},
		{name: "incremental manifest required", full: empty, fromIncr: true, data: data, wantErr: "without having parsed incremental snapshot manifest"},
		{name: "empty archive member", full: empty},
		{name: "truncated archive account still fails", full: empty, data: data[:len(data)-5], wantErr: "truncated appendvec account data"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			accountsDir := t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(accountsDir, "accounts"), 0o755))
			shardLogger := NewShardLogger(1, t.TempDir())
			closeShardLogger := sync.OnceFunc(func() { require.NoError(t, shardLogger.Close(context.Background())) })
			t.Cleanup(closeShardLogger)
			wg := &sync.WaitGroup{}
			largestFileID := &atomic.Uint64{}
			stakes := &stakeIndexCollector{}
			pools, err := initWorkerPools(wg, shardLogger, tt.full, tt.incremental, accountsDir, largestFileID, stakes)
			require.NoError(t, err)
			t.Cleanup(pools.Release)
			require.NoError(t, invokeSnapshotTask(wg, pools.appendVecCopying, appendVecCopyingTask{
				Filename: "accounts/20.21", TarBuffer: bytes.NewBuffer(tt.data), FromIncrementalSnapshot: tt.fromIncr,
			}))
			err = waitForSnapshotWorkers(wg, pools)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			closeShardLogger()
			require.Equal(t, int64(tt.wantEntries*(32+vlen)), shardLogger.TotalBytes())
			require.Equal(t, fileID, largestFileID.Load())
			require.Len(t, stakes.entries, tt.wantEntries)
			for i, entry := range stakes.entries {
				require.Equal(t, keys[i], entry.Pubkey)
				require.Equal(t, fileID, entry.FileId)
				require.Equal(t, uint64(i)*oneAccountSize, entry.Offset)
			}
		})
	}
}
