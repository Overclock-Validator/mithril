package snapshot

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/progress"
	"github.com/gagliardetto/solana-go"
	"github.com/klauspost/compress/zstd"
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
		testAppendVecDirs(t, accountsDir),
		&stakeIndexCollector{},
	)
	require.NoError(t, err)
	t.Cleanup(pools.Release)

	err = invokeSnapshotTask(wg, pools.appendVecCopying, appendVecCopyingTask{
		Filename: "accounts/20.21",
		Buf:      malformed,
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
			stakes := &stakeIndexCollector{}
			dirs := testAppendVecDirs(t, accountsDir)
			pools, err := initWorkerPools(wg, shardLogger, tt.full, tt.incremental, dirs, stakes)
			require.NoError(t, err)
			t.Cleanup(pools.Release)
			require.NoError(t, invokeSnapshotTask(wg, pools.appendVecCopying, appendVecCopyingTask{
				Filename: "accounts/20.21", Buf: tt.data, FromIncrementalSnapshot: tt.fromIncr,
			}))
			err = waitForSnapshotWorkers(wg, pools)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			closeShardLogger()
			logged := 0
			for _, sh := range shardLogger.shards {
				logged += sh.logSize
			}
			require.Equal(t, tt.wantEntries*(32+vlen), logged)
			require.Equal(t, fileID, dirs.largest.Load())
			require.Len(t, stakes.entries, tt.wantEntries)
			for i, entry := range stakes.entries {
				require.Equal(t, keys[i], entry.Pubkey)
				require.Equal(t, fileID, entry.FileId)
				require.Equal(t, uint64(i)*oneAccountSize, entry.Offset)
			}
		})
	}
}

func testAppendVecDirs(t *testing.T, accountsDbDir string) *appendVecDirs {
	t.Helper()
	dirs, err := makeAppendVecDirs([]string{accountsDbDir})
	require.NoError(t, err)
	t.Cleanup(dirs.close)
	return dirs
}

func TestReadTarProgressWithWorkers(t *testing.T) {
	oldCopy, oldBuild, oldCommit := SnapshotAppendVecCopyingWorkers, SnapshotIndexEntryBuilderWorkers, SnapshotIndexEntryCommitterWorkers
	oldDecoderConcurrency := ZstdDecoderConcurrency
	SnapshotAppendVecCopyingWorkers, SnapshotIndexEntryBuilderWorkers, SnapshotIndexEntryCommitterWorkers = 1, 1, 1
	ZstdDecoderConcurrency = 2 // Exercise decoder prefetch even on a one-CPU runner.
	t.Cleanup(func() {
		SnapshotAppendVecCopyingWorkers, SnapshotIndexEntryBuilderWorkers, SnapshotIndexEntryCommitterWorkers = oldCopy, oldBuild, oldCommit
		ZstdDecoderConcurrency = oldDecoderConcurrency
	})
	for _, tc := range []struct {
		name        string
		malformed   bool
		observe     bool
		incremental bool
	}{
		{name: "complete", observe: true}, {name: "worker_failure", malformed: true, observe: true},
		{name: "complete_without_observer"}, {name: "worker_failure_without_observer", malformed: true},
		{name: "incremental_complete", observe: true, incremental: true},
		{name: "incremental_worker_failure", malformed: true, observe: true, incremental: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var key solana.PublicKey
			key[0] = 7
			var encoded bytes.Buffer
			require.NoError(t, (&accountsdb.AppendVecAccount{Pubkey: key, Lamports: 1,
				Owner: solana.PublicKeyFromBytes(addresses.StakeProgramAddr[:]), DataLen: 4, Data: []byte{1, 2, 3, 4}}).Marshal(&encoded))
			member := encoded.Bytes()
			if tc.malformed {
				member = member[:len(member)-5]
			}
			dir := filepath.Join(t.TempDir(), "PRIVATE_ARCHIVE_PATH")
			require.NoError(t, os.Mkdir(dir, 0o700))
			filename := filepath.Join(dir, "snapshot.tar.zst")
			var compressed bytes.Buffer
			zw, err := zstd.NewWriter(&compressed, zstd.WithEncoderConcurrency(1))
			require.NoError(t, err)
			tw := tar.NewWriter(zw)
			seed := []byte("status-cache-seed")
			for _, entry := range []struct {
				name string
				data []byte
			}{{agaveStatusCacheArchiveMember, seed}, {"ignored/PRIVATE_URL_CANARY", bytes.Repeat([]byte("ignored"), 128)}, {"accounts/20.21", member}} {
				require.NoError(t, tw.WriteHeader(&tar.Header{Name: entry.name, Mode: 0o600, Size: int64(len(entry.data))}))
				_, err = tw.Write(entry.data)
				require.NoError(t, err)
			}
			require.NoError(t, tw.Close())
			require.NoError(t, zw.Close())
			require.NoError(t, os.WriteFile(filename, compressed.Bytes(), 0o600))
			info, err := os.Stat(filename)
			require.NoError(t, err)
			require.Less(t, info.Size(), int64(64<<10))
			accountsDir := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(accountsDir, "accounts"), 0o700))
			logger := NewShardLogger(1, t.TempDir())
			closeLogger := sync.OnceFunc(func() { require.NoError(t, logger.Close(context.Background())) })
			t.Cleanup(closeLogger)
			wg, stakes := &sync.WaitGroup{}, &stakeIndexCollector{}
			manifest := &SnapshotManifest{AccountsDb: &AccountsDbFields{Storages: map[uint64]SlotAcctVecs{}}}
			var incrementalManifest *SnapshotManifest
			if tc.incremental {
				incrementalManifest = manifest
			}
			pools, err := initWorkerPools(wg, logger, manifest,
				incrementalManifest, testAppendVecDirs(t, accountsDir), stakes)
			require.NoError(t, err)
			t.Cleanup(func() { wg.Wait(); pools.Release() })
			dp := progress.NewDualProgress()
			var mu sync.Mutex
			var observations []progress.DualProgressObservation
			overflow := false
			if tc.observe {
				require.True(t, dp.SetObserver(func(o progress.DualProgressObservation) {
					mu.Lock()
					defer mu.Unlock()
					if len(observations) == 32 {
						overflow = true
						return
					}
					observations = append(observations, o)
				}))
			}
			if tc.incremental {
				full := progress.NewDualProgress()
				require.True(t, full.SetIncrementalProgress(func(slot uint64, kind string) *progress.DualProgress {
					require.Equal(t, uint64(20), slot)
					require.Equal(t, "local", kind)
					return dp
				}))
				full.Start()
				full.Stop()
				dp = full.NewIncrementalProgress(20, "local")
				require.NotNil(t, dp)
			}
			dp.Start()
			t.Cleanup(dp.Stop)
			seedPath := filepath.Join(dir, "retained-status-cache")
			readErr := readTar(ctx, wg, filename, pools, readTarOptions{progress: dp, isIncremental: tc.incremental, statusCachePath: seedPath})
			workerErr := waitForSnapshotWorkers(wg, pools)
			if readErr == nil {
				readErr = workerErr
			}
			phase := "full_archive_complete"
			if tc.incremental {
				phase = "incremental_archive_complete"
			}
			if tc.malformed {
				require.ErrorContains(t, readErr, "truncated appendvec account data")
				phase = "full_archive_interrupted"
				if tc.incremental {
					phase = "incremental_archive_interrupted"
				}
				dp.Interrupt(readErr)
			} else {
				require.NoError(t, readErr)
				dp.Stop()
				assertStatusCacheFileBytes(t, seedPath, seed)
				closeLogger()
				logged := 0
				for _, shard := range logger.shards {
					logged += shard.logSize
				}
				require.Equal(t, 32+vlen, logged)
				require.Len(t, stakes.entries, 1)
				require.Equal(t, key, stakes.entries[0].Pubkey)
				require.Equal(t, uint64(21), stakes.entries[0].FileId)
				require.Zero(t, stakes.entries[0].Offset)
			}
			require.Equal(t, info.Size(), dp.Download.Current())
			require.Equal(t, info.Size(), dp.Download.Total())
			require.Equal(t, int64(len(seed)+len(member)), dp.Extract.Current())
			if !tc.observe {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			require.False(t, overflow)
			require.GreaterOrEqual(t, len(observations), 2)
			require.Equal(t, "active", observations[0].State)
			last := observations[len(observations)-1]
			require.Equal(t, phase, last.State)
			require.True(t, last.CompressedTotalKnown)
			require.Equal(t, uint64(info.Size()), last.CompressedInputBytes)
			require.Equal(t, uint64(info.Size()), last.CompressedReportedTotal)
			require.Equal(t, uint64(len(seed)+len(member)), last.AccountPayloadBytes)
			for i, o := range observations {
				require.Equal(t, observations[0].StartedAt, o.StartedAt)
				if i < len(observations)-1 {
					require.Equal(t, "active", o.State)
				}
				if i != 0 {
					require.False(t, o.CapturedAt.Before(observations[i-1].CapturedAt))
					require.GreaterOrEqual(t, o.CompressedInputBytes, observations[i-1].CompressedInputBytes)
					require.GreaterOrEqual(t, o.AccountPayloadBytes, observations[i-1].AccountPayloadBytes)
				}
			}
			wire, err := json.Marshal(observations)
			require.NoError(t, err)
			for _, canary := range []string{"PRIVATE_ARCHIVE_PATH", "PRIVATE_URL_CANARY", "truncated appendvec"} {
				require.False(t, strings.Contains(string(wire), canary))
			}
		})
	}
}
