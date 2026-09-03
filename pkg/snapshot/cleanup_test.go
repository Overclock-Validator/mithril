package snapshot

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCleanAccountsDbDirRemovesRebuildArtifacts(t *testing.T) {
	root := t.TempDir()
	checkpointDir := filepath.Join(root, "transaction-status-checkpoints")
	streamIndexPath := filepath.Join(root, accountsdb.StreamIndexFileName)
	streamIndexPartialPath := streamIndexPath + ".partial"
	streamIndexBuildDir := filepath.Join(root, "mithril_db_build")
	streamIndexCollisionList := filepath.Join(root, "."+accountsdb.StreamIndexFileName+".collisions-123")
	streamIndexPartsDir := filepath.Join(root, "streamhash-parts-123")
	deltaJournalPath := filepath.Join(root, accountsdb.DeltaIndexJournalFileName)
	deltaRewritePath := filepath.Join(root, accountsdb.DeltaIndexJournalRewriteFileName)
	deltaCheckpointPath := filepath.Join(root, accountsdb.DeltaCheckpointFilePrefix+"00000000000000000001.desc")
	deltaCheckpointBuildDir := filepath.Join(root, accountsdb.DeltaCheckpointBuildDirPrefix+"123")
	rootCatalogPath := filepath.Join(root, accountsdb.RootIndexCatalogFileName)
	rootCatalogPartialPath := filepath.Join(root, accountsdb.RootIndexCatalogFileName+".tmp-123")
	shardedJournalPath := filepath.Join(root, accountsdb.ShardedDeltaIndexJournalFileName)
	shardedJournalRewritePath := filepath.Join(root, accountsdb.ShardedDeltaIndexJournalRewriteFileName)
	shardedCheckpointDir := filepath.Join(root, accountsdb.ShardedDeltaCheckpointDirName)
	shardedBaseDir := filepath.Join(root, "accounts-index-v2-g00000000000000000001-0123456789abcdef0123456789abcdef")
	prefixLookalikeDir := filepath.Join(root, "accounts-index-v2-g-user-data")
	bootstrapHighPath := filepath.Join(root, "bootstrap_high_file_id")
	bankHashPath := filepath.Join(root, "bank_hash")
	stakeIndexPath := filepath.Join(root, "stake_pubkeys.idx")
	bootstrapHighPartial := filepath.Join(root, ".bootstrap-high-123456.tmp")
	bootstrapHighLookalike := filepath.Join(root, ".bootstrap-high-operator.tmp")
	bootstrapHighSymlink := filepath.Join(root, ".bootstrap-high-654321.tmp")
	bootstrapHighSymlinkTarget := filepath.Join(root, "operator-marker")
	largestFileIDPartial := filepath.Join(root, ".largest-file-id-123456.tmp")
	largestFileIDLookalike := filepath.Join(root, ".largest-file-id-operator.tmp")
	largestFileIDSymlink := filepath.Join(root, ".largest-file-id-654321.tmp")
	deltaCheckpointLookalike := filepath.Join(root, accountsdb.DeltaCheckpointFilePrefix+"operator-notes")
	deltaBuildLookalike := filepath.Join(root, accountsdb.DeltaCheckpointBuildDirPrefix+"operator-notes")
	streamPartsLookalike := filepath.Join(root, "streamhash-parts-operator-notes")
	require.NoError(t, os.MkdirAll(checkpointDir, 0o755))
	require.NoError(t, os.MkdirAll(streamIndexBuildDir, 0o755))
	require.NoError(t, os.MkdirAll(streamIndexPartsDir, 0o755))
	require.NoError(t, os.MkdirAll(deltaCheckpointBuildDir, 0o755))
	require.NoError(t, os.MkdirAll(shardedCheckpointDir, 0o755))
	require.NoError(t, os.MkdirAll(shardedBaseDir, 0o755))
	require.NoError(t, os.MkdirAll(prefixLookalikeDir, 0o755))
	require.NoError(t, os.MkdirAll(deltaBuildLookalike, 0o755))
	require.NoError(t, os.MkdirAll(streamPartsLookalike, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(checkpointDir, "stale.bin"), []byte("stale"), 0o644))
	require.NoError(t, os.WriteFile(streamIndexPath, []byte("stale"), 0o644))
	require.NoError(t, os.WriteFile(streamIndexPartialPath, []byte("stale"), 0o644))
	require.NoError(t, os.WriteFile(streamIndexCollisionList, []byte("stale"), 0o644))
	require.NoError(t, os.WriteFile(deltaJournalPath, []byte("stale"), 0o644))
	require.NoError(t, os.WriteFile(deltaRewritePath, []byte("stale"), 0o644))
	require.NoError(t, os.WriteFile(deltaCheckpointPath, []byte("stale"), 0o644))
	require.NoError(t, os.WriteFile(rootCatalogPath, []byte("stale"), 0o644))
	require.NoError(t, os.WriteFile(rootCatalogPartialPath, []byte("stale"), 0o644))
	require.NoError(t, os.WriteFile(shardedJournalPath, []byte("stale"), 0o644))
	require.NoError(t, os.WriteFile(shardedJournalRewritePath, []byte("stale"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(shardedCheckpointDir, "stale"), []byte("stale"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(shardedBaseDir, "stale"), []byte("stale"), 0o644))
	require.NoError(t, os.WriteFile(bootstrapHighPath, []byte("stale"), 0o644))
	require.NoError(t, os.WriteFile(bankHashPath, []byte("stale"), 0o644))
	require.NoError(t, os.WriteFile(stakeIndexPath, []byte("stale"), 0o644))
	require.NoError(t, os.WriteFile(bootstrapHighPartial, []byte("stale"), 0o644))
	require.NoError(t, os.WriteFile(bootstrapHighLookalike, []byte("operator"), 0o644))
	require.NoError(t, os.WriteFile(bootstrapHighSymlinkTarget, []byte("operator"), 0o644))
	require.NoError(t, os.Symlink(bootstrapHighSymlinkTarget, bootstrapHighSymlink))
	require.NoError(t, os.WriteFile(largestFileIDPartial, []byte("stale"), 0o644))
	require.NoError(t, os.WriteFile(largestFileIDLookalike, []byte("operator"), 0o644))
	require.NoError(t, os.Symlink(bootstrapHighSymlinkTarget, largestFileIDSymlink))
	require.NoError(t, os.WriteFile(deltaCheckpointLookalike, []byte("operator"), 0o644))

	require.NoError(t, CleanAccountsDbDir(root))
	assert.NoDirExists(t, checkpointDir)
	assert.NoFileExists(t, streamIndexPath)
	assert.NoFileExists(t, streamIndexPartialPath)
	assert.NoFileExists(t, streamIndexCollisionList)
	assert.NoDirExists(t, streamIndexBuildDir)
	assert.NoDirExists(t, streamIndexPartsDir)
	assert.NoFileExists(t, deltaJournalPath)
	assert.NoFileExists(t, deltaRewritePath)
	assert.NoFileExists(t, deltaCheckpointPath)
	assert.NoDirExists(t, deltaCheckpointBuildDir)
	assert.NoFileExists(t, rootCatalogPath)
	assert.NoFileExists(t, rootCatalogPartialPath)
	assert.NoFileExists(t, shardedJournalPath)
	assert.NoFileExists(t, shardedJournalRewritePath)
	assert.NoDirExists(t, shardedCheckpointDir)
	assert.NoDirExists(t, shardedBaseDir)
	assert.NoFileExists(t, bootstrapHighPath)
	assert.NoFileExists(t, bankHashPath)
	assert.NoFileExists(t, stakeIndexPath)
	assert.NoFileExists(t, bootstrapHighPartial)
	assert.FileExists(t, bootstrapHighLookalike)
	assert.FileExists(t, bootstrapHighSymlink)
	assert.FileExists(t, bootstrapHighSymlinkTarget)
	assert.NoFileExists(t, largestFileIDPartial)
	assert.FileExists(t, largestFileIDLookalike)
	assert.FileExists(t, largestFileIDSymlink)
	assert.FileExists(t, deltaCheckpointLookalike)
	assert.DirExists(t, deltaBuildLookalike)
	assert.DirExists(t, streamPartsLookalike)
	assert.DirExists(t, prefixLookalikeDir, "cleanup must not recursively remove a prefix lookalike")
	assert.FileExists(t, filepath.Join(root, accountsdb.ProductionAccountIndexLockFileName))
}

func TestCleanAccountsDbDirRefusesConcurrentStoreOwner(t *testing.T) {
	root := t.TempDir()
	selected := filepath.Join(root, accountsdb.RootIndexCatalogFileName)
	require.NoError(t, os.WriteFile(selected, []byte("selected"), 0o644))
	locked := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- accountsdb.WithExclusiveProductionAccountIndexStore(root, func() error {
			close(locked)
			<-release
			return nil
		})
	}()
	select {
	case <-locked:
	case <-time.After(5 * time.Second):
		t.Fatal("exclusive store owner did not acquire lock")
	}

	err := CleanAccountsDbDir(root)
	require.ErrorIs(t, err, accountsdb.ErrProductionAccountIndexInUse)
	assert.FileExists(t, selected)
	close(release)
	require.NoError(t, <-done)
	require.NoError(t, CleanAccountsDbDir(root))
	assert.NoFileExists(t, selected)
	assert.FileExists(t, filepath.Join(root, accountsdb.ProductionAccountIndexLockFileName))
}

func TestCleanSnapshotDownloadDirHandlesSupportedFormatsWithoutBroadPartialDeletion(t *testing.T) {
	directory := t.TempDir()
	const validSnapshotHash = "11111111111111111111111111111111"
	full := filepath.Join(directory, "snapshot-1-"+validSnapshotHash+".tar.zst")
	incremental := filepath.Join(directory, "incremental-snapshot-1-2-"+validSnapshotHash+".tar.lz4")
	partial := filepath.Join(directory, "snapshot-3-"+validSnapshotHash+".tar.lz4.partial")
	operatorPartial := filepath.Join(directory, "operator-notes.partial")
	symlinkTarget := filepath.Join(directory, "operator-target")
	symlinkPartial := filepath.Join(directory, "snapshot-4-"+validSnapshotHash+".tar.zst.partial")
	for _, path := range []string{full, incremental, partial, operatorPartial, symlinkTarget} {
		require.NoError(t, os.WriteFile(path, []byte("data"), 0o644))
	}
	require.NoError(t, os.Symlink(symlinkTarget, symlinkPartial))

	CleanSnapshotDownloadDir(directory, 0)
	assert.NoFileExists(t, full)
	assert.NoFileExists(t, incremental)
	assert.NoFileExists(t, partial)
	assert.FileExists(t, operatorPartial)
	assert.FileExists(t, symlinkTarget)
	assert.FileExists(t, symlinkPartial)
}

func TestBuildSnapshotAccountsIndexRejectsMissingRuns(t *testing.T) {
	root := t.TempDir()
	logsDir := filepath.Join(root, "empty-logs")
	require.NoError(t, os.Mkdir(logsDir, 0o755))

	err := buildSnapshotAccountsIndex(t.Context(), root, logsDir, nil)
	require.ErrorContains(t, err, "no StreamHash run files")
	assert.NoFileExists(t, filepath.Join(root, accountsdb.StreamIndexFileName))
}

func TestBuildSnapshotAccountsIndexPublishesOnlyProductionV2(t *testing.T) {
	oldShardCount := accountsdb.ProductionAccountIndexShardCount
	oldWorkers := accountsdb.ProductionAccountIndexCheckpointWorkers
	oldMaxConcurrentSeals := accountsdb.ProductionAccountIndexMaxConcurrentSeals
	accountsdb.ProductionAccountIndexShardCount = 1
	accountsdb.ProductionAccountIndexCheckpointWorkers = 1
	accountsdb.ProductionAccountIndexMaxConcurrentSeals = 1
	t.Cleanup(func() {
		accountsdb.ProductionAccountIndexShardCount = oldShardCount
		accountsdb.ProductionAccountIndexCheckpointWorkers = oldWorkers
		accountsdb.ProductionAccountIndexMaxConcurrentSeals = oldMaxConcurrentSeals
	})

	root := t.TempDir()
	logsDir := filepath.Join(root, "logs")
	require.NoError(t, os.Mkdir(logsDir, 0o755))
	record := make([]byte, accountsdb.StreamIndexRunRecordSize)
	record[0] = 1
	var encodedEntry [24]byte
	accountsdb.AccountIndexEntry{Slot: 11, FileId: 7, Offset: 16}.Marshal(&encodedEntry)
	copy(record[32:], encodedEntry[:])
	require.NoError(t, os.WriteFile(filepath.Join(logsDir, "0000.run"), record, 0o644))

	guard, err := accountsdb.AcquireExclusiveProductionAccountIndexStore(root)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, guard.Close()) })
	require.NoError(t, buildSnapshotAccountsIndex(t.Context(), root, logsDir, guard))
	require.NoError(t, guard.Close())
	require.NoError(t, accountsdb.ValidateProductionAccountIndexArtifacts(root))
	assert.FileExists(t, filepath.Join(root, accountsdb.RootIndexCatalogFileName))
	assert.FileExists(t, filepath.Join(root, accountsdb.ShardedDeltaIndexJournalFileName))
	assert.NoFileExists(t, filepath.Join(root, accountsdb.StreamIndexFileName))
	assert.NoFileExists(t, filepath.Join(root, accountsdb.DeltaIndexJournalFileName))
}

func TestPrepareSnapshotIndexWorkDirCleansDefaultStaging(t *testing.T) {
	oldTempDir := SnapshotIndexTempDir
	SnapshotIndexTempDir = ""
	t.Cleanup(func() { SnapshotIndexTempDir = oldTempDir })

	root := t.TempDir()
	workDir, cleanup, err := prepareSnapshotIndexWorkDir(root)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(workDir, "000.run"), nil, 0o644))
	cleanup()
	assert.NoDirExists(t, workDir)
}
