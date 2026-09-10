package snapshot

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardLoggerSetupFailureReturnsErrorAndUnwinds(t *testing.T) {
	dir := t.TempDir()
	preexisting := filepath.Join(dir, "001")
	require.NoError(t, os.WriteFile(preexisting, []byte("operator data"), 0o600))

	logger, err := NewShardLogger(2, dir)
	require.Nil(t, logger)
	require.ErrorContains(t, err, "create snapshot shard 1")
	assert.NoFileExists(t, filepath.Join(dir, "000"), "constructor must remove shards it created before failing")
	contents, readErr := os.ReadFile(preexisting)
	require.NoError(t, readErr)
	assert.Equal(t, []byte("operator data"), contents, "O_EXCL must preserve a preexisting path")

	linkedDir := filepath.Join(t.TempDir(), "linked")
	require.NoError(t, os.Symlink(t.TempDir(), linkedDir))
	logger, err = NewShardLogger(1, linkedDir)
	require.Nil(t, logger)
	require.ErrorContains(t, err, "not a real directory")

	logger, err = NewShardLogger(1001, dir)
	require.Nil(t, logger)
	require.ErrorContains(t, err, "exceeds maximum")

	for _, count := range []int{0, -1} {
		logger, err = NewShardLogger(count, dir)
		require.Nil(t, logger)
		require.ErrorContains(t, err, "must be positive")
	}
}

func TestShardLoggerAbortDrainsAndDiscardsRawLogs(t *testing.T) {
	dir := t.TempDir()
	logger, err := NewShardLogger(2, dir)
	require.NoError(t, err)
	logger.EnqueueRequest(solana.PublicKey{1}, accountsdb.AccountIndexEntry{Slot: 1, FileId: 2, Offset: 8})
	require.NoError(t, logger.Abort())
	require.NoError(t, logger.Abort(), "Abort must be idempotent")
	assert.NoFileExists(t, filepath.Join(dir, "000"))
	assert.NoFileExists(t, filepath.Join(dir, "001"))
	assert.NoFileExists(t, filepath.Join(dir, "000.run"))
	require.ErrorContains(t, logger.Close(context.Background()), "aborted")
}

func TestShardLoggerConcurrentEnqueueAndCloseIsSafe(t *testing.T) {
	dir := t.TempDir()
	logger, err := NewShardLogger(2, dir)
	require.NoError(t, err)

	start := make(chan struct{})
	var producers sync.WaitGroup
	for worker := range 4 {
		producers.Add(1)
		go func() {
			defer producers.Done()
			<-start
			for ordinal := range 16 {
				logger.EnqueueRequest(
					solana.PublicKey{byte(worker), byte(ordinal)},
					accountsdb.AccountIndexEntry{Slot: uint64(ordinal + 1), FileId: uint64(worker + 1), Offset: 8},
				)
			}
		}()
	}
	closed := make(chan error, 1)
	go func() {
		<-start
		closed <- logger.Close(context.Background())
	}()
	close(start)
	producers.Wait()
	require.NoError(t, <-closed)
	require.NoError(t, logger.Close(context.Background()), "Close must be idempotent")
}

func TestShardLoggerWritesSortedDeduplicatedRuns(t *testing.T) {
	dir := t.TempDir()
	logger, err := NewShardLogger(2, dir)
	require.NoError(t, err)

	low := solana.PublicKey{0x10, 1}
	high := solana.PublicKey{0x90, 2}
	// Enqueue out of order. For duplicate keys, the highest slot wins; ties
	// choose the highest file ID and then the highest appendvec offset.
	requests := []struct {
		key   solana.PublicKey
		entry accountsdb.AccountIndexEntry
	}{
		{high, accountsdb.AccountIndexEntry{Slot: 3, FileId: 7, Offset: 8}},
		{low, accountsdb.AccountIndexEntry{Slot: 8, FileId: 99, Offset: 128}},
		{low, accountsdb.AccountIndexEntry{Slot: 9, FileId: 1, Offset: 8}},
		{low, accountsdb.AccountIndexEntry{Slot: 9, FileId: 2, Offset: 8}},
		{low, accountsdb.AccountIndexEntry{Slot: 9, FileId: 2, Offset: 16}},
	}
	for _, request := range requests {
		logger.EnqueueRequest(request.key, request.entry)
	}
	require.NoError(t, logger.Close(context.Background()))

	assert.Equal(t, int64(len(requests)*accountsdb.StreamIndexRunRecordSize), logger.TotalBytes())
	assert.Equal(t, logger.TotalBytes(), logger.BytesDone())
	assert.NoFileExists(t, filepath.Join(dir, "000"))
	assert.NoFileExists(t, filepath.Join(dir, "001"))
	assert.NoFileExists(t, filepath.Join(dir, "000.sst"))
	assert.FileExists(t, filepath.Join(dir, "000.run"))
	assert.FileExists(t, filepath.Join(dir, "001.run"))

	source, err := accountsdb.OpenStreamIndexRunSource(dir)
	require.NoError(t, err)
	var keys []solana.PublicKey
	var entries []accountsdb.AccountIndexEntry
	require.NoError(t, source.Scan(context.Background(), func(key solana.PublicKey, entry accountsdb.AccountIndexEntry) error {
		keys = append(keys, key)
		entries = append(entries, entry)
		return nil
	}))
	require.Equal(t, []solana.PublicKey{low, high}, keys)
	require.Len(t, entries, 2)
	assert.Equal(t, accountsdb.AccountIndexEntry{Slot: 9, FileId: 2, Offset: 16}, entries[0])
	assert.Equal(t, accountsdb.AccountIndexEntry{Slot: 3, FileId: 7, Offset: 8}, entries[1])
	assert.Less(t, bytes.Compare(keys[0][:], keys[1][:]), 0)

	oldShardCount := accountsdb.ProductionAccountIndexShardCount
	oldWorkers := accountsdb.ProductionAccountIndexCheckpointWorkers
	oldMaxConcurrentSeals := accountsdb.ProductionAccountIndexMaxConcurrentSeals
	accountsdb.ProductionAccountIndexShardCount = 2
	accountsdb.ProductionAccountIndexCheckpointWorkers = 1
	accountsdb.ProductionAccountIndexMaxConcurrentSeals = 1
	t.Cleanup(func() {
		accountsdb.ProductionAccountIndexShardCount = oldShardCount
		accountsdb.ProductionAccountIndexCheckpointWorkers = oldWorkers
		accountsdb.ProductionAccountIndexMaxConcurrentSeals = oldMaxConcurrentSeals
	})
	accountsDir := t.TempDir()
	guard, err := accountsdb.AcquireExclusiveProductionAccountIndexStore(accountsDir)
	require.NoError(t, err)
	require.NoError(t, buildSnapshotAccountsIndex(context.Background(), accountsDir, dir, guard))
	require.NoError(t, guard.Close())
	assert.NoDirExists(t, filepath.Join(accountsDir, "mithril_db"))
	assert.NoDirExists(t, filepath.Join(accountsDir, "mithril_db_build"))
	require.NoError(t, accountsdb.ValidateProductionAccountIndexArtifacts(accountsDir))
	config, err := accountsdb.CurrentProductionAccountIndexConfig()
	require.NoError(t, err)
	index, err := accountsdb.OpenProductionAccountIndex(accountsDir, config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Close()) })
	for i, key := range keys {
		got, _, found, err := index.LookupCandidate(key)
		require.NoError(t, err)
		require.True(t, found)
		assert.Equal(t, entries[i], got)
	}
}

func TestShardLoggerExternalSortBoundsAdversarialPrefixSkew(t *testing.T) {
	previousChunkMB := SnapshotShardSortChunkMB
	SnapshotShardSortChunkMB = 1
	t.Cleanup(func() { SnapshotShardSortChunkMB = previousChunkMB })

	dir := t.TempDir()
	logger, err := NewShardLogger(1, dir)
	require.NoError(t, err)
	const uniqueKeys = 40_000 // More than two 1 MiB chunks at 56 bytes/record.
	winnerKey := solana.PublicKey{}
	binary.BigEndian.PutUint64(winnerKey[24:], uniqueKeys+1)
	logger.EnqueueRequest(winnerKey, accountsdb.AccountIndexEntry{Slot: 7, FileId: 1, Offset: 8})
	for i := 1; i <= uniqueKeys; i++ {
		key := solana.PublicKey{}
		// Every key has the same eight-byte shard prefix: this is the worst-case
		// distribution for range sharding, but must not alter the memory bound.
		binary.BigEndian.PutUint64(key[24:], uint64(i))
		logger.EnqueueRequest(key, accountsdb.AccountIndexEntry{Slot: uint64(i), FileId: 1, Offset: 8})
		if i == uniqueKeys/2 {
			logger.EnqueueRequest(winnerKey, accountsdb.AccountIndexEntry{Slot: 8, FileId: 9, Offset: 16})
		}
	}
	logger.EnqueueRequest(winnerKey, accountsdb.AccountIndexEntry{Slot: 8, FileId: 9, Offset: 24})
	require.NoError(t, logger.Close(context.Background()))

	source, err := accountsdb.OpenStreamIndexRunSource(dir)
	require.NoError(t, err)
	count := 0
	var previous solana.PublicKey
	foundWinner := false
	require.NoError(t, source.Scan(context.Background(), func(key solana.PublicKey, entry accountsdb.AccountIndexEntry) error {
		if count > 0 {
			assert.Less(t, bytes.Compare(previous[:], key[:]), 0)
		}
		if key == winnerKey {
			foundWinner = true
			assert.Equal(t, accountsdb.AccountIndexEntry{Slot: 8, FileId: 9, Offset: 24}, entry)
		}
		previous = key
		count++
		return nil
	}))
	assert.Equal(t, uniqueKeys+1, count)
	assert.True(t, foundWinner)
	assert.Equal(t, logger.TotalBytes(), logger.BytesDone())
	temps, err := filepath.Glob(filepath.Join(dir, "*.partial"))
	require.NoError(t, err)
	assert.Empty(t, temps)
}
