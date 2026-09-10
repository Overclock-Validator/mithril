package global

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func globalStakeIndexTestEntry(seed byte, fileID, offset uint64) accountsdb.StakeIndexEntry {
	var pubkey solana.PublicKey
	for index := range pubkey {
		pubkey[index] = seed + byte(index)
	}
	return accountsdb.StakeIndexEntry{Pubkey: pubkey, FileId: fileID, Offset: offset}
}

func resetStakeIndexTestState(t *testing.T) {
	t.Helper()
	instance.stakeIndexIOMutex.Lock()
	instance.pendingStakeMutex.Lock()
	instance.pendingStakeBySlot = nil
	instance.cachedStakeEntries = nil
	instance.cachedStakeIndexPath = ""
	instance.entriesFlushedSinceCompact = 0
	instance.pendingStakeMutex.Unlock()
	instance.stakeIndexIOMutex.Unlock()
	t.Cleanup(func() {
		instance.stakeIndexIOMutex.Lock()
		instance.pendingStakeMutex.Lock()
		instance.pendingStakeBySlot = nil
		instance.cachedStakeEntries = nil
		instance.cachedStakeIndexPath = ""
		instance.entriesFlushedSinceCompact = 0
		instance.pendingStakeMutex.Unlock()
		instance.stakeIndexIOMutex.Unlock()
	})
}

func TestLoadStakePubkeyIndexDeduplicatesSortsAndKeysCacheByPath(t *testing.T) {
	resetStakeIndexTestState(t)
	directoryA := t.TempDir()
	directoryB := t.TempDir()
	first := globalStakeIndexTestEntry(1, 20, 200)
	freshest := globalStakeIndexTestEntry(1, 10, 100)
	second := globalStakeIndexTestEntry(2, 5, 50)
	require.NoError(t, accountsdb.WriteStakePubkeyIndex(
		filepath.Join(directoryA, StakePubkeyIndexFileName),
		[]accountsdb.StakeIndexEntry{first, second, freshest},
	))

	loaded, err := LoadStakePubkeyIndex(directoryA)
	require.NoError(t, err)
	require.Len(t, loaded, 2)
	assert.Equal(t, second, loaded[0])
	assert.Equal(t, freshest, loaded[1], "last duplicate must keep the freshest location hint")

	other := globalStakeIndexTestEntry(9, 9, 9)
	require.NoError(t, accountsdb.WriteStakePubkeyIndex(
		filepath.Join(directoryB, StakePubkeyIndexFileName),
		[]accountsdb.StakeIndexEntry{other},
	))
	loaded, err = LoadStakePubkeyIndex(directoryB)
	require.NoError(t, err)
	assert.Equal(t, []accountsdb.StakeIndexEntry{other}, loaded, "a cache from another AccountsDB must not leak across paths")
}

func TestFlushPendingStakePubkeysRestoresEntriesOnFailure(t *testing.T) {
	resetStakeIndexTestState(t)
	directory := t.TempDir()
	path := filepath.Join(directory, StakePubkeyIndexFileName)
	referent := filepath.Join(directory, "referent")
	require.NoError(t, os.WriteFile(referent, []byte("unchanged"), 0o644))
	require.NoError(t, os.Symlink(referent, path))

	entry := globalStakeIndexTestEntry(3, 30, 300)
	EnqueuePendingStakePubkey(42, entry.Pubkey)
	flushed, err := FlushPendingStakePubkeysThrough(directory, 42)
	require.Error(t, err)
	assert.Zero(t, flushed)
	pending := PendingStakeEntriesSnapshot()
	require.Len(t, pending, 1)
	assert.Equal(t, entry.Pubkey, pending[0].Pubkey)

	require.NoError(t, os.Remove(path))
	flushed, err = FlushPendingStakePubkeysThrough(directory, 42)
	require.NoError(t, err)
	assert.Equal(t, 1, flushed)
	assert.Empty(t, PendingStakeEntriesSnapshot())
	loaded, err := LoadStakePubkeyIndex(directory)
	require.NoError(t, err)
	require.Len(t, loaded, 1)
	assert.Equal(t, entry.Pubkey, loaded[0].Pubkey)
}

func TestFlushPendingStakePubkeysInvalidatesLoadedCache(t *testing.T) {
	resetStakeIndexTestState(t)
	directory := t.TempDir()
	path := filepath.Join(directory, StakePubkeyIndexFileName)
	initial := globalStakeIndexTestEntry(1, 10, 100)
	require.NoError(t, accountsdb.WriteStakePubkeyIndex(path, []accountsdb.StakeIndexEntry{initial}))
	loaded, err := LoadStakePubkeyIndex(directory)
	require.NoError(t, err)
	require.Len(t, loaded, 1)

	newEntry := globalStakeIndexTestEntry(2, 0, 0)
	EnqueuePendingStakePubkey(7, newEntry.Pubkey)
	flushed, err := FlushPendingStakePubkeysThrough(directory, 7)
	require.NoError(t, err)
	assert.Equal(t, 1, flushed)

	loaded, err = LoadStakePubkeyIndex(directory)
	require.NoError(t, err)
	require.Len(t, loaded, 2)
	assert.Equal(t, newEntry.Pubkey, loaded[0].Pubkey)
	assert.Equal(t, initial.Pubkey, loaded[1].Pubkey)
}

func TestCompactStakePubkeyIndexAtomicallyRewritesDeduplicatedCache(t *testing.T) {
	resetStakeIndexTestState(t)
	directory := t.TempDir()
	path := filepath.Join(directory, StakePubkeyIndexFileName)
	old := globalStakeIndexTestEntry(1, 20, 200)
	fresh := globalStakeIndexTestEntry(1, 10, 100)
	other := globalStakeIndexTestEntry(2, 5, 50)
	require.NoError(t, accountsdb.WriteStakePubkeyIndex(
		path,
		[]accountsdb.StakeIndexEntry{old, other, fresh},
	))
	loaded, err := LoadStakePubkeyIndex(directory)
	require.NoError(t, err)
	require.Len(t, loaded, 2)

	instance.pendingStakeMutex.Lock()
	instance.entriesFlushedSinceCompact = compactThreshold
	instance.pendingStakeMutex.Unlock()
	require.NoError(t, CompactStakePubkeyIndex(directory))

	onDisk, format, err := accountsdb.ReadStakePubkeyIndex(path)
	require.NoError(t, err)
	assert.Equal(t, accountsdb.StakeIndexVersion, format.Version)
	assert.Equal(t, loaded, onDisk)
	instance.pendingStakeMutex.Lock()
	assert.Zero(t, instance.entriesFlushedSinceCompact)
	instance.pendingStakeMutex.Unlock()
}

func TestStakePubkeyIndexConcurrentLoadAndFlush(t *testing.T) {
	resetStakeIndexTestState(t)
	directory := t.TempDir()
	path := filepath.Join(directory, StakePubkeyIndexFileName)
	initial := globalStakeIndexTestEntry(200, 1, 1)
	require.NoError(t, accountsdb.WriteStakePubkeyIndex(path, []accountsdb.StakeIndexEntry{initial}))

	const writers = 16
	start := make(chan struct{})
	errorsCh := make(chan error, writers*2)
	var wait sync.WaitGroup
	for index := 0; index < writers; index++ {
		index := index
		wait.Add(2)
		go func() {
			defer wait.Done()
			<-start
			entry := globalStakeIndexTestEntry(byte(index+1), 0, 0)
			EnqueuePendingStakePubkey(uint64(index+1), entry.Pubkey)
			_, err := FlushPendingStakePubkeysThrough(directory, uint64(index+1))
			errorsCh <- err
		}()
		go func() {
			defer wait.Done()
			<-start
			_, err := LoadStakePubkeyIndex(directory)
			errorsCh <- err
		}()
	}
	close(start)
	wait.Wait()
	close(errorsCh)
	for err := range errorsCh {
		require.NoError(t, err)
	}

	loaded, err := LoadStakePubkeyIndex(directory)
	require.NoError(t, err)
	assert.Len(t, loaded, writers+1)
}
