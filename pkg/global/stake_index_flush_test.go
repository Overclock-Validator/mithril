package global

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func isolateStakeIndexFlushState(t *testing.T) {
	t.Helper()
	instance.pendingStakeMutex.Lock()
	pending, cached, flushed := instance.pendingStakeBySlot, instance.cachedStakeEntries, instance.entriesFlushedSinceCompact
	instance.pendingStakeBySlot = make(map[uint64][]accountsdb.StakeIndexEntry)
	instance.cachedStakeEntries = nil
	instance.entriesFlushedSinceCompact = 0
	instance.pendingStakeMutex.Unlock()
	t.Cleanup(func() {
		instance.pendingStakeMutex.Lock()
		instance.pendingStakeBySlot, instance.cachedStakeEntries, instance.entriesFlushedSinceCompact = pending, cached, flushed
		instance.pendingStakeMutex.Unlock()
	})
}

// Read the raw records, bypassing LoadStakePubkeyIndex's read-time deduplication
// so an assertion on their count detects unnecessary on-disk growth.
func rawStakeIndexEntries(t *testing.T, dir string) []accountsdb.StakeIndexEntry {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, StakePubkeyIndexFileName))
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(data), 8)
	require.Equal(t, accountsdb.StakeIndexMagic[:], data[:4])
	require.Equal(t, accountsdb.StakeIndexVersion, binary.LittleEndian.Uint32(data[4:8]))
	require.Zero(t, (len(data)-8)%accountsdb.StakeIndexRecordSize)
	var entries []accountsdb.StakeIndexEntry
	for offset := 8; offset < len(data); offset += accountsdb.StakeIndexRecordSize {
		var entry accountsdb.StakeIndexEntry
		copy(entry.Pubkey[:], data[offset:offset+32])
		entry.FileId = binary.LittleEndian.Uint64(data[offset+32 : offset+40])
		entry.Offset = binary.LittleEndian.Uint64(data[offset+40 : offset+48])
		entries = append(entries, entry)
	}
	return entries
}

func TestStakeIndexFlushDeduplicatesCommittedAndPendingKeys(t *testing.T) {
	isolateStakeIndexFlushState(t)
	dir := t.TempDir()
	shared, committedOnly, pendingOnly, future := solana.PublicKey{31}, solana.PublicKey{32}, solana.PublicKey{33}, solana.PublicKey{34}
	EnqueuePendingStakePubkey(5, shared)
	EnqueuePendingStakePubkey(5, shared)
	EnqueuePendingStakePubkey(7, shared)
	EnqueuePendingStakePubkey(7, pendingOnly)
	EnqueuePendingStakePubkey(7, pendingOnly)
	EnqueuePendingStakePubkey(9, shared)
	EnqueuePendingStakePubkey(9, future)

	n, err := FlushPendingStakePubkeysThrough(dir, 7, shared, committedOnly, shared, committedOnly)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.ElementsMatch(t, []accountsdb.StakeIndexEntry{{Pubkey: shared}, {Pubkey: committedOnly}, {Pubkey: pendingOnly}}, rawStakeIndexEntries(t, dir))
	require.Empty(t, PendingStakePubkeysForSlot(5))
	require.Empty(t, PendingStakePubkeysForSlot(7))
	require.Equal(t, []solana.PublicKey{shared, future}, PendingStakePubkeysForSlot(9))
	require.Equal(t, 3, instance.entriesFlushedSinceCompact)

	n, err = FlushPendingStakePubkeysThrough(dir, 7)
	require.NoError(t, err)
	require.Zero(t, n)
	require.Len(t, rawStakeIndexEntries(t, dir), 3, "an empty flush must not append any records")

	n, err = FlushPendingStakePubkeysThrough(dir, 9, shared, future, future)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Len(t, rawStakeIndexEntries(t, dir), 5, "deduplication is scoped to each flush, not all historical writes")
	require.Empty(t, PendingStakeEntriesSnapshot())
	require.Equal(t, 5, instance.entriesFlushedSinceCompact)
}

func TestStakeIndexFlushKeepsLatestEligibleLocationHint(t *testing.T) {
	isolateStakeIndexFlushState(t)
	dir := t.TempDir()
	key, sameSlotKey, futureOnlyHint := solana.PublicKey{41}, solana.PublicKey{42}, solana.PublicKey{43}
	older := accountsdb.StakeIndexEntry{Pubkey: key, FileId: 50, Offset: 100}
	newer := accountsdb.StakeIndexEntry{Pubkey: key, FileId: 70, Offset: 200}
	future := accountsdb.StakeIndexEntry{Pubkey: key, FileId: 90, Offset: 300}
	sameSlotLast := accountsdb.StakeIndexEntry{Pubkey: sameSlotKey, FileId: 75, Offset: 400}
	futureOnly := accountsdb.StakeIndexEntry{Pubkey: futureOnlyHint, FileId: 95, Offset: 500}
	instance.pendingStakeBySlot = map[uint64][]accountsdb.StakeIndexEntry{
		9: {future, futureOnly},
		7: {newer, {Pubkey: sameSlotKey, FileId: 74, Offset: 100}, sameSlotLast, {Pubkey: sameSlotKey}},
		8: {{Pubkey: key}}, // Later hintless writes must not erase the usable hint.
		5: {older},
	}

	n, err := FlushPendingStakePubkeysThrough(dir, 8, key, key, sameSlotKey, futureOnlyHint)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.ElementsMatch(t, []accountsdb.StakeIndexEntry{newer, sameSlotLast, {Pubkey: futureOnlyHint}}, rawStakeIndexEntries(t, dir))
	require.Equal(t, []accountsdb.StakeIndexEntry{future, futureOnly}, PendingStakeEntriesSnapshot(), "future hints cannot leak into the flushed batch")

	n, err = FlushPendingStakePubkeysThrough(dir, 9, key, futureOnlyHint)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	entries := rawStakeIndexEntries(t, dir)
	require.Len(t, entries, 5)
	require.ElementsMatch(t, []accountsdb.StakeIndexEntry{future, futureOnly}, entries[3:])
	require.Empty(t, PendingStakeEntriesSnapshot())
}
