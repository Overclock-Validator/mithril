package accountsdb

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

// The chunked iterator capture must return exactly what one point lookup per
// key returns: present and absent keys, keys at chunk edges, and non-account
// index keys (other lengths) interleaved with account keys.
func TestCaptureUndoEntriesMatchesPointLookups(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	rng := rand.New(rand.NewSource(1))
	var keys [][32]byte
	batch := db.Index.NewBatch()
	var buf [24]byte
	for i := 0; i < 5*undoCaptureMinChunk+17; i++ {
		var k [32]byte
		rng.Read(k[:])
		keys = append(keys, k)
		if rng.Intn(3) == 0 {
			continue // absent: a key the batch creates
		}
		(&AccountIndexEntry{Slot: uint64(i), FileId: uint64(rng.Intn(1000)), Offset: uint64(i) * 8}).Marshal(&buf)
		require.NoError(t, batch.Set(k[:], buf[:], nil))
		// A longer key sharing the prefix must not be mistaken for the account key.
		if i%7 == 0 {
			require.NoError(t, batch.Set(append(append([]byte(nil), k[:]...), 1), []byte("other"), nil))
		}
	}
	require.NoError(t, batch.Commit(pebble.Sync))
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })

	got, err := db.captureUndoEntries(keys)
	require.NoError(t, err)
	require.Len(t, got, len(keys))
	present := 0
	for i, k := range keys {
		val, closer, gerr := db.Index.Get(k[:])
		if gerr == pebble.ErrNotFound {
			require.False(t, got[i].valid, "key %d", i)
			continue
		}
		require.NoError(t, gerr)
		want, derr := UnmarshalAcctIdxEntryValue(val)
		closer.Close()
		require.NoError(t, derr)
		require.True(t, got[i].valid, "key %d", i)
		require.Equal(t, want, got[i].entry, "key %d", i)
		present++
	}
	require.Positive(t, present)
	require.Less(t, present, len(keys))

	empty, err := db.captureUndoEntries(nil)
	require.NoError(t, err)
	require.Empty(t, empty)
	one, err := db.captureUndoEntries(keys[:1])
	require.NoError(t, err)
	require.Len(t, one, 1)
}

// Undo pointers recorded by a fold equal the index entries the keys had
// before it, and new keys record no previous entry.
func TestCommitBatchRecordsPreviousEntries(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	first := commitTestBatch(t, db, 100, foldAcct(1, 10, []byte("a")), foldAcct(2, 20, []byte("b")))
	second := commitTestBatch(t, db, 200, foldAcct(2, 21, []byte("b2")), foldAcct(3, 30, []byte("c")))

	m, err := ReadSegmentManifest(segmentManifestPath(db.AcctsDir, 200, second.FileId))
	require.NoError(t, err)
	require.Len(t, m.Records, 2)
	for _, rec := range m.Records {
		switch rec.Pubkey[0] {
		case 2:
			require.True(t, rec.PrevValid)
			require.Equal(t, first.FileId, rec.Prev.FileId)
			require.Equal(t, uint64(100), rec.Prev.Slot)
		case 3:
			require.False(t, rec.PrevValid)
		default:
			t.Fatalf("unexpected key %x", rec.Pubkey)
		}
	}
}
