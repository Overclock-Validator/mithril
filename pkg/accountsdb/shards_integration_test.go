package accountsdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Exercise the snapshot layout through the APIs used by Alpenglow replay, then
// fold, restart, and rewind across the snapshot-to-durable-segment boundary.
func TestShardedSnapshotFoldReopenAndRewind(t *testing.T) {
	paths := []string{t.TempDir(), t.TempDir()}
	for _, p := range paths {
		require.NoError(t, os.MkdirAll(filepath.Join(p, "accounts"), 0755))
	}
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], 2)
	require.NoError(t, os.WriteFile(filepath.Join(paths[0], "num_shards"), b[:], 0644))
	binary.LittleEndian.PutUint64(b[:], 1)
	for _, name := range []string{"largest_file_id", "bootstrap_high_file_id"} {
		require.NoError(t, os.WriteFile(filepath.Join(paths[0], name), b[:], 0644))
	}
	db, err := OpenDbPaths(paths)
	require.NoError(t, err)
	defer func() { db.CloseDb() }()
	db.InitCaches()
	db.RootedDurable = true
	old := []*accounts.Account{foldAcct(1, 11, []byte("one")), foldAcct(2, 22, []byte("two"))}
	keys := []solana.PublicKey{old[0].Key, old[1].Key}
	for i, acct := range old {
		acct.Slot = 100
		var data bytes.Buffer
		// Start at an unaligned host offset, as packed snapshot appendvecs may do.
		data.Write([]byte{0, 0, 0})
		av := AppendVecAccount{Pubkey: acct.Key, Lamports: acct.Lamports, Owner: acct.Owner, RentEpoch: acct.RentEpoch, DataLen: uint64(len(acct.Data)), Data: acct.Data}
		require.NoError(t, av.Marshal(&data))
		require.NoError(t, os.WriteFile(filepath.Join(paths[i], "accounts", "data"), data.Bytes(), 0644))
		var idx [24]byte
		(&AccountIndexEntry{Slot: 100, FileId: uint64(i), Offset: 3}).Marshal(&idx)
		require.NoError(t, db.Index.Set(acct.Key[:], idx[:], nil))
	}
	got, err := db.GetAccountsBatch(context.Background(), 100, keys)
	require.NoError(t, err)
	require.Equal(t, old, got)
	for i, k := range keys {
		require.Equal(t, old[i], mustColdRead(t, db, 100, k))
	}
	first, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 101, Delta: []*accounts.Account{foldAcct(1, 33, []byte("new"))}}}, 101, map[uint64][32]byte{101: bh(101)}, []byte("ctx-101"))
	require.NoError(t, err)
	require.Zero(t, first.FileId%2)
	db.CloseDb()
	db, err = OpenDbPaths(paths)
	require.NoError(t, err)
	db.InitCaches()
	db.RootedDurable = true
	_, err = db.RecoverFoldState()
	require.NoError(t, err)
	require.Equal(t, uint64(33), mustColdRead(t, db, 101, keys[0]).Lamports)
	require.Equal(t, old[1], mustColdRead(t, db, 101, keys[1]))
	second, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 102, Delta: []*accounts.Account{foldAcct(2, 44, []byte("new-two"))}}}, 102, map[uint64][32]byte{102: bh(102)}, []byte("ctx-102"))
	require.NoError(t, err)
	require.Greater(t, second.FileId, first.FileId)
	_, err = db.RewindToBatchBoundary(101)
	require.NoError(t, err)
	require.Equal(t, old[1], mustColdRead(t, db, 101, keys[1]))
}
