package accountsdb

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newMultiDirTestDb scaffolds a snapshot-built store over n accounts
// directories with bootstrap file IDs up to bootstrapHigh.
func newMultiDirTestDb(t *testing.T, n int, bootstrapHigh uint64) (*AccountsDb, []string) {
	t.Helper()
	paths := make([]string, n)
	for i := range paths {
		paths[i] = t.TempDir()
		require.NoError(t, os.MkdirAll(filepath.Join(paths[i], "accounts"), 0o755))
	}
	var high [8]byte
	binary.LittleEndian.PutUint64(high[:], bootstrapHigh)
	require.NoError(t, os.WriteFile(filepath.Join(paths[0], "largest_file_id"), high[:], 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(paths[0], "bootstrap_high_file_id"), high[:], 0o644))
	require.NoError(t, WriteAccountsDirCount(paths[0], n))
	db, err := OpenDbPaths(paths)
	require.NoError(t, err)
	db.RootedDurable = true
	db.InitCaches()
	return db, paths
}

// writeBootstrapAppendVec writes an appendvec of accts as <slot>.<fileId> in the
// directory the file ID maps to, and indexes the accounts listed in live.
func writeBootstrapAppendVec(t *testing.T, db *AccountsDb, slot, fileId uint64, accts []*accounts.Account, live ...int) {
	t.Helper()
	var buf bytes.Buffer
	offsets := make([]uint64, len(accts))
	for i, a := range accts {
		offsets[i] = uint64(buf.Len())
		ava := AppendVecAccount{DataLen: uint64(len(a.Data)), Pubkey: a.Key, Lamports: a.Lamports, RentEpoch: a.RentEpoch, Owner: a.Owner, Data: a.Data}
		_, err := ava.MarshalReturningLength(&buf)
		require.NoError(t, err)
	}
	require.NoError(t, os.WriteFile(db.appendVecPath(slot, fileId), buf.Bytes(), 0o644))
	var idxBuf [24]byte
	for _, i := range live {
		(&AccountIndexEntry{Slot: slot, FileId: fileId, Offset: offsets[i]}).Marshal(&idxBuf)
		require.NoError(t, db.Index.Set(accts[i].Key[:], idxBuf[:], nil))
	}
}

func TestNextFileIdKeepsDirectory(t *testing.T) {
	db, _ := newMultiDirTestDb(t, 3, 10)
	defer db.CloseDb()
	prev := db.LargestFileId.Load()
	for _, dir := range []int{0, 2, 1, 0, 0, 2} {
		id := db.nextFileId(dir)
		assert.Greater(t, id, prev)
		assert.Equal(t, uint64(dir), id%3)
		assert.Equal(t, db.AcctsDirs[dir], db.appendVecDir(id))
		prev = id
	}

	single, _ := newFoldTestDb(t)
	defer single.CloseDb()
	start := single.LargestFileId.Load()
	assert.Equal(t, start+1, single.nextFileId(0), "one directory allocates sequentially as before")
	assert.Equal(t, filepath.Join(single.AcctsDir, "5.9"), single.appendVecPath(5, 9))
}

func TestOpenDbPathsChecksDirectoryCount(t *testing.T) {
	db, paths := newMultiDirTestDb(t, 2, 4)
	db.CloseDb()

	_, err := OpenDb(paths[0])
	require.ErrorContains(t, err, "built with 2")
	_, err = OpenDbPaths([]string{paths[0], paths[1], t.TempDir()})
	require.Error(t, err)
	_, err = OpenDbPaths([]string{paths[0], paths[0]})
	require.ErrorContains(t, err, "alias or overlap")

	// A legacy single-directory store has no count file and refuses extra paths.
	_, legacyDir := newFoldTestDb(t)
	assert.NoFileExists(t, filepath.Join(legacyDir, AccountsDirCountFile))
	extra := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(extra, "accounts"), 0o755))
	_, err = OpenDbPaths([]string{legacyDir, extra})
	require.ErrorContains(t, err, "built with 1")
}

// A snapshot appendvec in a secondary directory reads cold, a fold lands in
// the primary, compaction keeps its output on the source's disk, and recovery
// sweeps orphans in every directory.
func TestMultiDirReadFoldCompactRecover(t *testing.T) {
	db, paths := newMultiDirTestDb(t, 2, 5)
	defer func() { db.CloseDb() }()

	accts := []*accounts.Account{
		foldAcct(1, 100, make([]byte, 1024)),
		foldAcct(2, 200, []byte("live")),
		foldAcct(3, 300, make([]byte, 1024)),
	}
	writeBootstrapAppendVec(t, db, 50, 3, accts, 1) // file ID 3 lives in directory 1
	src := filepath.Join(paths[1], "accounts", "50.3")
	require.FileExists(t, src)
	assert.Equal(t, uint64(200), mustColdRead(t, db, 50, solana.PublicKey{2}).Lamports)

	r := commitTestBatch(t, db, 110, foldAcct(9, 900, []byte("fold")))
	assert.Equal(t, uint64(0), r.FileId%2, "fold segments are allocated in the primary")
	assert.FileExists(t, filepath.Join(paths[0], "accounts", SegmentDataName(110, r.FileId)))
	assert.Equal(t, uint64(900), mustColdRead(t, db, 110, solana.PublicKey{9}).Lamports)

	stats, err := db.CompactOnce(CompactionConfig{RewindHorizonBatches: 1, MinDeadFraction: 0.5})
	require.NoError(t, err)
	assert.Equal(t, 1, stats.FilesCompacted)
	assert.NoFileExists(t, src)
	got := mustColdRead(t, db, 50, solana.PublicKey{2})
	assert.Equal(t, uint64(200), got.Lamports)
	assert.Equal(t, []byte("live"), got.Data)
	v, closer, err := db.Index.Get(accts[1].Key[:])
	require.NoError(t, err)
	entry, err := UnmarshalAcctIdxEntry(v)
	closer.Close()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), entry.FileId%2, "compaction output stays in the source's directory")
	assert.FileExists(t, filepath.Join(paths[1], "accounts", SegmentDataName(50, entry.FileId)))
	assert.FileExists(t, segmentManifestPath(db.AcctsDir, 50, entry.FileId), "manifests stay in the primary")

	// An undecided orphan above the bootstrap high-water in a secondary directory.
	orphan := filepath.Join(paths[1], "accounts", "60.7")
	require.NoError(t, os.WriteFile(orphan, []byte("junk"), 0o644))
	removed, err := db.removeUnreferencedDataFiles()
	require.NoError(t, err)
	assert.Equal(t, []string{orphan}, removed)

	// Everything still resolves after reopening with the same directories.
	db.CloseDb()
	db, err = OpenDbPaths(paths)
	require.NoError(t, err)
	db.RootedDurable = true
	db.InitCaches()
	assert.Equal(t, uint64(200), mustColdRead(t, db, 50, solana.PublicKey{2}).Lamports)
	assert.Equal(t, uint64(900), mustColdRead(t, db, 110, solana.PublicKey{9}).Lamports)
}
