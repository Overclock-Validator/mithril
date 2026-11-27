package accountsdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

var sidecarProgram = [32]byte{0xaa}

func sidecarTestConfig() KeySidecarConfig {
	return KeySidecarConfig{Programs: [][32]byte{sidecarProgram}, KeyOffset: 32, MinDataLen: 165}
}

// keyedAcct is a record of sidecarProgram whose data names owner at offset 32.
func keyedAcct(key byte, lamports uint64, owner [32]byte) *accounts.Account {
	data := make([]byte, 165)
	copy(data[32:64], owner[:])
	return &accounts.Account{Key: solana.PublicKey{key}, Lamports: lamports, Owner: sidecarProgram, Data: data}
}

func scanKeys(t *testing.T, db *AccountsDb, keys ...[32]byte) ([]scannedRecord, ScanStats) {
	t.Helper()
	var got []scannedRecord
	stats, err := db.ScanRecords(t.Context(), ScanRecordsConfig{Workers: 1, Programs: [][32]byte{sidecarProgram}, Keys: keys},
		func(pubkey, _ [32]byte, lamports uint64, _ []byte) {
			got = append(got, scannedRecord{solana.PublicKey(pubkey), lamports})
		})
	require.NoError(t, err)
	return got, stats
}

func TestKeySidecarScanReadsOnlyMatchingRecords(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	require.NoError(t, db.EnableKeySidecars(sidecarTestConfig()))
	alice, bob := [32]byte{1, 2, 3, 4, 5}, [32]byte{9}
	// carol shares alice's 4-byte fingerprint.
	carol := [32]byte{1, 2, 3, 4, 6}
	commitTestBatch(t, db, 10, keyedAcct(1, 100, alice), keyedAcct(2, 200, bob), keyedAcct(3, 300, carol), foldAcct(4, 400, nil))
	commitTestBatch(t, db, 20, keyedAcct(1, 150, alice))

	got, stats := scanKeys(t, db, alice)
	require.ElementsMatch(t, []scannedRecord{{solana.PublicKey{1}, 100}, {solana.PublicKey{1}, 150}}, got,
		"both versions of alice's account; carol's fingerprint matches but her key does not")
	require.Equal(t, 2, stats.SidecarFiles, "fold segments carry sidecars")
	require.EqualValues(t, 3, stats.Records, "only fingerprint matches are read")
}

func TestKeySidecarRebuiltWhenMissingOrStale(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	alice := [32]byte{7}
	// Written before sidecars are enabled, like snapshot appendvecs.
	r := commitTestBatch(t, db, 10, keyedAcct(1, 100, alice))
	require.NoError(t, db.EnableKeySidecars(sidecarTestConfig()))
	ks := db.keySidecars.Load()

	got, stats := scanKeys(t, db, alice)
	require.Equal(t, []scannedRecord{{solana.PublicKey{1}, 100}}, got)
	require.Zero(t, stats.SidecarFiles, "the first scan reads the file whole")
	require.FileExists(t, ks.path(10, r.FileId), "and writes its sidecar")

	got, stats = scanKeys(t, db, alice)
	require.Equal(t, []scannedRecord{{solana.PublicKey{1}, 100}}, got)
	require.Equal(t, 1, stats.SidecarFiles)

	// A torn sidecar is ignored and rewritten.
	raw, err := os.ReadFile(ks.path(10, r.FileId))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(ks.path(10, r.FileId), raw[:len(raw)-3], 0o644))
	got, stats = scanKeys(t, db, alice)
	require.Equal(t, []scannedRecord{{solana.PublicKey{1}, 100}}, got)
	require.Zero(t, stats.SidecarFiles)

	// So is one built under another configuration.
	other := sidecarTestConfig()
	other.MinDataLen = 166
	require.NoError(t, db.EnableKeySidecars(other))
	_, stats = scanKeys(t, db, alice)
	require.Zero(t, stats.SidecarFiles)
}

func TestKeySidecarFollowsCompaction(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	require.NoError(t, db.EnableKeySidecars(sidecarTestConfig()))
	ks := db.keySidecars.Load()
	alice := [32]byte{7}
	first := commitTestBatch(t, db, 10, keyedAcct(1, 100, alice), keyedAcct(2, 200, [32]byte{8}), keyedAcct(3, 300, [32]byte{8}))
	// Overwrite most of the first segment so compaction rewrites it.
	commitTestBatch(t, db, 20, keyedAcct(2, 201, [32]byte{8}), keyedAcct(3, 301, [32]byte{8}))
	commitTestBatch(t, db, 30)
	commitTestBatch(t, db, 40)
	stats, err := db.CompactOnce(CompactionConfig{RewindHorizonBatches: 1, MinDeadFraction: 0.5})
	require.NoError(t, err)
	require.Positive(t, stats.FilesCompacted)
	require.NoFileExists(t, ks.path(10, first.FileId), "the source's sidecar goes with it")

	got, scan := scanKeys(t, db, alice)
	require.Equal(t, []scannedRecord{{solana.PublicKey{1}, 100}}, got, "found in the compaction output")
	require.Equal(t, scan.Files, scan.SidecarFiles, "the output carries a sidecar")
}

func TestKeySidecarStaleFilesAreRemoved(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	require.NoError(t, db.EnableKeySidecars(sidecarTestConfig()))
	ks := db.keySidecars.Load()
	r := commitTestBatch(t, db, 10, keyedAcct(1, 100, [32]byte{7}))
	orphan := ks.path(5, r.FileId-1)
	require.NoError(t, os.WriteFile(orphan, []byte("x"), 0o644))
	newer := ks.path(10, r.FileId+5) // a file created after the scan listed
	require.NoError(t, os.WriteFile(newer, []byte("x"), 0o644))
	scanKeys(t, db, [32]byte{7})
	require.NoFileExists(t, orphan)
	require.FileExists(t, newer)
	require.FileExists(t, ks.path(10, r.FileId))
}

func TestScanByKeyNeedsSidecars(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	_, err := db.ScanRecords(t.Context(), ScanRecordsConfig{Keys: [][32]byte{{1}}}, func(_, _ [32]byte, _ uint64, _ []byte) {})
	require.Error(t, err)
	require.NoError(t, db.EnableKeySidecars(sidecarTestConfig()))
	_, err = db.ScanRecords(t.Context(), ScanRecordsConfig{Programs: [][32]byte{{0xbb}}, Keys: [][32]byte{{1}}}, func(_, _ [32]byte, _ uint64, _ []byte) {})
	require.Error(t, err, "a program the sidecars do not cover")
	require.DirExists(t, filepath.Join(db.AcctsDir, keySidecarDir))
}
