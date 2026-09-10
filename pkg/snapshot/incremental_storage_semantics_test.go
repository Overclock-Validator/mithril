package snapshot

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/klauspost/compress/zstd"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

func TestSnapshotVerificationAppendVecsUsesFullPlusPostBaseIncrementalStorages(t *testing.T) {
	full := testSnapshotManifestWithStorages(100, map[uint64]AcctVec{
		0:   {Id: 999, FileSize: 10},
		90:  {Id: 900, FileSize: 20},
		95:  {Id: 950, FileSize: 30},
		100: {Id: 1000, FileSize: 35},
	})
	// Agave serializes the complete current storage table into an incremental
	// manifest. The physical incremental archive nevertheless contains only
	// storages newer than the full snapshot's base slot.
	incremental := testSnapshotManifestWithStorages(120, map[uint64]AcctVec{
		// Inherited rows are metadata, not physical delta inputs. Deliberately
		// vary one and add one that is absent from the full manifest: neither
		// may alter the verification input set.
		0:   {Id: 998, FileSize: 11},
		80:  {Id: 950, FileSize: 30},
		90:  {Id: 901, FileSize: 21},
		100: {Id: 1002, FileSize: 36},
		101: {Id: 1001, FileSize: 40},
		120: {Id: 1200, FileSize: 50},
	})

	specs, err := snapshotVerificationAppendVecs(full, incremental)
	require.NoError(t, err)
	require.Equal(t, []accountsdb.SnapshotAppendVecSpec{
		{Slot: 0, FileID: 999, FileSize: 10},
		{Slot: 90, FileID: 900, FileSize: 20},
		{Slot: 95, FileID: 950, FileSize: 30},
		{Slot: 100, FileID: 1000, FileSize: 35},
		{Slot: 101, FileID: 1001, FileSize: 40},
		{Slot: 120, FileID: 1200, FileSize: 50},
	}, specs)
}

func TestExpectedIncrementalSnapshotAppendVecsIgnoresInheritedManifestRows(t *testing.T) {
	full := testSnapshotManifestWithStorages(100, map[uint64]AcctVec{
		90: {Id: 900, FileSize: 20},
	})
	incremental := testSnapshotManifestWithStorages(120, map[uint64]AcctVec{
		// Agave reconstructs inherited storage from the physical full archive;
		// these serialized pre-base rows do not describe delta TAR members.
		80:  {Id: 800, FileSize: 10},
		90:  {Id: 901, FileSize: 21},
		100: {Id: 1002, FileSize: 30},
		101: {Id: 1001, FileSize: 40},
		120: {Id: 1200, FileSize: 50},
	})

	expected, err := expectedIncrementalSnapshotAppendVecs(full, incremental)
	require.NoError(t, err)
	require.Equal(t, map[snapshotAppendVecKey]uint64{
		{slot: 101, fileID: 1001}: 40,
		{slot: 120, fileID: 1200}: 50,
	}, expected)

	// Rows at or below the base are not archive members, so even storage-table
	// details that are invalid for a physical modern appendvec must not leak
	// into incremental archive validation.
	incremental.AccountsDb.Storages[90] = SlotAcctVecs{Slot: 90}
	expected, err = expectedIncrementalSnapshotAppendVecs(full, incremental)
	require.NoError(t, err)
	require.Equal(t, map[snapshotAppendVecKey]uint64{
		{slot: 101, fileID: 1001}: 40,
		{slot: 120, fileID: 1200}: 50,
	}, expected)
}

func TestExpectedIncrementalSnapshotAppendVecsAcceptsDeltaOnlyManifest(t *testing.T) {
	full := testSnapshotManifestWithStorages(100, map[uint64]AcctVec{
		90: {Id: 900, FileSize: 20},
	})
	incremental := testSnapshotManifestWithStorages(120, map[uint64]AcctVec{
		101: {Id: 1001, FileSize: 40},
		120: {Id: 1200, FileSize: 50},
	})

	expected, err := expectedIncrementalSnapshotAppendVecs(full, incremental)
	require.NoError(t, err)
	require.Equal(t, map[snapshotAppendVecKey]uint64{
		{slot: 101, fileID: 1001}: 40,
		{slot: 120, fileID: 1200}: 50,
	}, expected)
}

func TestReadTarIncrementalRequiresOnlyPostBasePhysicalStorages(t *testing.T) {
	const (
		baseSlot        = uint64(100)
		incrementalSlot = uint64(120)
	)
	full := testSnapshotManifestWithStorages(baseSlot, map[uint64]AcctVec{
		0:  {Id: 999, FileSize: 10},
		90: {Id: 900, FileSize: 20},
	})
	postBaseContents := []byte("post-base-appendvec")
	incremental := testSnapshotManifestWithStorages(incrementalSlot, map[uint64]AcctVec{
		0:   {Id: 998, FileSize: 11},
		90:  {Id: 901, FileSize: 21},
		120: {Id: 1200, FileSize: uint64(len(postBaseContents))},
	})
	manifestBytes := []byte("incremental-manifest-with-complete-storage-table")
	incremental.rawDigest = sha256.Sum256(manifestBytes)
	incremental.hasRawDigest = true

	dir := t.TempDir()
	accountsDir := filepath.Join(dir, "accountsdb")
	require.NoError(t, os.MkdirAll(filepath.Join(accountsDir, "accounts"), 0o755))
	archive := filepath.Join(dir, "incremental.tar.zst")
	writeIncrementalStorageSemanticsArchive(t, archive, manifestBytes, map[snapshotAppendVecKey][]byte{
		{slot: 120, fileID: 1200}: postBaseContents,
	})

	wg := &sync.WaitGroup{}
	consumed := make(chan appendVecCopyingTask, 1)
	copyPool, err := ants.NewPoolWithFunc(1, func(value any) {
		defer wg.Done()
		consumed <- value.(appendVecCopyingTask)
	})
	require.NoError(t, err)
	t.Cleanup(copyPool.Release)
	pools := &snapshotWorkerPools{
		appendVecCopying:    copyPool,
		errors:              &snapshotWorkerErrors{},
		manifest:            full,
		incrementalManifest: incremental,
		accountsDbDir:       accountsDir,
	}

	err = readTar(context.Background(), wg, archive, pools, readTarOptions{
		isIncremental:   true,
		statusCachePath: filepath.Join(dir, "status-cache"),
	})
	require.NoError(t, err)
	require.Equal(t, appendVecCopyingTask{
		Path:     filepath.Join(accountsDir, "accounts", "120.1200"),
		Slot:     120,
		FileID:   1200,
		FileSize: uint64(len(postBaseContents)),
	}, <-consumed)
	require.FileExists(t, filepath.Join(accountsDir, "accounts", "120.1200"))
	require.NoFileExists(t, filepath.Join(accountsDir, "accounts", "0.999"))
	require.NoFileExists(t, filepath.Join(accountsDir, "accounts", "90.900"))
}

func TestReadTarIncrementalRejectsInheritedPhysicalMember(t *testing.T) {
	full := testSnapshotManifestWithStorages(100, map[uint64]AcctVec{
		90: {Id: 900, FileSize: 20},
	})
	inheritedContents := []byte("inherited-appendvec")
	incremental := testSnapshotManifestWithStorages(120, map[uint64]AcctVec{
		90: {Id: 900, FileSize: uint64(len(inheritedContents))},
	})
	manifestBytes := []byte("incremental-manifest-with-inherited-storage")
	incremental.rawDigest = sha256.Sum256(manifestBytes)
	incremental.hasRawDigest = true

	dir := t.TempDir()
	archive := filepath.Join(dir, "incremental-with-inherited-member.tar.zst")
	writeIncrementalStorageSemanticsArchive(t, archive, manifestBytes, map[snapshotAppendVecKey][]byte{
		{slot: 90, fileID: 900}: inheritedContents,
	})
	pools := &snapshotWorkerPools{
		errors:              &snapshotWorkerErrors{},
		manifest:            full,
		incrementalManifest: incremental,
	}

	err := readTar(context.Background(), &sync.WaitGroup{}, archive, pools, readTarOptions{
		isIncremental:   true,
		statusCachePath: filepath.Join(dir, "status-cache"),
	})
	require.ErrorContains(t, err, "appendvec slot=90 file_id=900 absent")
}

func TestReadTarIncrementalStillRequiresEveryPostBasePhysicalMember(t *testing.T) {
	full := testSnapshotManifestWithStorages(100, map[uint64]AcctVec{
		90: {Id: 900, FileSize: 20},
	})
	incremental := testSnapshotManifestWithStorages(120, map[uint64]AcctVec{
		90:  {Id: 900, FileSize: 20},
		120: {Id: 1200, FileSize: 50},
	})
	manifestBytes := []byte("incremental-manifest-missing-post-base-storage")
	incremental.rawDigest = sha256.Sum256(manifestBytes)
	incremental.hasRawDigest = true

	dir := t.TempDir()
	archive := filepath.Join(dir, "incremental-missing-post-base-member.tar.zst")
	writeIncrementalStorageSemanticsArchive(t, archive, manifestBytes, nil)
	pools := &snapshotWorkerPools{
		errors:              &snapshotWorkerErrors{},
		manifest:            full,
		incrementalManifest: incremental,
	}

	err := readTar(context.Background(), &sync.WaitGroup{}, archive, pools, readTarOptions{
		isIncremental:   true,
		statusCachePath: filepath.Join(dir, "status-cache"),
	})
	require.ErrorContains(t, err, "omitted 1 manifest appendvec")
	require.ErrorContains(t, err, "slot=120 file_id=1200")
}

func testSnapshotManifestWithStorages(slot uint64, storages map[uint64]AcctVec) *SnapshotManifest {
	storageTable := make(map[uint64]SlotAcctVecs, len(storages))
	for storageSlot, appendVec := range storages {
		storageTable[storageSlot] = SlotAcctVecs{
			Slot:     storageSlot,
			AcctVecs: []AcctVec{appendVec},
		}
	}
	return &SnapshotManifest{
		Bank:       &DeserializableVersionedBank{Slot: slot},
		AccountsDb: &AccountsDbFields{Slot: slot, Storages: storageTable},
	}
}

func writeIncrementalStorageSemanticsArchive(
	t *testing.T,
	filename string,
	manifest []byte,
	appendVecs map[snapshotAppendVecKey][]byte,
) {
	t.Helper()
	f, err := os.Create(filename)
	require.NoError(t, err)
	zw, err := zstd.NewWriter(f)
	require.NoError(t, err)
	tw := tar.NewWriter(zw)

	writeMember := func(name string, contents []byte) {
		t.Helper()
		require.NoError(t, tw.WriteHeader(&tar.Header{
			Name: name,
			Mode: 0o644,
			Size: int64(len(contents)),
		}))
		_, err := tw.Write(contents)
		require.NoError(t, err)
	}
	writeMember("version", []byte("1.2.0"))
	writeMember("snapshots/120/120", manifest)
	writeMember(agaveStatusCacheArchiveMember, make([]byte, 32))
	for key, contents := range appendVecs {
		writeMember(fmt.Sprintf("accounts/%d.%d", key.slot, key.fileID), contents)
	}

	require.NoError(t, tw.Close())
	require.NoError(t, zw.Close())
	require.NoError(t, f.Close())
}
