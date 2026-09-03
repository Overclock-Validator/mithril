package snapshot

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func testSnapshotLtHash(seed byte) *lthash.LtHash {
	serialized := make([]byte, 2048)
	serialized[0] = seed
	return new(lthash.LtHash).InitWithHash(serialized)
}

func testSnapshotArchiveHash(manifest *SnapshotManifest) string {
	return solana.HashFromBytes(manifest.LtHash.Checksum()).String()
}

func TestParseAppendVecTarPathRequiresCanonicalName(t *testing.T) {
	slot, fileID, isAppendVec, err := parseAppendVecTarPath("accounts/123.456")
	require.NoError(t, err)
	require.True(t, isAppendVec)
	require.Equal(t, uint64(123), slot)
	require.Equal(t, uint64(456), fileID)

	for _, name := range []string{
		"accounts/123.456suffix",
		"accounts/123.456.extra",
		"accounts/0123.456",
		"accounts/123.0456",
		"accounts/../123.456",
		"accounts/nested/123.456",
		"accounts/-1.2",
	} {
		_, _, _, err := parseAppendVecTarPath(name)
		require.Errorf(t, err, "accepted %q", name)
	}

	_, _, isAppendVec, err = parseAppendVecTarPath("snapshots/123/123")
	require.NoError(t, err)
	require.False(t, isAppendVec)
}

func TestWriteSnapshotAppendVecIsAtomicAndDoesNotReplace(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, "accounts"), 0o755))
	task := appendVecCopyingTask{Data: []byte("first"), Slot: 12, FileID: 34, FileSize: 5}
	require.NoError(t, writeSnapshotAppendVec(root, task))
	path := filepath.Join(root, "accounts", "12.34")
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("first"), contents)
	require.NoError(t, writeSnapshotAppendVec(root, task), "an exact retry must be idempotent")

	task.Data = []byte("other")
	err = writeSnapshotAppendVec(root, task)
	require.Error(t, err)
	contents, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, []byte("first"), contents)
}

func TestWriteSnapshotAppendVecRejectsManifestLengthMismatch(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, "accounts"), 0o755))
	task := appendVecCopyingTask{Data: []byte("short"), Slot: 1, FileID: 2, FileSize: 100}
	err := writeSnapshotAppendVec(root, task)
	require.ErrorContains(t, err, "does not match manifest length")
	require.NoFileExists(t, filepath.Join(root, "accounts", "1.2"))
}

func TestStreamSnapshotAppendVecPublishesAndMapsWithoutWholeFileBuffer(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, "accounts"), 0o755))
	const contents = "streamed-appendvec"
	path, copied, err := streamSnapshotAppendVec(root, 5, 6, uint64(len(contents)), strings.NewReader(contents))
	require.NoError(t, err)
	require.Equal(t, int64(len(contents)), copied)
	require.Equal(t, filepath.Join(root, "accounts", "5.6"), path)

	mapping, cleanup, err := mmapSnapshotAppendVec(path, uint64(len(contents)))
	require.NoError(t, err)
	require.Equal(t, []byte(contents), mapping)
	require.NoError(t, cleanup())

	_, _, err = streamSnapshotAppendVec(root, 7, 8, 100, strings.NewReader("short"))
	require.Error(t, err)
	require.NoFileExists(t, filepath.Join(root, "accounts", "7.8"))
}

func TestExpectedSnapshotAppendVecsRequiresExactUniqueStorageTable(t *testing.T) {
	manifest := &SnapshotManifest{AccountsDb: &AccountsDbFields{Storages: map[uint64]SlotAcctVecs{
		1: {Slot: 1, AcctVecs: []AcctVec{{Id: 2, FileSize: 10}}},
	}}}
	expected, err := expectedSnapshotAppendVecs(manifest)
	require.NoError(t, err)
	require.Equal(t, map[snapshotAppendVecKey]uint64{
		{slot: 1, fileID: 2}: 10,
	}, expected)

	manifest.AccountsDb.Storages[1] = SlotAcctVecs{
		Slot: 1, AcctVecs: []AcctVec{{Id: 2, FileSize: 10}, {Id: 2, FileSize: 10}},
	}
	_, err = expectedSnapshotAppendVecs(manifest)
	require.ErrorContains(t, err, "requires exactly one")

	manifest.AccountsDb.Storages[1] = SlotAcctVecs{Slot: 1}
	_, err = expectedSnapshotAppendVecs(manifest)
	require.ErrorContains(t, err, "requires exactly one")

	manifest.AccountsDb.Storages[1] = SlotAcctVecs{Slot: 2, AcctVecs: []AcctVec{{Id: 2, FileSize: 10}}}
	_, err = expectedSnapshotAppendVecs(manifest)
	require.ErrorContains(t, err, "does not match embedded slot")
}

func TestExpectedFullSnapshotAppendVecsRejectsStorageNewerThanBank(t *testing.T) {
	manifest := &SnapshotManifest{
		Bank: &DeserializableVersionedBank{Slot: 100},
		AccountsDb: &AccountsDbFields{Slot: 100, Storages: map[uint64]SlotAcctVecs{
			101: {Slot: 101, AcctVecs: []AcctVec{{Id: 2, FileSize: 10}}},
		}},
	}

	_, err := expectedFullSnapshotAppendVecs(manifest)
	require.ErrorContains(t, err, "storage slot 101 is newer than bank slot 100")
}

func TestSnapshotVerificationAppendVecsCombinesAndSortsExactPair(t *testing.T) {
	full := &SnapshotManifest{
		Bank: &DeserializableVersionedBank{Slot: 9},
		AccountsDb: &AccountsDbFields{Slot: 9, Storages: map[uint64]SlotAcctVecs{
			9: {Slot: 9, AcctVecs: []AcctVec{{Id: 90, FileSize: 900}}},
			2: {Slot: 2, AcctVecs: []AcctVec{{Id: 20, FileSize: 200}}},
		}}}
	incremental := &SnapshotManifest{
		Bank: &DeserializableVersionedBank{Slot: 12},
		AccountsDb: &AccountsDbFields{Slot: 12, Storages: map[uint64]SlotAcctVecs{
			12: {Slot: 12, AcctVecs: []AcctVec{{Id: 120, FileSize: 1200}}},
		}}}

	specs, err := snapshotVerificationAppendVecs(full, incremental)
	require.NoError(t, err)
	require.Equal(t, []accountsdb.SnapshotAppendVecSpec{
		{Slot: 2, FileID: 20, FileSize: 200},
		{Slot: 9, FileID: 90, FileSize: 900},
		{Slot: 12, FileID: 120, FileSize: 1200},
	}, specs)

	incremental.AccountsDb.Storages[9] = SlotAcctVecs{
		Slot: 9, AcctVecs: []AcctVec{{Id: 91, FileSize: 901}},
	}
	specs, err = snapshotVerificationAppendVecs(full, incremental)
	require.NoError(t, err)
	require.Equal(t, []accountsdb.SnapshotAppendVecSpec{
		{Slot: 2, FileID: 20, FileSize: 200},
		{Slot: 9, FileID: 90, FileSize: 900},
		{Slot: 12, FileID: 120, FileSize: 1200},
	}, specs, "inherited incremental rows are not physical archive members")
	delete(incremental.AccountsDb.Storages, 9)

	incremental.AccountsDb.Storages[12] = SlotAcctVecs{
		Slot: 12, AcctVecs: []AcctVec{{Id: 90, FileSize: 1200}},
	}
	_, err = snapshotVerificationAppendVecs(full, incremental)
	require.ErrorContains(t, err, "reuses appendvec file ID 90")
}

func TestSnapshotArchiveIdentityPinsFilenameAcrossMirrors(t *testing.T) {
	const filename = "incremental-snapshot-100-200-deadbeef.tar.zst"
	first, err := snapshotArchiveIdentity("https://mirror-a.example/snapshots/" + filename + "?token=one")
	require.NoError(t, err)
	second, err := snapshotArchiveIdentity("https://mirror-b.example/other/" + filename + "?token=two")
	require.NoError(t, err)
	local, err := snapshotArchiveIdentity(filepath.Join("/tmp", filename))
	require.NoError(t, err)
	require.Equal(t, filename, first)
	require.Equal(t, first, second)
	require.Equal(t, first, local)

	different, err := snapshotArchiveIdentity("https://mirror.example/incremental-snapshot-100-201-feedface.tar.zst")
	require.NoError(t, err)
	require.NotEqual(t, first, different)

	_, err = snapshotArchiveIdentity("https://mirror.example/")
	require.Error(t, err)
}

func TestValidateSnapshotManifestIdentityAndPair(t *testing.T) {
	full := &SnapshotManifest{
		Bank:       &DeserializableVersionedBank{Slot: 100},
		AccountsDb: &AccountsDbFields{Slot: 100},
		LtHash:     testSnapshotLtHash(1),
	}
	incremental := &SnapshotManifest{
		Bank:       &DeserializableVersionedBank{Slot: 120, IsDelta: true},
		AccountsDb: &AccountsDbFields{Slot: 120},
		LtHash:     testSnapshotLtHash(2),
		BankIncrementalSnapshotPersistence: &BankIncrementalSnapshotPersistence{
			FullSlot: 100,
		},
	}
	fullFilename := fmt.Sprintf("snapshot-100-%s.tar.zst", testSnapshotArchiveHash(full))
	incrementalFilename := fmt.Sprintf(
		"incremental-snapshot-100-120-%s.tar.zst",
		testSnapshotArchiveHash(incremental),
	)
	require.NoError(t, validateSnapshotManifestIdentity(full, 100))
	require.NoError(t, validateSnapshotManifestPair(full, incremental))
	require.NoError(t, validateSnapshotArchiveManifestSlots(
		fullFilename,
		full,
		"https://mirror.example/"+incrementalFilename+"?token=one",
		incremental,
	))

	full.Bank.Slot = 99
	require.ErrorContains(t, validateSnapshotManifestIdentity(full, 100), "does not match bank slot")
	full.Bank.Slot = 100
	incremental.BankIncrementalSnapshotPersistence.FullSlot = 101
	require.ErrorContains(t, validateSnapshotManifestPair(full, incremental), "does not match selected full")
	incremental.BankIncrementalSnapshotPersistence = nil
	require.NoError(t, validateSnapshotManifestPair(full, incremental), "current Agave may omit legacy persistence")
	require.ErrorContains(t, validateSnapshotArchiveManifestSlots(
		fullFilename,
		full,
		fmt.Sprintf("incremental-snapshot-101-120-%s.tar.zst", testSnapshotArchiveHash(incremental)),
		incremental,
	), "base slot 101")

	wrongHash := solana.Hash{9}.String()
	require.ErrorContains(t, validateSnapshotArchiveManifestSlots(
		fmt.Sprintf("snapshot-100-%s.tar.zst", wrongHash),
		full,
		"",
		nil,
	), "does not match manifest AccountsLtHash checksum")
	require.ErrorContains(t, validateSnapshotArchiveManifestSlots(
		fullFilename,
		full,
		fmt.Sprintf("incremental-snapshot-100-120-%s.tar.zst", wrongHash),
		incremental,
	), "does not match manifest AccountsLtHash checksum")

	full.LtHash = nil
	require.ErrorContains(t, validateSnapshotArchiveManifestSlots(
		fullFilename,
		full,
		"",
		nil,
	), "has no AccountsLtHash")
}

func TestParseSnapshotArchiveIdentityRequiresCanonicalBase58Hash(t *testing.T) {
	validHash := solana.Hash{1}.String()
	slot, parsedHash, err := parseFullSnapshotArchiveIdentity(
		fmt.Sprintf("snapshot-42-%s.tar.lz4", validHash),
	)
	require.NoError(t, err)
	require.Equal(t, uint64(42), slot)
	require.Equal(t, solana.Hash{1}, parsedHash)

	baseSlot, endSlot, parsedHash, err := parseIncrementalSnapshotArchiveIdentity(
		fmt.Sprintf("incremental-snapshot-42-43-%s.tar.zst", validHash),
	)
	require.NoError(t, err)
	require.Equal(t, uint64(42), baseSlot)
	require.Equal(t, uint64(43), endSlot)
	require.Equal(t, solana.Hash{1}, parsedHash)

	for _, filename := range []string{
		"snapshot-42-deadbeef.tar.zst",
		"snapshot-42-.tar.zst",
		fmt.Sprintf("snapshot-42-%s-extra.tar.zst", validHash),
		"incremental-snapshot-42-43-deadbeef.tar.zst",
	} {
		_, fullErr := parseFullSnapshotArchiveSlot(filename)
		_, _, incrementalErr := parseIncrementalSnapshotArchiveSlots(filename)
		require.Truef(t, fullErr != nil && incrementalErr != nil, "accepted %q", filename)
	}
}

func TestFinalizeSnapshotBootstrapArtifactsPublishesValidatedMarkers(t *testing.T) {
	root := t.TempDir()
	var bankHash [32]byte
	bankHash[0] = 9
	var stakePubkey [32]byte
	stakePubkey[0] = 1
	require.NoError(t, finalizeSnapshotBootstrapArtifacts(root, 42, bankHash, []accountsdb.StakeIndexEntry{{
		Pubkey: stakePubkey,
		FileId: 42,
	}}))

	largest, err := accountsdb.ReadLargestFileID(root)
	require.NoError(t, err)
	require.Equal(t, uint64(42), largest)
	bootstrapHigh, err := accountsdb.ReadBootstrapHighFileID(root)
	require.NoError(t, err)
	require.Equal(t, uint64(42), bootstrapHigh)
	contents, err := os.ReadFile(filepath.Join(root, "bank_hash"))
	require.NoError(t, err)
	require.Equal(t, bankHash[:], contents)
	require.DirExists(t, filepath.Join(root, "bankhash_db"))
}

func TestFinalizeSnapshotBootstrapArtifactsRejectsEmptyStakeIndexBeforePublication(t *testing.T) {
	root := t.TempDir()
	err := finalizeSnapshotBootstrapArtifacts(root, 42, [32]byte{}, nil)
	require.ErrorContains(t, err, "no stake-index entries")
	require.NoFileExists(t, filepath.Join(root, accountsdb.LargestFileIDFileName))
}
