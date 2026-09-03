package accountsdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type snapshotVerifyFile struct {
	slot     uint64
	fileID   uint64
	accounts []*accounts.Account
}

type snapshotVerifyFixture struct {
	root     string
	guard    *ProductionAccountIndexStoreGuard
	config   ProductionAccountIndexConfig
	specs    []SnapshotAppendVecSpec
	selected map[solana.PublicKey]*accounts.Account
}

func snapshotVerifyKey(value byte) solana.PublicKey {
	var key solana.PublicKey
	key[0] = value
	key[7] = value ^ 0x5a
	key[31] = value
	return key
}

func snapshotVerifyAccount(key byte, lamports uint64, data string) *accounts.Account {
	return &accounts.Account{
		Key:        snapshotVerifyKey(key),
		Lamports:   lamports,
		Data:       []byte(data),
		Owner:      snapshotVerifyKey(key + 100),
		Executable: key&1 != 0,
		RentEpoch:  uint64(key) + 500,
	}
}

func newSnapshotVerifyFixture(t *testing.T, files []snapshotVerifyFile) snapshotVerifyFixture {
	t.Helper()
	root := t.TempDir()
	accountsDir := filepath.Join(root, "accounts")
	require.NoError(t, os.Mkdir(accountsDir, 0o755))

	type selectedRecord struct {
		account *accounts.Account
		entry   AccountIndexEntry
	}
	selected := make(map[solana.PublicKey]selectedRecord)
	specs := make([]SnapshotAppendVecSpec, 0, len(files))
	for _, file := range files {
		var encoded bytes.Buffer
		for _, account := range file.accounts {
			require.NotNil(t, account)
			entry := AccountIndexEntry{
				Slot:   file.slot,
				FileId: file.fileID,
				Offset: uint64(encoded.Len()),
			}
			_, err := (&AppendVecAccount{
				DataLen:    uint64(len(account.Data)),
				Pubkey:     account.Key,
				Lamports:   account.Lamports,
				RentEpoch:  account.RentEpoch,
				Owner:      account.Owner,
				Executable: account.Executable,
				Data:       account.Data,
			}).MarshalReturningLength(&encoded)
			require.NoError(t, err)
			prior, exists := selected[account.Key]
			if !exists || snapshotVerifyEntryNewer(entry, prior.entry) {
				selected[account.Key] = selectedRecord{account: account, entry: entry}
			}
		}
		path := filepath.Join(accountsDir, SegmentDataName(file.slot, file.fileID))
		require.NoError(t, os.WriteFile(path, encoded.Bytes(), 0o644))
		specs = append(specs, SnapshotAppendVecSpec{
			Slot:     file.slot,
			FileID:   file.fileID,
			FileSize: uint64(encoded.Len()),
		})
	}

	sourceRecords := make([]productionIndexFixtureRecord, 0, len(selected))
	selectedAccounts := make(map[solana.PublicKey]*accounts.Account, len(selected))
	for key, record := range selected {
		sourceRecords = append(sourceRecords, productionIndexFixtureRecord{key: key, entry: record.entry})
		selectedAccounts[key] = record.account
	}
	sort.Slice(sourceRecords, func(i, j int) bool {
		return bytes.Compare(sourceRecords[i].key[:], sourceRecords[j].key[:]) < 0
	})

	guard, err := AcquireExclusiveProductionAccountIndexStore(root)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, guard.Close()) })
	config := productionIndexTestConfig()
	config.ShardCount = 1
	require.NoError(t, InitializeProductionAccountIndexWithStoreGuard(
		t.Context(),
		root,
		&productionIndexFixtureSource{records: sourceRecords},
		config,
		guard,
	))
	return snapshotVerifyFixture{
		root:     root,
		guard:    guard,
		config:   config,
		specs:    specs,
		selected: selectedAccounts,
	}
}

func snapshotVerifyEntryNewer(left, right AccountIndexEntry) bool {
	if left.Slot != right.Slot {
		return left.Slot > right.Slot
	}
	if left.FileId != right.FileId {
		return left.FileId > right.FileId
	}
	return left.Offset > right.Offset
}

func snapshotVerifyExpected(
	t *testing.T,
	selected map[solana.PublicKey]*accounts.Account,
) (*lthash.LtHash, uint64) {
	t.Helper()
	var result lthash.LtHash
	var hasher lthash.AccountHasher
	var contribution lthash.LtHash
	var capitalization uint64
	for _, account := range selected {
		require.LessOrEqual(t, account.Lamports, math.MaxUint64-capitalization)
		capitalization += account.Lamports
		hasher.HashInto(&contribution, account)
		result.MixIn(&contribution)
	}
	return &result, capitalization
}

func verifySnapshotFixture(t *testing.T, fixture snapshotVerifyFixture) error {
	t.Helper()
	hash, capitalization := snapshotVerifyExpected(t, fixture.selected)
	return VerifySnapshotAccountsStateWithStoreGuard(
		t.Context(), fixture.root, fixture.specs, hash, capitalization, fixture.guard,
	)
}

func TestVerifySnapshotAccountsStateSuccessDuplicateNewestWinsAndZeroLamports(t *testing.T) {
	old := snapshotVerifyAccount(1, 5, "old")
	zero := snapshotVerifyAccount(2, 0, "zero-lamport-data-is-not-hashed")
	newest := snapshotVerifyAccount(1, 7, "newest")
	other := snapshotVerifyAccount(3, 11, "other")
	fixture := newSnapshotVerifyFixture(t, []snapshotVerifyFile{
		{slot: 10, fileID: 20, accounts: []*accounts.Account{old, zero}},
		{slot: 11, fileID: 21, accounts: []*accounts.Account{newest, other}},
	})

	require.NoError(t, verifySnapshotFixture(t, fixture))
}

func TestVerifySnapshotAccountsStateRejectsHashCapitalizationAndNilHash(t *testing.T) {
	fixture := newSnapshotVerifyFixture(t, []snapshotVerifyFile{{
		slot: 10, fileID: 20, accounts: []*accounts.Account{snapshotVerifyAccount(1, 9, "value")},
	}})
	hash, capitalization := snapshotVerifyExpected(t, fixture.selected)

	wrongHash := new(lthash.LtHash).InitWithBytes([]byte("not the snapshot state"))
	err := VerifySnapshotAccountsStateWithStoreGuard(
		t.Context(), fixture.root, fixture.specs, wrongHash, capitalization, fixture.guard,
	)
	assert.ErrorIs(t, err, ErrSnapshotAccountsStateVerification)
	assert.ErrorContains(t, err, "AccountsLtHash mismatch")

	err = VerifySnapshotAccountsStateWithStoreGuard(
		t.Context(), fixture.root, fixture.specs, hash, capitalization+1, fixture.guard,
	)
	assert.ErrorIs(t, err, ErrSnapshotAccountsStateVerification)
	assert.ErrorContains(t, err, "capitalization mismatch")

	err = VerifySnapshotAccountsStateWithStoreGuard(
		t.Context(), fixture.root, fixture.specs, nil, capitalization, fixture.guard,
	)
	assert.ErrorIs(t, err, ErrSnapshotAccountsStateVerification)
	assert.ErrorContains(t, err, "no AccountsLtHash")
}

func TestVerifySnapshotAccountsStateRejectsCapitalizationOverflow(t *testing.T) {
	fixture := newSnapshotVerifyFixture(t, []snapshotVerifyFile{{
		slot:   10,
		fileID: 20,
		accounts: []*accounts.Account{
			snapshotVerifyAccount(1, math.MaxUint64, "maximum"),
			snapshotVerifyAccount(2, 1, "overflow"),
		},
	}})
	var irrelevant lthash.LtHash
	err := VerifySnapshotAccountsStateWithStoreGuard(
		t.Context(), fixture.root, fixture.specs, &irrelevant, 0, fixture.guard,
	)
	assert.ErrorIs(t, err, ErrSnapshotAccountsStateVerification)
	assert.ErrorContains(t, err, "capitalization overflows")
}

func TestVerifySnapshotAccountsStateRejectsMissingAndExtraSelectedKeys(t *testing.T) {
	t.Run("selected location is absent", func(t *testing.T) {
		// Build a fixture whose exact base includes one location that
		// no declared appendvec contains.
		root := t.TempDir()
		accountsDir := filepath.Join(root, "accounts")
		require.NoError(t, os.Mkdir(accountsDir, 0o755))
		account := snapshotVerifyAccount(4, 13, "only physical account")
		var encoded bytes.Buffer
		require.NoError(t, (&AppendVecAccount{
			DataLen: uint64(len(account.Data)), Pubkey: account.Key, Lamports: account.Lamports,
			Owner: account.Owner, Data: account.Data,
		}).Marshal(&encoded))
		require.NoError(t, os.WriteFile(filepath.Join(accountsDir, SegmentDataName(12, 30)), encoded.Bytes(), 0o644))
		ghost := snapshotVerifyKey(5)
		source := &productionIndexFixtureSource{records: []productionIndexFixtureRecord{
			{key: account.Key, entry: AccountIndexEntry{Slot: 12, FileId: 30, Offset: 0}},
			{key: ghost, entry: AccountIndexEntry{Slot: 99, FileId: 999, Offset: 0}},
		}}
		guard, err := AcquireExclusiveProductionAccountIndexStore(root)
		require.NoError(t, err)
		defer func() { require.NoError(t, guard.Close()) }()
		config := productionIndexTestConfig()
		config.ShardCount = 1
		require.NoError(t, InitializeProductionAccountIndexWithStoreGuard(t.Context(), root, source, config, guard))
		hash, cap := snapshotVerifyExpected(t, map[solana.PublicKey]*accounts.Account{account.Key: account})
		err = VerifySnapshotAccountsStateWithStoreGuard(
			t.Context(), root,
			[]SnapshotAppendVecSpec{{Slot: 12, FileID: 30, FileSize: uint64(encoded.Len())}},
			hash, cap, guard,
		)
		assert.ErrorContains(t, err, "selects 2 keys but 1 exact selected locations")
	})

	t.Run("physical key is absent from base", func(t *testing.T) {
		first := snapshotVerifyAccount(1, 9, "indexed")
		extra := snapshotVerifyAccount(2, 10, "not indexed")

		// Build the base without the extra key while retaining both physical
		// records in the appendvec.
		root := t.TempDir()
		accountsDir := filepath.Join(root, "accounts")
		require.NoError(t, os.Mkdir(accountsDir, 0o755))
		var encoded bytes.Buffer
		for _, account := range []*accounts.Account{first, extra} {
			require.NoError(t, (&AppendVecAccount{
				DataLen: uint64(len(account.Data)), Pubkey: account.Key, Lamports: account.Lamports,
				Owner: account.Owner, Data: account.Data,
			}).Marshal(&encoded))
		}
		require.NoError(t, os.WriteFile(filepath.Join(accountsDir, SegmentDataName(10, 20)), encoded.Bytes(), 0o644))
		guard, err := AcquireExclusiveProductionAccountIndexStore(root)
		require.NoError(t, err)
		defer func() { require.NoError(t, guard.Close()) }()
		config := productionIndexTestConfig()
		config.ShardCount = 1
		require.NoError(t, InitializeProductionAccountIndexWithStoreGuard(
			t.Context(), root,
			&productionIndexFixtureSource{records: []productionIndexFixtureRecord{{
				key: first.Key, entry: AccountIndexEntry{Slot: 10, FileId: 20, Offset: 0},
			}}},
			config, guard,
		))
		hash, cap := snapshotVerifyExpected(t, map[solana.PublicKey]*accounts.Account{first.Key: first})
		err = VerifySnapshotAccountsStateWithStoreGuard(
			t.Context(), root,
			[]SnapshotAppendVecSpec{{Slot: 10, FileID: 20, FileSize: uint64(encoded.Len())}},
			hash, cap, guard,
		)
		assert.ErrorContains(t, err, "is absent from the immutable base")
	})
}

func TestVerifySnapshotAccountsStateRejectsMalformedTruncatedAndSymlinkFiles(t *testing.T) {
	newFixture := func(t *testing.T) snapshotVerifyFixture {
		return newSnapshotVerifyFixture(t, []snapshotVerifyFile{{
			slot: 10, fileID: 20, accounts: []*accounts.Account{snapshotVerifyAccount(1, 9, "value")},
		}})
	}

	t.Run("malformed data length", func(t *testing.T) {
		fixture := newFixture(t)
		path := filepath.Join(fixture.root, "accounts", SegmentDataName(10, 20))
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		require.NoError(t, err)
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], maxAppendVecAccountDataLen+1)
		_, err = file.WriteAt(encoded[:], dataLenOffset)
		require.NoError(t, err)
		require.NoError(t, file.Close())
		err = verifySnapshotFixture(t, fixture)
		assert.ErrorContains(t, err, "exceeds maximum")
	})

	t.Run("invalid executable", func(t *testing.T) {
		fixture := newFixture(t)
		path := filepath.Join(fixture.root, "accounts", SegmentDataName(10, 20))
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		require.NoError(t, err)
		_, err = file.WriteAt([]byte{2}, 96)
		require.NoError(t, err)
		require.NoError(t, file.Close())
		err = verifySnapshotFixture(t, fixture)
		assert.ErrorContains(t, err, "invalid executable byte")
	})

	t.Run("zero terminator requires an all-zero tail", func(t *testing.T) {
		fixture := newFixture(t)
		path := filepath.Join(fixture.root, "accounts", SegmentDataName(10, 20))
		tail := make([]byte, hdrLen+17)
		tail[len(tail)-1] = 1
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
		require.NoError(t, err)
		_, err = file.Write(tail)
		require.NoError(t, err)
		require.NoError(t, file.Close())
		fixture.specs[0].FileSize += uint64(len(tail))

		err = verifySnapshotFixture(t, fixture)
		assert.ErrorContains(t, err, "non-zero byte after appendvec terminator")
	})

	t.Run("all-zero unused tail remains compatible", func(t *testing.T) {
		fixture := newFixture(t)
		path := filepath.Join(fixture.root, "accounts", SegmentDataName(10, 20))
		tail := make([]byte, hdrLen+17)
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
		require.NoError(t, err)
		_, err = file.Write(tail)
		require.NoError(t, err)
		require.NoError(t, file.Close())
		fixture.specs[0].FileSize += uint64(len(tail))

		require.NoError(t, verifySnapshotFixture(t, fixture))
	})

	t.Run("truncated size", func(t *testing.T) {
		fixture := newFixture(t)
		path := filepath.Join(fixture.root, "accounts", SegmentDataName(10, 20))
		require.NoError(t, os.Truncate(path, int64(fixture.specs[0].FileSize-1)))
		err := verifySnapshotFixture(t, fixture)
		assert.ErrorContains(t, err, "size mismatch")
	})

	t.Run("symlink", func(t *testing.T) {
		fixture := newFixture(t)
		path := filepath.Join(fixture.root, "accounts", SegmentDataName(10, 20))
		target := filepath.Join(fixture.root, "target")
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(target, data, 0o644))
		require.NoError(t, os.Remove(path))
		require.NoError(t, os.Symlink(target, path))
		err = verifySnapshotFixture(t, fixture)
		assert.Error(t, err)
	})

	t.Run("fifo", func(t *testing.T) {
		fixture := newFixture(t)
		path := filepath.Join(fixture.root, "accounts", SegmentDataName(10, 20))
		require.NoError(t, os.Remove(path))
		require.NoError(t, unix.Mkfifo(path, 0o644))
		err := verifySnapshotFixture(t, fixture)
		assert.ErrorContains(t, err, "not a regular file")
	})
}

func TestVerifySnapshotAccountsStateRejectsCanceledNonFreshAndDuplicateInputs(t *testing.T) {
	fixture := newSnapshotVerifyFixture(t, []snapshotVerifyFile{{
		slot: 10, fileID: 20, accounts: []*accounts.Account{snapshotVerifyAccount(1, 9, "value")},
	}})
	hash, capitalization := snapshotVerifyExpected(t, fixture.selected)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	err := VerifySnapshotAccountsStateWithStoreGuard(
		canceled, fixture.root, fixture.specs, hash, capitalization, fixture.guard,
	)
	assert.ErrorIs(t, err, context.Canceled)

	duplicate := append(append([]SnapshotAppendVecSpec(nil), fixture.specs...), fixture.specs[0])
	err = VerifySnapshotAccountsStateWithStoreGuard(
		t.Context(), fixture.root, duplicate, hash, capitalization, fixture.guard,
	)
	assert.ErrorContains(t, err, "duplicate appendvec specification")

	journal := filepath.Join(fixture.root, ShardedDeltaIndexJournalFileName)
	file, err := os.OpenFile(journal, os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	_, err = file.Write([]byte{1})
	require.NoError(t, err)
	require.NoError(t, file.Close())
	err = VerifySnapshotAccountsStateWithStoreGuard(
		t.Context(), fixture.root, fixture.specs, hash, capitalization, fixture.guard,
	)
	assert.ErrorContains(t, err, "journal is not fresh")
}

func TestVerifySnapshotAccountsStateRequiresMatchingLiveGuard(t *testing.T) {
	fixture := newSnapshotVerifyFixture(t, []snapshotVerifyFile{{
		slot: 10, fileID: 20, accounts: []*accounts.Account{snapshotVerifyAccount(1, 9, "value")},
	}})
	hash, capitalization := snapshotVerifyExpected(t, fixture.selected)

	err := VerifySnapshotAccountsStateWithStoreGuard(
		t.Context(), fixture.root, fixture.specs, hash, capitalization, nil,
	)
	assert.ErrorIs(t, err, ErrSnapshotAccountsStateVerification)

	otherRoot := t.TempDir()
	err = VerifySnapshotAccountsStateWithStoreGuard(
		t.Context(), otherRoot, fixture.specs, hash, capitalization, fixture.guard,
	)
	assert.ErrorContains(t, err, "guard is for")

	require.NoError(t, fixture.guard.Close())
	err = VerifySnapshotAccountsStateWithStoreGuard(
		t.Context(), fixture.root, fixture.specs, hash, capitalization, fixture.guard,
	)
	assert.ErrorContains(t, err, "closed or transferred")

	assert.True(t, errors.Is(err, ErrSnapshotAccountsStateVerification))
}
