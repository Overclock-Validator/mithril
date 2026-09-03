package accountsdb

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type productionAccountsDBSourceRecord struct {
	key   solana.PublicKey
	entry AccountIndexEntry
}

type productionAccountsDBSource []productionAccountsDBSourceRecord

func (source productionAccountsDBSource) Scan(
	ctx context.Context,
	visit func(solana.PublicKey, AccountIndexEntry) error,
) error {
	for i := range source {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(source[i].key, source[i].entry); err != nil {
			return err
		}
	}
	return ctx.Err()
}

type productionAccountsDBFixture struct {
	root           string
	config         ProductionAccountIndexConfig
	baseSlot       uint64
	baseFileID     uint64
	basePath       string
	baseAccounts   map[solana.PublicKey]*accounts.Account
	baseIndexEntry map[solana.PublicKey]AccountIndexEntry
}

func newProductionAccountsDBFixture(
	t *testing.T,
	base []*accounts.Account,
) productionAccountsDBFixture {
	return newProductionAccountsDBFixtureWithConfig(t, base, nil)
}

func newProductionAccountsDBFixtureWithConfig(
	t *testing.T,
	base []*accounts.Account,
	tune func(*ProductionAccountIndexConfig),
) productionAccountsDBFixture {
	t.Helper()
	root := t.TempDir()
	accountsDir := filepath.Join(root, "accounts")
	require.NoError(t, os.Mkdir(accountsDir, 0o755))
	const baseSlot, baseFileID = uint64(100), uint64(7)

	sorted := append([]*accounts.Account(nil), base...)
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i].Key[:], sorted[j].Key[:]) < 0
	})
	var encoded bytes.Buffer
	source := make(productionAccountsDBSource, 0, len(sorted))
	baseAccounts := make(map[solana.PublicKey]*accounts.Account, len(sorted))
	baseEntries := make(map[solana.PublicKey]AccountIndexEntry, len(sorted))
	for _, account := range sorted {
		require.NotNil(t, account)
		entry := AccountIndexEntry{
			Slot:   baseSlot,
			FileId: baseFileID,
			Offset: uint64(encoded.Len()),
		}
		appendVecAccount := AppendVecAccount{
			DataLen:    uint64(len(account.Data)),
			Pubkey:     account.Key,
			Lamports:   account.Lamports,
			RentEpoch:  account.RentEpoch,
			Owner:      account.Owner,
			Executable: account.Executable,
			Data:       account.Data,
		}
		_, err := appendVecAccount.MarshalReturningLength(&encoded)
		require.NoError(t, err)
		source = append(source, productionAccountsDBSourceRecord{key: account.Key, entry: entry})
		baseAccounts[account.Key] = account
		baseEntries[account.Key] = entry
	}
	basePath := filepath.Join(accountsDir, SegmentDataName(baseSlot, baseFileID))
	require.NoError(t, os.WriteFile(basePath, encoded.Bytes(), 0o644))

	require.NoError(t, WriteLargestFileID(root, baseFileID))
	require.NoError(t, WriteBootstrapHighFileID(root, baseFileID))

	config := DefaultProductionAccountIndexConfig()
	config.ShardCount = 4
	config.CheckpointWorkers = 1
	config.MaxConcurrentSeals = 1
	config.SealMaxAge = time.Hour
	if tune != nil {
		tune(&config)
	}
	require.NoError(t, config.Validate())
	require.NoError(t, InitializeProductionAccountIndex(t.Context(), root, source, config))
	require.NoError(t, ValidateProductionAccountIndexArtifacts(root))
	return productionAccountsDBFixture{
		root:           root,
		config:         config,
		baseSlot:       baseSlot,
		baseFileID:     baseFileID,
		basePath:       basePath,
		baseAccounts:   baseAccounts,
		baseIndexEntry: baseEntries,
	}
}

func TestAccountsDbProductionV2ZeroLamportFoldRebasesAndCompacts(t *testing.T) {
	base := foldAcct(0x31, 310, []byte("delete-me"))
	fixture := newProductionAccountsDBFixtureWithConfig(
		t,
		[]*accounts.Account{base},
		func(config *ProductionAccountIndexConfig) {
			config.ShardCount = 1
			config.SealKeys = 1
			config.RebaseKeys = 1
		},
	)
	db := openProductionAccountsDBFixture(t, fixture)
	defer func() { db.CloseDb() }()
	recoverProductionAccountsDBFixture(t, db)

	deleted := foldAcct(0x31, 0, nil)
	db.foldHooks.afterManifestRename = func() { panic("crash after zero-lamport manifest decision") }
	crashed := false
	func() {
		defer func() { crashed = recover() != nil }()
		_, _ = db.CommitBatch(
			[]accounts.SlotDelta{{Slot: 110, Delta: []*accounts.Account{deleted}}},
			110,
			nil,
			nil,
		)
	}()
	db.foldHooks = foldTestHooks{}
	require.True(t, crashed)
	headers, err := ListFoldManifests(db.AcctsDir)
	require.NoError(t, err)
	require.Len(t, headers, 1)
	deletionDataPath := filepath.Join(db.AcctsDir, SegmentDataName(headers[0].ThroughSlot, headers[0].FileId))
	deletionManifestPath := headers[0].Path
	manifest, err := ReadSegmentManifest(deletionManifestPath)
	require.NoError(t, err)
	require.Len(t, manifest.Records, 1)
	assert.True(t, manifest.Records[0].Tombstone)
	assert.True(t, manifest.Records[0].PrevValid, "rewind must retain the deleted account's prior location")

	db.CloseDb()
	db = openProductionAccountsDBFixture(t, fixture)
	recovery := recoverProductionAccountsDBFixture(t, db)
	assert.Equal(t, []uint64{1}, recovery.ReplayedBatches, "recovery must replay the decided deletion as a tombstone")

	_, _, found, err := db.lookupExactAccountIndexEntry(base.Key)
	require.NoError(t, err)
	assert.False(t, found, "a zero-lamport fold must publish an exact tombstone")
	evictProductionIntegrationKeys(db, base.Key)
	batch, stats, err := db.GetAccountsBatchSharedWithStats(t.Context(), 110, []solana.PublicKey{base.Key})
	require.NoError(t, err)
	require.Len(t, batch, 1)
	assert.Zero(t, batch[0].Lamports)
	assert.Equal(t, uint64(1), stats.DeltaIndexTombstones)

	sealCtx, cancelSeal := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelSeal()
	require.NoError(t, db.ProductionIndex.ForceSeal(sealCtx))
	assert.GreaterOrEqual(t, db.ProductionIndex.Stats().RebaseCount, uint64(1))
	catalog, err := ReadRootIndexCatalog(fixture.root)
	require.NoError(t, err)
	require.Len(t, catalog.Shards, 1)
	assert.Zero(t, catalog.Shards[0].DeltaGeneration)
	_, _, found, err = db.ProductionIndex.LookupCandidate(base.Key)
	require.NoError(t, err)
	assert.False(t, found, "the rebased immutable index must omit a tombstoned key")

	_, err = db.CommitBatch(nil, 120, nil, nil)
	require.NoError(t, err)
	// Horizon 1 preserves both the head transition and its target boundary.
	// Advance once more so the slot-110 tombstone lies strictly behind that
	// boundary and is eligible for physical reclamation.
	_, err = db.CommitBatch(nil, 130, nil, nil)
	require.NoError(t, err)
	compactStats, err := db.CompactOnceContext(t.Context(), CompactionConfig{
		RewindHorizonBatches: 1,
		MinDeadFraction:      0.5,
		MaxMoveBytesPerCycle: 1 << 20,
		MaxScanBytesPerCycle: 2 << 20,
	})
	require.NoError(t, err)
	assert.Positive(t, compactStats.FilesDeleted)
	assert.NoFileExists(t, deletionDataPath, "the tombstone's appendvec record must be dead to compaction")
	assert.NoFileExists(t, deletionManifestPath)

	db.CloseDb()
	require.NoError(t, ValidateProductionAccountIndexArtifacts(fixture.root))
	db = openProductionAccountsDBFixture(t, fixture)
	recovery = recoverProductionAccountsDBFixture(t, db)
	assert.Equal(t, uint64(3), recovery.BatchSeq)
	assert.Equal(t, uint64(130), recovery.DurableThrough)
	evictProductionIntegrationKeys(db, base.Key)
	_, err = db.GetAccount(130, base.Key)
	require.ErrorIs(t, err, ErrNoAccount)
}

func TestProductionV2RejectsClassicInPlaceStorePath(t *testing.T) {
	fixture := newProductionAccountsDBFixture(t, nil)
	db := openProductionAccountsDBFixture(t, fixture)
	defer db.CloseDb()
	db.RootedDurable = false
	callbackCalled := false

	err := db.StoreAccounts([]*accounts.Account{foldAcct(9, 1, []byte("unsafe"))}, 2, func() {
		callbackCalled = true
	})
	assert.ErrorIs(t, err, ErrProductionAccountIndexRequiresRootedDurable)
	assert.False(t, callbackCalled)
	assert.Zero(t, db.StoreQueueLen())
}

func openProductionAccountsDBFixture(t *testing.T, fixture productionAccountsDBFixture) *AccountsDb {
	t.Helper()
	db, err := OpenDbWithProductionAccountIndexConfig(fixture.root, fixture.config)
	require.NoError(t, err)
	require.NotNil(t, db.ProductionIndex)
	assert.Nil(t, db.Index)
	assert.Nil(t, db.BaseIndex)
	db.RootedDurable = true
	db.InitCaches()
	return db
}

func TestAccountsDbGuardedOpenTransfersStoreOwnership(t *testing.T) {
	fixture := newProductionAccountsDBFixture(t, []*accounts.Account{foldAcct(0x7a, 1, nil)})
	guard, err := AcquireExclusiveProductionAccountIndexStore(fixture.root)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, guard.Close()) })

	bankhashPath := filepath.Join(fixture.root, "bankhash_db")
	require.NoError(t, os.WriteFile(bankhashPath, []byte("not-a-directory"), 0o644))
	_, err = OpenDbWithProductionAccountIndexConfigAndStoreGuard(fixture.root, fixture.config, guard)
	require.Error(t, err)
	err = WithExclusiveProductionAccountIndexStore(fixture.root, func() error { return nil })
	require.ErrorIs(t, err, ErrProductionAccountIndexInUse, "failed open must retain the bootstrap guard")
	require.NoError(t, os.Remove(bankhashPath))

	db, err := OpenDbWithProductionAccountIndexConfigAndStoreGuard(fixture.root, fixture.config, guard)
	require.NoError(t, err)
	require.NotNil(t, db.ProductionIndex)
	require.NoError(t, guard.Close(), "guard must be empty after transfer")
	err = WithExclusiveProductionAccountIndexStore(fixture.root, func() error { return nil })
	require.ErrorIs(t, err, ErrProductionAccountIndexInUse)
	require.NoError(t, db.CloseDb())
	require.NoError(t, WithExclusiveProductionAccountIndexStore(fixture.root, func() error { return nil }))
}

func recoverProductionAccountsDBFixture(t *testing.T, db *AccountsDb) RecoveryResult {
	t.Helper()
	recovery, err := db.RecoverFoldState()
	require.NoError(t, err)
	return recovery
}

func evictProductionIntegrationKeys(db *AccountsDb, keys ...solana.PublicKey) {
	for _, key := range keys {
		db.CommonAcctsCache.Delete(key)
		db.VoteAcctCache.Delete(key)
	}
}

func assertProductionAccount(
	t *testing.T,
	want *accounts.Account,
	got *accounts.Account,
) {
	t.Helper()
	require.NotNil(t, got)
	assert.Equal(t, want.Key, got.Key)
	assert.Equal(t, want.Lamports, got.Lamports)
	assert.Equal(t, want.Owner, got.Owner)
	assert.Equal(t, want.Executable, got.Executable)
	assert.Equal(t, want.RentEpoch, got.RentEpoch)
	assert.Equal(t, want.Data, got.Data)
}

func TestAccountsDbProductionV2BatchCommitTombstoneAndReopen(t *testing.T) {
	baseA := foldAcct(0x11, 111, []byte("base-a"))
	baseB := foldAcct(0x52, 222, []byte("base-b-padding"))
	fixture := newProductionAccountsDBFixture(t, []*accounts.Account{baseB, baseA})
	db := openProductionAccountsDBFixture(t, fixture)
	defer func() { db.CloseDb() }()
	recovery := recoverProductionAccountsDBFixture(t, db)
	assert.Zero(t, recovery.BatchSeq)
	assert.Zero(t, recovery.DurableThrough)

	missing := solana.PublicKey{0xee}
	request := []solana.PublicKey{baseB.Key, missing, baseA.Key, baseB.Key}
	batch, stats, err := db.GetAccountsBatchSharedWithStats(t.Context(), fixture.baseSlot, request)
	require.NoError(t, err)
	require.Len(t, batch, len(request))
	assertProductionAccount(t, baseB, batch[0])
	assert.Equal(t, missing, batch[1].Key)
	assert.Zero(t, batch[1].Lamports)
	assertProductionAccount(t, baseA, batch[2])
	assertProductionAccount(t, baseB, batch[3])
	assert.Equal(t, uint64(4), stats.RequestedKeys)
	assert.Equal(t, uint64(3), stats.UniqueKeys)
	assert.Equal(t, uint64(1), stats.DuplicateKeys)
	assert.Equal(t, uint64(3), stats.UniqueDurableKeys)
	assert.Equal(t, uint64(4), stats.DeltaIndexProbes)
	assert.Equal(t, uint64(4), stats.BaseIndexProbes)
	assert.Equal(t, uint64(3), stats.BaseIndexHits)
	assert.Positive(t, stats.AppendVecRequestedBytes)
	assert.Positive(t, stats.AppendVecPhysicalReadBytes)

	boundary, err := db.CommitBatch(nil, 110, nil, []byte("boundary-110"))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), boundary.BatchSeq)
	updatedA := foldAcct(0x11, 1_111, []byte("updated-a"))
	newC := foldAcct(0x93, 333, []byte("new-c"))
	change, err := db.CommitBatch(
		[]accounts.SlotDelta{{Slot: 120, Delta: []*accounts.Account{updatedA, newC}}},
		120,
		nil,
		[]byte("boundary-120"),
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), change.BatchSeq)

	evictProductionIntegrationKeys(db, baseA.Key, baseB.Key, newC.Key)
	batch, stats, err = db.GetAccountsBatchSharedWithStats(
		t.Context(), 120, []solana.PublicKey{newC.Key, baseB.Key, updatedA.Key},
	)
	require.NoError(t, err)
	assertProductionAccount(t, newC, batch[0])
	assertProductionAccount(t, baseB, batch[1])
	assertProductionAccount(t, updatedA, batch[2])
	assert.Equal(t, uint64(2), stats.DeltaIndexHits)
	assert.Equal(t, uint64(1), stats.BaseIndexHits)

	rewind, err := db.RewindToBatchBoundary(110)
	require.NoError(t, err)
	assert.Equal(t, uint64(110), rewind.NewThrough)
	assert.Equal(t, 1, rewind.UndoneBatches)
	assert.Equal(t, []byte("boundary-110"), rewind.ResumeCtx)

	indexSnapshot, err := db.ProductionIndex.NewSnapshot([]solana.PublicKey{newC.Key})
	require.NoError(t, err)
	value, source, found, err := indexSnapshot.LookupAt(0)
	require.NoError(t, err)
	require.NoError(t, indexSnapshot.Close())
	require.True(t, found)
	assert.Equal(t, accountIndexSourceDelta, source)
	assert.True(t, value.Tombstone, "rewind of a post-snapshot creation must publish an exact tombstone")

	evictProductionIntegrationKeys(db, baseA.Key, baseB.Key, newC.Key)
	batch, stats, err = db.GetAccountsBatchSharedWithStats(
		t.Context(), 110, []solana.PublicKey{baseA.Key, newC.Key, baseB.Key},
	)
	require.NoError(t, err)
	assertProductionAccount(t, baseA, batch[0])
	assert.Equal(t, newC.Key, batch[1].Key)
	assert.Zero(t, batch[1].Lamports)
	assertProductionAccount(t, baseB, batch[2])
	assert.Equal(t, uint64(1), stats.DeltaIndexTombstones)

	db.CloseDb()
	require.NoError(t, ValidateProductionAccountIndexArtifacts(fixture.root))
	db = openProductionAccountsDBFixture(t, fixture)
	recovery = recoverProductionAccountsDBFixture(t, db)
	assert.Equal(t, uint64(1), recovery.BatchSeq)
	assert.Equal(t, uint64(110), recovery.DurableThrough)
	assert.Equal(t, []byte("boundary-110"), recovery.ResumeCtx)

	evictProductionIntegrationKeys(db, baseA.Key, baseB.Key, newC.Key)
	batch, _, err = db.GetAccountsBatchSharedWithStats(
		t.Context(), 110, []solana.PublicKey{baseB.Key, newC.Key, baseA.Key},
	)
	require.NoError(t, err)
	assertProductionAccount(t, baseB, batch[0])
	assert.Zero(t, batch[1].Lamports)
	assertProductionAccount(t, baseA, batch[2])
}

func TestAccountsDbProductionV2CompactsRetiredBootstrapAppendVecAndReopens(t *testing.T) {
	largeBase := foldAcct(0x21, 210, bytes.Repeat([]byte{0xab}, 128<<10))
	survivor := foldAcct(0x72, 720, []byte("survivor"))
	fixture := newProductionAccountsDBFixture(t, []*accounts.Account{largeBase, survivor})
	db := openProductionAccountsDBFixture(t, fixture)
	defer func() { db.CloseDb() }()
	recoverProductionAccountsDBFixture(t, db)

	updated := foldAcct(0x21, 2_100, []byte("small replacement"))
	_, err := db.CommitBatch(
		[]accounts.SlotDelta{{Slot: 110, Delta: []*accounts.Account{updated}}},
		110,
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = db.CommitBatch(nil, 120, nil, nil)
	require.NoError(t, err)

	stats, err := db.CompactOnceContext(t.Context(), CompactionConfig{
		RewindHorizonBatches: 1,
		MinDeadFraction:      0.50,
		MaxMoveBytesPerCycle: 1 << 20,
		MaxScanBytesPerCycle: 2 << 20,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, stats.FilesCompacted)
	assert.Positive(t, stats.LiveBytesMoved)
	assert.Greater(t, stats.BytesReclaimed, int64(100<<10))
	assert.NoFileExists(t, fixture.basePath)
	assert.True(t, db.ProductionIndex.IsRetired(fixture.baseSlot, fixture.baseFileID))

	relocated, _, found, err := db.lookupExactAccountIndexEntry(survivor.Key)
	require.NoError(t, err)
	require.True(t, found)
	assert.NotEqual(t, fixture.baseFileID, relocated.FileId)
	evictProductionIntegrationKeys(db, updated.Key, survivor.Key)
	batch, _, err := db.GetAccountsBatchSharedWithStats(
		t.Context(), 120, []solana.PublicKey{survivor.Key, updated.Key},
	)
	require.NoError(t, err)
	assertProductionAccount(t, survivor, batch[0])
	assertProductionAccount(t, updated, batch[1])

	db.CloseDb()
	require.NoError(t, ValidateProductionAccountIndexArtifacts(fixture.root))
	db = openProductionAccountsDBFixture(t, fixture)
	recovery := recoverProductionAccountsDBFixture(t, db)
	assert.Equal(t, uint64(2), recovery.BatchSeq)
	assert.Equal(t, uint64(120), recovery.DurableThrough)
	assert.True(t, db.ProductionIndex.IsRetired(fixture.baseSlot, fixture.baseFileID))
	evictProductionIntegrationKeys(db, updated.Key, survivor.Key)
	batch, _, err = db.GetAccountsBatchSharedWithStats(
		t.Context(), 120, []solana.PublicKey{updated.Key, survivor.Key},
	)
	require.NoError(t, err)
	assertProductionAccount(t, updated, batch[0])
	assertProductionAccount(t, survivor, batch[1])
}
