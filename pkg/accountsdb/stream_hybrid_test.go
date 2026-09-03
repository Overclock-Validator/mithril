package accountsdb

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type streamHybridFixture struct {
	db              *AccountsDb
	dir             string
	baseAccounts    map[solana.PublicKey]*accounts.Account
	baseEntries     map[solana.PublicKey]AccountIndexEntry
	baseAppendVec   string
	baseAppendVecID uint64
}

// newStreamHybridFixture uses CommitBatch only to manufacture a realistic,
// encoded bootstrap appendvec and exact locations. It exports those locations
// as a sorted snapshot run, builds the immutable StreamHash base, then starts
// with an empty mutable journal, as the snapshot builder does.
func newStreamHybridFixture(t *testing.T) streamHybridFixture {
	t.Helper()

	db, dir := newFoldTestDb(t)
	base := []*accounts.Account{
		foldAcct(0x11, 111, []byte("base-a")),
		foldAcct(0x22, 222, []byte("base-b")),
		foldAcct(0x33, 333, []byte("base-c")),
	}
	bootstrap, err := db.CommitBatch(
		[]accounts.SlotDelta{{Slot: 100, Delta: base}},
		100,
		nil,
		nil,
	)
	require.NoError(t, err)

	baseEntries := make(map[solana.PublicKey]AccountIndexEntry, len(base))
	baseAccounts := make(map[solana.PublicKey]*accounts.Account, len(base))
	for _, acct := range base {
		entry, _, found, lookupErr := db.lookupExactAccountIndexEntry(acct.Key)
		require.NoError(t, lookupErr)
		require.True(t, found)
		baseEntries[acct.Key] = entry
		baseAccounts[acct.Key] = acct
	}
	baseAppendVec := filepath.Join(db.AcctsDir, SegmentDataName(100, bootstrap.FileId))
	db.CloseDb()

	// This appendvec now represents snapshot/bootstrap state, not retained fold
	// history. Keep recovery from replaying it into the fresh mutable journal.
	require.NoError(t, os.Remove(segmentManifestPath(filepath.Join(dir, "accounts"), 100, bootstrap.FileId)))
	require.NoError(t, os.Remove(filepath.Join(dir, DeltaIndexJournalFileName)))
	require.NoError(t, os.Remove(filepath.Join(dir, BootstrapHighFileIDFileName)))
	require.NoError(t, WriteBootstrapHighFileID(dir, bootstrap.FileId))

	runDir := filepath.Join(dir, "stream-index-runs")
	require.NoError(t, os.MkdirAll(runDir, 0o755))
	run := make([]byte, len(base)*StreamIndexRunRecordSize)
	for i, acct := range base {
		record := run[i*StreamIndexRunRecordSize : (i+1)*StreamIndexRunRecordSize]
		copy(record[:32], acct.Key[:])
		var encodedEntry [24]byte
		entry := baseEntries[acct.Key]
		entry.Marshal(&encodedEntry)
		copy(record[32:], encodedEntry[:])
	}
	require.NoError(t, os.WriteFile(filepath.Join(runDir, "000000.run"), run, 0o644))
	source, err := OpenStreamIndexRunSource(runDir)
	require.NoError(t, err)
	_, buildErr := BuildStreamAccountIndex(
		context.Background(),
		source,
		filepath.Join(dir, StreamIndexFileName),
		runtime.GOMAXPROCS(0),
	)
	require.NoError(t, buildErr)
	require.NoError(t, os.RemoveAll(runDir))
	require.NoError(t, InitializeMutableAccountIndex(dir))

	hybrid, err := OpenDb(dir)
	require.NoError(t, err)
	hybrid.RootedDurable = true
	hybrid.InitCaches()
	recovery, err := hybrid.RecoverFoldState()
	require.NoError(t, err)
	assert.Zero(t, recovery.BatchSeq)
	assert.Zero(t, recovery.DurableThrough)
	require.NotNil(t, hybrid.BaseIndex)

	return streamHybridFixture{
		db:              hybrid,
		dir:             dir,
		baseAccounts:    baseAccounts,
		baseEntries:     baseEntries,
		baseAppendVec:   baseAppendVec,
		baseAppendVecID: bootstrap.FileId,
	}
}

func reopenStreamHybridFixture(t *testing.T, db *AccountsDb, dir string) *AccountsDb {
	t.Helper()
	db.CloseDb()
	reopened, err := OpenDb(dir)
	require.NoError(t, err)
	reopened.RootedDurable = true
	reopened.InitCaches()
	_, err = reopened.RecoverFoldState()
	require.NoError(t, err)
	require.NotNil(t, reopened.BaseIndex)
	return reopened
}

func requireColdStreamAccount(t *testing.T, db *AccountsDb, want *accounts.Account, throughSlot uint64) {
	t.Helper()
	db.CommonAcctsCache.Delete(want.Key)
	db.VoteAcctCache.Delete(want.Key)
	got, err := db.GetAccount(throughSlot, want.Key)
	require.NoError(t, err)
	assert.Equal(t, want.Key, got.Key)
	assert.Equal(t, want.Lamports, got.Lamports)
	assert.Equal(t, want.Data, got.Data)
}

func requireDeltaMissing(t *testing.T, db *AccountsDb, key solana.PublicKey) {
	t.Helper()
	_, found, err := db.Index.LookupWithError(key)
	require.NoError(t, err)
	assert.False(t, found)
}

func requireDeltaEntry(t *testing.T, db *AccountsDb, key solana.PublicKey) AccountIndexEntry {
	t.Helper()
	value, found, err := db.Index.LookupWithError(key)
	require.NoError(t, err)
	require.True(t, found)
	require.False(t, value.Tombstone)
	return value.Entry
}

func manifestRecordFor(t *testing.T, manifest *SegmentManifest, key solana.PublicKey) ManifestRecord {
	t.Helper()
	for _, record := range manifest.Records {
		if record.Pubkey == [32]byte(key) {
			return record
		}
	}
	require.FailNow(t, "manifest does not contain key", "%s", key)
	return ManifestRecord{}
}

func findStreamBaseFalsePositive(t *testing.T, db *AccountsDb) solana.PublicKey {
	t.Helper()
	for i := uint64(1); i <= 1<<20; i++ {
		var key solana.PublicKey
		key[0] = 0xfa
		binary.LittleEndian.PutUint64(key[8:16], i*0x9e3779b97f4a7c15)
		binary.LittleEndian.PutUint64(key[24:32], i*0xd6e8feb86659fd93)
		_, candidate, err := db.BaseIndex.LookupCandidate(key)
		require.NoError(t, err)
		if !candidate {
			continue
		}
		_, _, exact, err := db.lookupExactAccountIndexEntry(key)
		require.NoError(t, err)
		if !exact {
			return key
		}
	}
	require.FailNow(t, "failed to find StreamHash fingerprint false positive")
	return solana.PublicKey{}
}

func TestStreamHybridBaseDeltaRewindAndReopen(t *testing.T) {
	fixture := newStreamHybridFixture(t)
	db := fixture.db
	defer func() { db.CloseDb() }()

	baseA := fixture.baseAccounts[solana.PublicKey{0x11}]
	baseB := fixture.baseAccounts[solana.PublicKey{0x22}]
	baseC := fixture.baseAccounts[solana.PublicKey{0x33}]
	missing := solana.PublicKey{0x99}
	samePrefixMissing := baseA.Key
	samePrefixMissing[31] = 0xfe
	falsePositiveMissing := findStreamBaseFalsePositive(t, db)

	// Scalar and batch cold reads resolve from the immutable base. The batch
	// must retain caller order, duplicate values, and the missing placeholder.
	requireDeltaMissing(t, db, baseA.Key)
	requireDeltaMissing(t, db, baseB.Key)
	requireColdStreamAccount(t, db, baseA, 100)
	_, err := db.GetAccount(100, samePrefixMissing)
	require.ErrorIs(t, err, ErrNoAccount,
		"a trailing-byte variant must be independently hashed and remain an exact miss")
	_, err = db.GetAccount(100, falsePositiveMissing)
	require.ErrorIs(t, err, ErrNoAccount,
		"a fingerprint false positive must fail the authoritative appendvec pubkey check")
	for _, acct := range []*accounts.Account{baseA, baseB, baseC} {
		db.CommonAcctsCache.Delete(acct.Key)
		db.VoteAcctCache.Delete(acct.Key)
	}
	request := []solana.PublicKey{
		baseC.Key, missing, samePrefixMissing, falsePositiveMissing, baseA.Key, baseB.Key, baseA.Key,
	}
	got, batchStats, err := db.GetAccountsBatchSharedWithStats(context.Background(), 100, request)
	require.NoError(t, err)
	require.Len(t, got, len(request))
	assert.Equal(t, uint64(333), got[0].Lamports)
	assert.Equal(t, missing, got[1].Key)
	assert.Zero(t, got[1].Lamports)
	assert.Equal(t, samePrefixMissing, got[2].Key)
	assert.Zero(t, got[2].Lamports)
	assert.Equal(t, falsePositiveMissing, got[3].Key)
	assert.Zero(t, got[3].Lamports)
	assert.Equal(t, uint64(111), got[4].Lamports)
	assert.Equal(t, uint64(222), got[5].Lamports)
	assert.Equal(t, got[4].Data, got[6].Data)
	assert.Equal(t, uint64(4), batchStats.BaseIndexHits)
	assert.Equal(t, uint64(len(request)), batchStats.DeltaIndexProbes)
	assert.Zero(t, batchStats.DeltaIndexHits)
	assert.Equal(t, uint64(len(request)), batchStats.BaseIndexProbes)
	assert.GreaterOrEqual(t, batchStats.BaseIndexCandidates, batchStats.BaseIndexHits)
	assert.GreaterOrEqual(t, batchStats.BaseIndexFalsePositives, uint64(1))

	// Establish a real post-snapshot rewind boundary, then overwrite one base
	// key and add one key that has no base version.
	boundary, err := db.CommitBatch(nil, 110, nil, []byte("boundary-110"))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), boundary.BatchSeq)
	updatedA := foldAcct(0x11, 1111, []byte("delta-a"))
	newD := foldAcct(0x44, 444, []byte("delta-d"))
	change, err := db.CommitBatch(
		[]accounts.SlotDelta{{Slot: 120, Delta: []*accounts.Account{updatedA, newD}}},
		120,
		nil,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), change.BatchSeq)

	manifest, err := ReadSegmentManifest(segmentManifestPath(db.AcctsDir, 120, change.FileId))
	require.NoError(t, err)
	basePrev := manifestRecordFor(t, manifest, baseA.Key)
	require.True(t, basePrev.PrevValid)
	assert.Equal(t, fixture.baseEntries[baseA.Key], basePrev.Prev,
		"undo must capture the exact immutable-base appendvec location")
	newPrev := manifestRecordFor(t, manifest, newD.Key)
	assert.False(t, newPrev.PrevValid)

	requireColdStreamAccount(t, db, updatedA, 120)
	requireColdStreamAccount(t, db, newD, 120)
	assert.Equal(t, change.FileId, requireDeltaEntry(t, db, updatedA.Key).FileId)

	rewind, err := db.RewindToBatchBoundary(110)
	require.NoError(t, err)
	assert.Equal(t, uint64(110), rewind.NewThrough)
	assert.Equal(t, 1, rewind.UndoneBatches)
	requireColdStreamAccount(t, db, baseA, 110)
	_, err = db.GetAccount(110, newD.Key)
	require.ErrorIs(t, err, ErrNoAccount)
	assert.Equal(t, fixture.baseEntries[baseA.Key], requireDeltaEntry(t, db, baseA.Key))

	tombstone, found, err := db.Index.LookupWithError(newD.Key)
	require.NoError(t, err)
	require.True(t, found)
	assert.True(t, tombstone.Tombstone)

	db = reopenStreamHybridFixture(t, db, fixture.dir)
	requireColdStreamAccount(t, db, baseA, 110)
	requireColdStreamAccount(t, db, baseB, 110)
	_, err = db.GetAccount(110, newD.Key)
	require.True(t, errors.Is(err, ErrNoAccount))
}

func TestStreamHybridBatchMixesDeltaBaseTombstonesAndMisses(t *testing.T) {
	fixture := newStreamHybridFixture(t)
	db := fixture.db
	defer db.CloseDb()

	baseB := fixture.baseAccounts[solana.PublicKey{0x22}]
	baseC := fixture.baseAccounts[solana.PublicKey{0x33}]
	updatedA := foldAcct(0x11, 1111, []byte("delta-a"))
	newD := foldAcct(0x44, 444, []byte("delta-d"))
	_, err := db.CommitBatch(
		[]accounts.SlotDelta{{Slot: 110, Delta: []*accounts.Account{updatedA, newD}}},
		110,
		nil,
		nil,
	)
	require.NoError(t, err)

	tombstoned := solana.PublicKey{0xdd, 1}
	require.NoError(t, db.Index.Apply(
		[]deltaIndexMutation{tombstoneDeltaMutation(tombstoned)}, nil, true,
	))
	for _, acct := range []*accounts.Account{baseB, baseC, updatedA, newD} {
		db.CommonAcctsCache.Delete(acct.Key)
		db.VoteAcctCache.Delete(acct.Key)
	}

	request := make([]solana.PublicKey, 0, 24)
	for i := 0; i < 4; i++ {
		missing := solana.PublicKey{0xee, byte(i)}
		request = append(request,
			baseB.Key,
			updatedA.Key,
			tombstoned,
			missing,
			baseC.Key,
			newD.Key,
		)
	}
	got, stats, err := db.GetAccountsBatchSharedWithStats(context.Background(), 110, request)
	require.NoError(t, err)
	require.Len(t, got, len(request))
	for i, acct := range got {
		switch i % 6 {
		case 0:
			assert.Equal(t, baseB.Lamports, acct.Lamports)
		case 1:
			assert.Equal(t, updatedA.Lamports, acct.Lamports)
		case 2, 3:
			assert.Zero(t, acct.Lamports)
		case 4:
			assert.Equal(t, baseC.Lamports, acct.Lamports)
		case 5:
			assert.Equal(t, newD.Lamports, acct.Lamports)
		}
	}
	assert.Equal(t, uint64(16), stats.IndexHits)
	assert.Equal(t, uint64(8), stats.IndexMisses)
	assert.Equal(t, uint64(24), stats.DeltaIndexProbes)
	assert.Equal(t, uint64(8), stats.DeltaIndexHits)
	assert.Equal(t, uint64(4), stats.DeltaIndexTombstones)
	assert.Equal(t, uint64(12), stats.BaseIndexProbes)
	assert.Equal(t, uint64(8), stats.BaseIndexHits)
}

func TestStreamHybridCompactsBaseAppendVecDurably(t *testing.T) {
	fixture := newStreamHybridFixture(t)
	db := fixture.db
	defer func() { db.CloseDb() }()

	baseB := fixture.baseAccounts[solana.PublicKey{0x22}]
	baseC := fixture.baseAccounts[solana.PublicKey{0x33}]

	// Advance the head far enough that the snapshot appendvec is outside a
	// one-batch rewind horizon. One dead base record then makes it compactable.
	_, err := db.CommitBatch(nil, 110, nil, nil)
	require.NoError(t, err)
	updatedA := foldAcct(0x11, 1111, []byte("delta-a"))
	newD := foldAcct(0x44, 444, []byte("delta-d"))
	_, err = db.CommitBatch(
		[]accounts.SlotDelta{{Slot: 120, Delta: []*accounts.Account{updatedA, newD}}},
		120,
		nil,
		nil,
	)
	require.NoError(t, err)
	_, err = db.CommitBatch(nil, 130, nil, nil)
	require.NoError(t, err)

	info, err := os.Stat(fixture.baseAppendVec)
	require.NoError(t, err)
	stats, err := db.CompactOnce(CompactionConfig{
		RewindHorizonBatches: 1,
		MinDeadFraction:      0.1,
		MaxMoveBytesPerCycle: 1 << 20,
		MaxScanBytesPerCycle: info.Size(),
	})
	require.NoError(t, err)
	assert.Equal(t, 1, stats.FilesCompacted)
	assert.NoFileExists(t, fixture.baseAppendVec)
	retired, err := db.isAppendVecRetired(100, fixture.baseAppendVecID)
	require.NoError(t, err)
	assert.True(t, retired)

	entryB := requireDeltaEntry(t, db, baseB.Key)
	entryC := requireDeltaEntry(t, db, baseC.Key)
	assert.NotEqual(t, fixture.baseAppendVecID, entryB.FileId)
	assert.Equal(t, entryB.FileId, entryC.FileId)
	requireColdStreamAccount(t, db, updatedA, 130)
	requireColdStreamAccount(t, db, baseB, 130)
	requireColdStreamAccount(t, db, baseC, 130)

	db = reopenStreamHybridFixture(t, db, fixture.dir)
	requireColdStreamAccount(t, db, updatedA, 130)
	requireColdStreamAccount(t, db, baseB, 130)
	requireColdStreamAccount(t, db, baseC, 130)
	falsePositiveMissing := findStreamBaseFalsePositive(t, db)
	_, err = db.GetAccount(130, falsePositiveMissing)
	require.ErrorIs(t, err, ErrNoAccount,
		"a candidate into a durably retired base path remains an exact miss")
}

func TestStreamHybridMissingActiveBaseAppendVecFailsClosed(t *testing.T) {
	fixture := newStreamHybridFixture(t)
	db := fixture.db
	defer db.CloseDb()
	baseB := fixture.baseAccounts[solana.PublicKey{0x22}]
	db.CommonAcctsCache.Delete(baseB.Key)
	db.VoteAcctCache.Delete(baseB.Key)
	require.NoError(t, os.Remove(fixture.baseAppendVec))

	_, err := db.GetAccount(100, baseB.Key)
	require.ErrorContains(t, err, "active base-index appendvec")
	_, _, err = db.GetAccountsBatchSharedWithStats(context.Background(), 100, []solana.PublicKey{baseB.Key})
	require.ErrorContains(t, err, "active base-index appendvec")
	_, _, found, err := db.lookupExactAccountIndexEntry(baseB.Key)
	require.ErrorContains(t, err, "active base-index appendvec")
	assert.False(t, found)
}

func TestStreamHybridFullyDeadCompactionRetiresBasePath(t *testing.T) {
	fixture := newStreamHybridFixture(t)
	db := fixture.db
	defer func() { db.CloseDb() }()

	updates := []*accounts.Account{
		foldAcct(0x11, 1111, []byte("delta-a")),
		foldAcct(0x22, 2222, []byte("delta-b")),
		foldAcct(0x33, 3333, []byte("delta-c")),
	}
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 110, Delta: updates}}, 110, nil, nil)
	require.NoError(t, err)
	_, err = db.CommitBatch(nil, 120, nil, nil)
	require.NoError(t, err)
	_, err = db.CommitBatch(nil, 130, nil, nil)
	require.NoError(t, err)
	info, err := os.Stat(fixture.baseAppendVec)
	require.NoError(t, err)

	stats, err := db.CompactOnce(CompactionConfig{
		RewindHorizonBatches: 1,
		MinDeadFraction:      0.1,
		MaxMoveBytesPerCycle: 1 << 20,
		MaxScanBytesPerCycle: info.Size(),
	})
	require.NoError(t, err)
	assert.Equal(t, 1, stats.FilesDeleted)
	assert.NoFileExists(t, fixture.baseAppendVec)
	retired, err := db.isAppendVecRetired(100, fixture.baseAppendVecID)
	require.NoError(t, err)
	assert.True(t, retired)

	// The retirement mutation is journaled before unlinking. A reopen must
	// therefore still distinguish retirement from data loss.
	db = reopenStreamHybridFixture(t, db, fixture.dir)
	retired, err = db.isAppendVecRetired(100, fixture.baseAppendVecID)
	require.NoError(t, err)
	assert.True(t, retired)
}

func TestOpenDbFailsClosedWhenStreamBaseAndManifestDisagree(t *testing.T) {
	db, dir := newFoldTestDb(t)
	db.CloseDb()

	basePath := filepath.Join(dir, StreamIndexFileName)
	require.NoError(t, os.WriteFile(basePath, []byte("unpublished"), 0o644))
	_, err := OpenDb(dir)
	require.ErrorContains(t, err, "exists without")
	require.NoError(t, os.Remove(basePath))

	require.NoError(t, publishStreamIndexManifest(dir, streamIndexFileIdentity{
		Size: 1,
	}))
	_, err = OpenDb(dir)
	require.ErrorContains(t, err, "exists without")
}

func TestOpenDbRejectsCorruptStreamBaseBeforeServingReads(t *testing.T) {
	fixture := newStreamHybridFixture(t)
	fixture.db.CloseDb()

	basePath := filepath.Join(fixture.dir, StreamIndexFileName)
	f, err := os.OpenFile(basePath, os.O_RDWR, 0)
	require.NoError(t, err)
	var original [1]byte
	_, err = f.ReadAt(original[:], 0)
	require.NoError(t, err)
	corrupt := [1]byte{original[0] ^ 0xff}
	_, err = f.WriteAt(corrupt[:], 0)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	// Both explicit validation and OpenDb verify the standalone manifest's
	// complete-file identity before any account reads can be served.
	require.ErrorContains(t, ValidateStreamIndexArtifacts(fixture.dir), "SHA-256 mismatch")
	_, err = OpenDb(fixture.dir)
	require.ErrorContains(t, err, "SHA-256 mismatch")
}
