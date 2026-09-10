package accountsdb

import (
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCommitBatchDiskAdmissionIsBeforeEveryFoldSideEffect(t *testing.T) {
	db, root := newFoldTestDb(t)
	defer db.CloseDb()

	injected := errors.New("injected disk pressure")
	var admittedBytes atomic.Uint64
	require.NoError(t, db.SetFoldDiskAdmission(func(required uint64) (func(), error) {
		admittedBytes.Store(required)
		return nil, injected
	}))
	beforeHighWater := db.LargestFileId.Load()
	beforeSelector, err := os.ReadFile(filepath.Join(root, LargestFileIDFileName))
	require.NoError(t, err)

	_, err = db.CommitBatch(
		[]accounts.SlotDelta{{Slot: 1, Delta: []*accounts.Account{foldAcct(1, 1, []byte("never-written"))}}},
		1,
		map[uint64][32]byte{1: bh(1)},
		[]byte("never-written"),
	)
	require.ErrorIs(t, err, injected)
	assert.Greater(t, admittedBytes.Load(), foldDiskFixedHeadroomBytes)
	assert.Equal(t, beforeHighWater, db.LargestFileId.Load())
	afterSelector, readErr := os.ReadFile(filepath.Join(root, LargestFileIDFileName))
	require.NoError(t, readErr)
	assert.Equal(t, beforeSelector, afterSelector)
	entries, readErr := os.ReadDir(db.AcctsDir)
	require.NoError(t, readErr)
	assert.Empty(t, entries, "rejected admission must not allocate appendvecs or manifests")
	_, _, found, lookupErr := db.lookupExactAccountIndexEntry(foldAcct(1, 1, nil).Key)
	require.NoError(t, lookupErr)
	assert.False(t, found)
	_, bankHashErr := db.GetBankHashForSlot(1)
	assert.Error(t, bankHashErr)
}

func TestFoldDiskAdmissionReleaseCoversCompleteCommit(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	var held atomic.Bool
	var releases atomic.Uint64
	require.NoError(t, db.SetFoldDiskAdmission(func(uint64) (func(), error) {
		require.True(t, held.CompareAndSwap(false, true))
		return func() {
			require.True(t, held.CompareAndSwap(true, false))
			releases.Add(1)
		}, nil
	}))
	db.foldHooks.afterIndexCommit = func() { assert.True(t, held.Load()) }

	_, err := db.CommitBatch(
		[]accounts.SlotDelta{{Slot: 1, Delta: []*accounts.Account{foldAcct(1, 1, nil)}}},
		1,
		nil,
		[]byte("ctx"),
	)
	require.NoError(t, err)
	assert.False(t, held.Load())
	assert.Equal(t, uint64(1), releases.Load())
}

func TestCompactOnceNonBlockingNeverWaitsForFoldMutex(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	db.foldMu.Lock()
	started := time.Now()
	stats, err := db.CompactOnceContext(t.Context(), CompactionConfig{NonBlocking: true})
	db.foldMu.Unlock()
	require.ErrorIs(t, err, ErrCompactionBusy)
	assert.Zero(t, stats.CandidatesScanned)
	assert.Less(t, time.Since(started), 250*time.Millisecond)
}

func TestCompactionOutputReserveFailsBeforeAllocation(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	large := make([]byte, 2048)
	source := commitTestBatch(t, db, 110,
		foldAcct(1, 100, large),
		foldAcct(2, 200, []byte("live")),
	)
	commitTestBatch(t, db, 120, foldAcct(1, 101, large))
	commitTestBatch(t, db, 130, foldAcct(1, 102, large))
	beforeHighWater := db.LargestFileId.Load()
	db.diskSpaceProbe = func(string) (AppendVecFilesystemSpace, error) {
		return AppendVecFilesystemSpace{AvailableBytes: 128, TotalBytes: 1 << 30}, nil
	}

	_, err := db.CompactOnceContext(t.Context(), CompactionConfig{
		RewindHorizonBatches: 1,
		MinDeadFraction:      0.5,
		MinOutputFreeBytes:   128,
	})
	require.ErrorIs(t, err, ErrAppendVecDiskPressure)
	assert.Equal(t, beforeHighWater, db.LargestFileId.Load(), "preflight must precede file-ID allocation")
	assert.FileExists(t, filepath.Join(db.AcctsDir, SegmentDataName(110, source.FileId)))
}

func TestCompactionPreflightChargesTinyAccountRelocationWALBeforeSideEffects(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	dead := foldAcct(250, 1, make([]byte, 2<<20))
	dead.Key[1] = 250
	batch := make([]*accounts.Account, 0, 1025)
	batch = append(batch, dead)
	for i := 0; i < 1024; i++ {
		account := foldAcct(byte(i), uint64(i+2), nil)
		account.Key[1] = byte(i >> 8)
		account.Key[2] = 1
		batch = append(batch, account)
	}
	source := commitTestBatch(t, db, 110, batch...)
	newDead := foldAcct(250, 2, []byte("new"))
	newDead.Key = dead.Key
	commitTestBatch(t, db, 120, newDead)
	newerDead := foldAcct(250, 3, []byte("newer"))
	newerDead.Key = dead.Key
	commitTestBatch(t, db, 130, newerDead)

	sourcePath := filepath.Join(db.AcctsDir, SegmentDataName(110, source.FileId))
	file, err := os.Open(sourcePath)
	require.NoError(t, err)
	info, err := file.Stat()
	require.NoError(t, err)
	var liveBytes, liveRecords uint64
	require.NoError(t, scanAppendVecRecords(t.Context(), file, info.Size(), func(record appendVecScanRecord) error {
		if record.Pubkey != dead.Key {
			liveBytes += record.Span
			liveRecords++
		}
		return nil
	}))
	require.NoError(t, file.Close())
	require.Equal(t, uint64(1024), liveRecords)

	const configuredReserve = uint64(128)
	oldOutputOnlyRequirement := saturatingAddUint64(liveBytes, configuredReserve)
	db.diskSpaceProbe = func(string) (AppendVecFilesystemSpace, error) {
		return AppendVecFilesystemSpace{
			AvailableBytes: oldOutputOnlyRequirement,
			TotalBytes:     1 << 40,
		}, nil
	}
	beforeHighWater := db.LargestFileId.Load()
	beforeEntries, err := os.ReadDir(db.AcctsDir)
	require.NoError(t, err)
	beforeNames := make([]string, len(beforeEntries))
	for i := range beforeEntries {
		beforeNames[i] = beforeEntries[i].Name()
	}

	_, err = db.CompactOnceContext(t.Context(), CompactionConfig{
		RewindHorizonBatches: 1,
		MinDeadFraction:      0.5,
		MinOutputFreeBytes:   configuredReserve,
	})
	require.ErrorIs(t, err, ErrAppendVecDiskPressure)
	var pressure *AppendVecDiskPressureError
	require.ErrorAs(t, err, &pressure)
	wantRequired := saturatingAddUint64(liveBytes, compactionRelocationWALBytes(liveRecords))
	wantRequired = saturatingAddUint64(wantRequired, db.compactionRewriteAndMetadataHeadroomBytes())
	wantRequired = saturatingAddUint64(wantRequired, configuredReserve)
	assert.Equal(t, wantRequired, pressure.RequiredBytes)
	assert.Greater(t, pressure.RequiredBytes, oldOutputOnlyRequirement,
		"output bytes alone fit; relocation WAL and metadata must force refusal")
	assert.Equal(t, beforeHighWater, db.LargestFileId.Load(), "preflight must precede file-ID allocation")
	afterEntries, readErr := os.ReadDir(db.AcctsDir)
	require.NoError(t, readErr)
	afterNames := make([]string, len(afterEntries))
	for i := range afterEntries {
		afterNames[i] = afterEntries[i].Name()
	}
	assert.Equal(t, beforeNames, afterNames, "preflight refusal must create no output or manifest")
	assert.FileExists(t, sourcePath)
}

func TestCompactionRelocationWALByteEstimateIncludesEveryFrameHeaderAndSaturates(t *testing.T) {
	const records = uint64(compactRelocationChunkKeys + 1)
	want := records*deltaMutationSize + 2*deltaFrameHeaderSize +
		compactionRetirementWALMutations*deltaMutationSize + deltaFrameHeaderSize
	assert.Equal(t, want, compactionRelocationWALBytes(records))
	assert.Equal(t, uint64(^uint64(0)), compactionRelocationWALBytes(^uint64(0)))
}

func TestCompactionPreflightHeadroomIncludesMaximumProductionJournalRewrite(t *testing.T) {
	db := &AccountsDb{ProductionIndex: &ProductionAccountIndex{
		mutable: &ShardedMutableAccountIndex{config: ShardedMutableIndexConfig{
			MaxHotBytes:     480,
			BytesPerKey:     128,
			BytesPerRetired: 48,
		}},
	}}
	// The 480-byte accounting envelope can contain at most ten 48-byte
	// retirement entries. Their exact-state rewrite is one 64-byte header plus
	// ten 64-byte mutation records, after the 32-byte journal header.
	want := compactionFixedMetadataHeadroomBytes + deltaJournalHeaderSize +
		deltaFrameHeaderSize + 10*deltaMutationSize
	assert.Equal(t, uint64(want), db.compactionRewriteAndMetadataHeadroomBytes())

	db.ProductionIndex.mutable.config.MaxHotBytes = ^uint64(0)
	db.ProductionIndex.mutable.config.BytesPerKey = 1
	db.ProductionIndex.mutable.config.BytesPerRetired = 1
	assert.Equal(t, uint64(^uint64(0)), db.compactionRewriteAndMetadataHeadroomBytes())
}

func TestEmergencyCompactionDefersScratchStarvedCandidateAndDeletesLaterDeadFile(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	large := make([]byte, 2048)
	scratchStarved := commitTestBatch(t, db, 110,
		foldAcct(1, 100, large),
		foldAcct(2, 200, []byte("still-live")),
	)
	fullyDead := commitTestBatch(t, db, 120, foldAcct(3, 300, []byte("old")))
	commitTestBatch(t, db, 130,
		foldAcct(1, 101, large),
		foldAcct(3, 301, []byte("newer")),
	)
	commitTestBatch(t, db, 140,
		foldAcct(1, 102, large),
		foldAcct(3, 302, []byte("newest")),
	)
	beforeHighWater := db.LargestFileId.Load()
	db.diskSpaceProbe = func(string) (AppendVecFilesystemSpace, error) {
		return AppendVecFilesystemSpace{AvailableBytes: 128, TotalBytes: 1 << 30}, nil
	}
	cfg := CompactionConfig{
		RewindHorizonBatches:          1,
		MinDeadFraction:               0.5,
		MinOutputFreeBytes:            128,
		ContinueOnOutputSpacePressure: true,
	}

	stats, err := db.CompactOnceContext(t.Context(), cfg)
	require.NoError(t, err, "a later zero-scratch deletion made reclamation progress")
	assert.Equal(t, 1, stats.OutputSpaceDeferrals)
	assert.Equal(t, 1, stats.FilesDeleted)
	assert.Greater(t, stats.BytesReclaimed, int64(0))
	assert.True(t, stats.PassComplete)
	assert.Equal(t, beforeHighWater, db.LargestFileId.Load(), "deferral and deletion must not allocate an output ID")
	assert.FileExists(t, filepath.Join(db.AcctsDir, SegmentDataName(110, scratchStarved.FileId)))
	assert.NoFileExists(t, filepath.Join(db.AcctsDir, SegmentDataName(120, fullyDead.FileId)))

	stats, err = db.CompactOnceContext(t.Context(), cfg)
	require.ErrorIs(t, err, ErrAppendVecDiskPressure,
		"a stable pass with only scratch-starved work must preserve the pressure error")
	assert.Equal(t, 1, stats.OutputSpaceDeferrals)
	assert.Zero(t, stats.BytesReclaimed)
	assert.True(t, stats.PassComplete)
}

func TestRoutineCompactionHardSourceCapDefersBeforeScanAndContinues(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	large := make([]byte, 2048)
	oversized := commitTestBatch(t, db, 110,
		foldAcct(1, 100, large),
		foldAcct(2, 200, []byte("still-live")),
	)
	fullyDead := commitTestBatch(t, db, 120, foldAcct(3, 300, []byte("old")))
	commitTestBatch(t, db, 130,
		foldAcct(1, 101, large),
		foldAcct(3, 301, []byte("newer")),
	)
	commitTestBatch(t, db, 140,
		foldAcct(1, 102, large),
		foldAcct(3, 302, []byte("newest")),
	)
	beforeHighWater := db.LargestFileId.Load()
	var scannedSources atomic.Uint64
	db.foldHooks.afterCompactionSourceScan = func() { scannedSources.Add(1) }

	stats, err := db.CompactOnceContext(t.Context(), CompactionConfig{
		RewindHorizonBatches: 1,
		MinDeadFraction:      0.5,
		MaxSourceBytes:       1024,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, stats.SourceSizeDeferrals)
	assert.Greater(t, stats.MaxDeferredSourceBytes, int64(1024))
	assert.Equal(t, 1, stats.CandidatesScanned)
	assert.Equal(t, uint64(1), scannedSources.Load(), "oversized source must be rejected before liveness scanning")
	assert.Equal(t, 1, stats.FilesDeleted, "later bounded candidate must still be considered")
	assert.True(t, stats.PassComplete)
	assert.Equal(t, beforeHighWater, db.LargestFileId.Load())
	assert.FileExists(t, filepath.Join(db.AcctsDir, SegmentDataName(110, oversized.FileId)))
	assert.NoFileExists(t, filepath.Join(db.AcctsDir, SegmentDataName(120, fullyDead.FileId)))
}

func TestEstimateFoldDiskBytesIsConservativeAndOverflowSafe(t *testing.T) {
	required, err := estimateFoldDiskBytes([]accounts.SlotDelta{{
		Slot: 1,
		Delta: []*accounts.Account{
			foldAcct(1, 1, []byte{1}),
			foldAcct(1, 2, make([]byte, 9)), // duplicate is still charged
			nil,
		},
	}})
	require.NoError(t, err)
	assert.Equal(t, foldDiskFixedHeadroomBytes+2*foldDiskBytesPerInputAccount+8+16, required)
}
