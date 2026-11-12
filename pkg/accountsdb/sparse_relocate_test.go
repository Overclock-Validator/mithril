//go:build linux

package accountsdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func sparseRelocationKey(n uint64) solana.PublicKey {
	var key solana.PublicKey
	binary.BigEndian.PutUint64(key[:8], n+1)
	return key
}

// The live eight-byte accounts each strand a separate filesystem page after
// their neighboring large accounts become obsolete. One old live account is
// still referenced by the rewind horizon, so whole-source relocation is unsafe.
func fragmentedSparseFixture(t *testing.T) (*AccountsDb, string, string, CompactionConfig, AccountIndexEntry) {
	t.Helper()
	db, dir := newFoldTestDb(t)
	var initial, updated []*accounts.Account
	for n := uint64(0); n < 2200; n++ {
		dead := foldAcct(1, 10, bytes.Repeat([]byte{0x55}, 4096))
		dead.Key = sparseRelocationKey(n * 2)
		live := foldAcct(1, 20, []byte("original"))
		live.Key = sparseRelocationKey(n*2 + 1)
		initial = append(initial, dead, live)
		changed := foldAcct(1, 11, nil)
		changed.Key = dead.Key
		updated = append(updated, changed)
	}
	r := commitTestBatch(t, db, 110, initial...)
	commitTestBatch(t, db, 120, updated...)
	commitTestBatch(t, db, 130, foldAcct(250, 10, nil))
	changed := foldAcct(1, 21, []byte("changed"))
	changed.Key = sparseRelocationKey(1)
	commitTestBatch(t, db, 140, changed)
	cfg := CompactionConfig{RewindHorizonBatches: 2, MaxFilesPerCycle: 1}
	_, err := db.CompactOnce(cfg)
	require.NoError(t, err)
	source := filepath.Join(db.AcctsDir, SegmentDataName(110, r.FileId))
	m, err := ReadSegmentManifest(source + segManifestSuffix)
	require.NoError(t, err)
	require.Equal(t, ManifestKindSparse, m.Kind)
	var undo AccountIndexEntry
	for _, rec := range m.Records {
		if rec.Pubkey == changed.Key {
			undo = AccountIndexEntry{Slot: 110, FileId: r.FileId, Offset: rec.Offset}
		}
	}
	require.NotZero(t, undo.FileId)
	commitTestBatch(t, db, 150, foldAcct(250, 11, nil))
	return db, dir, source, cfg, undo
}

func TestSparseRelocationPreservesLiveAndUndoAcrossRecovery(t *testing.T) {
	for _, walOff := range []bool{false, true} {
		for _, crash := range []string{"none", "output_manifest", "index", "source_manifest", "punch"} {
			t.Run(fmt.Sprintf("wal_off=%t/crash=%s", walOff, crash), func(t *testing.T) {
				previous := DisableIndexWAL
				DisableIndexWAL = walOff
				t.Cleanup(func() { DisableIndexWAL = previous })
				db, dir, source, cfg, undo := fragmentedSparseFixture(t)
				t.Cleanup(func() { db.CloseDb() })
				before, err := os.Stat(source)
				require.NoError(t, err)
				expected := make(map[solana.PublicKey]*accounts.Account)
				for n := uint64(1); n < 2200; n += 199 {
					key := sparseRelocationKey(n*2 + 1)
					expected[key] = mustColdRead(t, db, 150, key).Clone()
				}
				expected[sparseRelocationKey(1)] = mustColdRead(t, db, 150, sparseRelocationKey(1)).Clone()
				bankhash, err := db.GetBankHashForSlot(150)
				require.NoError(t, err)
				panicNow := func() { panic("interrupted sparse relocation") }
				switch crash {
				case "output_manifest":
					db.compactHooks.afterCompactManifest = panicNow
				case "index":
					db.compactHooks.afterCompactIndex = panicNow
				case "source_manifest":
					db.compactHooks.afterSparseMarker = panicNow
				case "punch":
					db.compactHooks.afterSparsePunch = panicNow
				}
				if crash == "none" {
					stats, err := db.CompactOnce(cfg)
					require.NoError(t, err)
					require.Equal(t, 1, stats.FilesCompacted)
					require.Greater(t, stats.LiveBytesMoved, int64(0))
					require.Less(t, stats.LiveBytesMoved, int64(1<<20))
					require.Greater(t, stats.BytesReclaimed, int64(5<<20))
				} else {
					require.Panics(t, func() { _, _ = db.CompactOnce(cfg) })
				}
				db = reopenFoldTestDb(t, db, dir)
				recovered, err := db.RecoverFoldState()
				require.NoError(t, err)
				require.Equal(t, uint64(150), recovered.DurableThrough)
				_, err = db.CompactOnce(cfg)
				require.NoError(t, err)
				after, err := os.Stat(source)
				require.NoError(t, err, "the retained undo source must survive relocation")
				require.Less(t, allocatedBytesFromInfo(after), allocatedBytesFromInfo(before)-(5<<20))
				for key, account := range expected {
					require.Equal(t, account, mustColdRead(t, db, 150, key))
				}
				afterHash, err := db.GetBankHashForSlot(150)
				require.NoError(t, err)
				require.Equal(t, bankhash, afterHash)
				marker, err := ReadSegmentManifest(source + segManifestSuffix)
				require.NoError(t, err)
				require.Len(t, marker.Records, 1)
				require.Equal(t, undo.Offset, marker.Records[0].Offset)
				_, err = db.RewindToBatchBoundary(130)
				require.NoError(t, err)
				require.Equal(t, []byte("original"), mustColdRead(t, db, 130, sparseRelocationKey(1)).Data)
				for n := uint64(1); n < 2200; n += 199 {
					require.Equal(t, []byte("original"), mustColdRead(t, db, 130, sparseRelocationKey(n*2+1)).Data)
				}
			})
		}
	}
}

func TestSparseRelocationKeepsCurrentRecordsAlsoReferencedByUndo(t *testing.T) {
	db, _, source, _, undo := fragmentedSparseFixture(t)
	defer db.CloseDb()
	m, err := ReadSegmentManifest(source + segManifestSuffix)
	require.NoError(t, err)
	shared := m.Records[1]
	keep := map[uint64][32]byte{undo.Offset: sparseRelocationKey(1), shared.Offset: shared.Pubkey}
	f, err := os.Open(source)
	require.NoError(t, err)
	before := make([]byte, hdrLen+8)
	_, err = f.ReadAt(before, int64(shared.Offset))
	require.NoError(t, err)
	require.NoError(t, f.Close())
	c := compactCandidate{name: filepath.Base(source), path: source, slot: 110, fileId: m.FileId, size: int64(m.DataLen), manifestPath: source + segManifestSuffix}
	db.foldMu.Lock()
	defer db.foldMu.Unlock()
	var cache sparseIndexCache
	moved, err := db.compactSparseLive(c, keep, &cache, defaultMinDeadFraction, 1<<20)
	require.NoError(t, err)
	require.Greater(t, moved.moved, int64(0))
	cache = sparseIndexCache{}
	_, _, _, err = db.sparsifyFile(c, keep, &cache)
	require.NoError(t, err)
	marker, err := ReadSegmentManifest(source + segManifestSuffix)
	require.NoError(t, err)
	require.Len(t, marker.Records, 2)
	f, err = os.Open(source)
	require.NoError(t, err)
	defer f.Close()
	after := make([]byte, len(before))
	_, err = f.ReadAt(after, int64(shared.Offset))
	require.NoError(t, err)
	require.Equal(t, before, after, "copying a current record must preserve its pinned source bytes")
	entry, found, err := cache.lookup(db, shared.Pubkey)
	require.NoError(t, err)
	require.True(t, found)
	require.NotEqual(t, m.FileId, entry.FileId)
}

func TestSparseRelocationWaitsForReadersBeforeMovingIndex(t *testing.T) {
	db, _, _, cfg, _ := fragmentedSparseFixture(t)
	defer db.CloseDb()
	before := snapshotIndex(t, db)
	ready, published := make(chan struct{}), make(chan struct{})
	db.compactHooks.afterCompactManifest = func() { close(ready) }
	db.compactHooks.afterCompactIndex = func() { close(published) }
	db.appendVecReadMu.RLock()
	locked := true
	defer func() {
		if locked {
			db.appendVecReadMu.RUnlock()
		}
	}()
	done := make(chan error, 1)
	go func() { _, err := db.CompactOnce(cfg); done <- err }()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("relocation did not reach publication")
	}
	select {
	case <-published:
		t.Fatal("index moved while a source reader held its snapshot")
	case <-time.After(20 * time.Millisecond):
	}
	require.Equal(t, before, snapshotIndex(t, db))
	db.appendVecReadMu.RUnlock()
	locked = false
	require.NoError(t, <-done)
	select {
	case <-published:
	default:
		t.Fatal("index was not published after the reader drained")
	}
}

func TestSparseRelocationRespectsMoveAndDiskBudgets(t *testing.T) {
	for _, budget := range []string{"move", "disk"} {
		t.Run(budget, func(t *testing.T) {
			db, _, _, cfg, _ := fragmentedSparseFixture(t)
			defer db.CloseDb()
			before := snapshotIndex(t, db)
			largest := db.LargestFileId.Load()
			if budget == "move" {
				cfg.MaxMoveBytesPerCycle = 1
			} else {
				db.MinFreeBytes = math.MaxUint64
			}
			stats, err := db.CompactOnce(cfg)
			require.NoError(t, err, "low relocation budget must leave in-place reclamation available")
			require.Zero(t, stats.LiveBytesMoved)
			require.Equal(t, before, snapshotIndex(t, db))
			require.Equal(t, largest, db.LargestFileId.Load(), "budget check precedes file ID allocation")
		})
	}
}
