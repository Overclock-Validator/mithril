package accountsdb

import (
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Read the candidate and then reconstruct independently using the stock cold
// scanner. Preserve the candidate's cache for the next fold or mutation.
func rollingCompareCold(t *testing.T, db *AccountsDb, head foldMeta, horizon uint64) (*compactionPins, RewindScanStats) {
	t.Helper()
	inv, err := db.scanCompactionInventory()
	require.NoError(t, err)
	var stats RewindScanStats
	got, headers, err := db.compactionPinSet(head, horizon, inv, &stats)
	require.NoError(t, err)
	cached := db.compactPinCache
	t.Run("cold", func(t *testing.T) {

		db.compactPinCache = nil
		defer func() { db.compactPinCache = cached }()
		var coldStats RewindScanStats
		want, wantHeaders, err := db.compactionPinSet(head, horizon, inv, &coldStats)
		require.NoError(t, err)
		require.Equal(t, "full", coldStats.Path)
		require.Equal(t, want.whole, got.whole, "whole-file pins at seq %d", head.BatchSeq)
		require.Equal(t, want.records, got.records, "record pin keys at seq %d", head.BatchSeq)
		require.Equal(t, wantHeaders, headers, "manifest headers at seq %d", head.BatchSeq)
	})
	return got, stats
}

// The first two additions grow history. Each following addition moves the
// boundary and its persisted predecessor exactly as a completed scan would.
func TestRollingRewindCapturedMainnet(t *testing.T) {
	fixture := os.Getenv("MITHRIL_PIN_MANIFEST_FIXTURE")
	if fixture == "" {
		t.Skip("requires six captured fold manifests")
	}

	files, err := filepath.Glob(filepath.Join(fixture, "*.manifest"))
	require.NoError(t, err)
	require.Len(t, files, 6)
	sort.Strings(files)
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	const horizon = 2
	metas := make([]foldMeta, 0, len(files))
	for i, path := range files {
		m, err := ReadSegmentManifest(path)
		require.NoError(t, err)
		head := foldMeta{BatchSeq: m.BatchSeq, ThroughSlot: m.ThroughSlot, FileId: m.FileId}
		if i > 0 {
			require.Equal(t, metas[i-1].BatchSeq+1, head.BatchSeq)
		}
		metas = append(metas, head)
		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(db.AcctsDir, filepath.Base(path)), raw, 0600))
		require.NoError(t, db.Index.Set(metaKeyLastBatch, encodeFoldMeta(head), pebble.Sync))
		if i == 0 {
			require.NoError(t, db.persistCompactionHistoryFloor(head))
		}
		_, stats := rollingCompareCold(t, db, head, horizon)
		expected := "full"
		if i == 1 || i == 2 {
			expected = "append"
		}
		if i > 2 {
			expected = "roll"
		}
		require.Equal(t, expected, stats.Path, "fixture %d seq %d", i, head.BatchSeq)
		if i >= horizon {
			require.NoError(t, db.persistCompactionHistoryFloor(metas[i-horizon]))
		}
		t.Logf("slot=%d seq=%d mode=%s manifests_decoded=%d parity=true", head.ThroughSlot, head.BatchSeq, stats.Path, stats.DecodedManifests)
	}
}

func rollingRef(key byte, file, offset uint64) ManifestRecord {
	return ManifestRecord{Pubkey: [32]byte{key}, PrevValid: true, Prev: AccountIndexEntry{FileId: file, Offset: offset}}
}

func rollingManifest(t *testing.T, db *AccountsDb, seq uint64, records ...ManifestRecord) foldMeta {
	t.Helper()
	head := foldMeta{BatchSeq: seq, ThroughSlot: 10 * seq, FileId: 100 + seq}
	manifest := &SegmentManifest{Version: segManifestVersion, Kind: ManifestKindFold, BatchSeq: seq,
		FromSlot: head.ThroughSlot - 10, ThroughSlot: head.ThroughSlot, FileId: head.FileId,
		Records: records, DataLen: 4096}
	for i := range manifest.Records {
		manifest.Records[i].Offset = uint64(128 * i)
		manifest.Records[i].OwnerSlot = head.ThroughSlot
	}
	require.NoError(t, WriteSegmentManifest(db.AcctsDir, manifest))
	require.NoError(t, db.Index.Set(metaKeyLastBatch, encodeFoldMeta(head), pebble.Sync))
	return head
}

func TestRollingRewindSharedProtections(t *testing.T) {

	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	shared := rollingRef(1, 4000, 16)
	oldOnly := rollingRef(2, 4000, 32)
	parkedOnly := rollingRef(3, 4001, 16)
	seq1 := rollingManifest(t, db, 1, parkedOnly)
	path := segmentManifestPath(db.AcctsDir, seq1.ThroughSlot, seq1.FileId)
	raw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path+".rewound", raw, 0600))
	seq2 := rollingManifest(t, db, 2)
	rollingManifest(t, db, 3, shared, oldOnly)
	seq4 := rollingManifest(t, db, 4, shared)
	require.NoError(t, db.persistCompactionHistoryFloor(seq2))
	pins, _ := rollingCompareCold(t, db, seq4, 2)
	require.Equal(t, shared.Pubkey, pins.records[4000][16])
	require.Equal(t, oldOnly.Pubkey, pins.records[4000][32])
	seq5 := rollingManifest(t, db, 5)
	pins, _ = rollingCompareCold(t, db, seq5, 2)
	require.NotContains(t, pins.whole, seq2.FileId)
	require.Contains(t, pins.whole, seq1.FileId)
	require.Contains(t, pins.whole, parkedOnly.Prev.FileId)
	require.Equal(t, parkedOnly.Pubkey, pins.records[4001][16])
	require.NotContains(t, pins.records[4000], uint64(32))
	require.Equal(t, shared.Pubkey, pins.records[4000][16])
	seq6 := rollingManifest(t, db, 6, rollingRef(8, 4000, 16))
	pins, _ = rollingCompareCold(t, db, seq6, 2)
	require.Equal(t, [32]byte{8}, pins.records[4000][16], "replacement at a location whose last prior owner expired")
}

func TestRollingRewindConflictAtomic(t *testing.T) {

	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	rollingManifest(t, db, 1)
	seq2 := rollingManifest(t, db, 2)
	rollingManifest(t, db, 3, rollingRef(4, 5000, 24))
	seq4 := rollingManifest(t, db, 4, rollingRef(1, 4000, 16))
	require.NoError(t, db.persistCompactionHistoryFloor(seq2))
	old, _ := rollingCompareCold(t, db, seq4, 2)
	seq5 := rollingManifest(t, db, 5, rollingRef(9, 6000, 8), rollingRef(2, 4000, 16))
	inv, err := db.scanCompactionInventory()
	require.NoError(t, err)
	_, _, err = db.compactionPinSet(seq5, 2, inv, &RewindScanStats{})
	require.ErrorContains(t, err, "conflicting undo pointers")
	require.Nil(t, db.compactPinCache)
	require.NotContains(t, old.whole, seq5.FileId)
	require.Contains(t, old.whole, seq2.FileId)
	require.NotContains(t, old.records, uint64(6000))
	require.Equal(t, [32]byte{4}, old.records[5000][24])
	require.Equal(t, [32]byte{1}, old.records[4000][16])

	_, _, err = db.compactionPinSet(seq5, 2, inv, &RewindScanStats{})
	require.ErrorContains(t, err, "conflicting undo pointers")
}

// Exercise the production 64-fold window through a real CompactOnce, persisted
// history frontier, a real rewind and a replacement fork. Check account state
// independently of the pin-map differential after the rewind.
func TestRollingRewindExpiryAndRewind(t *testing.T) {

	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	const horizon = 64
	cfg := CompactionConfig{RewindHorizonBatches: horizon}
	for seq := uint64(1); seq <= 71; seq++ {
		slot := 10 * seq
		commitTestBatch(t, db, slot, foldAcct(1, seq, []byte{byte(seq)}))
		stats, err := db.CompactOnce(cfg)
		require.NoError(t, err)
		if seq >= 67 {
			expected := "roll"
			require.Equal(t, expected, stats.RewindScan.Path, "after fold %d", seq)
		}
		head, _, err := db.readFoldMeta()
		require.NoError(t, err)
		rollingCompareCold(t, db, head, horizon)
	}
	_, err := db.RewindToBatchBoundary(680)
	require.NoError(t, err)
	require.EqualValues(t, 68, mustColdRead(t, db, 680, solana.PublicKey{1}).Lamports)
	head, _, err := db.readFoldMeta()
	require.NoError(t, err)
	_, state := rollingCompareCold(t, db, head, horizon)
	require.Equal(t, "full", state.Path)
	commitTestBatch(t, db, 690, foldAcct(1, 900, []byte("alternate")))
	head, _, err = db.readFoldMeta()
	require.NoError(t, err)
	rollingCompareCold(t, db, head, horizon)
	require.EqualValues(t, 900, mustColdRead(t, db, 690, solana.PublicKey{1}).Lamports)
}

// Rewriting a still-promised manifest with the same name, size and mtime
// changes which undo record is protected. A subsequent scan must see it.
func TestRollingRewindAlteredManifestRebuilds(t *testing.T) {

	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	rollingManifest(t, db, 1)
	seq2 := rollingManifest(t, db, 2)
	rollingManifest(t, db, 3)
	seq4 := rollingManifest(t, db, 4, rollingRef(2, 4000, 16))
	require.NoError(t, db.persistCompactionHistoryFloor(seq2))
	rollingCompareCold(t, db, seq4, 2)
	path := segmentManifestPath(db.AcctsDir, seq4.ThroughSlot, seq4.FileId)
	st, err := os.Stat(path)
	require.NoError(t, err)
	m, err := ReadSegmentManifest(path)
	require.NoError(t, err)
	m.Records[0].Prev.Offset = 48
	require.NoError(t, os.WriteFile(path, m.encode(), 0600))
	require.NoError(t, os.Chtimes(path, st.ModTime(), st.ModTime()))
	seq5 := rollingManifest(t, db, 5)
	pins, stats := rollingCompareCold(t, db, seq5, 2)
	require.Equal(t, "full", stats.Path)
	require.NotContains(t, pins.records[4000], uint64(16))
	require.Equal(t, [32]byte{2}, pins.records[4000][48])
}
