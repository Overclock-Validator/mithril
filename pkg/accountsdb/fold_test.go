package accountsdb

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/cockroachdb/pebble"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newFoldTestDb scaffolds an on-disk AccountsDb with both sidecars present
// (largest_file_id + bootstrap_high_file_id), matching a snapshot-built store.
func newFoldTestDb(t *testing.T) (*AccountsDb, string) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "accounts"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bootstrap_high_file_id"), make([]byte, 8), 0o644))
	db, err := OpenDb(dir)
	require.NoError(t, err)
	db.RootedDurable = true // production storage mode (node.go forces it on)
	db.InitCaches()
	return db, dir
}

func reopenFoldTestDb(t *testing.T, db *AccountsDb, dir string) *AccountsDb {
	t.Helper()
	db.CloseDb()
	re, err := OpenDb(dir)
	require.NoError(t, err)
	re.RootedDurable = true
	re.InitCaches()
	return re
}

func foldAcct(b byte, lamports uint64, data []byte) *accounts.Account {
	return &accounts.Account{
		Key:       solana.PublicKey{b},
		Lamports:  lamports,
		Data:      data,
		Owner:     [32]byte{b, b},
		RentEpoch: uint64(b),
	}
}

func foldDeltas(slots ...accounts.SlotDelta) []accounts.SlotDelta { return slots }

func bh(slot uint64) [32]byte {
	var h [32]byte
	binary.LittleEndian.PutUint64(h[:], slot)
	return h
}

func mustColdRead(t *testing.T, db *AccountsDb, throughSlot uint64, key solana.PublicKey) *accounts.Account {
	t.Helper()
	db.CommonAcctsCache.Delete(key)
	db.VoteAcctCache.Delete(key)
	got, err := db.GetAccount(throughSlot, key)
	require.NoError(t, err)
	return got
}

// Happy path: a folded batch's newest versions read back from disk, bankhashes
// land, and the fold meta advances.
func TestCommitBatchReadsBackAndDedupes(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	older := foldAcct(1, 100, []byte("old"))
	newer := foldAcct(1, 999, []byte("new-version"))
	other := foldAcct(2, 50, []byte("12345")) // dataLen 5 -> exercises padding

	res, err := db.CommitBatch(foldDeltas(
		accounts.SlotDelta{Slot: 101, Delta: []*accounts.Account{older, other}},
		accounts.SlotDelta{Slot: 103, Delta: []*accounts.Account{nil, newer}}, // nil entries skipped
	), 104, map[uint64][32]byte{101: bh(101), 103: bh(103), 104: bh(104)}, []byte("ctx-104"))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), res.BatchSeq)
	assert.Equal(t, uint64(104), res.ThroughSlot)
	assert.Equal(t, 2, res.Keys, "union across slots must dedupe key 1")

	got := mustColdRead(t, db, 104, solana.PublicKey{1})
	assert.Equal(t, uint64(999), got.Lamports, "newest version must win the dedupe")
	assert.Equal(t, []byte("new-version"), got.Data)

	got2 := mustColdRead(t, db, 104, solana.PublicKey{2})
	assert.Equal(t, uint64(50), got2.Lamports)

	hb, err := db.GetBankHashForSlot(103)
	require.NoError(t, err)
	want := bh(103)
	assert.Equal(t, want[:], hb)

	meta, ok, err := db.readFoldMeta()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, foldMeta{BatchSeq: 1, ThroughSlot: 104, FileId: res.FileId}, meta)

	// Undo pointers for a second batch must name the first batch's entries.
	newest := foldAcct(1, 1234, []byte("v3"))
	res2, err := db.CommitBatch(foldDeltas(
		accounts.SlotDelta{Slot: 106, Delta: []*accounts.Account{newest}},
	), 108, map[uint64][32]byte{108: bh(108)}, []byte("ctx-108"))
	require.NoError(t, err)

	manifest, err := ReadSegmentManifest(segmentManifestPath(db.AcctsDir, 108, res2.FileId))
	require.NoError(t, err)
	require.Len(t, manifest.Records, 1)
	require.True(t, manifest.Records[0].PrevValid, "key existed before -> undo pointer set")
	assert.Equal(t, res.FileId, manifest.Records[0].Prev.FileId, "undo pointer names the prior segment")
	assert.Equal(t, []byte("ctx-108"), manifest.ResumeCtx)
}

// Manifest encode/decode round-trip, and CRC detection of a torn manifest.
func TestSegmentManifestRoundTripAndTornDetection(t *testing.T) {
	dir := t.TempDir()
	m := &SegmentManifest{
		Version:     segManifestVersion,
		Kind:        ManifestKindFold,
		BatchSeq:    7,
		FromSlot:    99,
		ThroughSlot: 130,
		FileId:      42,
		DataLen:     1024,
		DataCRC:     0xDEADBEEF,
		Bankhashes:  []SlotBankhash{{Slot: 100, Bankhash: bh(100)}, {Slot: 130, Bankhash: bh(130)}},
		Records: []ManifestRecord{
			{Pubkey: [32]byte{1}, Offset: 0, OwnerSlot: 100, PrevValid: true, Prev: AccountIndexEntry{Slot: 90, FileId: 3, Offset: 77}},
			{Pubkey: [32]byte{2}, Offset: 136, OwnerSlot: 130},
		},
		ResumeCtx: []byte(`{"slot":130}`),
	}
	require.NoError(t, WriteSegmentManifest(dir, m))

	got, err := ReadSegmentManifest(segmentManifestPath(dir, 130, 42))
	require.NoError(t, err)
	assert.Equal(t, m, got)

	// Flip one byte -> torn.
	path := segmentManifestPath(dir, 130, 42)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	data[len(data)/2] ^= 0xFF
	require.NoError(t, os.WriteFile(path, data, 0o644))
	_, err = ReadSegmentManifest(path)
	assert.ErrorIs(t, err, ErrTornManifest)
}

// Crash-point injection across the commit protocol. Each stage panics once,
// the store reopens, RecoverFoldState runs, and the outcome must match the
// crash matrix: before the manifest rename the batch never happened (orphan
// GC'd); at/after the rename the batch is decided and recovery completes it.
func TestCommitBatchCrashMatrix(t *testing.T) {
	cases := []struct {
		name           string
		arm            func(h *foldTestHooks, boom func())
		batch2Survives bool
		replayed       bool // recovery had to replay the manifest into the index
	}{
		{"after_segment_fsync", func(h *foldTestHooks, boom func()) { h.afterSegmentFsync = boom }, false, false},
		{"after_manifest_rename", func(h *foldTestHooks, boom func()) { h.afterManifestRename = boom }, true, true},
		{"before_index_commit", func(h *foldTestHooks, boom func()) { h.beforeIndexCommit = boom }, true, true},
		{"after_index_commit", func(h *foldTestHooks, boom func()) { h.afterIndexCommit = boom }, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, dir := newFoldTestDb(t)

			base := foldAcct(1, 111, []byte("base"))
			_, err := db.CommitBatch(foldDeltas(
				accounts.SlotDelta{Slot: 101, Delta: []*accounts.Account{base}},
			), 105, map[uint64][32]byte{105: bh(105)}, []byte("ctx-105"))
			require.NoError(t, err)

			tc.arm(&db.foldHooks, func() { panic("injected crash: " + tc.name) })
			v2 := foldAcct(1, 222, []byte("second"))
			func() {
				defer func() { require.NotNil(t, recover(), "hook must panic") }()
				_, _ = db.CommitBatch(foldDeltas(
					accounts.SlotDelta{Slot: 106, Delta: []*accounts.Account{v2}},
				), 110, map[uint64][32]byte{110: bh(110)}, []byte("ctx-110"))
			}()

			db = reopenFoldTestDb(t, db, dir)
			defer db.CloseDb()
			res, err := db.RecoverFoldState()
			require.NoError(t, err)

			if tc.batch2Survives {
				assert.Equal(t, uint64(110), res.DurableThrough, "manifest was durable -> commit decided")
				assert.Equal(t, uint64(2), res.BatchSeq)
				assert.Equal(t, []byte("ctx-110"), res.ResumeCtx)
				got := mustColdRead(t, db, 110, solana.PublicKey{1})
				assert.Equal(t, uint64(222), got.Lamports)
				if tc.replayed {
					assert.Equal(t, []uint64{2}, res.ReplayedBatches)
				} else {
					assert.Empty(t, res.ReplayedBatches)
				}
			} else {
				assert.Equal(t, uint64(105), res.DurableThrough, "undecided batch must vanish")
				assert.Equal(t, uint64(1), res.BatchSeq)
				assert.Equal(t, []byte("ctx-105"), res.ResumeCtx)
				got := mustColdRead(t, db, 105, solana.PublicKey{1})
				assert.Equal(t, uint64(111), got.Lamports)
				assert.NotEmpty(t, res.OrphansRemoved, "orphan segment must be GC'd")
			}

			// A fresh fold after recovery continues the sequence cleanly.
			v3 := foldAcct(1, 333, []byte("third"))
			res3, err := db.CommitBatch(foldDeltas(
				accounts.SlotDelta{Slot: 111, Delta: []*accounts.Account{v3}},
			), 112, map[uint64][32]byte{112: bh(112)}, nil)
			require.NoError(t, err)
			assert.Equal(t, res.BatchSeq+1, res3.BatchSeq, "BatchSeq must stay contiguous after recovery")
		})
	}
}

// Recovery is idempotent: a second run finds nothing to replay and reports the
// same watermark.
func TestRecoverFoldStateIdempotent(t *testing.T) {
	db, dir := newFoldTestDb(t)
	_, err := db.CommitBatch(foldDeltas(
		accounts.SlotDelta{Slot: 101, Delta: []*accounts.Account{foldAcct(1, 1, nil)}},
	), 105, map[uint64][32]byte{105: bh(105)}, []byte("ctx"))
	require.NoError(t, err)

	db = reopenFoldTestDb(t, db, dir)
	defer db.CloseDb()
	first, err := db.RecoverFoldState()
	require.NoError(t, err)
	second, err := db.RecoverFoldState()
	require.NoError(t, err)
	assert.Equal(t, first.DurableThrough, second.DurableThrough)
	assert.Equal(t, first.BatchSeq, second.BatchSeq)
	assert.Empty(t, second.ReplayedBatches)
	assert.Empty(t, second.OrphansRemoved)
}

// A gap in the BatchSeq run bounds recovery: contiguous manifests replay, and
// everything above the gap is undecided -> deleted.
func TestRecoverFoldStateStopsAtGapAndRemovesAbove(t *testing.T) {
	db, dir := newFoldTestDb(t)

	for i, through := range []uint64{105, 110, 115} {
		_, err := db.CommitBatch(foldDeltas(
			accounts.SlotDelta{Slot: through - 1, Delta: []*accounts.Account{foldAcct(byte(i+1), uint64(i+1), nil)}},
		), through, map[uint64][32]byte{through: bh(through)}, nil)
		require.NoError(t, err)
	}

	// Rewind the index to "nothing applied" and delete seq 2's manifest to
	// create a gap: seq 1 must replay, seq 3 must be treated as undecided.
	require.NoError(t, db.Index.Delete(metaKeyLastBatch, pebble.Sync))
	headers, err := ListFoldManifests(db.AcctsDir)
	require.NoError(t, err)
	require.Len(t, headers, 3)
	require.NoError(t, os.Remove(headers[1].Path))

	db = reopenFoldTestDb(t, db, dir)
	defer db.CloseDb()
	res, err := db.RecoverFoldState()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), res.BatchSeq)
	assert.Equal(t, uint64(105), res.DurableThrough)
	assert.Equal(t, []uint64{1}, res.ReplayedBatches)
	assert.NotEmpty(t, res.OrphansRemoved, "seq-3 manifest + segment must be removed")

	remaining, err := ListFoldManifests(db.AcctsDir)
	require.NoError(t, err)
	assert.Len(t, remaining, 1)
}

// A torn manifest stops the replay run exactly like a gap.
func TestRecoverFoldStateStopsAtTornManifest(t *testing.T) {
	db, dir := newFoldTestDb(t)
	for i, through := range []uint64{105, 110} {
		_, err := db.CommitBatch(foldDeltas(
			accounts.SlotDelta{Slot: through - 1, Delta: []*accounts.Account{foldAcct(byte(i+1), uint64(i+1), nil)}},
		), through, map[uint64][32]byte{through: bh(through)}, nil)
		require.NoError(t, err)
	}
	require.NoError(t, db.Index.Delete(metaKeyLastBatch, pebble.Sync))

	headers, err := ListFoldManifests(db.AcctsDir)
	require.NoError(t, err)
	data, err := os.ReadFile(headers[1].Path)
	require.NoError(t, err)
	data[len(data)-2] ^= 0xFF // corrupt inside the CRC-covered region
	require.NoError(t, os.WriteFile(headers[1].Path, data, 0o644))

	db = reopenFoldTestDb(t, db, dir)
	defer db.CloseDb()
	res, err := db.RecoverFoldState()
	require.NoError(t, err)
	assert.Equal(t, uint64(1), res.BatchSeq)
	assert.Equal(t, []uint64{1}, res.ReplayedBatches)
}

// WAL-off mode: losing the index tail (simulated by wiping entries + meta) is
// fully repaired by manifest replay — the manifests ARE the index redo log.
func TestRecoverFoldStateRebuildsIndexTailWithWALOff(t *testing.T) {
	db, dir := newFoldTestDb(t)
	db.IndexWALDisabled = true

	a1 := foldAcct(1, 11, []byte("one"))
	a2 := foldAcct(2, 22, []byte("two"))
	_, err := db.CommitBatch(foldDeltas(
		accounts.SlotDelta{Slot: 101, Delta: []*accounts.Account{a1}},
	), 105, map[uint64][32]byte{105: bh(105)}, []byte("ctx-105"))
	require.NoError(t, err)
	_, err = db.CommitBatch(foldDeltas(
		accounts.SlotDelta{Slot: 107, Delta: []*accounts.Account{a2}},
	), 110, map[uint64][32]byte{110: bh(110)}, []byte("ctx-110"))
	require.NoError(t, err)

	// Simulate the lost memtable tail: wipe both entries and the meta.
	require.NoError(t, db.Index.Delete(metaKeyLastBatch, pebble.Sync))
	require.NoError(t, db.Index.Delete(a1.Key[:], pebble.Sync))
	require.NoError(t, db.Index.Delete(a2.Key[:], pebble.Sync))

	db = reopenFoldTestDb(t, db, dir)
	defer db.CloseDb()
	res, err := db.RecoverFoldState()
	require.NoError(t, err)
	assert.Equal(t, []uint64{1, 2}, res.ReplayedBatches)
	assert.Equal(t, uint64(110), res.DurableThrough)
	assert.Equal(t, []byte("ctx-110"), res.ResumeCtx)

	got1 := mustColdRead(t, db, 110, solana.PublicKey{1})
	assert.Equal(t, uint64(11), got1.Lamports)
	got2 := mustColdRead(t, db, 110, solana.PublicKey{2})
	assert.Equal(t, uint64(22), got2.Lamports)
}

// Fresh store: no manifests, no meta — recovery reports a clean zero state.
func TestRecoverFoldStateFreshStore(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	res, err := db.RecoverFoldState()
	require.NoError(t, err)
	assert.Zero(t, res.DurableThrough)
	assert.Zero(t, res.BatchSeq)
	assert.Nil(t, res.ResumeCtx)
	assert.Empty(t, res.ReplayedBatches)
}

// FileIds are never reused across a crash (I7): the high-water mark persists
// before any data byte, so an aborted fold still consumes its fileId.
func TestCommitBatchNeverReusesFileIdAfterCrash(t *testing.T) {
	db, dir := newFoldTestDb(t)

	db.foldHooks.afterSegmentFsync = func() { panic("crash before manifest") }
	func() {
		defer func() { _ = recover() }()
		_, _ = db.CommitBatch(foldDeltas(
			accounts.SlotDelta{Slot: 101, Delta: []*accounts.Account{foldAcct(1, 1, nil)}},
		), 105, map[uint64][32]byte{105: bh(105)}, nil)
	}()
	db.foldHooks.afterSegmentFsync = nil

	db = reopenFoldTestDb(t, db, dir)
	defer db.CloseDb()
	_, err := db.RecoverFoldState()
	require.NoError(t, err)

	res, err := db.CommitBatch(foldDeltas(
		accounts.SlotDelta{Slot: 106, Delta: []*accounts.Account{foldAcct(1, 2, nil)}},
	), 110, map[uint64][32]byte{110: bh(110)}, nil)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, res.FileId, uint64(2), "aborted fold's fileId must stay consumed")
}
