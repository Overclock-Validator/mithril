package accountsdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A manifest-only fold (empty/skip slots with no account writes) must still
// commit: it records the batch's bankhashes + resume context and advances the
// durable watermark, so empty slots never block durable progress.
func TestCommitBatchEmptyFoldAdvancesWatermark(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	// A real fold first, then a bankhash-only fold over empty slots.
	commitTestBatch(t, db, 110, foldAcct(1, 100, []byte("v1")))
	res, err := db.CommitBatch(
		foldDeltas(
			accounts.SlotDelta{Slot: 111, Delta: nil},
			accounts.SlotDelta{Slot: 112, Delta: []*accounts.Account{nil}}, // only nils
		),
		112,
		map[uint64][32]byte{111: bh(111), 112: bh(112)},
		[]byte("ctx-112"),
	)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), res.BatchSeq, "empty fold still advances the batch sequence")
	assert.Equal(t, uint64(112), res.ThroughSlot)
	assert.Equal(t, 0, res.Keys)
	assert.Zero(t, res.Bytes, "no account bytes written")

	meta, ok, err := db.readFoldMeta()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, uint64(112), meta.ThroughSlot, "watermark advanced past the empty slots")

	hb, err := db.GetBankHashForSlot(112)
	require.NoError(t, err)
	want := bh(112)
	assert.Equal(t, want[:], hb, "empty slot's bankhash recorded")

	// The manifest carries the resume context and survives a reopen (recovery
	// verifies the zero-length segment's CRC/length without error).
	m, err := ReadSegmentManifest(segmentManifestPath(db.AcctsDir, 112, res.FileId))
	require.NoError(t, err)
	assert.Equal(t, []byte("ctx-112"), m.ResumeCtx)
	assert.Zero(t, m.DataLen)

	// The earlier account is still the only stored account.
	assert.Equal(t, uint64(100), mustColdRead(t, db, 112, solana.PublicKey{1}).Lamports)
}

// Newest-wins dedup depends on ascending delta order; a mis-ordered caller
// (e.g. a future fork-aware path) must be rejected, not silently fold stale state.
func TestCommitBatchRejectsNonAscendingDeltas(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()

	_, err := db.CommitBatch(
		foldDeltas(
			accounts.SlotDelta{Slot: 120, Delta: []*accounts.Account{foldAcct(1, 1, nil)}},
			accounts.SlotDelta{Slot: 110, Delta: []*accounts.Account{foldAcct(1, 2, nil)}}, // out of order
		),
		120,
		map[uint64][32]byte{120: bh(120)},
		nil,
	)
	require.ErrorContains(t, err, "strictly ascending")

	// Equal consecutive slots are also rejected (dedup would be order-dependent).
	_, err = db.CommitBatch(
		foldDeltas(
			accounts.SlotDelta{Slot: 110, Delta: []*accounts.Account{foldAcct(1, 1, nil)}},
			accounts.SlotDelta{Slot: 110, Delta: []*accounts.Account{foldAcct(1, 2, nil)}},
		),
		110,
		map[uint64][32]byte{110: bh(110)},
		nil,
	)
	require.ErrorContains(t, err, "strictly ascending")

	// A single delta and correctly-ordered deltas still fold.
	_, err = db.CommitBatch(
		foldDeltas(
			accounts.SlotDelta{Slot: 110, Delta: []*accounts.Account{foldAcct(1, 1, nil)}},
			accounts.SlotDelta{Slot: 111, Delta: []*accounts.Account{foldAcct(1, 2, nil)}},
		),
		111,
		map[uint64][32]byte{111: bh(111)},
		nil,
	)
	require.NoError(t, err)
}

// A leftover ".manifest.tmp" (crash between manifest tmp-write and rename) is
// unlinked by recovery — it was never committed (the rename is the commit point).
func TestRecoveryRemovesStaleTmpManifest(t *testing.T) {
	db, dir := newFoldTestDb(t)
	commitTestBatch(t, db, 110, foldAcct(1, 100, []byte("v1")))

	tmpPath := filepath.Join(db.AcctsDir, SegmentDataName(120, 999)+segManifestTmpSuffix)
	require.NoError(t, os.WriteFile(tmpPath, []byte("partial-manifest-bytes"), 0o644))

	db = reopenFoldTestDb(t, db, dir)
	defer db.CloseDb()
	rec, err := db.RecoverFoldState()
	require.NoError(t, err)

	assert.NoFileExists(t, tmpPath, "stale manifest tmp must be unlinked by recovery")
	assert.Contains(t, rec.OrphansRemoved, tmpPath)
	// The committed batch is untouched.
	assert.Equal(t, uint64(110), rec.DurableThrough)
	assert.Equal(t, uint64(100), mustColdRead(t, db, 110, solana.PublicKey{1}).Lamports)
}
