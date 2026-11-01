package accountsdb

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeUnfoldedDataFile writes an account data file above the bootstrap
// boundary with no manifest.
func writeUnfoldedDataFile(t *testing.T, db *AccountsDb, slot, fileId uint64) string {
	t.Helper()
	p := filepath.Join(db.AcctsDir, SegmentDataName(slot, fileId))
	require.NoError(t, os.WriteFile(p, []byte("account data"), 0o644))
	return p
}

// writeClassicDataFile leaves what classic replay's per-slot store left: an
// unfolded account data file and the slot's bank hash row, which classic
// replay wrote with its stores. The writer itself was removed, so the test
// reproduces its on-disk shape.
func writeClassicDataFile(t *testing.T, db *AccountsDb, slot, fileId uint64) string {
	t.Helper()
	var key [8]byte
	binary.LittleEndian.PutUint64(key[:], slot)
	hash := bh(slot)
	require.NoError(t, db.BankHashStore.Set(key[:], hash[:], nil))
	return writeUnfoldedDataFile(t, db, slot, fileId)
}

// writeClassicSlot does what classic replay's per-slot store did for an
// account whose data length changed: append it to a new file named for the
// slot, point the index at it, and record the slot's bank hash.
func writeClassicSlot(t *testing.T, db *AccountsDb, slot, fileId uint64, acct *accounts.Account) string {
	t.Helper()
	p := filepath.Join(db.AcctsDir, SegmentDataName(slot, fileId))
	f, err := os.Create(p)
	require.NoError(t, err)
	rec := AppendVecAccount{DataLen: uint64(len(acct.Data)), Pubkey: acct.Key, Lamports: acct.Lamports,
		RentEpoch: acct.RentEpoch, Owner: acct.Owner, Executable: acct.Executable, Data: acct.Data}
	require.NoError(t, rec.Marshal(f))
	require.NoError(t, f.Close())
	var entry [24]byte
	AccountIndexEntry{Slot: slot, FileId: fileId, Offset: 0}.Marshal(&entry)
	require.NoError(t, db.Index.Set(acct.Key[:], entry[:], nil))
	var key [8]byte
	binary.LittleEndian.PutUint64(key[:], slot)
	hash := bh(slot)
	require.NoError(t, db.BankHashStore.Set(key[:], hash[:], nil))
	return p
}

// Fold recovery must refuse a store classic replay wrote, before deleting
// anything: its account files have no manifest, which the orphan pass would
// otherwise remove as uncommitted writes.
func TestRecoverFoldStateRefusesClassicStore(t *testing.T) {
	db, dir := newFoldTestDb(t)
	defer func() { db.CloseDb() }()
	classic := writeClassicDataFile(t, db, 200, 5)

	h, err := db.InspectFoldHistory()
	require.NoError(t, err)
	assert.Equal(t, FoldHistory{Folded: false, KnownBootstrap: true, UnfoldedFiles: 1, ClassicFiles: 1}, h)
	assert.True(t, h.ClassicWrites())

	_, err = db.RecoverFoldState()
	require.ErrorIs(t, err, ErrClassicStore)
	assert.FileExists(t, classic, "recovery must not delete a classic store's account file")

	db = reopenFoldTestDb(t, db, dir)
	_, err = db.RecoverFoldState()
	require.ErrorIs(t, err, ErrClassicStore, "the refusal persists across restarts")
	assert.FileExists(t, classic)
}

// Classic replay run on a store that had already folded (an older node with
// consensus.protocol = "classic") leaves per-slot files the index points
// into. Recovery must refuse before deleting them: they hold the only copy of
// the accounts classic replay created or resized.
func TestRecoverFoldStateRefusesClassicWritesAfterAFold(t *testing.T) {
	db, dir := newFoldTestDb(t)
	defer func() { db.CloseDb() }()
	res, err := db.CommitBatch(foldDeltas(
		accounts.SlotDelta{Slot: 101, Delta: []*accounts.Account{foldAcct(1, 100, []byte("folded"))}},
	), 104, map[uint64][32]byte{104: bh(104)}, []byte("ctx-104"))
	require.NoError(t, err)
	classicAcct := foldAcct(2, 900, []byte("classic-only-account"))
	classic := writeClassicSlot(t, db, 110, res.FileId+1, classicAcct)

	h, err := db.InspectFoldHistory()
	require.NoError(t, err)
	assert.Equal(t, FoldHistory{Folded: true, KnownBootstrap: true, UnfoldedFiles: 1, ClassicFiles: 1, LiveFiles: 1}, h)
	assert.True(t, h.ClassicWrites())

	_, err = db.RecoverFoldState()
	require.ErrorIs(t, err, ErrClassicStore)
	assert.FileExists(t, classic, "recovery must not delete the only copy of a classic account")
	assert.Equal(t, uint64(900), mustColdRead(t, db, 110, classicAcct.Key).Lamports)

	db = reopenFoldTestDb(t, db, dir)
	_, err = db.RecoverFoldState()
	require.ErrorIs(t, err, ErrClassicStore, "the refusal persists across restarts")
	assert.FileExists(t, classic)
}

// A store with fold history keeps the orphan pass. An interrupted compaction
// leaves an unfolded copy of a committed file's records, named for that
// file's slot (which has a bank hash row), while the index still points at
// the source; recovery removes it, as it does unparseable leftovers.
func TestRecoverFoldStateKeepsOrphanGCAfterAFold(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	res, err := db.CommitBatch(foldDeltas(
		accounts.SlotDelta{Slot: 101, Delta: []*accounts.Account{foldAcct(1, 100, []byte("v1"))}},
	), 104, map[uint64][32]byte{104: bh(104)}, []byte("ctx-104"))
	require.NoError(t, err)
	segment, err := os.ReadFile(filepath.Join(db.AcctsDir, SegmentDataName(104, res.FileId)))
	require.NoError(t, err)
	compactOut := filepath.Join(db.AcctsDir, SegmentDataName(104, res.FileId+1))
	require.NoError(t, os.WriteFile(compactOut, segment, 0o644))
	torn := writeClassicDataFile(t, db, 110, res.FileId+2)

	h, err := db.InspectFoldHistory()
	require.NoError(t, err)
	assert.Equal(t, FoldHistory{Folded: true, KnownBootstrap: true, UnfoldedFiles: 2, ClassicFiles: 2}, h)
	assert.False(t, h.ClassicWrites())

	rec, err := db.RecoverFoldState()
	require.NoError(t, err)
	assert.Contains(t, rec.OrphansRemoved, compactOut)
	assert.Contains(t, rec.OrphansRemoved, torn)
	assert.NoFileExists(t, compactOut)
	assert.NoFileExists(t, torn)
	assert.Equal(t, uint64(100), mustColdRead(t, db, 104, solana.PublicKey{1}).Lamports)
}

// A store without data files past the snapshot (freshly built, or replayed
// without folding yet) is not a classic store, nor is a first fold
// interrupted before its manifest (an unfolded file whose slot has no bank
// hash) or after its temporary manifest; recovery removes those leftovers.
func TestFoldHistoryWithoutClassicWrites(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	h, err := db.InspectFoldHistory()
	require.NoError(t, err)
	assert.False(t, h.ClassicWrites())
	_, err = db.RecoverFoldState()
	require.NoError(t, err)

	segment := writeUnfoldedDataFile(t, db, 115, 6)
	h, err = db.InspectFoldHistory()
	require.NoError(t, err)
	assert.Equal(t, 1, h.UnfoldedFiles)
	assert.False(t, h.ClassicWrites())
	rec, err := db.RecoverFoldState()
	require.NoError(t, err)
	assert.Contains(t, rec.OrphansRemoved, segment)

	writeClassicDataFile(t, db, 120, 7)
	require.NoError(t, os.WriteFile(filepath.Join(db.AcctsDir, "120.7"+segManifestTmpSuffix), []byte("partial"), 0o644))
	h, err = db.InspectFoldHistory()
	require.NoError(t, err)
	assert.True(t, h.Folded, "a temporary manifest is evidence of a fold")
	assert.False(t, h.ClassicWrites())
	_, err = db.RecoverFoldState()
	require.NoError(t, err)
}
