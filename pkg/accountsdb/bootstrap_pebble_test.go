package accountsdb

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/cockroachdb/pebble"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestAccountsDbStoreGuardOwnership(t *testing.T) {
	db, root := newFoldTestDb(t)
	_, err := AcquireExclusiveAccountsDbStore(root)
	require.ErrorIs(t, err, ErrAccountsDbInUse)
	_, err = OpenDb(root)
	require.ErrorIs(t, err, ErrAccountsDbInUse)
	require.NoError(t, db.Shutdown(t.Context()))
	require.NoError(t, db.Shutdown(t.Context()))
	guard, err := AcquireExclusiveAccountsDbStore(root)
	require.NoError(t, err)
	defer guard.Close()
	_, err = OpenDbWithStoreGuard(t.TempDir(), guard)
	require.ErrorContains(t, err, "guard is for")
	reopened, err := OpenDbWithStoreGuard(root, guard)
	require.NoError(t, err)
	require.NoError(t, guard.Close(), "transferred ownership survives guard close")
	_, err = OpenDb(root)
	require.ErrorIs(t, err, ErrAccountsDbInUse)
	require.NoError(t, reopened.Shutdown(t.Context()))
}

func TestAccountsDbRejectsUnsupportedIndexBeforeCreatingFiles(t *testing.T) {
	for _, name := range []string{"accounts_index_v2.lock", "accounts_index.root", "accounts_delta_v2.journal"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(root, name), []byte("preserve"), 0600))
			_, err := OpenDb(root)
			require.ErrorContains(t, err, "unsupported account-index format")
			entries, err := os.ReadDir(root)
			require.NoError(t, err)
			require.Len(t, entries, 1)
		})
	}
}

func TestAccountsDbScanPinnedOrderBoundsAndCancellation(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	key := func(prefix uint64) solana.PublicKey {
		var k solana.PublicKey
		binary.BigEndian.PutUint64(k[:8], prefix)
		return k
	}
	keys := []solana.PublicKey{key(0), key(2), key(9), key(^uint64(0))}
	var value [24]byte
	AccountIndexEntry{FileId: 1}.Marshal(&value)
	for _, k := range keys {
		require.NoError(t, db.Index.Set(k[:], value[:], nil))
	}
	require.NoError(t, db.Index.Set(metaKeyLastBatch, encodeFoldMeta(foldMeta{}), nil))
	var got []solana.PublicKey
	require.NoError(t, db.ScanKeysBetweenPrefixes(t.Context(), 0, ^uint64(0), func(k solana.PublicKey) error {
		got = append(got, k)
		if len(got) == 1 {
			late := key(5)
			require.NoError(t, db.Index.Set(late[:], value[:], nil))
		}
		return nil
	}))
	require.Equal(t, keys, got, "snapshot excludes an insertion during iteration; metadata is skipped")
	got = nil
	require.NoError(t, db.ScanKeysBetweenPrefixes(t.Context(), 2, 9, func(k solana.PublicKey) error { got = append(got, k); return nil }))
	require.Equal(t, []solana.PublicKey{key(2), key(5), key(9)}, got)
	ctx, cancel := context.WithCancel(t.Context())
	require.ErrorIs(t, db.ScanKeysBetweenPrefixes(ctx, 0, ^uint64(0), func(solana.PublicKey) error { cancel(); return nil }), context.Canceled)
}

func TestAccountsDbGenesisRecoveryFailsBeforeCleanup(t *testing.T) {
	for _, damage := range []string{"manifest_crc", "segment_crc", "gap", "filename", "missing_selected", "watermark", "bootstrap"} {
		t.Run(damage, func(t *testing.T) {
			db, root := newFoldTestDb(t)
			defer db.CloseDb()
			db.LargestFileId.Store(1)
			require.NoError(t, WriteLargestFileID(root, 1))
			var bootstrap [8]byte
			binary.LittleEndian.PutUint64(bootstrap[:], 1)
			require.NoError(t, os.WriteFile(filepath.Join(root, "bootstrap_high_file_id"), bootstrap[:], 0600))
			for slot := uint64(1); slot <= 2; slot++ {
				_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: []*accounts.Account{foldAcct(7, slot, []byte("data"))}}}, slot, map[uint64][32]byte{slot: bh(slot)}, nil)
				require.NoError(t, err)
			}
			hs, err := ListFoldManifests(db.AcctsDir)
			require.NoError(t, err)
			switch damage {
			case "manifest_crc":
				raw, err := os.ReadFile(hs[0].Path)
				require.NoError(t, err)
				raw[len(raw)-1] ^= 1
				require.NoError(t, os.WriteFile(hs[0].Path, raw, 0600))
			case "segment_crc":
				path := filepath.Join(db.AcctsDir, SegmentDataName(hs[0].ThroughSlot, hs[0].FileId))
				raw, err := os.ReadFile(path)
				require.NoError(t, err)
				raw[56] ^= 1
				require.NoError(t, os.WriteFile(path, raw, 0600))
			case "gap":
				require.NoError(t, os.Remove(hs[0].Path))
			case "filename":
				require.NoError(t, os.Rename(hs[0].Path, filepath.Join(db.AcctsDir, "99.999.manifest")))
			case "missing_selected":
				require.NoError(t, os.Remove(hs[1].Path))
			case "watermark":
				require.NoError(t, db.Index.Set(metaKeyLastBatch, encodeFoldMeta(foldMeta{BatchSeq: 2, ThroughSlot: 99, FileId: hs[1].FileId}), pebble.Sync))
			case "bootstrap":
				require.NoError(t, os.WriteFile(filepath.Join(root, "bootstrap_high_file_id"), bootstrap[:4], 0600))
			}
			orphan := filepath.Join(db.AcctsDir, "99.999")
			require.NoError(t, os.WriteFile(orphan, []byte("keep"), 0600))
			before, err := os.ReadDir(db.AcctsDir)
			require.NoError(t, err)
			_, err = db.RecoverGenesisFoldState()
			require.Error(t, err)
			after, err := os.ReadDir(db.AcctsDir)
			require.NoError(t, err)
			require.Equal(t, before, after)
			raw, err := os.ReadFile(orphan)
			require.NoError(t, err)
			require.True(t, bytes.Equal(raw, []byte("keep")))
		})
	}
}

func TestAccountsDbGenesisRecoveryRepairsAdvisoryBankHashes(t *testing.T) {
	db, root := newFoldTestDb(t)
	defer db.CloseDb()
	db.LargestFileId.Store(1)
	require.NoError(t, WriteLargestFileID(root, 1))
	var bootstrap [8]byte
	binary.LittleEndian.PutUint64(bootstrap[:], 1)
	require.NoError(t, os.WriteFile(filepath.Join(root, "bootstrap_high_file_id"), bootstrap[:], 0600))
	for slot := uint64(1); slot <= 2; slot++ {
		_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: []*accounts.Account{foldAcct(7, slot, nil)}}}, slot, map[uint64][32]byte{slot: bh(slot)}, nil)
		require.NoError(t, err)
	}
	// Model the independently buffered bankhash WAL being lost even though the
	// account index's commit watermark survives. Both manifests are already applied.
	for slot := uint64(1); slot <= 2; slot++ {
		var key [8]byte
		binary.LittleEndian.PutUint64(key[:], slot)
		require.NoError(t, db.BankHashStore.Delete(key[:], pebble.Sync))
	}
	recovered, err := db.RecoverGenesisFoldState()
	require.NoError(t, err)
	require.Equal(t, uint64(2), recovered.DurableThrough)
	require.Empty(t, recovered.ReplayedBatches)
	for slot := uint64(1); slot <= 2; slot++ {
		got, err := db.GetBankHashForSlot(slot)
		require.NoError(t, err)
		want := bh(slot)
		require.Equal(t, want[:], got)
	}
}
