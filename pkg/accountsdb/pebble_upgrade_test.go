package accountsdb

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	pebblev1 "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/v2"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// writeV1Store creates a Pebble v1 store at its default format, as an older
// Mithril (for example PR 328's) writes it, holding keys 0..n-1.
func writeV1Store(t *testing.T, dir string, n int) {
	t.Helper()
	db, err := pebblev1.Open(dir, &pebblev1.Options{})
	require.NoError(t, err)
	var buf [24]byte
	for i := 0; i < n; i++ {
		k := solana.PublicKey{byte(i), byte(i >> 8), 1}
		(&AccountIndexEntry{Slot: uint64(i), FileId: 7, Offset: uint64(i) * 8}).Marshal(&buf)
		require.NoError(t, db.Set(k[:], buf[:], pebblev1.NoSync))
	}
	require.NoError(t, db.Flush())
	require.NoError(t, db.Close())
}

// An AccountsDB whose index and bankhash stores were written by Pebble v1
// opens without a rebuild: both are upgraded once and every key reads back.
func TestOpenDbUpgradesPebbleV1Stores(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "accounts"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0o644))
	writeV1Store(t, filepath.Join(dir, "mithril_db"), 300)
	writeV1Store(t, filepath.Join(dir, "bankhash_db"), 3)
	for _, store := range []string{"mithril_db", "bankhash_db"} {
		v, ok, err := storedPebbleFormat(filepath.Join(dir, store))
		require.NoError(t, err)
		require.True(t, ok)
		require.Less(t, v, uint64(pebble.FormatMinSupported), "the fixture must need the upgrade")
	}
	// Without the upgrade, Pebble v2 refuses these stores.
	refused, err := pebble.Open(filepath.Join(dir, "bankhash_db"), &pebble.Options{})
	require.Error(t, err)
	require.Nil(t, refused)

	for round := 0; round < 2; round++ { // the second open needs no upgrade
		db, err := OpenDb(dir)
		require.NoError(t, err, "round %d", round)
		for i := 0; i < 300; i++ {
			k := solana.PublicKey{byte(i), byte(i >> 8), 1}
			val, closer, err := db.Index.Get(k[:])
			require.NoError(t, err)
			e, err := UnmarshalAcctIdxEntryValue(val)
			closer.Close()
			require.NoError(t, err)
			require.Equal(t, AccountIndexEntry{Slot: uint64(i), FileId: 7, Offset: uint64(i) * 8}, e)
		}
		db.CloseDb()
		for _, store := range []string{"mithril_db", "bankhash_db"} {
			v, ok, err := storedPebbleFormat(filepath.Join(dir, store))
			require.NoError(t, err)
			require.True(t, ok)
			require.GreaterOrEqual(t, v, uint64(pebble.FormatMinSupported))
		}
	}
}

// Stores already readable by Pebble v2, and directories without a store,
// open exactly as before.
func TestOpenUpgradingPebbleLeavesV2StoresAlone(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	db, err := openUpgradingPebble(dir, &pebble.Options{})
	require.NoError(t, err, "a new store is created as before")
	var v [8]byte
	binary.LittleEndian.PutUint64(v[:], 42)
	require.NoError(t, db.Set([]byte("k"), v[:], pebble.Sync))
	require.NoError(t, db.Close())
	before, ok, err := storedPebbleFormat(dir)
	require.NoError(t, err)
	require.True(t, ok)

	db, err = openUpgradingPebble(dir, &pebble.Options{})
	require.NoError(t, err)
	got, closer, err := db.Get([]byte("k"))
	require.NoError(t, err)
	require.Equal(t, v[:], got)
	closer.Close()
	require.NoError(t, db.Close())
	after, _, err := storedPebbleFormat(dir)
	require.NoError(t, err)
	require.Equal(t, before, after)
}
