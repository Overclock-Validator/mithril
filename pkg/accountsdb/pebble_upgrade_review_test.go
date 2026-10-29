package accountsdb

import (
	"context"
	pebblev1 "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
)

// Ratcheting persists a format marker, but does not rewrite OPTIONS. The
// marker must govern whether an already compatible store needs migration.
func TestUpgradeUsesPersistedFormatAfterRatchet(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	old, err := pebblev1.Open(dir, &pebblev1.Options{})
	require.NoError(t, err)
	require.NoError(t, old.Set([]byte("key"), []byte("value"), pebblev1.Sync))
	require.NoError(t, old.RatchetFormatMajorVersion(pebblev1.FormatPrePebblev1MarkedCompacted))
	require.NoError(t, old.Close())
	db, err := openUpgradingPebble(dir, &pebble.Options{})
	require.NoError(t, err)
	got, closer, err := db.Get([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, "value", string(got))
	require.NoError(t, closer.Close())
	require.NoError(t, db.Close())
}

func TestUpgradePreservesNewerV2Format(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	db, err := pebble.Open(dir, &pebble.Options{})
	require.NoError(t, err)
	require.NoError(t, db.Set([]byte("key"), []byte("value"), pebble.Sync))
	require.NoError(t, db.RatchetFormatMajorVersion(pebble.FormatColumnarBlocks))
	require.NoError(t, db.Close())
	version, exists, err := storedPebbleFormat(dir)
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, uint64(pebble.FormatColumnarBlocks), version)
	db, err = openUpgradingPebble(dir, &pebble.Options{})
	require.NoError(t, err)
	require.Equal(t, pebble.FormatColumnarBlocks, db.FormatMajorVersion())
	require.NoError(t, db.Close())
}

func TestUpgradeRespectsExistingWriterAndV1Readback(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "store")
	old, err := pebblev1.Open(dir, &pebblev1.Options{})
	require.NoError(t, err)
	require.NoError(t, old.Set([]byte("key"), []byte("value"), pebblev1.Sync))
	rejected, err := openUpgradingPebble(dir, &pebble.Options{})
	require.Error(t, err)
	require.Nil(t, rejected)
	require.Equal(t, pebblev1.FormatMostCompatible, old.FormatMajorVersion())
	require.NoError(t, old.Close())
	db, err := openUpgradingPebble(dir, &pebble.Options{})
	require.NoError(t, err)
	require.NoError(t, db.Set([]byte("new"), []byte("newvalue"), pebble.Sync))
	require.NoError(t, db.Flush())
	require.NoError(t, db.Compact(context.Background(), []byte("a"), []byte("z"), false))
	require.NoError(t, db.Close())
	old, err = pebblev1.Open(dir, &pebblev1.Options{})
	require.NoError(t, err)
	for key, value := range map[string]string{"key": "value", "new": "newvalue"} {
		got, closer, err := old.Get([]byte(key))
		require.NoError(t, err)
		require.Equal(t, value, string(got))
		require.NoError(t, closer.Close())
	}
	require.NoError(t, old.Close())
}
