package turbine

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// With an archive, the retention floor moves slot files there instead of
// deleting them; space eviction and discards still delete.
func TestShredSpoolArchiveKeepsSlotsBehindTheFloor(t *testing.T) {
	dir, archive := t.TempDir(), filepath.Join(t.TempDir(), "archive")
	spool, err := OpenShredSpool(dir, 0)
	require.NoError(t, err)
	defer spool.Close()
	require.NoError(t, spool.SetArchive(archive))

	spool.Append(100, []byte("alpha"))
	spool.Append(100, []byte("beta"))
	spool.Append(101, []byte("gamma"))
	spool.Append(102, []byte("delta"))
	spool.DiscardSlot(101)

	spool.SetFloor(102)
	require.False(t, spool.HasSlot(100))
	got, torn, err := ReadSpoolFileIntact(filepath.Join(archive, SpoolFileName(100)))
	require.NoError(t, err)
	require.False(t, torn)
	require.Equal(t, [][]byte{[]byte("alpha"), []byte("beta")}, got)
	_, err = os.Stat(filepath.Join(archive, SpoolFileName(101)))
	require.True(t, os.IsNotExist(err), "a discarded slot is not archived")
	require.True(t, spool.HasSlot(102))
}

func TestShredSpoolWithoutArchiveDeletes(t *testing.T) {
	dir := t.TempDir()
	spool, err := OpenShredSpool(dir, 0)
	require.NoError(t, err)
	defer spool.Close()
	spool.Append(100, []byte("alpha"))
	spool.SetFloor(101)
	_, err = os.Stat(filepath.Join(dir, SpoolFileName(100)))
	require.True(t, os.IsNotExist(err))
}

func TestArchiveFromEnv(t *testing.T) {
	spool, err := OpenShredSpool(t.TempDir(), 0)
	require.NoError(t, err)
	defer spool.Close()
	t.Setenv(ShredArchiveEnv, "")
	dir, err := ArchiveFromEnv(spool)
	require.NoError(t, err)
	require.Empty(t, dir)
	want := filepath.Join(t.TempDir(), "a")
	t.Setenv(ShredArchiveEnv, want)
	dir, err = ArchiveFromEnv(spool)
	require.NoError(t, err)
	require.Equal(t, want, dir)
	require.DirExists(t, want)
}

// A file a stopped node left mid-append keeps its intact records.
func TestReadSpoolFileIntactSkipsATornTail(t *testing.T) {
	dir := t.TempDir()
	spool, err := OpenShredSpool(dir, 0)
	require.NoError(t, err)
	spool.Append(7, []byte("one"))
	spool.Append(7, []byte("two"))
	spool.Close()
	path := filepath.Join(dir, SpoolFileName(7))
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.Write([]byte{9, 0, 0, 0, 1, 2}) // a header with no packet after it
	require.NoError(t, err)
	require.NoError(t, f.Close())

	got, torn, err := ReadSpoolFileIntact(path)
	require.NoError(t, err)
	require.True(t, torn)
	require.Equal(t, [][]byte{[]byte("one"), []byte("two")}, got)
	_, err = ReadSpoolFile(path)
	require.Error(t, err, "the strict reader still refuses it")
}
