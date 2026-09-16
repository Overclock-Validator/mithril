package snapshot

import (
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestDuplicateShardDirectoriesDoNotTruncate(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "accounts")
	require.NoError(t, os.Mkdir(dir, 0755))
	file := filepath.Join(dir, "data")
	require.NoError(t, os.WriteFile(file, []byte("keep me"), 0644))
	alias := filepath.Join(root, "alias")
	require.NoError(t, os.Symlink(dir, alias))
	for _, paths := range [][]string{{dir, dir}, {dir, alias}, {dir, dir + "/nested"}} {
		_, err := openShardBigFiles(paths)
		require.Error(t, err)
		data, err := os.ReadFile(file)
		require.NoError(t, err)
		require.Equal(t, "keep me", string(data))
	}
	CleanAccountsDbDirs([]string{root, root})
	data, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Equal(t, "keep me", string(data))
}

func TestDirectIOPlatformGuard(t *testing.T) {
	flag, err := directIOFlag()
	if runtime.GOOS == "linux" {
		require.NoError(t, err)
		require.NotZero(t, flag)
		return
	}
	require.Error(t, err)
	path := filepath.Join(t.TempDir(), "data")
	require.NoError(t, os.WriteFile(path, []byte("keep me"), 0644))
	_, err = newShardWriter(path, true)
	require.Error(t, err)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "keep me", string(data))
	w, err := newShardWriter(path, false)
	require.NoError(t, err)
	require.NoError(t, w.close())
}

func TestShardWriterCloseReleasesBuffers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data")
	w, err := newShardWriter(path, false)
	require.NoError(t, err)
	_, err = w.append([]byte("finished data"))
	require.NoError(t, err)
	require.NoError(t, w.close())
	require.Nil(t, w.cur)
	require.Empty(t, w.free)
	require.NoError(t, w.close(), "deferred cleanup is idempotent")
	_, err = w.append([]byte("late write"))
	require.ErrorIs(t, err, os.ErrClosed)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "finished data", string(data))
}
