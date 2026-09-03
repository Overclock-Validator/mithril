package snapshot

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteFileAtomicallyOrdersDurabilityBarriers(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "sidecar")
	require.NoError(t, os.WriteFile(path, []byte("old"), 0o644))

	var events []string
	ops := defaultDurableFileOps
	ops.syncFile = func(file *os.File) error {
		events = append(events, "file-sync")
		return file.Sync()
	}
	ops.closeFile = func(file *os.File) error {
		events = append(events, "file-close")
		return file.Close()
	}
	ops.rename = func(oldPath, newPath string) error {
		events = append(events, "rename")
		return os.Rename(oldPath, newPath)
	}
	ops.syncDir = func(path string) error {
		events = append(events, "directory-sync")
		return nil
	}

	require.NoError(t, writeFileAtomicallyWithOps(path, []byte("new"), 0o640, ops))
	require.Equal(t, []string{"file-sync", "file-close", "rename", "directory-sync"}, events)
	contents, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), contents)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o640), info.Mode().Perm())
}

func TestWriteFileAtomicallyDoesNotPublishBeforeFileSync(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "sidecar")
	require.NoError(t, os.WriteFile(path, []byte("old"), 0o644))

	injected := errors.New("injected file sync failure")
	ops := defaultDurableFileOps
	ops.syncFile = func(*os.File) error { return injected }

	err := writeFileAtomicallyWithOps(path, []byte("new"), 0o644, ops)
	require.ErrorIs(t, err, injected)
	contents, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, []byte("old"), contents)
	matches, globErr := filepath.Glob(filepath.Join(directory, ".sidecar.tmp-*"))
	require.NoError(t, globErr)
	require.Empty(t, matches)
}

func TestWriteFileAtomicallyReportsPostRenameDirectorySyncFailure(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "sidecar")
	injected := errors.New("injected directory sync failure")
	ops := defaultDurableFileOps
	ops.syncDir = func(string) error { return injected }

	err := writeFileAtomicallyWithOps(path, []byte("published"), 0o644, ops)
	require.ErrorIs(t, err, injected)
	contents, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, []byte("published"), contents)
}
