package accountsdb

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBootstrapHighFileIDRoundTripIsWriteOnce(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, WriteBootstrapHighFileID(root, 42))

	got, err := ReadBootstrapHighFileID(root)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), got)

	err = WriteBootstrapHighFileID(root, 99)
	require.ErrorIs(t, err, ErrInvalidBootstrapHighFileID)
	got, err = ReadBootstrapHighFileID(root)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), got, "a second writer must not replace the lineage boundary")

	info, err := os.Stat(filepath.Join(root, BootstrapHighFileIDFileName))
	require.NoError(t, err)
	assert.Equal(t, int64(BootstrapHighFileIDSize), info.Size())
	matches, err := filepath.Glob(filepath.Join(root, ".bootstrap-high-*.tmp"))
	require.NoError(t, err)
	assert.Empty(t, matches)
}

func TestBootstrapHighFileIDConcurrentPublicationDoesNotReplace(t *testing.T) {
	root := t.TempDir()
	const writers = 16
	var wait sync.WaitGroup
	wait.Add(writers)
	errorsByWriter := make([]error, writers)
	for i := range writers {
		go func() {
			defer wait.Done()
			errorsByWriter[i] = WriteBootstrapHighFileID(root, uint64(i+1))
		}()
	}
	wait.Wait()

	successes := 0
	for _, err := range errorsByWriter {
		if err == nil {
			successes++
			continue
		}
		require.ErrorIs(t, err, ErrInvalidBootstrapHighFileID)
	}
	assert.Equal(t, 1, successes)
	got, err := ReadBootstrapHighFileID(root)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, got, uint64(1))
	assert.LessOrEqual(t, got, uint64(writers))
}

func TestBootstrapHighFileIDRejectsMalformedArtifacts(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(t *testing.T, root, path string)
	}{
		{
			name: "truncated",
			mutate: func(t *testing.T, _, path string) {
				require.NoError(t, os.Truncate(path, BootstrapHighFileIDSize-1))
			},
		},
		{
			name: "corrupt magic",
			mutate: func(t *testing.T, _, path string) {
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				data[0] ^= 0xff
				require.NoError(t, os.WriteFile(path, data, 0o644))
			},
		},
		{
			name: "corrupt value checksum",
			mutate: func(t *testing.T, _, path string) {
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				data[16] ^= 0xff
				require.NoError(t, os.WriteFile(path, data, 0o644))
			},
		},
		{
			name: "reserved bytes",
			mutate: func(t *testing.T, _, path string) {
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				data[24] = 1
				require.NoError(t, os.WriteFile(path, data, 0o644))
			},
		},
		{
			name: "symlink",
			mutate: func(t *testing.T, root, path string) {
				data, err := os.ReadFile(path)
				require.NoError(t, err)
				target := filepath.Join(root, "marker-target")
				require.NoError(t, os.WriteFile(target, data, 0o644))
				require.NoError(t, os.Remove(path))
				require.NoError(t, os.Symlink(target, path))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			require.NoError(t, WriteBootstrapHighFileID(root, 42))
			path := filepath.Join(root, BootstrapHighFileIDFileName)
			test.mutate(t, root, path)

			_, err := ReadBootstrapHighFileID(root)
			require.ErrorIs(t, err, ErrInvalidBootstrapHighFileID)
		})
	}
}

func TestRecoveryPreservesOrphansWhenBootstrapHighFileIDIsCorrupt(t *testing.T) {
	db, root := newFoldTestDb(t)
	defer db.CloseDb()
	orphan := filepath.Join(db.AcctsDir, SegmentDataName(10, 99))
	require.NoError(t, os.WriteFile(orphan, []byte("possibly committed"), 0o644))

	marker := filepath.Join(root, BootstrapHighFileIDFileName)
	data, err := os.ReadFile(marker)
	require.NoError(t, err)
	data[16] ^= 0x80
	require.NoError(t, os.WriteFile(marker, data, 0o644))

	_, err = db.RecoverFoldState()
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrInvalidBootstrapHighFileID))
	assert.FileExists(t, orphan, "recovery must fail closed before orphan deletion")
}
