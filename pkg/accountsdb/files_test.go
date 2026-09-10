package accountsdb

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeFileIDTestRoot(t *testing.T, high uint64) string {
	t.Helper()
	root := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(root, "accounts"), 0o755))
	require.NoError(t, WriteLargestFileID(root, high))
	return root
}

func TestLargestFileIDRoundTripAndCorruption(t *testing.T) {
	root := makeFileIDTestRoot(t, 42)
	high, err := ValidateLargestFileID(root)
	require.NoError(t, err)
	assert.Equal(t, uint64(42), high)

	path := filepath.Join(root, LargestFileIDFileName)
	encoded, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Len(t, encoded, LargestFileIDSize)
	encoded[16] ^= 0x80
	require.NoError(t, os.WriteFile(path, encoded, 0o644))
	_, err = ReadLargestFileID(root)
	assert.ErrorIs(t, err, ErrInvalidLargestFileID)

	require.NoError(t, os.WriteFile(path, encoded[:LargestFileIDSize-1], 0o644))
	_, err = ReadLargestFileID(root)
	assert.ErrorIs(t, err, ErrInvalidLargestFileID)
}

func TestLargestFileIDReconciliationRejectsRegressionAndReuse(t *testing.T) {
	t.Run("selector behind data", func(t *testing.T) {
		root := makeFileIDTestRoot(t, 9)
		require.NoError(t, os.WriteFile(filepath.Join(root, "accounts", "100.10"), nil, 0o644))
		_, err := ValidateLargestFileID(root)
		assert.ErrorIs(t, err, ErrInvalidLargestFileID)
		assert.ErrorContains(t, err, "below canonical on-disk file ID 10")
	})

	t.Run("final manifest reserves ID", func(t *testing.T) {
		root := makeFileIDTestRoot(t, 9)
		require.NoError(t, os.WriteFile(filepath.Join(root, "accounts", "100.10.manifest"), nil, 0o644))
		_, err := ValidateLargestFileID(root)
		assert.ErrorIs(t, err, ErrInvalidLargestFileID)
	})

	t.Run("global ID reused across slots", func(t *testing.T) {
		root := makeFileIDTestRoot(t, 10)
		require.NoError(t, os.WriteFile(filepath.Join(root, "accounts", "100.10"), nil, 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(root, "accounts", "101.10"), nil, 0o644))
		_, err := ValidateLargestFileID(root)
		assert.ErrorIs(t, err, ErrInvalidLargestFileID)
		assert.ErrorContains(t, err, "file ID 10 is reused")
	})

	t.Run("same data and manifest identity is valid", func(t *testing.T) {
		root := makeFileIDTestRoot(t, 10)
		require.NoError(t, os.WriteFile(filepath.Join(root, "accounts", "100.10"), nil, 0o644))
		require.NoError(t, os.WriteFile(filepath.Join(root, "accounts", "100.10.manifest"), nil, 0o644))
		high, err := ValidateLargestFileID(root)
		require.NoError(t, err)
		assert.Equal(t, uint64(10), high)
	})

	t.Run("non-canonical numeric identity", func(t *testing.T) {
		root := makeFileIDTestRoot(t, 10)
		require.NoError(t, os.WriteFile(filepath.Join(root, "accounts", "0100.10"), nil, 0o644))
		_, err := ValidateLargestFileID(root)
		assert.ErrorIs(t, err, ErrInvalidLargestFileID)
	})
}

func TestLargestFileIDAtomicPublicationOutcomes(t *testing.T) {
	root := makeFileIDTestRoot(t, 1)
	injected := errors.New("injected directory sync failure")
	renamed, err := writeLargestFileIDAtomic(root, 2, os.Rename, func(string) error { return injected })
	assert.True(t, renamed)
	assert.ErrorIs(t, err, injected)
	high, readErr := ReadLargestFileID(root)
	require.NoError(t, readErr)
	assert.Equal(t, uint64(2), high)

	root = makeFileIDTestRoot(t, 3)
	renamed, err = writeLargestFileIDAtomic(
		root,
		4,
		func(string, string) error { return injected },
		fsyncDir,
	)
	assert.False(t, renamed)
	assert.ErrorIs(t, err, injected)
	high, readErr = ReadLargestFileID(root)
	require.NoError(t, readErr)
	assert.Equal(t, uint64(3), high)
}

func TestFileIDAllocatorFencesCommitDecidedAndRejectsOverflow(t *testing.T) {
	root := makeFileIDTestRoot(t, 7)
	db := &AccountsDb{AcctsDir: filepath.Join(root, "accounts")}
	db.LargestFileId.Store(7)
	injected := errors.New("injected post-rename failure")
	calls := 0
	db.publishLargestFileID = func(string, uint64) (bool, error) {
		calls++
		return true, injected
	}
	_, err := db.allocateFileID()
	assert.ErrorIs(t, err, ErrLargestFileIDCommitDecided)
	assert.Equal(t, uint64(7), db.LargestFileId.Load())
	_, err = db.allocateFileID()
	assert.ErrorIs(t, err, ErrLargestFileIDCommitDecided)
	assert.Equal(t, 1, calls, "fenced allocator must not attempt another publication")

	db = &AccountsDb{AcctsDir: filepath.Join(root, "accounts")}
	db.LargestFileId.Store(math.MaxUint64)
	db.publishLargestFileID = func(string, uint64) (bool, error) {
		t.Fatal("overflowing allocator called persistence")
		return false, nil
	}
	_, err = db.allocateFileID()
	assert.ErrorIs(t, err, ErrLargestFileIDExhausted)
}
