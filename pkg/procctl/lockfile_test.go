package procctl

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Lockfile — flock-based single-instance guard. Contention is exercised
// within-process via a second fd (different OFD) on the same path.

func TestAcquireLock_HappyPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mithril.lock")

	lh, err := AcquireLock(path)
	require.NoError(t, err)
	require.NotNil(t, lh)
	require.NoError(t, lh.Release())

	// flock is advisory; the file persists after Release.
	_, err = os.Stat(path)
	assert.NoError(t, err)
}

// Lockfile (and parent dir) is created on demand; callers needn't MkdirAll.
func TestAcquireLock_CreatesLockFileIfMissing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "mithril.lock")

	lh, err := AcquireLock(path)
	require.NoError(t, err)
	defer lh.Release()

	info, err := os.Stat(path)
	require.NoError(t, err)
	// flock is on the inode, not content — file stays empty.
	assert.Equal(t, int64(0), info.Size())
}

// Second AcquireLock on a held path returns ErrLocked — the single-instance guard.
func TestAcquireLock_ContentionFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mithril.lock")

	first, err := AcquireLock(path)
	require.NoError(t, err)
	defer first.Release()

	second, err := AcquireLock(path)
	assert.Nil(t, second)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrLocked), "second acquire should return ErrLocked, got %v", err)
}

// Lock is re-acquirable after Release (stop-then-start-again path).
func TestAcquireLock_ReleaseAllowsReacquire(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mithril.lock")

	first, err := AcquireLock(path)
	require.NoError(t, err)
	require.NoError(t, first.Release())

	second, err := AcquireLock(path)
	require.NoError(t, err, "after Release the lock should be re-acquirable")
	require.NoError(t, second.Release())
}

// Double-Release is a silent no-op, so deferred Release() can't double-error.
func TestRelease_IsIdempotent(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mithril.lock")

	lh, err := AcquireLock(path)
	require.NoError(t, err)
	require.NoError(t, lh.Release())
	assert.NoError(t, lh.Release(), "second Release should be a silent no-op")
}

// TestLock reports false on an unheld lockfile — Detect uses this to spot stale PID files.
func TestTestLock_ReportsUnlocked(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mithril.lock")

	locked, err := TestLock(path)
	require.NoError(t, err)
	assert.False(t, locked)
}

func TestTestLock_ReportsLocked(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mithril.lock")

	lh, err := AcquireLock(path)
	require.NoError(t, err)
	defer lh.Release()

	locked, err := TestLock(path)
	require.NoError(t, err)
	assert.True(t, locked)
}

// Probing an absent lockfile reports unlocked, not an error.
func TestTestLock_AbsentFileIsUnlocked(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "never-created.lock")

	locked, err := TestLock(path)
	require.NoError(t, err)
	assert.False(t, locked)
}
