package mlog

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fresh dir with no "latest" yet: swap creates one pointing at target.
func TestSwapLatestSymlink_CreatesNewSymlink(t *testing.T) {
	dir := t.TempDir()
	target := "run-20260518-abc123"
	require.NoError(t, os.MkdirAll(filepath.Join(dir, target), 0755))

	err := swapLatestSymlink(dir, target, "abc12345")
	require.NoError(t, err)

	got, err := os.Readlink(filepath.Join(dir, "latest"))
	require.NoError(t, err)
	assert.Equal(t, target, got)
}

// an existing "latest" symlink is replaced, not errored on.
func TestSwapLatestSymlink_ReplacesExistingSymlink(t *testing.T) {
	dir := t.TempDir()
	targetA := "run-A"
	targetB := "run-B"
	require.NoError(t, os.MkdirAll(filepath.Join(dir, targetA), 0755))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, targetB), 0755))

	require.NoError(t, swapLatestSymlink(dir, targetA, "aaaa1111"))
	require.NoError(t, swapLatestSymlink(dir, targetB, "bbbb2222"))

	got, err := os.Readlink(filepath.Join(dir, "latest"))
	require.NoError(t, err)
	assert.Equal(t, targetB, got, "second swap should win")
}

func TestErrorfFlushesFileBeforeShutdown(t *testing.T) {
	Shutdown()
	t.Cleanup(Shutdown)

	dir := t.TempDir()
	require.NoError(t, Initialize(LogConfig{
		Dir:        dir,
		Level:      "debug",
		ToStdout:   false,
		MaxSizeMB:  100,
		MaxAgeDays: 1,
		MaxBackups: 1,
	}, "run123456"))

	Log.Errorf("panic marker before process abort")

	body, err := os.ReadFile(GetLogPath())
	require.NoError(t, err)
	assert.Contains(t, string(body), "panic marker before process abort")
}

// the .tmp.<runid> sentinel is gone after a successful swap (renamed away).
func TestSwapLatestSymlink_CleansUpTempFile(t *testing.T) {
	dir := t.TempDir()
	target := "run-X"
	shortRunID := "xxxx9999"
	require.NoError(t, os.MkdirAll(filepath.Join(dir, target), 0755))

	require.NoError(t, swapLatestSymlink(dir, target, shortRunID))

	tmpPath := filepath.Join(dir, "latest.tmp."+shortRunID)
	_, err := os.Lstat(tmpPath)
	assert.True(t, os.IsNotExist(err), "tmp symlink should be renamed away (got err: %v)", err)
}

// a leftover tmp from a prior crashed swap doesn't block a fresh swap.
func TestSwapLatestSymlink_RecoversFromStaleTmp(t *testing.T) {
	dir := t.TempDir()
	target := "run-Z"
	shortRunID := "zzzz0000"
	require.NoError(t, os.MkdirAll(filepath.Join(dir, target), 0755))
	// Simulate a prior crash that left the tmp around
	require.NoError(t, os.Symlink("some-old-run", filepath.Join(dir, "latest.tmp."+shortRunID)))

	require.NoError(t, swapLatestSymlink(dir, target, shortRunID))

	got, err := os.Readlink(filepath.Join(dir, "latest"))
	require.NoError(t, err)
	assert.Equal(t, target, got)
}

// Concurrent reader against rapid swaps: the atomic-rename swap must never expose
// ENOENT. On Linux otherErrs must be zero; macOS has a tolerated EINVAL window.
func TestSwapLatestSymlink_NoEnoentWindow(t *testing.T) {
	if testing.Short() {
		t.Skip("concurrent stress test; skipped in -short mode")
	}

	dir := t.TempDir()
	targets := []string{"run-A", "run-B", "run-C", "run-D"}
	for _, target := range targets {
		require.NoError(t, os.MkdirAll(filepath.Join(dir, target), 0755))
	}
	// Seed the symlink so the very first read in the loop has something.
	require.NoError(t, swapLatestSymlink(dir, targets[0], "seed0000"))

	var (
		stop      atomic.Bool
		wg        sync.WaitGroup
		enoent    atomic.Int64
		otherErrs atomic.Int64
		reads     atomic.Int64
	)

	// Reader: tight loop reading the symlink.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for !stop.Load() {
			reads.Add(1)
			_, err := os.Readlink(filepath.Join(dir, "latest"))
			if err == nil {
				continue
			}
			if os.IsNotExist(err) {
				enoent.Add(1)
			} else {
				otherErrs.Add(1)
			}
		}
	}()

	// Writer: rapid swaps with distinct runIDs to exercise the tmp path.
	for i := 0; i < 500; i++ {
		require.NoError(t, swapLatestSymlink(dir, targets[i%len(targets)],
			"run"+string(rune('0'+i%10))+"000000"))
	}
	time.Sleep(20 * time.Millisecond) // let reader sample post-last-swap
	stop.Store(true)
	wg.Wait()

	t.Logf("reads=%d enoent=%d other=%d", reads.Load(), enoent.Load(), otherErrs.Load())
	assert.Equal(t, int64(0), enoent.Load(),
		"atomic-rename swap must never expose ENOENT to concurrent readers")
	if runtime.GOOS == "darwin" {
		// macOS rename-over-symlink briefly exposes a non-symlink (EINVAL); tolerated.
		t.Logf("darwin: tolerated %d transient non-ENOENT reads (rename-over-symlink quirk)", otherErrs.Load())
	} else {
		assert.Equal(t, int64(0), otherErrs.Load(),
			"atomic-rename swap must not expose any broken (non-symlink) reads")
	}
}
