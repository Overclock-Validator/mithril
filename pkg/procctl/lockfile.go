package procctl

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// ErrLocked means the lock is held by another process (or another OFD).
// Callers use errors.Is to detect the "already running" case.
var ErrLocked = errors.New("lock already held")

// LockHandle owns an active flock. Release is idempotent. NOT safe for
// concurrent use: acquire and release on the same goroutine.
type LockHandle struct {
	f        *os.File
	released sync.Once
}

// AcquireLock opens path (0600 if absent) and takes a non-blocking exclusive
// flock, returning ErrLocked if another OFD holds it. The lock is per-OFD.
func AcquireLock(path string) (*LockHandle, error) {
	if err := EnsurePidDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("open lockfile %s: %w", path, err)
	}

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, fmt.Errorf("%s: %w", path, ErrLocked)
		}
		return nil, fmt.Errorf("flock %s: %w", path, err)
	}
	return &LockHandle{f: f}, nil
}

// Release unlocks and closes the descriptor. Only the first call acts; only
// it can return an error.
func (lh *LockHandle) Release() error {
	if lh == nil || lh.f == nil {
		return nil
	}
	var releaseErr error
	lh.released.Do(func() {
		// Unflock then close; close alone would also release the lock.
		if err := syscall.Flock(int(lh.f.Fd()), syscall.LOCK_UN); err != nil {
			releaseErr = fmt.Errorf("unflock: %w", err)
		}
		if err := lh.f.Close(); err != nil && releaseErr == nil {
			releaseErr = fmt.Errorf("close lockfile: %w", err)
		}
		lh.f = nil
	})
	return releaseErr
}

// TestLock probes whether path is flock-held: (true, nil) held, (false, nil)
// free, (false, err) on I/O failure. An absent lockfile reads as unlocked.
func TestLock(path string) (bool, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("open lockfile %s for probe: %w", path, err)
	}
	defer f.Close()

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return true, nil // held by someone else
		}
		return false, fmt.Errorf("probe flock %s: %w", path, err)
	}
	// Free — release immediately; a probe-only release error is benign.
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return false, nil
}
