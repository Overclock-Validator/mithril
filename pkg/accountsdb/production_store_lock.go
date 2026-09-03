package accountsdb

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"
)

const (
	// ProductionAccountIndexLockFileName is a stable, persistent inode used to
	// serialize every V2 selector owner and destructive store-wide operation.
	// It must never be unlinked during ordinary cleanup: replacing a locked
	// inode would let two processes believe they exclusively own the store.
	ProductionAccountIndexLockFileName = "accounts_index_v2.lock"
)

var ErrProductionAccountIndexInUse = errors.New("accountsdb: production account-index store is in use")

type productionAccountIndexStoreLock struct {
	file *os.File
}

// ProductionAccountIndexStoreGuard is an exclusive store lock whose lifetime
// can span a multi-stage snapshot bootstrap.  The ordinary callback API is
// intentionally insufficient for bootstrap: releasing it after cleanup would
// let another builder remove in-progress files or an opener observe a partly
// published store.
//
// A successful production-index open transfers the underlying lock into the
// returned index. Close is therefore idempotent and becomes a no-op after a
// transfer.
type ProductionAccountIndexStoreGuard struct {
	mu   sync.Mutex
	root string
	lock *productionAccountIndexStoreLock
}

// AcquireExclusiveProductionAccountIndexStore acquires a checked, nonblocking
// exclusive guard for a complete multi-stage operation.
func AcquireExclusiveProductionAccountIndexStore(root string) (*ProductionAccountIndexStoreGuard, error) {
	if root == "" {
		return nil, errors.New("accountsdb: empty production account-index lock root")
	}
	canonicalRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: resolve production account-index lock root: %w", err)
	}
	lock, err := acquireProductionAccountIndexStoreLock(canonicalRoot, true)
	if err != nil {
		return nil, err
	}
	return &ProductionAccountIndexStoreGuard{
		root: filepath.Clean(canonicalRoot),
		lock: lock,
	}, nil
}

func (guard *ProductionAccountIndexStoreGuard) validateRootLocked(root string) error {
	if guard == nil {
		return errors.New("accountsdb: nil production account-index store guard")
	}
	if guard.lock == nil {
		return errors.New("accountsdb: production account-index store guard is closed or transferred")
	}
	canonicalRoot, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("accountsdb: resolve guarded production account-index root: %w", err)
	}
	if filepath.Clean(canonicalRoot) != guard.root {
		return fmt.Errorf(
			"accountsdb: production account-index store guard is for %s, not %s",
			guard.root, filepath.Clean(canonicalRoot),
		)
	}
	return nil
}

// withProductionAccountIndexStoreLock borrows the underlying lock for an
// operation which must not acquire it recursively. The guard mutex remains
// held so Close or transfer cannot race the borrower.
func (guard *ProductionAccountIndexStoreGuard) withProductionAccountIndexStoreLock(
	root string,
	action func(*productionAccountIndexStoreLock) error,
) error {
	if action == nil {
		return errors.New("accountsdb: nil guarded production account-index action")
	}
	if guard == nil {
		return errors.New("accountsdb: nil production account-index store guard")
	}
	guard.mu.Lock()
	defer guard.mu.Unlock()
	if err := guard.validateRootLocked(root); err != nil {
		return err
	}
	return action(guard.lock)
}

// transferProductionAccountIndexStoreLockOnSuccess runs action while the guard
// remains indivisibly held and transfers ownership only if the complete action
// succeeds. The action must install the borrowed lock in its returned owner
// before reporting nil. On failure the guard remains live for caller unwind or
// retry.
func (guard *ProductionAccountIndexStoreGuard) transferProductionAccountIndexStoreLockOnSuccess(
	root string,
	action func(*productionAccountIndexStoreLock) error,
) error {
	if action == nil {
		return errors.New("accountsdb: nil production account-index transfer action")
	}
	if guard == nil {
		return errors.New("accountsdb: nil production account-index store guard")
	}
	guard.mu.Lock()
	defer guard.mu.Unlock()
	if err := guard.validateRootLocked(root); err != nil {
		return err
	}
	if err := action(guard.lock); err != nil {
		return err
	}
	guard.lock = nil
	return nil
}

// Close releases a guard which has not been transferred to an opened index.
func (guard *ProductionAccountIndexStoreGuard) Close() error {
	if guard == nil {
		return nil
	}
	guard.mu.Lock()
	defer guard.mu.Unlock()
	lock := guard.lock
	guard.lock = nil
	if lock == nil {
		return nil
	}
	return lock.Close()
}

func acquireProductionAccountIndexStoreLock(
	root string,
	exclusive bool,
) (*productionAccountIndexStoreLock, error) {
	if root == "" {
		return nil, errors.New("accountsdb: empty production account-index lock root")
	}
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: stat production account-index lock root: %w", err)
	}
	if !rootInfo.IsDir() {
		return nil, errors.New("accountsdb: production account-index lock root is not a real directory")
	}

	path := filepath.Join(root, ProductionAccountIndexLockFileName)
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o644)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: open production account-index store lock: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("accountsdb: wrap production account-index store lock")
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = file.Close()
		}
	}()

	openedInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("accountsdb: stat opened production account-index store lock: %w", err)
	}
	if !openedInfo.Mode().IsRegular() {
		return nil, errors.New("accountsdb: production account-index store lock is not a regular file")
	}
	operation := unix.LOCK_SH | unix.LOCK_NB
	if exclusive {
		operation = unix.LOCK_EX | unix.LOCK_NB
	}
	if err := unix.Flock(fd, operation); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, fmt.Errorf("%w: %s", ErrProductionAccountIndexInUse, path)
		}
		return nil, fmt.Errorf("accountsdb: lock production account-index store: %w", err)
	}
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: restat production account-index store lock: %w", err)
	}
	if !pathInfo.Mode().IsRegular() || !os.SameFile(openedInfo, pathInfo) {
		return nil, errors.New("accountsdb: production account-index store lock path changed while locking")
	}

	closeOnError = false
	return &productionAccountIndexStoreLock{file: file}, nil
}

func (lock *productionAccountIndexStoreLock) Close() error {
	if lock == nil || lock.file == nil {
		return nil
	}
	file := lock.file
	lock.file = nil
	return errors.Join(unix.Flock(int(file.Fd()), unix.LOCK_UN), file.Close())
}

// WithExclusiveProductionAccountIndexStore serializes destructive offline
// work, such as snapshot replacement, against initialization and the complete
// lifetime of an opened V2 index. The persistent lockfile itself must not be
// removed by action.
func WithExclusiveProductionAccountIndexStore(root string, action func() error) (retErr error) {
	if action == nil {
		return errors.New("accountsdb: nil exclusive account-index store action")
	}
	lock, err := acquireProductionAccountIndexStoreLock(root, true)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, lock.Close()) }()
	return action()
}
