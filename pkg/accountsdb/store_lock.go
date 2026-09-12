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
	// AccountsDbLockFileName is a stable, persistent inode used to
	// serialize every AccountsDB owner and destructive store-wide operation.
	// It must never be unlinked during ordinary cleanup: replacing a locked
	// inode would let two processes believe they exclusively own the store.
	AccountsDbLockFileName = "accountsdb.lock"
)

var ErrAccountsDbInUse = errors.New("accountsdb: AccountsDB store is in use")

type accountsDbStoreLock struct {
	file *os.File
}

// AccountsDbStoreGuard is an exclusive store lock whose lifetime
// can span a multi-stage snapshot bootstrap.  The ordinary callback API is
// intentionally insufficient for bootstrap: releasing it after cleanup would
// let another builder remove in-progress files or an opener observe a partly
// published store.
//
// A successful AccountsDB open transfers the underlying lock into the
// returned database. Close is therefore idempotent and becomes a no-op after a
// transfer.
type AccountsDbStoreGuard struct {
	mu   sync.Mutex
	root string
	lock *accountsDbStoreLock
}

// AcquireExclusiveAccountsDbStore acquires a checked, nonblocking
// exclusive guard for a complete multi-stage operation.
func AcquireExclusiveAccountsDbStore(root string) (*AccountsDbStoreGuard, error) {
	if root == "" {
		return nil, errors.New("accountsdb: empty AccountsDB lock root")
	}
	if err := RejectUnsupportedIndexArtifacts(root); err != nil {
		return nil, err
	}
	canonicalRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: resolve AccountsDB lock root: %w", err)
	}
	lock, err := acquireAccountsDbStoreLock(canonicalRoot, true)
	if err != nil {
		return nil, err
	}
	return &AccountsDbStoreGuard{
		root: filepath.Clean(canonicalRoot),
		lock: lock,
	}, nil
}

func (guard *AccountsDbStoreGuard) validateRootLocked(root string) error {
	if guard == nil {
		return errors.New("accountsdb: nil AccountsDB store guard")
	}
	if guard.lock == nil {
		return errors.New("accountsdb: AccountsDB store guard is closed or transferred")
	}
	canonicalRoot, err := filepath.Abs(root)
	if err != nil {
		return fmt.Errorf("accountsdb: resolve guarded AccountsDB root: %w", err)
	}
	if filepath.Clean(canonicalRoot) != guard.root {
		return fmt.Errorf(
			"accountsdb: AccountsDB store guard is for %s, not %s",
			guard.root, filepath.Clean(canonicalRoot),
		)
	}
	return nil
}

// withAccountsDbStoreLock borrows the underlying lock for an
// operation which must not acquire it recursively. The guard mutex remains
// held so Close or transfer cannot race the borrower.
func (guard *AccountsDbStoreGuard) withAccountsDbStoreLock(
	root string,
	action func(*accountsDbStoreLock) error,
) error {
	if action == nil {
		return errors.New("accountsdb: nil guarded AccountsDB action")
	}
	if guard == nil {
		return errors.New("accountsdb: nil AccountsDB store guard")
	}
	guard.mu.Lock()
	defer guard.mu.Unlock()
	if err := guard.validateRootLocked(root); err != nil {
		return err
	}
	return action(guard.lock)
}

// transferAccountsDbStoreLockOnSuccess runs action while the guard
// remains indivisibly held and transfers ownership only if the complete action
// succeeds. The action must install the borrowed lock in its returned owner
// before reporting nil. On failure the guard remains live for caller unwind or
// retry.
func (guard *AccountsDbStoreGuard) transferAccountsDbStoreLockOnSuccess(
	root string,
	action func(*accountsDbStoreLock) error,
) error {
	if action == nil {
		return errors.New("accountsdb: nil AccountsDB transfer action")
	}
	if guard == nil {
		return errors.New("accountsdb: nil AccountsDB store guard")
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

// Close releases a guard which has not been transferred to an opened database.
func (guard *AccountsDbStoreGuard) Close() error {
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

func acquireAccountsDbStoreLock(
	root string,
	exclusive bool,
) (*accountsDbStoreLock, error) {
	if root == "" {
		return nil, errors.New("accountsdb: empty AccountsDB lock root")
	}
	rootInfo, err := os.Lstat(root)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: stat AccountsDB lock root: %w", err)
	}
	if !rootInfo.IsDir() {
		return nil, errors.New("accountsdb: AccountsDB lock root is not a real directory")
	}

	path := filepath.Join(root, AccountsDbLockFileName)
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o644)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: open AccountsDB store lock: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("accountsdb: wrap AccountsDB store lock")
	}
	closeOnError := true
	defer func() {
		if closeOnError {
			_ = file.Close()
		}
	}()

	openedInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("accountsdb: stat opened AccountsDB store lock: %w", err)
	}
	if !openedInfo.Mode().IsRegular() {
		return nil, errors.New("accountsdb: AccountsDB store lock is not a regular file")
	}
	operation := unix.LOCK_SH | unix.LOCK_NB
	if exclusive {
		operation = unix.LOCK_EX | unix.LOCK_NB
	}
	if err := unix.Flock(fd, operation); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) || errors.Is(err, unix.EAGAIN) {
			return nil, fmt.Errorf("%w: %s", ErrAccountsDbInUse, path)
		}
		return nil, fmt.Errorf("accountsdb: lock AccountsDB store: %w", err)
	}
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("accountsdb: restat AccountsDB store lock: %w", err)
	}
	if !pathInfo.Mode().IsRegular() || !os.SameFile(openedInfo, pathInfo) {
		return nil, errors.New("accountsdb: AccountsDB store lock path changed while locking")
	}

	closeOnError = false
	return &accountsDbStoreLock{file: file}, nil
}

func (lock *accountsDbStoreLock) Close() error {
	if lock == nil || lock.file == nil {
		return nil
	}
	file := lock.file
	lock.file = nil
	return errors.Join(unix.Flock(int(file.Fd()), unix.LOCK_UN), file.Close())
}

// WithExclusiveAccountsDbStore serializes destructive offline
// work, such as snapshot replacement, against initialization and the complete
// lifetime of an opened AccountsDB. The persistent lockfile itself must not be
// removed by action.
func WithExclusiveAccountsDbStore(root string, action func() error) (retErr error) {
	if action == nil {
		return errors.New("accountsdb: nil exclusive account-index store action")
	}
	lock, err := acquireAccountsDbStoreLock(root, true)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, lock.Close()) }()
	return action()
}
