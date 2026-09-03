package accountsdb

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// openStableRegularFile rejects symlinks and other non-regular objects before
// opening a persistent artifact, then verifies that open did not race a path
// replacement. Callers that perform a long read must call
// validateStableRegularFile after the read as well.
func openStableRegularFile(path string) (*os.File, os.FileInfo, error) {
	return openStableRegularFileWithFlags(path, unix.O_RDONLY)
}

// openStableRegularFileReadWrite is the writable counterpart used for a
// persistent artifact whose recovery may truncate an ordinary torn tail. It
// retains the same no-follow and path/inode identity guarantees as the
// read-only helper; callers should acquire the artifact's advisory lock and
// validateStableRegularFile once more before performing any mutation.
func openStableRegularFileReadWrite(path string) (*os.File, os.FileInfo, error) {
	return openStableRegularFileWithFlags(path, unix.O_RDWR)
}

func openStableRegularFileWithFlags(path string, accessFlags int) (*os.File, os.FileInfo, error) {
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, nil, err
	}
	if !pathInfo.Mode().IsRegular() {
		return nil, nil, fmt.Errorf("persistent artifact %s is not a regular file", path)
	}
	file, openedInfo, err := openRegularFileNoFollowWithFlags(path, accessFlags)
	if err != nil {
		return nil, nil, err
	}
	if !os.SameFile(pathInfo, openedInfo) {
		_ = file.Close()
		return nil, nil, fmt.Errorf("persistent artifact %s changed while opening", path)
	}
	return file, openedInfo, nil
}

// openAtomicSelectorRegularFile opens without following a final-component
// symlink but permits a regular file to be atomically replaced between Lstat
// and open. Either complete inode is a valid selector read.
func openAtomicSelectorRegularFile(path string) (*os.File, os.FileInfo, error) {
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, nil, err
	}
	if !pathInfo.Mode().IsRegular() {
		return nil, nil, fmt.Errorf("persistent selector %s is not a regular file", path)
	}
	return openRegularFileNoFollow(path)
}

func openRegularFileNoFollow(path string) (*os.File, os.FileInfo, error) {
	return openRegularFileNoFollowWithFlags(path, unix.O_RDONLY)
}

func openRegularFileNoFollowWithFlags(path string, accessFlags int) (*os.File, os.FileInfo, error) {
	if accessFlags != unix.O_RDONLY && accessFlags != unix.O_RDWR {
		return nil, nil, fmt.Errorf("unsupported persistent artifact access flags %#x", accessFlags)
	}
	fd, err := unix.Open(path, accessFlags|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, nil, fmt.Errorf("open persistent artifact %s returned an invalid descriptor", path)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, nil, fmt.Errorf("persistent artifact %s is not a regular file", path)
	}
	return file, info, nil
}

// validateStableRegularFile detects replacement, truncation, extension, and
// ordinary in-place modification while an artifact is being read. Persistent
// index readers also hold the store lifetime lock, so this is principally a
// fail-closed guard against corrupt or incorrectly managed directories.
func validateStableRegularFile(file *os.File, path string, openedInfo os.FileInfo) error {
	if file == nil || openedInfo == nil {
		return errors.New("invalid stable regular-file validation state")
	}
	if err := validateOpenedRegularFile(file, openedInfo); err != nil {
		return fmt.Errorf("persistent artifact %s changed while reading: %w", path, err)
	}
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !pathInfo.Mode().IsRegular() || !os.SameFile(openedInfo, pathInfo) {
		return fmt.Errorf("persistent artifact %s changed while reading", path)
	}
	return nil
}

// validateOpenedRegularFile permits an atomic path replacement after open but
// still rejects mutation of the inode actually consumed. This is appropriate
// for selectors whose old and new complete versions are both valid reads.
func validateOpenedRegularFile(file *os.File, openedInfo os.FileInfo) error {
	if file == nil || openedInfo == nil {
		return errors.New("invalid open regular-file validation state")
	}
	finalInfo, err := file.Stat()
	if err != nil {
		return err
	}
	if !finalInfo.Mode().IsRegular() || !os.SameFile(openedInfo, finalInfo) ||
		finalInfo.Size() != openedInfo.Size() ||
		!finalInfo.ModTime().Equal(openedInfo.ModTime()) {
		return errors.New("opened persistent artifact changed while reading")
	}
	return nil
}

// validateRegularFilePathIdentity is the cheap handoff counterpart to a prior
// complete artifact verification.  It proves that path still names the exact
// regular-file inode, size and modification timestamp which was verified.
// Callers must retain their higher-level exclusion lock between the complete
// verification and this check; this helper deliberately does not rehash a
// multi-gigabyte immutable artifact.
func validateRegularFilePathIdentity(path string, verifiedInfo os.FileInfo) error {
	if path == "" || verifiedInfo == nil {
		return errors.New("invalid persistent artifact path identity")
	}
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !pathInfo.Mode().IsRegular() ||
		!os.SameFile(verifiedInfo, pathInfo) ||
		pathInfo.Size() != verifiedInfo.Size() ||
		!pathInfo.ModTime().Equal(verifiedInfo.ModTime()) {
		return fmt.Errorf("persistent artifact %s changed after verification", path)
	}
	return nil
}

// hashStableRegularFile hashes exactly the size observed after a symlink-safe
// open. The one-byte probe prevents a concurrently extended file from turning
// an old prefix hash into a valid identity without allowing an unbounded read.
func hashStableRegularFile(path string) (uint64, [sha256.Size]byte, error) {
	var digest [sha256.Size]byte
	file, info, err := openStableRegularFile(path)
	if err != nil {
		return 0, digest, err
	}
	defer file.Close()
	if info.Size() <= 0 {
		return 0, digest, errors.New("persistent artifact is empty")
	}

	hasher := sha256.New()
	buffer := make([]byte, 1<<20)
	written, err := io.CopyBuffer(hasher, io.LimitReader(file, info.Size()), buffer)
	if err != nil {
		return 0, digest, err
	}
	if written != info.Size() {
		return 0, digest, fmt.Errorf("hashed %d bytes, expected %d", written, info.Size())
	}
	var trailing [1]byte
	n, readErr := file.Read(trailing[:])
	if n != 0 {
		return 0, digest, errors.New("artifact grew while hashing")
	}
	if !errors.Is(readErr, io.EOF) {
		if readErr == nil {
			readErr = io.ErrNoProgress
		}
		return 0, digest, readErr
	}
	if err := validateStableRegularFile(file, path, info); err != nil {
		return 0, digest, err
	}
	copy(digest[:], hasher.Sum(nil))
	return uint64(info.Size()), digest, nil
}
