package snapshot

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

type durableFileOps struct {
	createTemp func(string, string) (*os.File, error)
	syncFile   func(*os.File) error
	closeFile  func(*os.File) error
	rename     func(string, string) error
	remove     func(string) error
	syncDir    func(string) error
}

var defaultDurableFileOps = durableFileOps{
	createTemp: os.CreateTemp,
	syncFile:   (*os.File).Sync,
	closeFile:  (*os.File).Close,
	rename:     os.Rename,
	remove:     os.Remove,
	syncDir:    syncDirectory,
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written <= 0 || written > len(data) {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

// writeFileAtomically durably replaces path. The temporary file is created in
// the same directory, so rename is atomic and the final directory Sync makes
// the name durable across a crash.
func writeFileAtomically(path string, data []byte, permissions os.FileMode) error {
	return writeFileAtomicallyWithOps(path, data, permissions, defaultDurableFileOps)
}

// writeNewFileBeforeDirectoryBarrier publishes a new file atomically without
// replacing an existing name. The caller must Sync the parent directory after
// all files in its batch have been published.
func writeNewFileBeforeDirectoryBarrier(path string, data []byte, permissions os.FileMode) error {
	ops := defaultDurableFileOps
	ops.rename = func(oldPath, newPath string) error {
		return unix.Renameat2(unix.AT_FDCWD, oldPath, unix.AT_FDCWD, newPath, unix.RENAME_NOREPLACE)
	}
	ops.syncDir = func(string) error { return nil }
	return writeFileAtomicallyWithOps(path, data, permissions, ops)
}

func writeFileAtomicallyWithOps(path string, data []byte, permissions os.FileMode, ops durableFileOps) (returnErr error) {
	directory := filepath.Dir(path)
	info, err := os.Lstat(directory)
	if err != nil {
		return fmt.Errorf("inspect destination directory %s: %w", directory, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("destination parent %s is not a real directory", directory)
	}

	temp, err := ops.createTemp(directory, "."+filepath.Base(path)+".tmp-")
	if err != nil {
		return fmt.Errorf("create temporary file for %s: %w", path, err)
	}
	tempPath := temp.Name()
	closed := false
	defer func() {
		if !closed {
			returnErr = errors.Join(returnErr, ops.closeFile(temp))
		}
		if err := ops.remove(tempPath); err != nil && !os.IsNotExist(err) {
			returnErr = errors.Join(returnErr, fmt.Errorf("remove temporary file %s: %w", tempPath, err))
		}
	}()

	if err := temp.Chmod(permissions); err != nil {
		return fmt.Errorf("set permissions on temporary file for %s: %w", path, err)
	}
	if err := writeFull(temp, data); err != nil {
		return fmt.Errorf("write temporary file for %s: %w", path, err)
	}
	if err := ops.syncFile(temp); err != nil {
		return fmt.Errorf("sync temporary file for %s: %w", path, err)
	}
	if err := ops.closeFile(temp); err != nil {
		closed = true
		return fmt.Errorf("close temporary file for %s: %w", path, err)
	}
	closed = true
	if err := ops.rename(tempPath, path); err != nil {
		return fmt.Errorf("publish %s: %w", path, err)
	}
	if err := ops.syncDir(directory); err != nil {
		return fmt.Errorf("sync parent directory after publishing %s: %w", path, err)
	}
	return nil
}

func syncDirectory(path string) (returnErr error) {
	directory, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open directory %s for sync: %w", path, err)
	}
	defer func() {
		returnErr = errors.Join(returnErr, directory.Close())
	}()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync directory %s: %w", path, err)
	}
	return nil
}
