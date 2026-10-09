package util

import "golang.org/x/sys/unix"

// RenameNoReplace atomically publishes a path only if the destination is absent.
// Callers must sync the containing directory to make the rename durable.
func RenameNoReplace(from, to string) error {
	return unix.Renameat2(unix.AT_FDCWD, from, unix.AT_FDCWD, to, unix.RENAME_NOREPLACE)
}
