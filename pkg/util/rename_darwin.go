package util

import "golang.org/x/sys/unix"

// RenameNoReplace atomically publishes a path only if the destination is absent.
// Callers must sync the containing directory to make the rename durable.
func RenameNoReplace(from, to string) error { return unix.RenamexNp(from, to, unix.RENAME_EXCL) }
