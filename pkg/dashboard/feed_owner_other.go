//go:build !unix

package dashboard

import "os"

// Ownership cannot be checked here, so the private-directory check fails
// closed and stale sockets are never removed.
func ownedByEffectiveUser(os.FileInfo) bool { return false }

func isConnectionRefused(error) bool { return false }
