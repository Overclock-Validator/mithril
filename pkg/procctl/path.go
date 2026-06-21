// Package procctl provides Mithril's process-control primitives: single-instance
// enforcement (lock + PID file), stale-PID detection, signalling, and an audit log.
package procctl

import (
	"fmt"
	"os"
	"path/filepath"
)

const (
	// dirName is the per-user subdirectory holding the PID file, lock file,
	// and audit log. Created with 0700.
	dirName = "mithril"

	// pidFileBase is the JSON file recording the running process's identity.
	pidFileBase = "mithril.pid"

	// lockFileBase holds the flock. Separate from the PID file so the PID
	// file can be rewritten without disturbing the lock.
	lockFileBase = "mithril.lock"

	// auditLogBase logs every control action as key=value lines. Never rotated.
	auditLogBase = "control.audit"

	// envOverride points all three files at one path; its directory holds
	// the lock and audit log too.
	envOverride = "MITHRIL_PID_FILE"
)

// DefaultPidFile resolves the PID file path, in precedence order:
//  1. $MITHRIL_PID_FILE
//  2. $XDG_STATE_HOME/mithril/mithril.pid
//  3. $HOME/.local/state/mithril/mithril.pid (default)
//  4. $XDG_RUNTIME_DIR/mithril/mithril.pid (only when HOME is unset)
//  5. /tmp/mithril/mithril.pid (last resort)
//
// $XDG_RUNTIME_DIR is avoided by default (logind wipes it on logout). Stale
// files are safe — Matches() rejects them.
func DefaultPidFile() string {
	if v := os.Getenv(envOverride); v != "" {
		return v
	}
	if v := os.Getenv("XDG_STATE_HOME"); v != "" {
		return filepath.Join(v, dirName, pidFileBase)
	}
	if home, _ := os.UserHomeDir(); home != "" {
		return filepath.Join(home, ".local", "state", dirName, pidFileBase)
	}
	// No HOME (some containers/init): runtime dir if present, else /tmp. Both are
	// ephemeral, so MITHRIL_PID_FILE is preferred there.
	if v := os.Getenv("XDG_RUNTIME_DIR"); v != "" {
		return filepath.Join(v, dirName, pidFileBase)
	}
	return filepath.Join("/tmp", dirName, pidFileBase)
}

// DefaultLockFile returns the lock file path co-located with DefaultPidFile.
// The lock file is always empty; only its flock state matters.
func DefaultLockFile() string {
	return filepath.Join(filepath.Dir(DefaultPidFile()), lockFileBase)
}

// DefaultAuditLog returns the audit log path co-located with DefaultPidFile.
// The audit log is append-only and never rotated.
func DefaultAuditLog() string {
	return filepath.Join(filepath.Dir(DefaultPidFile()), auditLogBase)
}

// EnsurePidDir creates dir as 0700 when missing. It never chmods an existing
// dir (MITHRIL_PID_FILE may point at a shared dir like /tmp); files stay 0600.
func EnsurePidDir(dir string) error {
	if info, err := os.Stat(dir); err == nil {
		if !info.IsDir() {
			return fmt.Errorf("pid path parent %s is not a directory", dir)
		}
		return nil
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("stat pid dir %s: %w", dir, err)
	}

	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create pid dir %s: %w", dir, err)
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return fmt.Errorf("chmod pid dir %s to 0700: %w", dir, err)
	}
	return nil
}
