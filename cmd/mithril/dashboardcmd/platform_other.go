//go:build !linux

package dashboardcmd

import "syscall"

// cleanupOrphanLightbringer is a no-op off Linux: no safe PID identity to prove
// which orphan is ours. Cleanup is left to an explicit operator step.
func cleanupOrphanLightbringer() {}

// spawnSysProcAttr detaches the child via Setsid (supported on macOS/BSD too).
func spawnSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setsid: true}
}

// statFSType: no-op off Linux (no portable filesystem-magic access).
func statFSType(path string) (int64, bool) {
	_ = path
	return 0, false
}
