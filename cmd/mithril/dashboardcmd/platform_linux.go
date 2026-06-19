//go:build linux

package dashboardcmd

import "syscall"

// cleanupOrphanLightbringer is a no-op on Linux: Pdeathsig makes the kernel
// SIGTERM the child when mithril is force-killed.
func cleanupOrphanLightbringer() {}

// spawnSysProcAttr detaches the `mithril run` child via Setsid (new session) so
// it outlives the dashboard. Pdeathsig is left unset.
func spawnSysProcAttr() *syscall.SysProcAttr {
	return &syscall.SysProcAttr{Setsid: true}
}

// statFSType returns the filesystem's f_type magic number (e.g. NFS_SUPER_MAGIC),
// used to refuse PID dirs on network filesystems where flock is unreliable.
func statFSType(path string) (int64, bool) {
	var s syscall.Statfs_t
	if err := syscall.Statfs(path, &s); err != nil {
		return 0, false
	}
	// uint32 cast keeps high-bit magics (CIFS, SMB2) positive; a direct int64
	// would sign-extend them to negative on 32-bit Linux.
	return int64(uint32(s.Type)), true
}
