//go:build linux || darwin

package config

import "syscall"

// freeBytesForFS reports free bytes via statfs: Bavail * Bsize. Both cast through
// uint64 since field types differ across OS.
func freeBytesForFS(path string) (uint64, bool) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, false
	}
	return uint64(st.Bavail) * uint64(st.Bsize), true
}

// writableFS checks write access via access(2). 0x2 is W_OK (same on Linux/Darwin).
func writableFS(path string) bool {
	return syscall.Access(path, 0x2) == nil
}

// deviceID returns the filesystem device ID, used to tell whether two paths share a disk.
func deviceID(path string) (uint64, bool) {
	var st syscall.Stat_t
	if err := syscall.Stat(path, &st); err != nil {
		return 0, false
	}
	return uint64(st.Dev), true
}
