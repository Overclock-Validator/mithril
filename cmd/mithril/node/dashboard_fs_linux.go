package node

import "syscall"

func filesystemBlockSize(stat syscall.Statfs_t) uint64 {
	if stat.Frsize > 0 {
		return uint64(stat.Frsize)
	}
	if stat.Frsize < 0 {
		return 0
	}
	return uint64(stat.Bsize)
}
