//go:build linux

package accountsdb

import (
	"golang.org/x/sys/unix"
	"os"
	"syscall"
)

func physicalCensusSignature(info os.FileInfo) (censusSignature, bool) {
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok || s.Blocks < 0 || info.Size() < 0 {
		return censusSignature{}, false
	}
	return censusSignature{uint64(s.Dev), uint64(s.Ino), uint64(info.Mode()), uint64(s.Nlink), uint64(info.Size()), uint64(s.Blocks), uint64(s.Mtim.Sec)*1e9 + uint64(s.Mtim.Nsec), uint64(s.Ctim.Sec)*1e9 + uint64(s.Ctim.Nsec)}, true
}

func censusMetadataOpenFlags() int { return unix.O_PATH | unix.O_NOFOLLOW | unix.O_CLOEXEC }
func censusRootOpenFlags() int     { return censusMetadataOpenFlags() | unix.O_DIRECTORY }
func physicalCensusMountIdentity(file *os.File) (uint64, bool) {
	var stat unix.Statx_t
	if unix.Statx(int(file.Fd()), "", unix.AT_EMPTY_PATH|unix.AT_STATX_DONT_SYNC, unix.STATX_MNT_ID, &stat) != nil || stat.Mask&unix.STATX_MNT_ID == 0 || stat.Mnt_id == 0 {
		return 0, false
	}
	return stat.Mnt_id, true
}
