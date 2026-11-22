//go:build darwin

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
	return censusSignature{uint64(s.Dev), uint64(s.Ino), uint64(info.Mode()), uint64(s.Nlink), uint64(info.Size()), uint64(s.Blocks), uint64(s.Mtimespec.Sec)*1e9 + uint64(s.Mtimespec.Nsec), uint64(s.Ctimespec.Sec)*1e9 + uint64(s.Ctimespec.Nsec)}, true
}

func censusMetadataOpenFlags() int {
	return unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_NONBLOCK | unix.O_CLOEXEC
}
func censusRootOpenFlags() int { return censusMetadataOpenFlags() | unix.O_DIRECTORY }

// No equivalent per-mount identity is established here. Native observation is
// unavailable on Darwin; the filesystem algorithm can still be checked locally.
func physicalCensusMountIdentity(*os.File) (uint64, bool) { return 0, false }
