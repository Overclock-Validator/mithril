//go:build linux

package metrics

import (
	"io"
	"os"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

func readCgroup() (CgroupObservation, cgroupBinding, bool) {
	o, binding := readCgroupFiles(func(name string, limit int) ([]byte, error) {
		return readHostFile(name, limit, false)
	}, openCgroup, time.Now)
	return o, binding, true
}

func openCgroup(location cgroupLocation) (cgroupAccess, error) {
	mount, err := openCgroupDirectory(location.mountPoint)
	if err != nil {
		return cgroupAccess{}, err
	}
	defer mount.Close()
	dir, err := openCgroupDirectory(location.directory)
	if err != nil {
		return cgroupAccess{}, err
	}
	binding, err := cgroupDirectoryBinding(location, mount, dir)
	if err != nil {
		dir.Close()
		return cgroupAccess{}, err
	}
	return cgroupAccess{
		binding: binding,
		read:    func(name string) ([]byte, error) { return readCgroupFile(dir, name) },
		validate: func() bool {
			currentMount, err := openCgroupDirectory(location.mountPoint)
			if err != nil {
				return false
			}
			defer currentMount.Close()
			currentDir, err := openCgroupDirectory(location.directory)
			if err != nil {
				return false
			}
			defer currentDir.Close()
			current, err := cgroupDirectoryBinding(location, currentMount, currentDir)
			return err == nil && current == binding
		},
		close: func() { dir.Close() },
	}, nil
}

func openCgroupDirectory(name string) (*os.File, error) {
	if !validCgroupPath(name) {
		return nil, errCgroupRecord
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	for part := range strings.SplitSeq(strings.TrimPrefix(name, "/"), "/") {
		if part == "" {
			continue
		}
		next, err := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
		unix.Close(fd)
		if err != nil {
			return nil, err
		}
		fd = next
	}
	return os.NewFile(uintptr(fd), name), nil
}

func cgroupDirectoryBinding(location cgroupLocation, mount, dir *os.File) (cgroupBinding, error) {
	var mountStat, stat unix.Stat_t
	var mountFS, fs unix.Statfs_t
	if unix.Fstat(int(mount.Fd()), &mountStat) != nil || unix.Fstat(int(dir.Fd()), &stat) != nil ||
		unix.Fstatfs(int(mount.Fd()), &mountFS) != nil || unix.Fstatfs(int(dir.Fd()), &fs) != nil ||
		mountFS.Type != unix.CGROUP2_SUPER_MAGIC || fs.Type != unix.CGROUP2_SUPER_MAGIC || mountStat.Dev != stat.Dev {
		return cgroupBinding{}, errCgroupRecord
	}
	return cgroupBinding{location: location, mountDevice: uint64(mountStat.Dev), mountInode: mountStat.Ino, device: uint64(stat.Dev), inode: stat.Ino}, nil
}

func readCgroupFile(dir *os.File, name string) ([]byte, error) {
	if name == "" || strings.ContainsAny(name, "/\x00") || name == "." || name == ".." {
		return nil, errCgroupRecord
	}
	fd, err := unix.Openat(int(dir.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), name)
	defer f.Close()
	stat, err := f.Stat()
	if err != nil || !stat.Mode().IsRegular() {
		return nil, errCgroupRecord
	}
	data, err := io.ReadAll(io.LimitReader(f, cgroupFileLimit+1))
	if err != nil || len(data) > cgroupFileLimit {
		return nil, errCgroupRecord
	}
	return data, nil
}
