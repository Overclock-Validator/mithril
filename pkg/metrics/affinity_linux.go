//go:build linux

package metrics

import (
	"io"
	"os"
	"slices"
	"strconv"
	"time"

	"golang.org/x/sys/unix"
)

func readThreadAffinity(topology TopologyObservation) ThreadAffinityObservation {
	return readAffinityFiles(topology, readHostFile, openAffinityTasks, time.Now)
}

func openAffinityTasks(c *topologyCapture) (affinityTaskAccess, error) {
	if err := affinityOpen(c); err != nil {
		return affinityTaskAccess{}, err
	}
	fd, err := unix.Open("/proc/self/task", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if !affinityCheck(c) {
		if err == nil {
			unix.Close(fd)
		}
		return affinityTaskAccess{}, c.failureError()
	}
	if err != nil {
		return affinityTaskAccess{}, err
	}
	dir := os.NewFile(uintptr(fd), "own-task-directory")
	if _, err := affinityFDIdentity(c, fd, unix.S_IFDIR); err != nil {
		dir.Close()
		return affinityTaskAccess{}, err
	}
	return affinityTaskAccess{
		list:  func() ([]affinityThread, error) { return listAffinityTasks(c, dir) },
		read:  func(thread affinityThread) ([]byte, error) { return readAffinityTask(c, dir, thread) },
		close: func() { dir.Close() },
	}, nil
}

func affinityFDIdentity(c *topologyCapture, fd int, mode uint32) (affinityThread, error) {
	if !affinityCheck(c) {
		return affinityThread{}, c.failureError()
	}
	var stat unix.Stat_t
	err := unix.Fstat(fd, &stat)
	if !affinityCheck(c) {
		return affinityThread{}, c.failureError()
	}
	if err != nil {
		return affinityThread{}, err
	}
	if stat.Mode&unix.S_IFMT != mode || stat.Ino == 0 {
		return affinityThread{}, errTopologyRecord
	}
	if !affinityCheck(c) {
		return affinityThread{}, c.failureError()
	}
	var fs unix.Statfs_t
	err = unix.Fstatfs(fd, &fs)
	if !affinityCheck(c) {
		return affinityThread{}, c.failureError()
	}
	if err != nil {
		return affinityThread{}, err
	}
	if fs.Type != unix.PROC_SUPER_MAGIC {
		return affinityThread{}, errTopologyRecord
	}
	return affinityThread{device: uint64(stat.Dev), inode: stat.Ino}, nil
}

func listAffinityTasks(c *topologyCapture, dir *os.File) ([]affinityThread, error) {
	if !affinityCheck(c) {
		return nil, c.failureError()
	}
	_, err := dir.Seek(0, io.SeekStart)
	if !affinityCheck(c) {
		return nil, c.failureError()
	}
	if err != nil {
		return nil, err
	}
	// Linux directory names have at most 255 bytes. This reserves the complete
	// bounded return, including invalid names, before any directory read.
	if err := affinityRoom(c, (affinityThreadLimit+1)*256); err != nil {
		return nil, err
	}
	names, err := dir.Readdirnames(affinityThreadLimit + 1)
	for _, name := range names {
		c.bytes += uint64(len(name) + 1)
	}
	if !affinityCheck(c) {
		return nil, c.failureError()
	}
	if err != nil && err != io.EOF {
		return nil, err
	}
	if len(names) > affinityThreadLimit {
		return nil, errTopologyLimit
	}
	threads := make([]affinityThread, len(names))
	for i, name := range names {
		id, err := affinityTID(name)
		if err != nil {
			return nil, err
		}
		threads[i].id = id
	}
	slices.SortFunc(threads, func(a, b affinityThread) int {
		if a.id < b.id {
			return -1
		}
		if a.id > b.id {
			return 1
		}
		return 0
	})
	for i := range threads {
		if i != 0 && threads[i].id == threads[i-1].id {
			return nil, errTopologyRecord
		}
		if !affinityCheck(c) {
			return nil, c.failureError()
		}
		var stat unix.Stat_t
		err := unix.Fstatat(int(dir.Fd()), strconv.FormatUint(uint64(threads[i].id), 10), &stat, unix.AT_SYMLINK_NOFOLLOW)
		if !affinityCheck(c) {
			return nil, c.failureError()
		}
		if os.IsNotExist(err) {
			return nil, errAffinityChanged
		}
		if err != nil {
			return nil, err
		}
		if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Ino == 0 {
			return nil, errTopologyRecord
		}
		threads[i].device, threads[i].inode = uint64(stat.Dev), stat.Ino
	}
	if err := validateAffinityThreads(threads); err != nil {
		return nil, err
	}
	return threads, nil
}

func readAffinityTask(c *topologyCapture, dir *os.File, thread affinityThread) ([]byte, error) {
	name := strconv.FormatUint(uint64(thread.id), 10)
	if _, err := affinityTID(name); err != nil || thread.inode == 0 {
		return nil, errTopologyRecord
	}
	if err := affinityOpen(c); err != nil {
		return nil, err
	}
	fd, err := unix.Openat(int(dir.Fd()), name, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if !affinityCheck(c) {
		if err == nil {
			unix.Close(fd)
		}
		return nil, c.failureError()
	}
	if os.IsNotExist(err) {
		return nil, errAffinityChanged
	}
	if err != nil {
		return nil, err
	}
	defer unix.Close(fd)
	identity, err := affinityFDIdentity(c, fd, unix.S_IFDIR)
	if err != nil {
		return nil, err
	}
	if identity.device != thread.device || identity.inode != thread.inode {
		return nil, errAffinityChanged
	}
	if err := affinityRoom(c, affinityStatusLimit+1); err != nil {
		return nil, err
	}
	if err := affinityOpen(c); err != nil {
		return nil, err
	}
	statusFD, err := unix.Openat(fd, "status", unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, 0)
	if !affinityCheck(c) {
		if err == nil {
			unix.Close(statusFD)
		}
		return nil, c.failureError()
	}
	if os.IsNotExist(err) {
		return nil, errAffinityChanged
	}
	if err != nil {
		return nil, err
	}
	status := os.NewFile(uintptr(statusFD), "own-thread-status")
	defer status.Close()
	if _, err := affinityFDIdentity(c, statusFD, unix.S_IFREG); err != nil {
		return nil, err
	}
	if !affinityCheck(c) {
		return nil, c.failureError()
	}
	data, err := io.ReadAll(io.LimitReader(status, affinityStatusLimit+1))
	c.bytes += uint64(len(data))
	if !affinityCheck(c) {
		return nil, c.failureError()
	}
	if err != nil {
		return nil, err
	}
	if len(data) > affinityStatusLimit {
		return nil, errTopologyLimit
	}
	return data, nil
}
