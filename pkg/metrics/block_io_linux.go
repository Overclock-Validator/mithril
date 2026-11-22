//go:build linux

package metrics

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type ioDirectoryIdentity struct {
	device, inode uint64
}

func ioDirectoryIdentityOf(info os.FileInfo) (ioDirectoryIdentity, bool) {
	if info == nil || !info.IsDir() {
		return ioDirectoryIdentity{}, false
	}
	s, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return ioDirectoryIdentity{}, false
	}
	return ioDirectoryIdentity{device: uint64(s.Dev), inode: s.Ino}, true
}

func readSysfsIOFile(root *os.Root, name string, limit int) ([]byte, error) {
	f, err := root.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	var fs unix.Statfs_t
	if err != nil || !info.Mode().IsRegular() || unix.Fstatfs(int(f.Fd()), &fs) != nil || fs.Type != unix.SYSFS_MAGIC {
		return nil, errIORecord
	}
	data, err := io.ReadAll(io.LimitReader(f, int64(limit)+1))
	if err != nil || len(data) > limit {
		return nil, errIORecord
	}
	return data, nil
}

func readBlockIO(directory string) (o BlockIOObservation, supported bool) {
	o.StartedAt, supported = time.Now(), true
	withinBudget := func() bool {
		now := time.Now()
		return !now.Before(o.StartedAt) && now.Sub(o.StartedAt) <= time.Second
	}
	defer func() {
		o.SampledAt = time.Now()
		if o.SampledAt.Before(o.StartedAt) || o.SampledAt.Sub(o.StartedAt) > time.Second {
			o = BlockIOObservation{StartedAt: o.StartedAt, SampledAt: o.SampledAt}
		}
	}()
	if directory == "" {
		return
	}
	info, err := os.Stat(directory)
	before, ok := ioDirectoryIdentityOf(info)
	if err != nil || !ok || !withinBudget() {
		return
	}
	// These filesystems have one direct mounted block layer. Other layouts,
	// including multi-device and virtual filesystems, stay explicitly unknown.
	var fs unix.Statfs_t
	if unix.Statfs(directory, &fs) != nil || !withinBudget() || fs.Type != unix.EXT4_SUPER_MAGIC && fs.Type != unix.XFS_SUPER_MAGIC {
		return
	}
	major, minor := unix.Major(before.device), unix.Minor(before.device)
	if major == 0 {
		return
	}
	root, err := os.OpenRoot("/sys")
	if err != nil {
		return
	}
	defer root.Close()
	readAttribute := func(name string, limit int) ([]byte, error) {
		if !withinBudget() {
			return nil, errIORecord
		}
		data, err := readSysfsIOFile(root, name, limit)
		if !withinBudget() {
			return nil, errIORecord
		}
		return data, err
	}
	name := fmt.Sprintf("dev/block/%d:%d", major, minor)
	linkBefore, err := root.Stat(name)
	deviceBefore, ok := ioDirectoryIdentityOf(linkBefore)
	if err != nil || !ok || !withinBudget() {
		return
	}
	dev, err := readAttribute(name+"/dev", 64)
	if err != nil || string(dev) != fmt.Sprintf("%d:%d\n", major, minor) {
		return
	}
	data, err := readAttribute(name+"/stat", 4096)
	if err != nil {
		return
	}
	counters, count, err := parseBlockIO(data)
	if err != nil {
		return
	}
	partition, err := readAttribute(name+"/partition", 64)
	if err != nil && !os.IsNotExist(err) {
		return
	}
	isPartition := err == nil
	if isPartition {
		value := strings.TrimSpace(string(partition))
		number, err := strconv.ParseUint(value, 10, 32)
		if err != nil || number == 0 || strconv.FormatUint(number, 10) != value {
			return
		}
	}
	linkAfter, err := root.Stat(name)
	deviceAfter, ok := ioDirectoryIdentityOf(linkAfter)
	if err != nil || !ok || deviceAfter != deviceBefore || !withinBudget() {
		return
	}
	afterInfo, err := os.Stat(directory)
	after, ok := ioDirectoryIdentityOf(afterInfo)
	if err != nil || !ok || before != after {
		return
	}
	o.Major, o.Minor = major, minor
	o.Counters, o.HasDiscard, o.HasFlush, o.Known = counters, count >= 15, count == 17 && !isPartition, true
	return
}
