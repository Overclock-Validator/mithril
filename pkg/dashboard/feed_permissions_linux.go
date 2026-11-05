//go:build linux

package dashboard

import (
	"errors"
	"fmt"
	"os"
	"os/user"
	"strconv"
	"syscall"
)

func inspectFeedGroup(dir, name string) (int, error) {
	if name == "" {
		return 0, errors.New("dashboard feed group must be named")
	}
	group, err := user.LookupGroup(name)
	if err != nil {
		return 0, fmt.Errorf("resolve dashboard feed group: %w", err)
	}
	gid, err := strconv.Atoi(group.Gid)
	if err != nil || gid < 0 {
		return 0, errors.New("invalid dashboard feed group ID")
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return 0, fmt.Errorf("inspect dashboard socket directory: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() || info.Mode()&(os.ModePerm|os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != os.ModeSetgid|0o710 ||
		stat.Uid != uint32(os.Geteuid()) || uint64(stat.Gid) != uint64(gid) {
		return 0, errors.New("dashboard feed directory must be owned by the node and feed group, with mode 2710")
	}
	return gid, nil
}

func configureFeedGroup(path string, gid int) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect dashboard feed socket: %w", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || info.Mode()&os.ModeSocket == 0 || stat.Uid != uint32(os.Geteuid()) || uint64(stat.Gid) != uint64(gid) {
		return errors.New("dashboard feed socket has unexpected owner or group")
	}
	if err := os.Chmod(path, 0o620); err != nil {
		return fmt.Errorf("set dashboard feed socket permissions: %w", err)
	}
	info, err = os.Lstat(path)
	if err != nil {
		return fmt.Errorf("verify dashboard feed socket: %w", err)
	}
	stat, ok = info.Sys().(*syscall.Stat_t)
	if !ok || info.Mode()&os.ModeSocket == 0 || info.Mode()&(os.ModePerm|os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0o620 ||
		stat.Uid != uint32(os.Geteuid()) || uint64(stat.Gid) != uint64(gid) {
		return errors.New("dashboard feed socket permissions changed unexpectedly")
	}
	return nil
}
