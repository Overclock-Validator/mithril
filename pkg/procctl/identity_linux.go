//go:build linux

package procctl

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// ReadIdentity reads identity from /proc: ExeInode from /proc/<pid>/exe, start
// time from /proc/<pid>/stat. ENOENT means reaped → ErrProcessNotFound.
func ReadIdentity(pid int) (*Identity, error) {
	// Stat follows the symlink to the underlying inode.
	exePath := fmt.Sprintf("/proc/%d/exe", pid)
	fi, err := os.Stat(exePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrProcessNotFound
		}
		return nil, fmt.Errorf("stat %s: %w", exePath, err)
	}
	stat, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, errors.New("unexpected Sys() type — not a syscall.Stat_t")
	}
	inode := stat.Ino

	// Diagnostic only; strip the " (deleted)" suffix for unlinked binaries.
	exeTarget, _ := os.Readlink(exePath)
	exeTarget = strings.TrimSuffix(exeTarget, " (deleted)")

	startTime, err := readProcStatStartTime(pid)
	if err != nil {
		return nil, err
	}

	return &Identity{
		Pid:            pid,
		ExeInode:       inode,
		StartTimeTicks: startTime,
		ExePath:        exeTarget,
	}, nil
}

// readProcStatStartTime reads /proc/<pid>/stat and extracts starttime.
// Translates ENOENT to ErrProcessNotFound.
func readProcStatStartTime(pid int) (uint64, error) {
	path := fmt.Sprintf("/proc/%d/stat", pid)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, ErrProcessNotFound
		}
		return 0, fmt.Errorf("read %s: %w", path, err)
	}
	v, err := parseStatStartTime(data)
	if err != nil {
		return 0, fmt.Errorf("parse %s: %w", path, err)
	}
	return v, nil
}

// parseStatStartTime extracts field 22 (starttime) from /proc/<pid>/stat,
// splitting after the last ')' since comm can contain spaces/parens.
func parseStatStartTime(data []byte) (uint64, error) {
	line := string(data)
	closeParen := strings.LastIndex(line, ")")
	if closeParen < 0 {
		return 0, fmt.Errorf("malformed stat line (no closing paren): %q", line)
	}
	tail := strings.TrimSpace(line[closeParen+1:])
	fields := strings.Fields(tail)
	const startTimeIndex = 19 // whole-line field 22, minus the 3 pre-state fields
	if len(fields) <= startTimeIndex {
		return 0, fmt.Errorf("stat line has %d post-comm fields, need at least %d",
			len(fields), startTimeIndex+1)
	}
	v, err := strconv.ParseUint(fields[startTimeIndex], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse starttime %q: %w", fields[startTimeIndex], err)
	}
	return v, nil
}
