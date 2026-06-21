package procctl

import (
	"errors"
	"fmt"
)

// ErrProcessNotFound indicates the target PID does not exist (or its
// identity source is unreadable in a way that implies absence).
var ErrProcessNotFound = errors.New("process not found")

// Identity uniquely identifies a process across PID reuse via (ExeInode,
// StartTimeTicks); ExePath is informational. macOS uses start time alone.
type Identity struct {
	Pid            int
	ExeInode       uint64 // 0 on macOS
	StartTimeTicks uint64 // USER_HZ clock ticks on Linux; Unix-nano on macOS
	ExePath        string // resolved binary path (best-effort)
}

// Matches reports whether pid's live identity matches recorded info. Every signal
// path clears this gate before kill(2); pair with a pidfd on Linux >=5.3.
func Matches(pid int, info *PidInfo) (bool, string) {
	if info == nil {
		return false, "no recorded identity"
	}
	if info.Pid != 0 && info.Pid != pid {
		return false, fmt.Sprintf("pid mismatch (recorded %d, checking %d)", info.Pid, pid)
	}
	if info.StartTimeTicks == 0 && info.ExeInode == 0 {
		return false, "recorded identity is incomplete"
	}
	live, err := ReadIdentity(pid)
	if err != nil {
		if errors.Is(err, ErrProcessNotFound) {
			return false, "process not running"
		}
		return false, fmt.Sprintf("cannot read identity: %v", err)
	}
	// Strongest, rename/delete-safe check; skipped on macOS (ExeInode zero).
	if live.ExeInode != 0 && info.ExeInode != 0 && live.ExeInode != info.ExeInode {
		return false, fmt.Sprintf("exe inode mismatch (recorded %d, live %d)",
			info.ExeInode, live.ExeInode)
	}
	// Collision-proof against PID reuse: a new process gets a new starttime.
	if info.StartTimeTicks != 0 && live.StartTimeTicks != info.StartTimeTicks {
		return false, fmt.Sprintf("start time mismatch (recorded %d, live %d) — pid likely reused",
			info.StartTimeTicks, live.StartTimeTicks)
	}
	return true, ""
}
