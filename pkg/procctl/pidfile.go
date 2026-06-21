package procctl

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

// pidFileSchemaVersion is the PID file format version; bump only on
// incompatible changes (readers reject any other value).
const pidFileSchemaVersion = 1

// maxPidFileSize caps ReadPidFile (content is ~500 bytes) as a DoS guard
// against a huge file in the PID-file slot.
const maxPidFileSize = 64 * 1024

// ErrPidFileNotFound is returned when the PID file is absent. Callers use
// errors.Is to distinguish "not running" from "corrupt — investigate."
var ErrPidFileNotFound = errors.New("pid file not found")

// PidInfo is the JSON content of mithril.pid. The (Pid, StartTimeTicks,
// ExeInode) triple is collision-proof for stale detection across PID reuse.
type PidInfo struct {
	SchemaVersion int `json:"schema_version"`

	// Process identity (stable for the process's lifetime)
	Pid            int    `json:"pid"`
	StartTimeTicks uint64 `json:"start_time_ticks,omitempty"` // /proc/<pid>/stat field 22 on Linux
	ExeInode       uint64 `json:"exe_inode,omitempty"`        // st_ino of /proc/<pid>/exe
	BinaryPath     string `json:"binary_path"`                // resolved absolute path

	// Session metadata
	RunID      string `json:"run_id"`
	ConfigPath string `json:"config_path,omitempty"`
	SpawnedBy  string `json:"spawned_by"` // "dashboard" | "external" | "cli"
	LogDir     string `json:"log_dir,omitempty"`
	StdoutPath string `json:"stdout_path,omitempty"` // dashboard-spawned child stdout tail
	StderrPath string `json:"stderr_path,omitempty"` // dashboard-spawned child stderr tail

	// Control-state metadata (overwritten by signal/stop helpers)
	StopInProgressBy int       `json:"stop_in_progress_by,omitempty"` // dashboard PID currently stopping; 0 = none
	StopInProgressAt time.Time `json:"stop_in_progress_at"`
}

// isStopProgressStale reports whether the recorded stopping dashboard is gone or
// older than ttl. Zero means "no stop in progress" (never stale).
func (p *PidInfo) isStopProgressStale(ttl time.Duration) bool {
	if p == nil || p.StopInProgressBy == 0 {
		return false
	}
	if p.StopInProgressBy <= 1 {
		// PID <=1 is never a valid owner; -1 would let kill(-1, 0) broadcast.
		return true
	}
	if !p.StopInProgressAt.IsZero() && time.Since(p.StopInProgressAt) > ttl {
		return true
	}
	// signal-0 ESRCH means the dashboard is gone.
	err := syscall.Kill(p.StopInProgressBy, 0)
	return errors.Is(err, syscall.ESRCH)
}

// ReadPidFile loads PidInfo, returning ErrPidFileNotFound when absent. The
// read is capped at maxPidFileSize so a huge file can't exhaust memory.
func ReadPidFile(path string) (*PidInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrPidFileNotFound
		}
		return nil, fmt.Errorf("open pid file %s: %w", path, err)
	}
	defer f.Close()

	data, err := io.ReadAll(io.LimitReader(f, maxPidFileSize))
	if err != nil {
		return nil, fmt.Errorf("read pid file %s: %w", path, err)
	}

	var info PidInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("parse pid file %s: %w", path, err)
	}
	if info.SchemaVersion != pidFileSchemaVersion {
		return nil, fmt.Errorf("pid file %s has unsupported schema version %d (expected %d)",
			path, info.SchemaVersion, pidFileSchemaVersion)
	}
	return &info, nil
}

// WritePidFile writes PidInfo atomically (tmp+rename, dir 0700, file 0600).
// SchemaVersion is forced to the current value.
func WritePidFile(path string, info *PidInfo) error {
	if info == nil {
		return fmt.Errorf("nil pid info")
	}
	info.SchemaVersion = pidFileSchemaVersion

	if err := EnsurePidDir(filepath.Dir(path)); err != nil {
		return err
	}

	data, err := json.MarshalIndent(info, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal pid info: %w", err)
	}
	if err := atomicWriteFile(path, data, 0600); err != nil {
		return fmt.Errorf("write pid file %s: %w", path, err)
	}
	return nil
}

// RemovePidFile deletes path. Absent files are not an error — cleanup
// paths should not have to special-case the "already gone" case.
func RemovePidFile(path string) error {
	err := os.Remove(path)
	if err == nil || os.IsNotExist(err) {
		return nil
	}
	return fmt.Errorf("remove pid file %s: %w", path, err)
}

// RemovePidFileIfMatches deletes path only when it still describes the expected
// run, so Close() can't delete a newer run's PID file.
func RemovePidFileIfMatches(path string, expected *PidInfo) error {
	if expected == nil {
		return fmt.Errorf("nil expected pid info")
	}
	current, err := ReadPidFile(path)
	if err != nil {
		if errors.Is(err, ErrPidFileNotFound) {
			return nil
		}
		return err
	}
	if !sameRunIdentity(current, expected) {
		return nil
	}
	return RemovePidFile(path)
}

func sameRunIdentity(a, b *PidInfo) bool {
	if a == nil || b == nil {
		return false
	}
	return a.Pid == b.Pid &&
		a.StartTimeTicks == b.StartTimeTicks &&
		a.ExeInode == b.ExeInode &&
		a.RunID == b.RunID
}

// UpdatePidLogDir records the per-run log dir after mlog init, via a second
// rewrite while still holding the lock.
func UpdatePidLogDir(path, logDir string) error {
	info, err := ReadPidFile(path)
	if err != nil {
		return err
	}
	info.LogDir = logDir
	return WritePidFile(path, info)
}

// UpdatePidOutputPaths records the dashboard-owned stdout/stderr files for a child.
// The PID check keeps a racing dashboard from annotating a different run's file.
func UpdatePidOutputPaths(path string, pid int, stdoutPath, stderrPath string) error {
	info, err := ReadPidFile(path)
	if err != nil {
		return err
	}
	if info.Pid != pid {
		return nil
	}
	info.StdoutPath = stdoutPath
	info.StderrPath = stderrPath
	return WritePidFile(path, info)
}

// updateStopInProgressIfMatches sets (pid != 0) or clears (0) stop_in_progress_by,
// only when the file still describes expected. Idempotent.
func updateStopInProgressIfMatches(path string, dashboardPid int, expected *PidInfo) error {
	if expected == nil {
		return fmt.Errorf("nil expected pid info")
	}
	info, err := ReadPidFile(path)
	if err != nil {
		return err
	}
	if !sameRunIdentity(info, expected) {
		return nil
	}
	info.StopInProgressBy = dashboardPid
	if dashboardPid == 0 {
		info.StopInProgressAt = time.Time{}
	} else {
		info.StopInProgressAt = time.Now()
	}
	return WritePidFile(path, info)
}
