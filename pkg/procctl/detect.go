package procctl

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/state"
)

// Status is the high-level result of Detect — OS truth only. The dashboard
// layers its own transient "starting / stopping / stuck" states on top.
type Status int

const (
	// StatusStopped — no mithril running and the last shutdown was clean
	// (or no state file exists).
	StatusStopped Status = iota

	// StatusRunning — a process matching the PID file's identity is alive.
	StatusRunning

	// StatusCrashed — no mithril running but the last session didn't record
	// a clean shutdown; AccountsDB may need rebuild.
	StatusCrashed
)

func (s Status) String() string {
	switch s {
	case StatusStopped:
		return "Stopped"
	case StatusRunning:
		return "Running"
	case StatusCrashed:
		return "Crashed"
	default:
		return fmt.Sprintf("Status(%d)", int(s))
	}
}

// Detection is the full result of Detect. All fields are populated on
// success; zero values mean "unknown" or "not applicable to this status."
type Detection struct {
	Status     Status
	Pid        int
	BinaryPath string
	RunID      string
	SpawnedBy  string
	ConfigPath string
	LogDir     string
	StdoutPath string
	StderrPath string
	LockHeld   bool

	// Timing — only meaningful when Status == Running.
	StartedAt time.Time
	Uptime    time.Duration

	// Post-mortem — only meaningful when Status ∈ {Stopped, Crashed}.
	LastShutdownReason string
	LastCleanExit      bool

	// Stop-in-progress metadata. Cleared if the dashboard that set it is
	// dead or the TTL has elapsed — only a live in-progress stop appears.
	StopInProgressBy int
	StopInProgressAt time.Time
}

// stopInProgressTTL bounds how long after a SIGTERM the stop_in_progress_by
// field stays meaningful; beyond it the setting dashboard is presumed gone.
const stopInProgressTTL = 5 * time.Minute

// Detect inspects the PID, lock, and state files to determine status (pure read).
// Empty accountsDbDir skips post-mortem classification; errors only on a corrupt PID file.
func Detect(pidPath, lockPath, accountsDbDir string) (*Detection, error) {
	info, err := ReadPidFile(pidPath)
	if err != nil && !errors.Is(err, ErrPidFileNotFound) {
		// Corrupt PID file — surface it so the dashboard shows "investigate".
		return nil, err
	}

	// Branch 1: PID file exists. Check identity and lock.
	if info != nil {
		if ok, _ := Matches(info.Pid, info); ok {
			lockHeld := false
			if lockPath != "" {
				lockHeld, _ = TestLock(lockPath)
			}
			det := &Detection{
				Status:     StatusRunning,
				Pid:        info.Pid,
				BinaryPath: info.BinaryPath,
				RunID:      info.RunID,
				SpawnedBy:  info.SpawnedBy,
				ConfigPath: info.ConfigPath,
				LogDir:     info.LogDir,
				StdoutPath: info.StdoutPath,
				StderrPath: info.StderrPath,
				LockHeld:   lockHeld,
			}
			// StartedAt/Uptime left unset: the dashboard derives run start
			// from the timestamped mlog runDir (LogDir) instead.

			// Carry stop-in-progress fields but drop stale ones, so a crashed
			// dashboard doesn't permanently grey out Force Stop.
			if info.StopInProgressBy != 0 && !info.isStopProgressStale(stopInProgressTTL) {
				det.StopInProgressBy = info.StopInProgressBy
				det.StopInProgressAt = info.StopInProgressAt
			}
			return det, nil
		}
		// PID file exists but the process is gone or its identity has
		// changed. Fall through to the post-mortem branch.
	}

	// Branch 2: No live process. Classify Stopped vs Crashed via state file.
	det := &Detection{Status: StatusStopped}
	if accountsDbDir == "" {
		return det, nil
	}
	st, err := state.LoadState(accountsDbDir)
	if err != nil {
		det.Status = StatusCrashed
		det.LastShutdownReason = fmt.Sprintf("state file unreadable: %v", err)
		return det, nil
	}
	if st == nil && accountsDbArtifactsExist(accountsDbDir) {
		det.Status = StatusCrashed
		det.LastShutdownReason = "incomplete AccountsDB found (state file missing)"
		return det, nil
	}
	clean, reason := state.WasCleanExit(st)
	det.LastShutdownReason = reason
	det.LastCleanExit = clean
	if st != nil && !clean {
		det.Status = StatusCrashed
	}
	return det, nil
}

func accountsDbArtifactsExist(accountsDbDir string) bool {
	for _, name := range []string{
		"mithril_db",
		"bankhash_db",
		"accounts",
		"largest_file_id",
		"bank_hash",
		"manifest",
		state.HistoryFileName,
	} {
		if _, err := os.Stat(filepath.Join(accountsDbDir, name)); err == nil {
			return true
		}
	}
	return false
}
