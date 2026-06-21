package procctl

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"time"
)

// ErrStopTimeout is returned by WaitStopped when the SIGTERM grace period
// elapses without the target exiting. Callers can offer a Force Stop option.
var ErrStopTimeout = errors.New("stop timed out - process still running")

// SpawnedByEnv tells a spawned `mithril run` who launched it; the child
// records it in mithril.pid so dashboards can tell apart their own launches.
const SpawnedByEnv = "MITHRIL_SPAWNED_BY"

// RunOpts is the input to AcquireForRun.
type RunOpts struct {
	PidPath    string // mithril.pid path (typically DefaultPidFile())
	LockPath   string // mithril.lock path (typically DefaultLockFile())
	RunID      string // unique run identifier (e.g., from replay.GenerateRunID)
	BinaryPath string // absolute path of the running binary (os.Executable())
	ConfigPath string // absolute path of the config in use (may be empty)
	LogDir     string // mlog run directory (for dashboard log-tail)
	SpawnedBy  string // "dashboard" | "external" | "cli"
}

// RunHandle is owned by a started mithril child; Close releases the lock and
// removes the PID file (sync.Once). os.Exit paths must remove it explicitly.
type RunHandle struct {
	lock      *LockHandle
	pidPath   string
	pidInfo   *PidInfo
	closeOnce sync.Once
	closeErr  error
}

// AcquireForRun takes the single-instance lock, captures identity, and writes the
// PID file. Returns ErrLocked if another mithril runs; Close the handle at exit.
func AcquireForRun(opts RunOpts) (*RunHandle, error) {
	lh, err := AcquireLock(opts.LockPath)
	if err != nil {
		return nil, err
	}

	id, err := ReadIdentity(os.Getpid())
	if err != nil {
		_ = lh.Release()
		return nil, fmt.Errorf("read own identity: %w", err)
	}

	info := &PidInfo{
		Pid:            os.Getpid(),
		StartTimeTicks: id.StartTimeTicks,
		ExeInode:       id.ExeInode,
		BinaryPath:     opts.BinaryPath,
		RunID:          opts.RunID,
		ConfigPath:     opts.ConfigPath,
		LogDir:         opts.LogDir,
		SpawnedBy:      opts.SpawnedBy,
	}
	if err := WritePidFile(opts.PidPath, info); err != nil {
		_ = lh.Release()
		return nil, fmt.Errorf("write pid file: %w", err)
	}

	return &RunHandle{lock: lh, pidPath: opts.PidPath, pidInfo: info}, nil
}

// Close releases the lock and removes the PID file. Safe to call repeatedly
// and concurrently; the first call's error is returned by every call.
func (h *RunHandle) Close() error {
	if h == nil {
		return nil
	}
	h.closeOnce.Do(func() {
		if err := RemovePidFileIfMatches(h.pidPath, h.pidInfo); err != nil {
			h.closeErr = err
		}
		if err := h.lock.Release(); err != nil && h.closeErr == nil {
			h.closeErr = err
		}
	})
	return h.closeErr
}

// ── Dashboard-side control surface ──

// SignalStop re-verifies identity, stamps stop_in_progress_by, and sends SIGTERM
// (does not wait — use WaitStopped). Returns ErrPidFileNotFound if nothing to stop.
func SignalStop(pidPath, auditPath string, dashboardPid int) error {
	info, err := ReadPidFile(pidPath)
	if err != nil {
		return err
	}
	signalHandle, err := OpenSignalHandle(info.Pid)
	if err != nil {
		// Already gone — report as not-running, not a hard failure.
		if errors.Is(err, ErrProcessNotFound) {
			_ = AppendAudit(auditPath, AuditEntry{
				Action: "STOP",
				Result: "already_exited",
				Pid:    info.Pid,
			})
			return ErrProcessNotFound
		}
		_ = AppendAudit(auditPath, AuditEntry{
			Action: "STOP",
			Result: "signal_failed",
			Pid:    info.Pid,
			Extra:  map[string]string{"error": err.Error()},
		})
		return fmt.Errorf("open signal handle for pid %d: %w", info.Pid, err)
	}
	defer signalHandle.Close()

	// If the PID was reused by an unrelated process, refuse to signal it.
	if ok, reason := Matches(info.Pid, info); !ok {
		_ = AppendAudit(auditPath, AuditEntry{
			Action: "STOP",
			Result: "refused",
			Pid:    info.Pid,
			Extra:  map[string]string{"reason": reason},
		})
		return fmt.Errorf("refuse to signal pid %d: %s", info.Pid, reason)
	}

	// Best-effort UI hint; failure here doesn't block the signal. Last-writer
	// wins between concurrent dashboards is fine (it's informational).
	_ = updateStopInProgressIfMatches(pidPath, dashboardPid, info)

	if err := signalHandle.Signal(syscall.SIGTERM); err != nil {
		// Process exited between the identity check and the signal — benign.
		if errors.Is(err, syscall.ESRCH) {
			_ = AppendAudit(auditPath, AuditEntry{
				Action: "STOP",
				Result: "already_exited",
				Pid:    info.Pid,
			})
			return ErrProcessNotFound
		}
		_ = AppendAudit(auditPath, AuditEntry{
			Action: "STOP",
			Result: "signal_failed",
			Pid:    info.Pid,
			Extra:  map[string]string{"error": err.Error()},
		})
		return fmt.Errorf("send SIGTERM to pid %d: %w", info.Pid, err)
	}

	_ = AppendAudit(auditPath, AuditEntry{
		Action: "STOP",
		Result: "signal_sent",
		Pid:    info.Pid,
		Extra:  map[string]string{"run_id": info.RunID},
	})
	return nil
}

// WaitStopped polls Detect each second until the process stops or timeout
// (ErrStopTimeout). Never auto-escalate to ForceKill — risks corruption.
func WaitStopped(pidPath, lockPath, accountsDir string, timeout time.Duration) (*Detection, error) {
	const pollInterval = 1 * time.Second
	deadline := time.Now().Add(timeout)
	var last *Detection
	for {
		det, err := Detect(pidPath, lockPath, accountsDir)
		if err != nil {
			return last, err
		}
		last = det
		if det.Status != StatusRunning {
			return det, nil
		}
		if time.Now().After(deadline) {
			return det, ErrStopTimeout
		}
		time.Sleep(pollInterval)
	}
}

// ForceKill delivers SIGKILL after re-verifying identity (skips shutdown defers).
// Race-free via pidfd on Linux >=5.3; kill(2) fallback has a tiny TOCTOU window.
func ForceKill(pidPath, auditPath string, dashboardPid int) error {
	info, err := ReadPidFile(pidPath)
	if err != nil {
		return err
	}
	signalHandle, err := OpenSignalHandle(info.Pid)
	if err != nil {
		// Already gone — not a failure, and no rebuild is needed.
		if errors.Is(err, ErrProcessNotFound) {
			_ = AppendAudit(auditPath, AuditEntry{
				Action: "FORCE_STOP",
				Result: "already_exited",
				Pid:    info.Pid,
			})
			return ErrProcessNotFound
		}
		_ = AppendAudit(auditPath, AuditEntry{
			Action: "FORCE_STOP",
			Result: "signal_failed",
			Pid:    info.Pid,
			Extra:  map[string]string{"error": err.Error(), "warning": "accountsdb-rebuild-recommended"},
		})
		return fmt.Errorf("open signal handle for pid %d: %w", info.Pid, err)
	}
	defer signalHandle.Close()
	if ok, reason := Matches(info.Pid, info); !ok {
		_ = AppendAudit(auditPath, AuditEntry{
			Action: "FORCE_STOP",
			Result: "refused",
			Pid:    info.Pid,
			Extra:  map[string]string{"reason": reason},
		})
		return fmt.Errorf("refuse to SIGKILL pid %d: %s", info.Pid, reason)
	}
	if err := signalHandle.Signal(syscall.SIGKILL); err != nil {
		_ = AppendAudit(auditPath, AuditEntry{
			Action: "FORCE_STOP",
			Result: "signal_failed",
			Pid:    info.Pid,
			Extra:  map[string]string{"error": err.Error(), "warning": "accountsdb-rebuild-recommended"},
		})
		return fmt.Errorf("SIGKILL pid %d: %w", info.Pid, err)
	}
	_ = AppendAudit(auditPath, AuditEntry{
		Action: "FORCE_STOP",
		Result: "killed",
		Pid:    info.Pid,
		Extra:  map[string]string{"run_id": info.RunID, "warning": "accountsdb-rebuild-recommended"},
	})
	return nil
}
