package procctl

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startStub launches a throwaway child in its own process group, reaped by one
// Wait() goroutine. Cleanup SIGKILLs the group; the channel closes on exit.
func startStub(t *testing.T, args ...string) (*exec.Cmd, <-chan struct{}) {
	t.Helper()
	cmd := exec.Command(args[0], args[1:]...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	require.NoError(t, cmd.Start())
	done := make(chan struct{})
	go func() { _ = cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		<-done
	})
	return cmd, done
}

// AcquireForRun (called by `mithril run` at startup) acquires the lock + writes
// the PID file, holding the lock until handle.Close.

func TestAcquireForRun_HappyPath(t *testing.T) {
	dir := t.TempDir()
	opts := RunOpts{
		PidPath:    filepath.Join(dir, "mithril.pid"),
		LockPath:   filepath.Join(dir, "mithril.lock"),
		RunID:      "test-run-1",
		BinaryPath: "/usr/local/bin/mithril",
		ConfigPath: "/etc/mithril.toml",
		LogDir:     "/var/log/mithril/test",
		SpawnedBy:  "test",
	}
	handle, err := AcquireForRun(opts)
	require.NoError(t, err)
	require.NotNil(t, handle)
	defer handle.Close()

	info, err := ReadPidFile(opts.PidPath)
	require.NoError(t, err)
	assert.Equal(t, os.Getpid(), info.Pid)
	assert.Equal(t, "test-run-1", info.RunID)
	assert.Equal(t, "test", info.SpawnedBy)
	assert.NotZero(t, info.StartTimeTicks)
}

// Second AcquireForRun returns ErrLocked (another mithril already running).
func TestAcquireForRun_ContentionReturnsErrLocked(t *testing.T) {
	dir := t.TempDir()
	opts := RunOpts{
		PidPath:  filepath.Join(dir, "mithril.pid"),
		LockPath: filepath.Join(dir, "mithril.lock"),
		RunID:    "first",
	}
	first, err := AcquireForRun(opts)
	require.NoError(t, err)
	defer first.Close()

	second, err := AcquireForRun(opts)
	assert.Nil(t, second)
	assert.True(t, errors.Is(err, ErrLocked))
}

// Close removes the PID file (leftovers would false-positive on next start).
func TestAcquireForRun_CloseRemovesPidFile(t *testing.T) {
	dir := t.TempDir()
	opts := RunOpts{
		PidPath:  filepath.Join(dir, "mithril.pid"),
		LockPath: filepath.Join(dir, "mithril.lock"),
		RunID:    "to-be-closed",
	}
	handle, err := AcquireForRun(opts)
	require.NoError(t, err)
	require.NoError(t, handle.Close())

	_, err = os.Stat(opts.PidPath)
	assert.True(t, os.IsNotExist(err), "PID file should be gone after Close")
}

// A stale handle's Close must not delete a newer run's PID file.
func TestAcquireForRun_CloseDoesNotRemoveDifferentPidFile(t *testing.T) {
	dir := t.TempDir()
	opts := RunOpts{
		PidPath:  filepath.Join(dir, "mithril.pid"),
		LockPath: filepath.Join(dir, "mithril.lock"),
		RunID:    "old-run",
	}
	handle, err := AcquireForRun(opts)
	require.NoError(t, err)

	replacement := &PidInfo{
		Pid:            os.Getpid(),
		StartTimeTicks: handle.pidInfo.StartTimeTicks + 1,
		ExeInode:       handle.pidInfo.ExeInode,
		RunID:          "new-run",
	}
	require.NoError(t, WritePidFile(opts.PidPath, replacement))
	require.NoError(t, handle.Close())

	info, err := ReadPidFile(opts.PidPath)
	require.NoError(t, err)
	assert.Equal(t, "new-run", info.RunID)
}

// Double Close is safe (defer + explicit Close).
func TestAcquireForRun_DoubleCloseIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	opts := RunOpts{
		PidPath:  filepath.Join(dir, "mithril.pid"),
		LockPath: filepath.Join(dir, "mithril.lock"),
		RunID:    "double-close",
	}
	handle, err := AcquireForRun(opts)
	require.NoError(t, err)
	require.NoError(t, handle.Close())
	assert.NoError(t, handle.Close())
}

// SignalStop + WaitStopped — the dashboard's Stop button code path.

// SignalStop delivers SIGTERM to the recorded process; it must exit.
func TestSignalStop_DeliversToRunningProcess(t *testing.T) {
	cmd, done := startStub(t, "sh", "-c", "sleep 30")
	time.Sleep(50 * time.Millisecond)

	dir := t.TempDir()
	pidPath := filepath.Join(dir, "mithril.pid")
	auditPath := filepath.Join(dir, "control.audit")

	id, err := ReadIdentity(cmd.Process.Pid)
	require.NoError(t, err)
	require.NoError(t, WritePidFile(pidPath, &PidInfo{
		Pid:            cmd.Process.Pid,
		ExeInode:       id.ExeInode,
		StartTimeTicks: id.StartTimeTicks,
		RunID:          "stop-target",
	}))

	require.NoError(t, SignalStop(pidPath, auditPath, os.Getpid()))

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("SignalStop did not deliver SIGTERM in time")
	}
}

// SignalStop stamps the dashboard PID so other dashboards see the in-flight stop.
func TestSignalStop_RecordsStopInProgress(t *testing.T) {
	cmd, _ := startStub(t, "sh", "-c", "sleep 30")
	time.Sleep(50 * time.Millisecond)

	dir := t.TempDir()
	pidPath := filepath.Join(dir, "mithril.pid")
	auditPath := filepath.Join(dir, "control.audit")

	id, err := ReadIdentity(cmd.Process.Pid)
	require.NoError(t, err)
	require.NoError(t, WritePidFile(pidPath, &PidInfo{
		Pid:            cmd.Process.Pid,
		ExeInode:       id.ExeInode,
		StartTimeTicks: id.StartTimeTicks,
		RunID:          "stop-target",
	}))

	dashboardPid := os.Getpid()
	require.NoError(t, SignalStop(pidPath, auditPath, dashboardPid))

	info, err := ReadPidFile(pidPath)
	require.NoError(t, err)
	assert.Equal(t, dashboardPid, info.StopInProgressBy)
	assert.False(t, info.StopInProgressAt.IsZero())
}

// SignalStop on an absent PID file returns ErrPidFileNotFound.
func TestSignalStop_AbsentPidFile(t *testing.T) {
	dir := t.TempDir()
	err := SignalStop(filepath.Join(dir, "ghost.pid"), filepath.Join(dir, "audit"), os.Getpid())
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrPidFileNotFound))
}

// WaitStopped observes Status != Running once the process exits.
func TestWaitStopped_DetectsExit(t *testing.T) {
	cmd, reapDone := startStub(t, "sh", "-c", "sleep 30")
	pid := cmd.Process.Pid

	dir := t.TempDir()
	pidPath := filepath.Join(dir, "mithril.pid")
	lockPath := filepath.Join(dir, "mithril.lock")
	accountsDir := dir

	id, err := ReadIdentity(pid)
	require.NoError(t, err)
	require.NoError(t, WritePidFile(pidPath, &PidInfo{
		Pid:            pid,
		ExeInode:       id.ExeInode,
		StartTimeTicks: id.StartTimeTicks,
		RunID:          "wait-target",
	}))

	// SIGTERM out of band so WaitStopped only observes exit; startStub's reap
	// clears the PID that WaitStopped polls.
	require.NoError(t, syscall.Kill(pid, syscall.SIGTERM))

	det, err := WaitStopped(pidPath, lockPath, accountsDir, 5*time.Second)
	require.NoError(t, err)
	assert.NotEqual(t, StatusRunning, det.Status)
	<-reapDone
}

// On timeout WaitStopped returns the still-Running Detection plus ErrStopTimeout.
func TestWaitStopped_TimeoutReturnsDetection(t *testing.T) {
	cmd, _ := startStub(t, "sh", "-c", "trap '' TERM; sleep 30") // ignore SIGTERM
	time.Sleep(100 * time.Millisecond)

	dir := t.TempDir()
	pidPath := filepath.Join(dir, "mithril.pid")
	lockPath := filepath.Join(dir, "mithril.lock")
	accountsDir := dir

	id, err := ReadIdentity(cmd.Process.Pid)
	require.NoError(t, err)
	require.NoError(t, WritePidFile(pidPath, &PidInfo{
		Pid:            cmd.Process.Pid,
		ExeInode:       id.ExeInode,
		StartTimeTicks: id.StartTimeTicks,
		RunID:          "stuck",
	}))

	det, err := WaitStopped(pidPath, lockPath, accountsDir, 500*time.Millisecond)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrStopTimeout))
	require.NotNil(t, det)
	assert.Equal(t, StatusRunning, det.Status)
}

// ForceKill — last-resort, user-confirmed.

// ForceKill kills a SIGTERM-ignoring process.
func TestForceKill_KillsStubbornProcess(t *testing.T) {
	cmd, done := startStub(t, "sh", "-c", "trap '' TERM; sleep 30")
	pid := cmd.Process.Pid
	time.Sleep(100 * time.Millisecond)

	dir := t.TempDir()
	pidPath := filepath.Join(dir, "mithril.pid")
	auditPath := filepath.Join(dir, "control.audit")

	id, err := ReadIdentity(pid)
	require.NoError(t, err)
	require.NoError(t, WritePidFile(pidPath, &PidInfo{
		Pid:            pid,
		ExeInode:       id.ExeInode,
		StartTimeTicks: id.StartTimeTicks,
		RunID:          "kill-target",
	}))

	require.NoError(t, ForceKill(pidPath, auditPath, os.Getpid()))

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ForceKill did not deliver SIGKILL")
	}
}

// ForceKill's audit entry carries a warning field flagging the corruption risk.
func TestForceKill_RecordsAuditWarning(t *testing.T) {
	cmd, _ := startStub(t, "sh", "-c", "sleep 30")
	pid := cmd.Process.Pid
	time.Sleep(50 * time.Millisecond)

	dir := t.TempDir()
	pidPath := filepath.Join(dir, "mithril.pid")
	auditPath := filepath.Join(dir, "control.audit")

	id, err := ReadIdentity(pid)
	require.NoError(t, err)
	require.NoError(t, WritePidFile(pidPath, &PidInfo{
		Pid:            pid,
		ExeInode:       id.ExeInode,
		StartTimeTicks: id.StartTimeTicks,
		RunID:          "kill-audit",
	}))

	require.NoError(t, ForceKill(pidPath, auditPath, os.Getpid()))

	data, err := os.ReadFile(auditPath)
	require.NoError(t, err)
	body := string(data)
	assert.Contains(t, body, "action=FORCE_STOP")
	assert.Contains(t, body, "warning=")
}
