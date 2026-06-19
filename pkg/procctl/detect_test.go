package procctl

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Detect combines pidfile + lock + identity + WasCleanExit into one Status (Stopped|Running|Crashed).

// spawnLiveChild starts a `sleep 30` child and returns its PID + a cleanup func.
func spawnLiveChild(t *testing.T) (int, func()) {
	t.Helper()
	cmd := exec.Command("sh", "-c", "sleep 30")
	require.NoError(t, cmd.Start())
	cleanup := func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}
	time.Sleep(30 * time.Millisecond) // let it actually start
	return cmd.Process.Pid, cleanup
}

// pidInfoForPid builds a PidInfo matching a live PID by reading its identity.
func pidInfoForPid(t *testing.T, pid int) *PidInfo {
	t.Helper()
	id, err := ReadIdentity(pid)
	require.NoError(t, err)
	return &PidInfo{
		Pid:            pid,
		ExeInode:       id.ExeInode,
		StartTimeTicks: id.StartTimeTicks,
		BinaryPath:     id.ExePath,
		RunID:          "test-run-id",
		SpawnedBy:      "test",
	}
}

// No PID file, no lock → Stopped.
func TestDetect_NoPidFile_IsStopped(t *testing.T) {
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "mithril.pid")
	lockPath := filepath.Join(dir, "mithril.lock")
	accountsDir := dir

	det, err := Detect(pidPath, lockPath, accountsDir)
	require.NoError(t, err)
	assert.Equal(t, StatusStopped, det.Status)
	assert.Equal(t, 0, det.Pid)
}

// Alive PID + matching PID file + held lock → Running.
func TestDetect_AlivePidAndLock_IsRunning(t *testing.T) {
	pid, cleanup := spawnLiveChild(t)
	defer cleanup()

	dir := t.TempDir()
	pidPath := filepath.Join(dir, "mithril.pid")
	lockPath := filepath.Join(dir, "mithril.lock")

	require.NoError(t, WritePidFile(pidPath, pidInfoForPid(t, pid)))

	lh, err := AcquireLock(lockPath)
	require.NoError(t, err)
	defer lh.Release()

	det, err := Detect(pidPath, lockPath, dir)
	require.NoError(t, err)
	assert.Equal(t, StatusRunning, det.Status)
	assert.Equal(t, pid, det.Pid)
	assert.Equal(t, "test-run-id", det.RunID)
	assert.True(t, det.LockHeld, "lock is held by this process; Detect should report LockHeld")
	// TODO: assert a real Uptime lower bound when populated.
	assert.Zero(t, det.Uptime, "Uptime is not populated yet")
}

// PID gone + no state file → Stopped (can't claim Crashed without evidence).
func TestDetect_PidFileButProcessGone_NoState_IsStopped(t *testing.T) {
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "mithril.pid")
	lockPath := filepath.Join(dir, "mithril.lock")

	bogus := &PidInfo{
		Pid:            9999999,
		StartTimeTicks: 1,
		ExeInode:       1,
		RunID:          "ghost",
	}
	require.NoError(t, WritePidFile(pidPath, bogus))

	det, err := Detect(pidPath, lockPath, dir)
	require.NoError(t, err)
	assert.Equal(t, StatusStopped, det.Status)
}

// No PID file + state shows session start with no clean shutdown → Crashed.
func TestDetect_PidFileGone_StateSaysCrashed_IsCrashed(t *testing.T) {
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "mithril.pid")
	lockPath := filepath.Join(dir, "mithril.lock")

	s := &state.MithrilState{
		StateSchemaVersion:      state.CurrentStateSchemaVersion,
		Stage:                   "ready",
		SnapshotSlot:            100,
		CurrentSessionStartedAt: time.Now().Add(-1 * time.Hour),
		// LastShutdownAt is zero → no clean shutdown recorded
	}
	require.NoError(t, s.Save(dir))

	det, err := Detect(pidPath, lockPath, dir)
	require.NoError(t, err)
	assert.Equal(t, StatusCrashed, det.Status)
	assert.NotEmpty(t, det.LastShutdownReason, "Crashed status should surface a reason")
}

// No PID file + state shows a clean shutdown → Stopped, LastCleanExit true.
func TestDetect_PidFileGone_StateSaysClean_IsStopped(t *testing.T) {
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "mithril.pid")
	lockPath := filepath.Join(dir, "mithril.lock")

	start := time.Now().Add(-2 * time.Hour)
	s := &state.MithrilState{
		StateSchemaVersion:      state.CurrentStateSchemaVersion,
		Stage:                   "ready",
		SnapshotSlot:            100,
		CurrentSessionStartedAt: start,
		LastShutdownAt:          start.Add(1 * time.Hour),
		LastShutdownReason:      state.ShutdownReasonNormal,
	}
	require.NoError(t, s.Save(dir))

	det, err := Detect(pidPath, lockPath, dir)
	require.NoError(t, err)
	assert.Equal(t, StatusStopped, det.Status)
	assert.True(t, det.LastCleanExit)
}

func TestDetect_CorruptStateFile_IsCrashed(t *testing.T) {
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "mithril.pid")
	lockPath := filepath.Join(dir, "mithril.lock")
	require.NoError(t, os.WriteFile(filepath.Join(dir, state.StateFileName), []byte("{"), 0600))

	det, err := Detect(pidPath, lockPath, dir)
	require.NoError(t, err)
	assert.Equal(t, StatusCrashed, det.Status)
	assert.Contains(t, det.LastShutdownReason, "state file unreadable")
}

func TestDetect_MissingStateWithAccountsArtifacts_IsCrashed(t *testing.T) {
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "mithril.pid")
	lockPath := filepath.Join(dir, "mithril.lock")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "manifest"), []byte("partial bootstrap artifact"), 0600))

	det, err := Detect(pidPath, lockPath, dir)
	require.NoError(t, err)
	assert.Equal(t, StatusCrashed, det.Status)
	assert.Contains(t, det.LastShutdownReason, "incomplete AccountsDB")
}

func TestDetect_CleanBuildingStateWithAccountsArtifacts_IsStopped(t *testing.T) {
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "mithril.pid")
	lockPath := filepath.Join(dir, "mithril.lock")
	require.NoError(t, os.WriteFile(filepath.Join(dir, "manifest"), []byte("partial bootstrap artifact"), 0600))

	start := time.Now().Add(-10 * time.Minute)
	s := &state.MithrilState{
		StateSchemaVersion:      state.CurrentStateSchemaVersion,
		Stage:                   "building",
		CurrentSessionStartedAt: start,
		LastShutdownAt:          start.Add(5 * time.Minute),
		LastShutdownReason:      state.ShutdownReasonNormal,
	}
	require.NoError(t, s.Save(dir))

	det, err := Detect(pidPath, lockPath, dir)
	require.NoError(t, err)
	assert.Equal(t, StatusStopped, det.Status)
	assert.True(t, det.LastCleanExit)
}

// Corrupt PID file surfaces an error, not a status.
func TestDetect_CorruptPidFile_SurfacesError(t *testing.T) {
	dir := t.TempDir()
	pidPath := filepath.Join(dir, "mithril.pid")
	lockPath := filepath.Join(dir, "mithril.lock")

	require.NoError(t, os.WriteFile(pidPath, []byte("not json"), 0600))

	_, err := Detect(pidPath, lockPath, dir)
	require.Error(t, err)
}

// stop_in_progress_by pointing at a dead dashboard PID is cleared, not propagated.
func TestDetect_StaleStopInProgress_NotShown(t *testing.T) {
	pid, cleanup := spawnLiveChild(t)
	defer cleanup()

	dir := t.TempDir()
	pidPath := filepath.Join(dir, "mithril.pid")
	lockPath := filepath.Join(dir, "mithril.lock")

	info := pidInfoForPid(t, pid)
	info.StopInProgressBy = 9999999 // dead dashboard pid
	info.StopInProgressAt = time.Now()
	require.NoError(t, WritePidFile(pidPath, info))

	lh, err := AcquireLock(lockPath)
	require.NoError(t, err)
	defer lh.Release()

	det, err := Detect(pidPath, lockPath, dir)
	require.NoError(t, err)
	assert.Equal(t, StatusRunning, det.Status)
	assert.Equal(t, 0, det.StopInProgressBy, "stale stop_in_progress_by must be cleared")
}
