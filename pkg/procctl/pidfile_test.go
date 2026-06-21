package procctl

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// samplePidInfo builds a deterministic PidInfo for write/read tests.
func samplePidInfo() *PidInfo {
	return &PidInfo{
		SchemaVersion:    pidFileSchemaVersion,
		Pid:              12345,
		StartTimeTicks:   1723456789,
		ExeInode:         5827319,
		BinaryPath:       "/home/operator/mithril/mithril",
		RunID:            "20260518-143209Z_abc1234_def56789",
		ConfigPath:       "/home/operator/mithril/config.toml",
		SpawnedBy:        "dashboard",
		LogDir:           "/mnt/mithril-logs/20260518-143209Z_abc1234_def56789",
		StdoutPath:       "/tmp/mithril-dashboard-spawn-stdout-123.log",
		StderrPath:       "/tmp/mithril-dashboard-spawn-stderr-123.log",
		StopInProgressBy: 0,
	}
}

// All fields survive a write→read JSON roundtrip.
func TestPidInfo_WriteRead_Roundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mithril.pid")
	want := samplePidInfo()

	require.NoError(t, WritePidFile(path, want))

	got, err := ReadPidFile(path)
	require.NoError(t, err)
	require.NotNil(t, got)
	assert.Equal(t, want.SchemaVersion, got.SchemaVersion)
	assert.Equal(t, want.Pid, got.Pid)
	assert.Equal(t, want.StartTimeTicks, got.StartTimeTicks)
	assert.Equal(t, want.ExeInode, got.ExeInode)
	assert.Equal(t, want.BinaryPath, got.BinaryPath)
	assert.Equal(t, want.RunID, got.RunID)
	assert.Equal(t, want.ConfigPath, got.ConfigPath)
	assert.Equal(t, want.SpawnedBy, got.SpawnedBy)
	assert.Equal(t, want.LogDir, got.LogDir)
	assert.Equal(t, want.StdoutPath, got.StdoutPath)
	assert.Equal(t, want.StderrPath, got.StderrPath)
}

// Absent file returns ErrPidFileNotFound, distinct from corrupt.
func TestReadPidFile_NotFound(t *testing.T) {
	dir := t.TempDir()
	got, err := ReadPidFile(filepath.Join(dir, "absent.pid"))
	assert.Nil(t, got)
	assert.ErrorIs(t, err, ErrPidFileNotFound)
}

// Corrupt JSON returns a wrapped error, not ErrPidFileNotFound.
func TestReadPidFile_CorruptJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.pid")
	require.NoError(t, os.WriteFile(path, []byte("{not json"), 0600))

	got, err := ReadPidFile(path)
	assert.Nil(t, got)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrPidFileNotFound, "corrupt JSON must not pretend to be 'not found'")
}

// Unknown schema versions are rejected.
func TestReadPidFile_WrongSchemaVersion(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.pid")
	body, _ := json.Marshal(map[string]any{"schema_version": 999, "pid": 1})
	require.NoError(t, os.WriteFile(path, body, 0600))

	got, err := ReadPidFile(path)
	assert.Nil(t, got)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "schema")
}

// Write is atomic via tmp+rename; no .tmp.* leftover after success.
func TestWritePidFile_AtomicViaTmpRename(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mithril.pid")
	require.NoError(t, WritePidFile(path, samplePidInfo()))

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.Equal(t, "mithril.pid", entries[0].Name())
}

// File is 0600 — JSON holds config paths and run IDs.
func TestWritePidFile_Permissions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mithril.pid")
	require.NoError(t, WritePidFile(path, samplePidInfo()))

	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0600), info.Mode().Perm())
}

func TestWritePidFile_CreatesParentDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "mithril.pid")
	require.NoError(t, WritePidFile(path, samplePidInfo()))

	_, err := os.Stat(path)
	assert.NoError(t, err)
}

func TestRemovePidFile_DeletesExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mithril.pid")
	require.NoError(t, WritePidFile(path, samplePidInfo()))
	require.NoError(t, RemovePidFile(path))

	_, err := os.Stat(path)
	assert.True(t, os.IsNotExist(err))
}

// Removing an absent file must not propagate ENOENT.
func TestRemovePidFile_AbsentIsNotError(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, RemovePidFile(filepath.Join(dir, "never_existed.pid")))
}

// UpdatePidLogDir sets the log dir without disturbing identity metadata.
func TestUpdatePidLogDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mithril.pid")
	require.NoError(t, WritePidFile(path, samplePidInfo()))

	newLogDir := "/mnt/mithril-logs/run-after-init"
	require.NoError(t, UpdatePidLogDir(path, newLogDir))

	got, err := ReadPidFile(path)
	require.NoError(t, err)
	assert.Equal(t, newLogDir, got.LogDir)
	assert.Equal(t, samplePidInfo().Pid, got.Pid)
	assert.Equal(t, samplePidInfo().RunID, got.RunID)
}

func TestUpdatePidOutputPaths(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mithril.pid")
	info := samplePidInfo()
	info.StdoutPath = ""
	info.StderrPath = ""
	require.NoError(t, WritePidFile(path, info))

	require.NoError(t, UpdatePidOutputPaths(path, info.Pid, "/tmp/stdout.log", "/tmp/stderr.log"))

	got, err := ReadPidFile(path)
	require.NoError(t, err)
	assert.Equal(t, "/tmp/stdout.log", got.StdoutPath)
	assert.Equal(t, "/tmp/stderr.log", got.StderrPath)
}

func TestUpdatePidOutputPaths_IgnoresDifferentPid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mithril.pid")
	info := samplePidInfo()
	info.StdoutPath = ""
	info.StderrPath = ""
	require.NoError(t, WritePidFile(path, info))

	require.NoError(t, UpdatePidOutputPaths(path, info.Pid+1, "/tmp/stdout.log", "/tmp/stderr.log"))

	got, err := ReadPidFile(path)
	require.NoError(t, err)
	assert.Empty(t, got.StdoutPath)
	assert.Empty(t, got.StderrPath)
}

// updateStopInProgressIfMatches sets/clears stop_in_progress_by when the run still matches.
func TestUpdateStopInProgressSetClear(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mithril.pid")
	expected := samplePidInfo()
	require.NoError(t, WritePidFile(path, expected))

	// Set.
	require.NoError(t, updateStopInProgressIfMatches(path, 99999, expected))
	got, err := ReadPidFile(path)
	require.NoError(t, err)
	assert.Equal(t, 99999, got.StopInProgressBy)
	assert.False(t, got.StopInProgressAt.IsZero(), "stop_in_progress_at should be stamped on set")

	// Clear.
	require.NoError(t, updateStopInProgressIfMatches(path, 0, got))
	got, err = ReadPidFile(path)
	require.NoError(t, err)
	assert.Equal(t, 0, got.StopInProgressBy)
	assert.True(t, got.StopInProgressAt.IsZero(), "stop_in_progress_at should be zeroed on clear")
}

func TestUpdateStopInProgressIfMatchesSkipsDifferentRun(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mithril.pid")
	oldRun := samplePidInfo()
	newRun := samplePidInfo()
	newRun.RunID = "new-run"
	newRun.StartTimeTicks++
	require.NoError(t, WritePidFile(path, newRun))

	require.NoError(t, updateStopInProgressIfMatches(path, 99999, oldRun))

	got, err := ReadPidFile(path)
	require.NoError(t, err)
	assert.Equal(t, "new-run", got.RunID)
	assert.Equal(t, 0, got.StopInProgressBy)
}

// isStopProgressStale staleness rules across PID sentinel, security gate, TTL, and liveness.
func TestPidInfo_StopStaleByDeadDashboard(t *testing.T) {
	// PID 0 is the "no stop in progress" sentinel — not stale.
	none := &PidInfo{StopInProgressBy: 0}
	assert.False(t, none.isStopProgressStale(time.Minute))

	// PID 1 (init) is never a real dashboard — always stale.
	pidOne := &PidInfo{StopInProgressBy: 1, StopInProgressAt: time.Now()}
	assert.True(t, pidOne.isStopProgressStale(time.Minute),
		"PID 1 should always be stale (not a legitimate dashboard PID)")

	// Negative PID must be stale before syscall.Kill sees it (kill(-1, 0) broadcast).
	negPid := &PidInfo{StopInProgressBy: -1, StopInProgressAt: time.Now()}
	assert.True(t, negPid.isStopProgressStale(time.Minute),
		"negative PID must be rejected as stale to prevent kill(-1, 0) broadcast")

	// Bogus high PID is almost certainly dead → stale.
	dead := &PidInfo{StopInProgressBy: 999999, StopInProgressAt: time.Now()}
	assert.True(t, dead.isStopProgressStale(time.Minute))

	// Old timestamp is stale via TTL regardless of liveness.
	staleByAge := &PidInfo{StopInProgressBy: 99999, StopInProgressAt: time.Now().Add(-10 * time.Minute)}
	assert.True(t, staleByAge.isStopProgressStale(time.Minute))

	// Live PID with a fresh timestamp — not stale (another dashboard mid-stop).
	livePid := &PidInfo{StopInProgressBy: os.Getpid(), StopInProgressAt: time.Now()}
	assert.False(t, livePid.isStopProgressStale(time.Minute),
		"live dashboard PID with fresh timestamp must not be stale")
}
