package state

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// minimalValidState populates the fields LoadState and Save require.
func minimalValidState() *MithrilState {
	return &MithrilState{
		StateSchemaVersion: CurrentStateSchemaVersion,
		Stage:              "ready",
		SnapshotSlot:       100,
	}
}

// StartSession sets CurrentSessionStartedAt within [before, after].
func TestStartSession_StampsCurrentTime(t *testing.T) {
	dir := t.TempDir()
	s := minimalValidState()

	before := time.Now()
	require.NoError(t, s.StartSession(dir))
	after := time.Now()

	assert.False(t, s.CurrentSessionStartedAt.IsZero(), "timestamp should be set")
	assert.False(t, s.CurrentSessionStartedAt.Before(before), "timestamp should be >= before")
	assert.False(t, s.CurrentSessionStartedAt.After(after), "timestamp should be <= after")
}

// CurrentSessionStartedAt survives a Save -> LoadState round-trip.
func TestStartSession_PersistsAcrossLoad(t *testing.T) {
	dir := t.TempDir()
	s := minimalValidState()
	require.NoError(t, s.StartSession(dir))
	saved := s.CurrentSessionStartedAt

	loaded, err := LoadState(dir)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	assert.True(t, loaded.CurrentSessionStartedAt.Equal(saved),
		"loaded timestamp should equal saved (got %v, want %v)",
		loaded.CurrentSessionStartedAt, saved)
}

// a second StartSession replaces the prior timestamp (one session per process).
func TestStartSession_OverwritesPriorValue(t *testing.T) {
	dir := t.TempDir()
	s := minimalValidState()
	require.NoError(t, s.StartSession(dir))
	first := s.CurrentSessionStartedAt
	time.Sleep(2 * time.Millisecond)
	require.NoError(t, s.StartSession(dir))
	second := s.CurrentSessionStartedAt

	assert.True(t, second.After(first), "second StartSession should advance the timestamp")
}

// a file without current_session_started_at loads with a zero-value field.
func TestLoadState_LegacyFileWithoutSessionField(t *testing.T) {
	dir := t.TempDir()
	legacyJSON := []byte(`{
		"state_schema_version": 2,
		"stage": "ready",
		"snapshot_slot": 100,
		"last_shutdown_reason": "graceful shutdown (Ctrl+C)",
		"last_shutdown_at": "2026-05-01T12:00:00Z"
	}`)
	require.NoError(t, os.WriteFile(filepath.Join(dir, StateFileName), legacyJSON, 0644))

	loaded, err := LoadState(dir)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	assert.True(t, loaded.CurrentSessionStartedAt.IsZero(),
		"legacy file should leave CurrentSessionStartedAt zero")
	assert.Equal(t, "graceful shutdown (Ctrl+C)", loaded.LastShutdownReason)
}

// TestWasCleanExit_NilState returns (false, reason) without panicking.
func TestWasCleanExit_NilState(t *testing.T) {
	clean, reason := WasCleanExit(nil)
	assert.False(t, clean)
	assert.NotEmpty(t, reason)
}

// happy path: clean shutdown, consistent timestamps, clean reason constant.
func TestWasCleanExit_FreshSession_CleanShutdown(t *testing.T) {
	start := time.Now().Add(-1 * time.Hour)
	s := &MithrilState{
		CurrentSessionStartedAt: start,
		LastShutdownAt:          start.Add(30 * time.Minute),
		LastShutdownReason:      ShutdownReasonNormal,
	}
	clean, _ := WasCleanExit(s)
	assert.True(t, clean)
}

// the other clean constant (replay completed) is also accepted.
func TestWasCleanExit_FreshSession_CompletedReason(t *testing.T) {
	start := time.Now().Add(-1 * time.Hour)
	s := &MithrilState{
		CurrentSessionStartedAt: start,
		LastShutdownAt:          start.Add(30 * time.Minute),
		LastShutdownReason:      ShutdownReasonCompleted,
	}
	clean, _ := WasCleanExit(s)
	assert.True(t, clean)
}

func TestWasCleanExit_FreshSession_StartupFailedBeforeReplay(t *testing.T) {
	start := time.Now().Add(-1 * time.Hour)
	s := &MithrilState{
		CurrentSessionStartedAt: start,
		LastShutdownAt:          start.Add(30 * time.Minute),
		LastShutdownReason:      ShutdownReasonStartupFailed + ": invalid port",
	}
	clean, reason := WasCleanExit(s)
	assert.True(t, clean)
	assert.Contains(t, reason, ShutdownReasonStartupFailed)
}

// session started but died before recording a shutdown (crash, OOM, SIGKILL).
func TestWasCleanExit_FreshSession_NoShutdownRecorded(t *testing.T) {
	s := &MithrilState{
		CurrentSessionStartedAt: time.Now().Add(-30 * time.Minute),
		// LastShutdownAt zero, LastShutdownReason empty
	}
	clean, reason := WasCleanExit(s)
	assert.False(t, clean)
	assert.NotEmpty(t, reason)
}

// shutdown timestamp predates the session start: stale, current session crashed.
func TestWasCleanExit_ShutdownBeforeSessionStart(t *testing.T) {
	priorShutdown := time.Now().Add(-2 * time.Hour)
	thisStart := time.Now().Add(-1 * time.Hour)
	s := &MithrilState{
		CurrentSessionStartedAt: thisStart,
		LastShutdownAt:          priorShutdown,
		LastShutdownReason:      ShutdownReasonNormal, // stale value from prior session
	}
	clean, reason := WasCleanExit(s)
	assert.False(t, clean, "stale shutdown timestamp must not be treated as clean")
	assert.NotEmpty(t, reason)
}

// non-clean reasons (stall, etc.) stay non-clean even with correct timestamps.
func TestWasCleanExit_FreshSession_StallReason(t *testing.T) {
	start := time.Now().Add(-1 * time.Hour)
	s := &MithrilState{
		CurrentSessionStartedAt: start,
		LastShutdownAt:          start.Add(30 * time.Minute),
		LastShutdownReason:      ShutdownReasonStall,
	}
	clean, _ := WasCleanExit(s)
	assert.False(t, clean, "stall is a recovery-required reason, not clean")
}

// legacy files (no session start) take the shutdown reason at face value.
func TestWasCleanExit_LegacyFile_CleanReason(t *testing.T) {
	s := &MithrilState{
		// CurrentSessionStartedAt is zero
		LastShutdownAt:     time.Now().Add(-30 * time.Minute),
		LastShutdownReason: ShutdownReasonNormal,
	}
	clean, reason := WasCleanExit(s)
	assert.True(t, clean, "legacy file with a clean reason should still be treated as clean")
	assert.Contains(t, reason, "legacy")
}

// legacy file with no reason returns false: cleanness can't be inferred.
func TestWasCleanExit_LegacyFile_EmptyReason(t *testing.T) {
	s := &MithrilState{
		// Everything zero/empty
	}
	clean, reason := WasCleanExit(s)
	assert.False(t, clean)
	assert.NotEmpty(t, reason)
}

func TestRecordSessionShutdown_PreservesReplayPosition(t *testing.T) {
	dir := t.TempDir()
	start := time.Now().Add(-1 * time.Minute)
	s := &MithrilState{
		StateSchemaVersion:      CurrentStateSchemaVersion,
		Stage:                   "ready",
		SnapshotSlot:            100,
		LastSlot:                123,
		LastBankhash:            "prior-bankhash",
		CurrentRunID:            "previous-run",
		CurrentSessionStartedAt: start,
	}

	require.NoError(t, s.RecordSessionShutdown(dir, &ShutdownContext{
		RunID:          "clean-early-stop",
		WriterVersion:  "test",
		WriterCommit:   "abcdef",
		WriterBranch:   "branch",
		ShutdownReason: ShutdownReasonNormal,
	}))

	loaded, err := LoadState(dir)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	assert.Equal(t, uint64(123), loaded.LastSlot)
	assert.Equal(t, "prior-bankhash", loaded.LastBankhash)
	assert.Equal(t, "previous-run", loaded.ParentRunID)
	assert.Equal(t, "clean-early-stop", loaded.CurrentRunID)

	clean, reason := WasCleanExit(loaded)
	assert.True(t, clean)
	assert.Equal(t, ShutdownReasonNormal, reason)
}
