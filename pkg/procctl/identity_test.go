package procctl

import (
	"errors"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Cross-platform identity tests (Linux + macOS). /proc parser edge cases live
// in identity_linux_test.go.

// Reading our own identity works on every platform.
func TestReadIdentity_Self(t *testing.T) {
	id, err := ReadIdentity(os.Getpid())
	require.NoError(t, err)
	require.NotNil(t, id)
	assert.Equal(t, os.Getpid(), id.Pid)
	assert.NotZero(t, id.StartTimeTicks, "start time must be populated on every platform")
	// ExeInode is Linux-only; macOS leaves it zero.
	if runtime.GOOS == "linux" {
		assert.NotZero(t, id.ExeInode, "Linux must populate ExeInode")
	}
}

// Absent PID (past pid_max) returns ErrProcessNotFound.
func TestReadIdentity_NonExistentPid(t *testing.T) {
	_, err := ReadIdentity(9999999)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrProcessNotFound),
		"expected ErrProcessNotFound, got %v", err)
}

// Matches against our own live identity is true.
func TestMatches_SelfHappyPath(t *testing.T) {
	id, err := ReadIdentity(os.Getpid())
	require.NoError(t, err)

	info := &PidInfo{
		Pid:            os.Getpid(),
		ExeInode:       id.ExeInode,
		StartTimeTicks: id.StartTimeTicks,
	}
	ok, reason := Matches(os.Getpid(), info)
	assert.True(t, ok, "matches own identity: %s", reason)
}

// PID-reuse case: right PID + exe but different start time → no match.
func TestMatches_StartTimeMismatchFails(t *testing.T) {
	id, err := ReadIdentity(os.Getpid())
	require.NoError(t, err)

	info := &PidInfo{
		Pid:            os.Getpid(),
		ExeInode:       id.ExeInode,
		StartTimeTicks: id.StartTimeTicks + 999_999_999, // synthetic mismatch
	}
	ok, reason := Matches(os.Getpid(), info)
	assert.False(t, ok)
	assert.Contains(t, reason, "start time")
}

// Different binary at the same PID → no match. Skipped on macOS (ExeInode is zero).
func TestMatches_ExeInodeMismatchFails(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("ExeInode is Linux-only")
	}
	id, err := ReadIdentity(os.Getpid())
	require.NoError(t, err)

	info := &PidInfo{
		Pid:            os.Getpid(),
		ExeInode:       id.ExeInode + 1, // synthetic mismatch
		StartTimeTicks: id.StartTimeTicks,
	}
	ok, reason := Matches(os.Getpid(), info)
	assert.False(t, ok)
	assert.Contains(t, reason, "inode")
}

// Recorded PID != live PID bails out before any /proc read.
func TestMatches_PidMismatch(t *testing.T) {
	info := &PidInfo{
		Pid:            99,
		StartTimeTicks: 12345,
	}
	ok, reason := Matches(100, info)
	assert.False(t, ok)
	assert.Contains(t, reason, "pid mismatch")
}

func TestMatches_IncompleteIdentityFailsClosed(t *testing.T) {
	info := &PidInfo{Pid: os.Getpid()}
	ok, reason := Matches(os.Getpid(), info)
	assert.False(t, ok)
	assert.Contains(t, reason, "incomplete")
}

// Nil PidInfo returns (false, reason), no crash.
func TestMatches_NilInfo(t *testing.T) {
	ok, reason := Matches(os.Getpid(), nil)
	assert.False(t, ok)
	assert.NotEmpty(t, reason)
}

// Matches against a reaped PID is false.
func TestMatches_DeadPidReturnsFalse(t *testing.T) {
	cmd := exec.Command("sh", "-c", "exit 0")
	require.NoError(t, cmd.Start())
	pid := cmd.Process.Pid
	require.NoError(t, cmd.Wait()) // reaps the zombie

	// Let the kernel flush /proc on Linux (macOS reaps via Wait()).
	time.Sleep(10 * time.Millisecond)

	info := &PidInfo{
		Pid:            pid,
		StartTimeTicks: 1, // arbitrary; ReadIdentity fails first
	}
	ok, reason := Matches(pid, info)
	assert.False(t, ok)
	assert.NotEmpty(t, reason)
}
