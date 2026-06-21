package procctl

import (
	"os/exec"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// SendSignal — race-free delivery (pidfd on Linux ≥5.3, syscall.Kill fallback
// elsewhere). Tests run against a real sleep subprocess.

// SIGTERM to a sleeping process makes it exit well before the sleep ends.
func TestSendSignal_DeliversSigtermToSleep(t *testing.T) {
	cmd := exec.Command("sh", "-c", "sleep 30")
	require.NoError(t, cmd.Start())
	pid := cmd.Process.Pid

	// Give the shell + sleep a tick to set up signal handlers.
	time.Sleep(50 * time.Millisecond)

	require.NoError(t, SendSignal(pid, syscall.SIGTERM))

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case <-done:
		// Expected — process exited promptly.
	case <-time.After(5 * time.Second):
		// If we timed out, force-kill so the test binary doesn't hang.
		_ = cmd.Process.Kill()
		<-done
		t.Fatal("process did not exit within 5s of SIGTERM")
	}
}

// Signaling an absent PID errors rather than silently succeeding.
func TestSendSignal_NonExistentPidReturnsErr(t *testing.T) {
	err := SendSignal(9999999, syscall.SIGTERM)
	require.Error(t, err)
	// Skip the errno check: pidfd and Kill paths wrap ESRCH differently.
	t.Logf("SendSignal to absent PID returned: %v", err)
}

// Signal 0 is a liveness/permission check (used by Detect and audit logging).
func TestSendSignal_Sig0IsLivenessCheck(t *testing.T) {
	cmd := exec.Command("sh", "-c", "sleep 5")
	require.NoError(t, cmd.Start())
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	time.Sleep(20 * time.Millisecond)

	require.NoError(t, SendSignal(cmd.Process.Pid, syscall.Signal(0)))
}

// PidfdAvailable is stable across calls and false on non-Linux.
func TestPidfdAvailable_DocsRuntime(t *testing.T) {
	a := PidfdAvailable()
	b := PidfdAvailable()
	assert.Equal(t, a, b, "PidfdAvailable should be stable")
	if runtime.GOOS != "linux" {
		assert.False(t, a, "PidfdAvailable must be false on non-Linux")
	}
	t.Logf("PidfdAvailable on %s: %v", runtime.GOOS, a)
}
