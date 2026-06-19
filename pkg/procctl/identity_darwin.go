//go:build darwin

package procctl

import (
	"errors"
	"fmt"
	"syscall"

	"golang.org/x/sys/unix"
)

// ReadIdentity reads process identity on macOS via kern.proc.pid sysctl. ExeInode
// stays zero; StartTimeTicks is Unix-nano start time. Dev-only; production is Linux.
func ReadIdentity(pid int) (*Identity, error) {
	// kill(0): ESRCH means gone.
	if err := syscall.Kill(pid, 0); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return nil, ErrProcessNotFound
		}
		// EPERM means alive but unsignalable — still a valid identity.
		if !errors.Is(err, syscall.EPERM) {
			return nil, fmt.Errorf("kill(0) %d: %w", pid, err)
		}
	}

	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil {
		// A missing PID surfaces as EIO (sometimes ESRCH) here; normalize it.
		if errors.Is(err, syscall.EIO) || errors.Is(err, syscall.ESRCH) {
			return nil, ErrProcessNotFound
		}
		return nil, fmt.Errorf("sysctl kern.proc.pid %d: %w", pid, err)
	}
	if kp == nil || kp.Proc.P_pid == 0 {
		return nil, ErrProcessNotFound
	}
	startNsec := kp.Proc.P_starttime.Nano()
	if startNsec <= 0 {
		return nil, fmt.Errorf("sysctl kern.proc.pid %d returned empty start time", pid)
	}

	// ByteSliceToString truncates at the first NUL; P_comm's trailing bytes
	// can hold stale kernel data that TrimRight would leave behind.
	exePath := unix.ByteSliceToString(kp.Proc.P_comm[:])

	return &Identity{
		Pid:            pid,
		ExeInode:       0, // unavailable on macOS
		StartTimeTicks: uint64(startNsec),
		ExePath:        exePath,
	}, nil
}
