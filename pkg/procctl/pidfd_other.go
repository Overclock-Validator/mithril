//go:build !linux

package procctl

import "syscall"

type SignalHandle struct {
	pid int
}

// PidfdAvailable is always false off Linux (pidfd is Linux-only).
func PidfdAvailable() bool { return false }

func OpenSignalHandle(pid int) (*SignalHandle, error) {
	return &SignalHandle{pid: pid}, nil
}

func (h *SignalHandle) Close() error {
	return nil
}

func (h *SignalHandle) Signal(sig syscall.Signal) error {
	return syscall.Kill(h.pid, sig)
}

// SendSignal sends sig via kill(2) off Linux.
func SendSignal(pid int, sig syscall.Signal) error {
	handle, err := OpenSignalHandle(pid)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Signal(sig)
}
