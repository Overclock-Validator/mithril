//go:build linux

package procctl

import (
	"errors"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

type SignalHandle struct {
	pid      int
	fd       int
	usePidfd bool
}

var (
	pidfdProbeOnce sync.Once
	pidfdSupported bool
)

// PidfdAvailable reports whether the kernel supports pidfd (>=5.3), probing
// once via pidfd_open(getpid()) and caching the result.
func PidfdAvailable() bool {
	pidfdProbeOnce.Do(func() {
		fd, err := unix.PidfdOpen(unix.Getpid(), 0)
		switch {
		case err == nil:
			_ = unix.Close(fd)
			pidfdSupported = true
		case errors.Is(err, unix.ENOSYS):
			pidfdSupported = false // pre-5.3, use kill(2)
		default:
			// Transient failure (e.g. fd exhaustion) — assume supported; per-call
			// open errors surface in OpenSignalHandle.
			pidfdSupported = true
		}
	})
	return pidfdSupported
}

// OpenSignalHandle opens a reusable signal target. With pidfd, open it before
// Matches() and signal the same handle after, closing the PID-reuse window.
func OpenSignalHandle(pid int) (*SignalHandle, error) {
	if PidfdAvailable() {
		fd, err := unix.PidfdOpen(pid, 0)
		if err != nil {
			// Already gone — treat as benign.
			if errors.Is(err, unix.ESRCH) {
				return nil, ErrProcessNotFound
			}
			return nil, err
		}
		return &SignalHandle{pid: pid, fd: fd, usePidfd: true}, nil
	}
	return &SignalHandle{pid: pid}, nil
}

func (h *SignalHandle) Close() error {
	if h == nil || !h.usePidfd {
		return nil
	}
	err := unix.Close(h.fd)
	h.usePidfd = false
	h.fd = -1
	return err
}

func (h *SignalHandle) Signal(sig syscall.Signal) error {
	if h == nil {
		return errors.New("nil signal handle")
	}
	if h.usePidfd {
		return unix.PidfdSendSignal(h.fd, unix.Signal(sig), nil, 0)
	}
	return syscall.Kill(h.pid, sig)
}

// SendSignal delivers sig via pidfd_send_signal when available, else kill(2)
// (signal 0 = liveness probe). The kill(2) fallback needs a Matches() re-verify.
func SendSignal(pid int, sig syscall.Signal) error {
	handle, err := OpenSignalHandle(pid)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Signal(sig)
}
