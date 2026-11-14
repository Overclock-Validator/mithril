//go:build linux

package metrics

import (
	"errors"
	"net"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

func readUDPSocketMemory(conn *net.UDPConn) SocketObservation {
	start := time.Now()
	var values [9]uint32
	size := uint32(socketMemoryBytes)
	raw, err := conn.SyscallConn()
	if err == nil {
		var optionErr error
		err = raw.Control(func(fd uintptr) {
			_, _, errno := unix.Syscall6(unix.SYS_GETSOCKOPT, fd,
				unix.SOL_SOCKET, unix.SO_MEMINFO,
				uintptr(unsafe.Pointer(&values[0])), uintptr(unsafe.Pointer(&size)), 0)
			if errno != 0 {
				optionErr = errno
			}
		})
		if err == nil {
			err = optionErr
		}
	}
	return finishSocketObservation(start, time.Now(), values, size, socketReadReason(err))
}

func socketReadReason(err error) SocketReason {
	switch {
	case err == nil:
		return SocketAvailable
	case errors.Is(err, net.ErrClosed) || errors.Is(err, unix.EBADF):
		return SocketClosed
	case errors.Is(err, unix.ENOPROTOOPT) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS):
		return SocketUnsupported
	default:
		return SocketReadFailed
	}
}
