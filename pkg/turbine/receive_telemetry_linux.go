//go:build linux

package turbine

import (
	"encoding/binary"
	"net"

	"golang.org/x/sys/unix"
)

// receiveControlSize holds one SCM_TIMESTAMPNS timespec and one SO_RXQ_OVFL
// counter, each behind its own control message header.
var receiveControlSize = unix.CmsgSpace(16) + unix.CmsgSpace(4)

// enableReceiveTelemetry asks the kernel to attach each packet's receive time
// (SO_TIMESTAMPNS, CLOCK_REALTIME) and the socket's cumulative drop count
// (SO_RXQ_OVFL). It reports whether timestamps are on; drop counts are best
// effort. Failure leaves the socket as it was.
func enableReceiveTelemetry(conn *net.UDPConn) bool {
	raw, err := conn.SyscallConn()
	if err != nil {
		return false
	}
	var timestamps bool
	_ = raw.Control(func(fd uintptr) {
		timestamps = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_TIMESTAMPNS, 1) == nil
		if timestamps {
			_ = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_RXQ_OVFL, 1)
		}
	})
	return timestamps
}

// parseReceiveControl reads the receive time and drop count from one packet's
// control data without allocating. Missing or malformed messages read as zero.
func parseReceiveControl(oob []byte) (receivedNanos int64, dropped uint32, haveDrops bool) {
	for len(oob) >= unix.SizeofCmsghdr {
		hdr := (*unix.Cmsghdr)(unsafePointer(&oob[0]))
		msgLen := int(hdr.Len)
		if msgLen < unix.SizeofCmsghdr || msgLen > len(oob) {
			return
		}
		data := oob[unix.CmsgLen(0):msgLen]
		if hdr.Level == unix.SOL_SOCKET {
			switch hdr.Type {
			case unix.SCM_TIMESTAMPNS:
				if len(data) >= 16 {
					sec := int64(binary.NativeEndian.Uint64(data[0:8]))
					nsec := int64(binary.NativeEndian.Uint64(data[8:16]))
					receivedNanos = sec*1_000_000_000 + nsec
				}
			case unix.SO_RXQ_OVFL:
				if len(data) >= 4 {
					dropped = binary.NativeEndian.Uint32(data[0:4])
					haveDrops = true
				}
			}
		}
		next := unix.CmsgSpace(msgLen - unix.CmsgLen(0))
		if next <= 0 || next > len(oob) {
			return
		}
		oob = oob[next:]
	}
	return
}
