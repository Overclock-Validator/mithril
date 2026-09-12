package gossip

import (
	"net"
	"syscall"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

// UDP receive-buffer targets. Turbine shreds arrive in multi-megabyte bursts
// — one large block is thousands of ~1.2KB shreds sent back-to-back, and
// repair responses burst onto the same socket — so the kernel socket buffer
// is the only thing absorbing line-rate bursts while the read loop drains.
// The stock Linux default (~208KiB) overflows on the FIRST large block and
// the kernel silently drops the rest: slots are seen (a few shreds land) but
// almost never complete, which presents as "turbine receives but repair and
// assembly deliver nothing". Sized to Agave's recommended validator tuning.
const (
	TurbineUDPReceiveBufferBytes  = 64 << 20
	TurbineUDPTransmitBufferBytes = 16 << 20
	GossipUDPReceiveBufferBytes   = 8 << 20
)

// BoostUDPReceiveBuffer raises conn's kernel receive buffer toward want and
// reports what was actually granted. Linux caps unprivileged requests at
// net.core.rmem_max (and reports roughly double the usable size for kernel
// bookkeeping); when the grant lands far below the ask, the WARN carries the
// exact sysctl remediation — silent UDP loss on this socket is otherwise
// nearly undiagnosable.
func BoostUDPReceiveBuffer(conn *net.UDPConn, want int, label string) {
	if conn == nil || want <= 0 {
		return
	}
	_ = conn.SetReadBuffer(want)
	got := udpReceiveBufferBytes(conn)
	if got < 0 {
		mlog.Log.Infof("%s: requested %dMiB UDP receive buffer (kernel grant unknown)", label, want>>20)
		return
	}
	if got < want {
		mlog.Log.Warnf("%s: kernel granted only %dKiB of the %dMiB UDP receive buffer request — shred/repair bursts WILL be dropped and blocks will rarely complete. Raise the cap: sudo sysctl -w net.core.rmem_max=134217728 net.core.rmem_default=134217728 (persist in /etc/sysctl.d/21-mithril.conf), then restart mithril",
			label, got>>10, want>>20)
		return
	}
	mlog.Log.Infof("%s: UDP receive buffer %dMiB", label, got>>20)
}

func udpReceiveBufferBytes(conn *net.UDPConn) int {
	raw, err := conn.SyscallConn()
	if err != nil {
		return -1
	}
	size := -1
	_ = raw.Control(func(fd uintptr) {
		if v, err := syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF); err == nil {
			size = v
		}
	})
	return size
}

// BoostUDPTransmitBuffer raises conn's kernel send buffer toward want and
// returns what was actually granted. A small SO_SNDBUF makes Linux sendmmsg
// accept only a prefix of a Turbine fanout batch; because at least one
// datagram was accepted, the syscall reports that short batch without an
// errno. Reporting the effective value here keeps that local backpressure
// distinguishable from a downstream network failure.
func BoostUDPTransmitBuffer(conn *net.UDPConn, want int, label string) int {
	if conn == nil || want <= 0 {
		return 0
	}
	_ = conn.SetWriteBuffer(want)
	got := udpTransmitBufferBytes(conn)
	if got < 0 {
		mlog.Log.Infof("%s: requested %dMiB UDP transmit buffer (kernel grant unknown)", label, want>>20)
		return 0
	}
	if got < want {
		mlog.Log.Warnf("%s: kernel granted only %dKiB of the %dMiB UDP transmit buffer request — retransmit fanout batches can be only partially accepted. Raise the cap: sudo sysctl -w net.core.wmem_max=134217728 net.core.wmem_default=134217728 (persist in /etc/sysctl.d/21-mithril.conf), then restart mithril",
			label, got>>10, want>>20)
		return got
	}
	mlog.Log.Infof("%s: UDP transmit buffer %dMiB", label, got>>20)
	return got
}

func udpTransmitBufferBytes(conn *net.UDPConn) int {
	raw, err := conn.SyscallConn()
	if err != nil {
		return -1
	}
	size := -1
	_ = raw.Control(func(fd uintptr) {
		if v, err := syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF); err == nil {
			size = v
		}
	})
	return size
}
