//go:build !linux

package metrics

import "net"

func readUDPSocketMemory(_ *net.UDPConn) SocketObservation {
	return SocketObservation{Reason: SocketUnsupported}
}
