//go:build !linux

package turbine

import "net"

const receiveControlSize = 0

func enableReceiveTelemetry(*net.UDPConn) bool { return false }

func parseReceiveControl([]byte) (int64, uint32, bool) { return 0, 0, false }
