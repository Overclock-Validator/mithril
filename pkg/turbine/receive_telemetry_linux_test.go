//go:build linux

package turbine

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func controlMessage(typ int32, data []byte) []byte {
	buf := make([]byte, unix.CmsgSpace(len(data)))
	hdr := (*unix.Cmsghdr)(unsafePointer(&buf[0]))
	hdr.Level = unix.SOL_SOCKET
	hdr.Type = typ
	hdr.SetLen(unix.CmsgLen(len(data)))
	copy(buf[unix.CmsgLen(0):], data)
	return buf
}

func TestParseReceiveControl(t *testing.T) {
	ts := make([]byte, 16)
	binary.NativeEndian.PutUint64(ts[0:], 1_700_000_000)
	binary.NativeEndian.PutUint64(ts[8:], 123_456_789)
	drops := make([]byte, 4)
	binary.NativeEndian.PutUint32(drops, 42)

	oob := append(controlMessage(unix.SCM_TIMESTAMPNS, ts), controlMessage(unix.SO_RXQ_OVFL, drops)...)
	got, dropped, haveDrops := parseReceiveControl(oob)
	require.Equal(t, int64(1_700_000_000_123_456_789), got)
	require.True(t, haveDrops)
	require.Equal(t, uint32(42), dropped)

	got, _, haveDrops = parseReceiveControl(nil)
	require.Zero(t, got)
	require.False(t, haveDrops)
	got, _, _ = parseReceiveControl(oob[:5]) // truncated header
	require.Zero(t, got)
}

// A real socket delivers a kernel timestamp close to now.
func TestReceiveTelemetryOnLoopback(t *testing.T) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer conn.Close()
	require.True(t, enableReceiveTelemetry(conn))
	sender, err := net.DialUDP("udp", nil, conn.LocalAddr().(*net.UDPAddr))
	require.NoError(t, err)
	defer sender.Close()
	before := time.Now().UnixNano()
	_, err = sender.Write([]byte("x"))
	require.NoError(t, err)
	buf, oob := make([]byte, 16), make([]byte, receiveControlSize)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(2*time.Second)))
	_, oobn, _, _, err := conn.ReadMsgUDP(buf, oob)
	require.NoError(t, err)
	got, _, _ := parseReceiveControl(oob[:oobn])
	require.GreaterOrEqual(t, got, before-int64(time.Second))
	require.LessOrEqual(t, got, time.Now().UnixNano())
}
