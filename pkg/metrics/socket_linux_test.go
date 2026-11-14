//go:build linux

package metrics

import (
	"errors"
	"fmt"
	"net"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

func TestSocketMemoryLinuxABIAndErrors(t *testing.T) {
	indices := [...]int{unix.SK_MEMINFO_RMEM_ALLOC, unix.SK_MEMINFO_RCVBUF,
		unix.SK_MEMINFO_WMEM_ALLOC, unix.SK_MEMINFO_SNDBUF, unix.SK_MEMINFO_FWD_ALLOC,
		unix.SK_MEMINFO_WMEM_QUEUED, unix.SK_MEMINFO_OPTMEM, unix.SK_MEMINFO_BACKLOG, unix.SK_MEMINFO_DROPS}
	for i, index := range indices {
		if index != i {
			t.Fatalf("ABI index %d = %d", i, index)
		}
	}
	if unix.SK_MEMINFO_VARS != 9 || unsafe.Sizeof([9]uint32{}) != socketMemoryBytes {
		t.Fatal("socket-memory ABI size changed")
	}
	for _, tc := range []struct {
		err  error
		want SocketReason
	}{
		{nil, SocketAvailable}, {net.ErrClosed, SocketClosed},
		{fmt.Errorf("read: %w", net.ErrClosed), SocketClosed}, {unix.EBADF, SocketClosed},
		{unix.ENOPROTOOPT, SocketUnsupported}, {unix.EOPNOTSUPP, SocketUnsupported},
		{unix.ENOSYS, SocketUnsupported}, {fmt.Errorf("read: %w", unix.ENOPROTOOPT), SocketUnsupported},
		{unix.EPERM, SocketReadFailed}, {unix.EINTR, SocketReadFailed},
		{errors.New("unknown"), SocketReadFailed},
	} {
		if got := socketReadReason(tc.err); got != tc.want {
			t.Fatalf("reason for %v = %d, want %d", tc.err, got, tc.want)
		}
	}
}

func TestSocketMemoryLinuxPreservesSocket(t *testing.T) {
	conn := socketMemoryTestListener(t)
	beforeLimits := socketMemoryTestLimits(t, conn)
	start := time.Now()
	got := ObserveUDPSocketMemory(conn)
	end := time.Now()
	if !got.Known || got.Reason != SocketAvailable || got.StartedAt.Before(start) || got.SampledAt.After(end) || got.SampledAt.Before(got.StartedAt) {
		t.Fatalf("live capture = %+v", got)
	}
	if got.Memory.ReceiveLimit != uint32(beforeLimits[0]) || got.Memory.SendLimit != uint32(beforeLimits[1]) {
		t.Fatalf("limits = %+v, want %v", got.Memory, beforeLimits)
	}
	if err := conn.SetDeadline(time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if expired := ObserveUDPSocketMemory(conn); !expired.Known {
		t.Fatalf("socket deadline affected Control capture: %+v", expired)
	}
	buf := make([]byte, 64)
	if _, _, err := conn.ReadFromUDP(buf); err == nil {
		t.Fatal("read deadline changed")
	} else {
		var timeout net.Error
		if !errors.As(err, &timeout) || !timeout.Timeout() {
			t.Fatalf("read deadline changed: %v", err)
		}
	}
	if _, err := conn.WriteToUDP([]byte("expired"), conn.LocalAddr().(*net.UDPAddr)); err == nil {
		t.Fatal("write deadline changed")
	} else {
		var timeout net.Error
		if !errors.As(err, &timeout) || !timeout.Timeout() {
			t.Fatalf("write deadline = %v", err)
		}
	}
	if err := conn.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	sender := socketMemoryTestListener(t)
	payload := []byte("socket observation must not consume this datagram")
	if n, err := sender.WriteToUDP(payload, conn.LocalAddr().(*net.UDPAddr)); err != nil || n != len(payload) {
		t.Fatalf("fixture send = %d, %v", n, err)
	}
	if queued := ObserveUDPSocketMemory(conn); !queued.Known {
		t.Fatalf("queued capture = %+v", queued)
	}
	if n, _, err := conn.ReadFromUDP(buf); err != nil || string(buf[:n]) != string(payload) {
		t.Fatalf("datagram after capture = %q, %v", buf[:n], err)
	}
	if afterLimits := socketMemoryTestLimits(t, conn); afterLimits != beforeLimits {
		t.Fatalf("socket limits changed: %v to %v", beforeLimits, afterLimits)
	}
}

func TestSocketMemoryLinuxClose(t *testing.T) {
	if got := ObserveUDPSocketMemory(&net.UDPConn{}); got.Known || got.Reason != SocketReadFailed || got.Memory != (SocketMemory{}) {
		t.Fatalf("uninitialized socket = %+v", got)
	}
	conn := socketMemoryTestListener(t)
	start := make(chan struct{})
	done := make(chan []SocketObservation, 1)
	go func() {
		<-start
		observations := make([]SocketObservation, 32)
		for i := range observations {
			observations[i] = ObserveUDPSocketMemory(conn)
		}
		done <- observations
	}()
	close(start)
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	for _, got := range <-done {
		if got.Known {
			if got.Reason != SocketAvailable {
				t.Fatalf("known close-race observation = %+v", got)
			}
		} else if got.Reason != SocketClosed || got.Memory != (SocketMemory{}) {
			t.Fatalf("unknown close-race observation = %+v", got)
		}
	}
	if got := ObserveUDPSocketMemory(conn); got.Known || got.Reason != SocketClosed || got.Memory != (SocketMemory{}) {
		t.Fatalf("closed socket = %+v", got)
	}
}

func socketMemoryTestListener(t *testing.T) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			t.Error(err)
		}
	})
	return conn
}

func socketMemoryTestLimits(t *testing.T, conn *net.UDPConn) [2]int {
	t.Helper()
	var limits [2]int
	raw, err := conn.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var optionErr error
	err = raw.Control(func(fd uintptr) {
		for i, option := range []int{unix.SO_RCVBUF, unix.SO_SNDBUF} {
			limits[i], optionErr = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, option)
			if optionErr != nil {
				return
			}
		}
	})
	if err != nil || optionErr != nil {
		t.Fatalf("read fixture limits: %v, %v", err, optionErr)
	}
	return limits
}
