//go:build linux

package metrics

import (
	"errors"
	"runtime"
	"time"

	"golang.org/x/sys/unix"
)

func readLinkTopology(topology TopologyObservation, affinity ThreadAffinityObservation) LinkTopologyObservation {
	// The socket and both namespace probes belong to the same OS thread.
	// No namespace is entered or changed by this sampler.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	return readLinkTopologyCapture(topology, affinity, interfaceTrafficNamespace, openLinkTopologySocket, time.Now)
}

func linkTopologySyscallError(err error) error {
	if errors.Is(err, unix.EAFNOSUPPORT) || errors.Is(err, unix.EPROTONOSUPPORT) || errors.Is(err, unix.EOPNOTSUPP) {
		return errLinkUnsupported
	}
	return err
}

func openLinkTopologySocket(c *linkTopologyCapture) (linkTopologySocket, error) {
	if !c.check() {
		return linkTopologySocket{}, c.base.failureError()
	}
	if err := affinityOpen(&c.base); err != nil {
		return linkTopologySocket{}, err
	}
	fd, err := unix.Socket(unix.AF_NETLINK, unix.SOCK_RAW|unix.SOCK_CLOEXEC|unix.SOCK_NONBLOCK, unix.NETLINK_ROUTE)
	if err != nil {
		return linkTopologySocket{}, linkTopologySyscallError(err)
	}
	owned := false
	defer func() {
		if !owned {
			unix.Close(fd)
		}
	}()
	if !c.check() {
		return linkTopologySocket{}, c.base.failureError()
	}
	// Zero multicast groups: this is a request socket, never a subscription.
	err = unix.Bind(fd, &unix.SockaddrNetlink{Family: unix.AF_NETLINK})
	if !c.check() {
		return linkTopologySocket{}, c.base.failureError()
	}
	if err != nil {
		return linkTopologySocket{}, linkTopologySyscallError(err)
	}
	address, err := unix.Getsockname(fd)
	if !c.check() {
		return linkTopologySocket{}, c.base.failureError()
	}
	if err != nil {
		return linkTopologySocket{}, err
	}
	local, ok := address.(*unix.SockaddrNetlink)
	if !ok || local.Family != unix.AF_NETLINK || local.Pid == 0 || local.Groups != 0 {
		return linkTopologySocket{}, errTopologyRecord
	}
	owned = true
	return linkTopologySocket{
		port: local.Pid,
		send: func(request []byte) error {
			// Only four fixed read requests can cross this concrete seam.
			if !linkTopologyReadRequestAllowed(request) {
				return errTopologyRecord
			}
			return linkTopologySyscallError(unix.Sendto(fd, request, unix.MSG_DONTWAIT, &unix.SockaddrNetlink{Family: unix.AF_NETLINK}))
		},
		receive: func(buffer []byte, wait time.Duration) (int, uint32, uint32, bool, error) {
			if len(buffer) != linkReceiveBytes || wait <= 0 || wait > linkReadLimit {
				return 0, 0, 0, false, errTopologyRecord
			}
			if !c.check() {
				return 0, 0, 0, false, c.base.failureError()
			}
			fds := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
			timeout := unix.NsecToTimespec(wait.Nanoseconds())
			n, err := unix.Ppoll(fds, &timeout, nil)
			if !c.check() {
				return 0, 0, 0, false, c.base.failureError()
			}
			if err != nil {
				return 0, 0, 0, false, err
			}
			if n == 0 {
				return 0, 0, 0, false, errTopologyExpired
			}
			if n != 1 || fds[0].Revents&unix.POLLIN == 0 || fds[0].Revents&(unix.POLLHUP|unix.POLLNVAL) != 0 {
				return 0, 0, 0, false, errTopologyRecord
			}
			// A pending socket error is read by Recvmsg and refused (including
			// ENOBUFS); it is never converted into a complete partial dump.
			n, oob, flags, from, err := unix.Recvmsg(fd, buffer, nil, unix.MSG_DONTWAIT)
			if err != nil {
				return n, 0, 0, false, linkTopologySyscallError(err)
			}
			sender, ok := from.(*unix.SockaddrNetlink)
			if !ok || sender.Family != unix.AF_NETLINK {
				return n, 0, 0, false, errTopologyRecord
			}
			return n, sender.Pid, sender.Groups, oob != 0 || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0, nil
		},
		close: func() { unix.Close(fd) },
	}, nil
}
