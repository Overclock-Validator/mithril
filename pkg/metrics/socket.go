package metrics

import (
	"net"
	"time"
)

// SocketReason distinguishes unavailable observations from known zero values.
type SocketReason uint8

const (
	SocketNotProvided SocketReason = iota
	SocketAvailable
	SocketUnsupported
	SocketClosed
	SocketReadFailed
	SocketInvalidResponse
	SocketCaptureExpired
	SocketClockInvalid
)

// SocketMemory contains Linux socket accounting bytes except Drops, which is a
// socket-lifetime uint32 counter. Allocation is not queued UDP payload size.
// The kernel reads these fields separately; they are not an atomic balance.
type SocketMemory struct {
	ReceiveAllocated uint32
	ReceiveLimit     uint32
	SendAllocated    uint32
	SendLimit        uint32
	ForwardAllocated uint32
	SendQueued       uint32
	OptionsAllocated uint32
	Backlog          uint32
	Drops            uint32
}

// SocketObservation is a detached read of one owner-supplied UDP socket. It
// carries no descriptor, address or ownership binding; the caller must reject
// a result if its socket generation changes during capture.
type SocketObservation struct {
	StartedAt, SampledAt time.Time
	Known                bool
	Reason               SocketReason
	Memory               SocketMemory
}

// SocketDropDelta is an observed counter difference, not a delivery verdict.
// The caller must establish that both observations belong to the same socket.
type SocketDropDelta struct {
	Known                bool
	Drops                uint64
	StartedAt, SampledAt time.Time
}

// ObservedSocketDrops uses existing reads only. Missing or regressing counters
// restart coverage instead of treating reset/wrap or unavailable data as zero.
func ObservedSocketDrops(before, after SocketObservation) SocketDropDelta {
	for _, o := range [...]SocketObservation{before, after} {
		if !o.Known || o.Reason != SocketAvailable || o.StartedAt.IsZero() || o.SampledAt.IsZero() ||
			o.SampledAt.Before(o.StartedAt) || o.SampledAt.Sub(o.StartedAt) > time.Second {
			return SocketDropDelta{}
		}
	}
	elapsed := after.SampledAt.Sub(before.SampledAt)
	if elapsed <= 0 || elapsed > 30*time.Second || after.Memory.Drops < before.Memory.Drops {
		return SocketDropDelta{}
	}
	return SocketDropDelta{Known: true, Drops: uint64(after.Memory.Drops - before.Memory.Drops),
		StartedAt: before.SampledAt, SampledAt: after.SampledAt}
}

const (
	socketMemoryBytes   = 9 * 4
	socketCaptureBudget = time.Second
)

// ObserveUDPSocketMemory reads accounting without consuming data or changing
// socket options/deadlines. Nil inputs and unsupported operating systems cause
// no clock or OS reads. Call only from an opted-in owner, not per viewer or packet.
// The one-second observed budget rejects a slow result; it cannot cancel a
// system call already in progress. This function retains no delta baseline.
func ObserveUDPSocketMemory(conn *net.UDPConn) SocketObservation {
	if conn == nil {
		return SocketObservation{Reason: SocketNotProvided}
	}
	return readUDPSocketMemory(conn)
}

// finishSocketObservation validates the fixed Linux ABI before copying values.
// Capture times retain their monotonic component for the owner's interval checks.
func finishSocketObservation(start, end time.Time, values [9]uint32, size uint32, reason SocketReason) SocketObservation {
	o := SocketObservation{StartedAt: start, SampledAt: end, Reason: reason}
	switch {
	case start.IsZero() || end.IsZero() || end.Before(start):
		o.Reason = SocketClockInvalid
	case end.Sub(start) > socketCaptureBudget:
		o.Reason = SocketCaptureExpired
	case reason == SocketUnsupported || reason == SocketClosed || reason == SocketReadFailed:
	case reason != SocketAvailable:
		o.Reason = SocketReadFailed
	case size != socketMemoryBytes:
		o.Reason = SocketInvalidResponse
	default:
		o.Known = true
		o.Memory = SocketMemory{
			ReceiveAllocated: values[0], ReceiveLimit: values[1],
			SendAllocated: values[2], SendLimit: values[3],
			ForwardAllocated: values[4], SendQueued: values[5],
			OptionsAllocated: values[6], Backlog: values[7], Drops: values[8],
		}
	}
	return o
}
