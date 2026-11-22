package node

import (
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
)

// payloadSample stays with one attached diagnostics owner; it is not a packet
// recorder and retains the original monotonic clocks from the existing sample.
type payloadSample struct {
	startedAt, sampledAt time.Time
	bytes                uint64
	known                bool
}

func payloadWindow(before, after payloadSample, retained *dashboard.ApplicationPayloadInterval) *dashboard.ApplicationPayloadInterval {
	if !before.known || !after.known || before.startedAt.IsZero() || !before.startedAt.Equal(after.startedAt) ||
		before.sampledAt.IsZero() || after.sampledAt.IsZero() || before.sampledAt == before.sampledAt.Round(0) ||
		after.sampledAt == after.sampledAt.Round(0) || after.bytes < before.bytes {
		return nil
	}
	if before.sampledAt == after.sampledAt {
		if before.bytes == after.bytes {
			return retained
		}
		return nil
	}
	duration := after.sampledAt.Sub(before.sampledAt)
	if duration <= 0 || duration > dashboard.NetworkDetailFreshFor {
		return nil
	}
	return &dashboard.ApplicationPayloadInterval{StartedAt: before.sampledAt.UTC(), SampledAt: after.sampledAt.UTC(),
		DurationNanoseconds: uint64(duration), Bytes: after.bytes - before.bytes}
}
