package accountsdb

import (
	"context"
	"time"
)

// PhysicalUsage is a dated observation of filesystem blocks attributed to the
// primary database tree and each secondary accounts tree. It is not live record
// bytes, exclusive device consumption, fragmentation, capacity or reusable free.
// Totals are present only after two complete matching metadata traversals.
type PhysicalUsage struct {
	StartedAt, CompletedAt           time.Time
	AllocatedBytes, LogicalFileBytes uint64
	DurationNanoseconds              uint64
	RegularFiles, Directories        uint64
}

type PhysicalUsageLimits struct {
	MaxEntries  uint64 // across both passes, including root directories
	MaxDuration time.Duration
}

var defaultPhysicalUsageLimits = PhysicalUsageLimits{MaxEntries: 2_000_000, MaxDuration: 5 * time.Second}

// The wall budget is checked between filesystem calls. A stalled kernel I/O
// cannot be preempted; callers must use one background worker, no DB locks, and
// never wait for that worker while closing a database.
func ObservePhysicalUsage(ctx context.Context, roots []string, limits PhysicalUsageLimits) (PhysicalUsage, string) {
	return observePhysicalUsage(ctx, roots, limits, nil)
}
