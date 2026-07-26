package sigverify

import "sync/atomic"

// The default group widths. They are the widths that suit the current
// verification kernel: the vectorized backend verifies eight signatures per
// AVX-512 group, so eight is where every lane is busy and per-signature cost
// stops falling steeply, and 64 is eight full groups.
//
// They are defaults rather than constants because the width that pays is a
// property of the kernel, not of this pipeline, and the kernel is expected to
// change. A multi-scalar formulation amortizes one doubling chain across the
// whole group, which does not pay at eight and pays substantially at several
// hundred. Nothing else about the drain policy depends on the numbers, so
// following the kernel is a configuration change and not a rewrite.
const (
	DefaultBatchTarget = 8
	DefaultMaxDrain    = 64

	// MaxConfigurableDrain is a typo guard, not a tuning limit. FairShare
	// already prevents a worker from taking a large group unless the queue is
	// genuinely that deep, so the ceiling exists only to reject an operator
	// value that is obviously a mistake.
	MaxConfigurableDrain = 4096
)

// Group widths are read on every drain by worker goroutines while Configure
// writes them during startup, so they are atomics rather than plain ints for
// the same reason bypass is.
var (
	batchTarget atomic.Int64
	maxDrain    atomic.Int64
)

func init() {
	batchTarget.Store(DefaultBatchTarget)
	maxDrain.Store(DefaultMaxDrain)
}

// BatchTarget is the group width worth aiming for.
//
// It is a TARGET, never a threshold. Nothing in this package waits to reach it.
// A producer that meters work out to its workers should hand over roughly this
// many per worker so a group is reachable at all; a consumer should take
// whatever is actually queued.
func BatchTarget() int { return int(batchTarget.Load()) }

// MaxDrain is how many work items a verification worker will coalesce into one
// batch. It bounds how long a worker can be busy on one group, and how much
// scratch that worker holds.
func MaxDrain() int { return int(maxDrain.Load()) }

// FairShare reports how many items one worker may take in a single pass,
// given how many are queued behind it and how many workers share the queue.
//
// Draining unconditionally is a trap. Eight workers facing eight queued items
// would let the first worker take all eight and verify them as one group while
// the other seven idle — one group of eight costs more wall-clock than eight
// workers each verifying one, so the "optimization" would be a latency
// regression exactly when the queue is shallow. Batching buys throughput per
// core; it must never buy it with parallelism that was already available.
//
// So a worker takes its share and no more: one item plus an equal cut of what
// remains. A shallow queue spreads across workers, and a deep queue — the
// catch-up case, where every worker is saturated regardless — gives everyone
// full groups.
//
// max caps the result. The floor is 1: the item already in hand is never
// given back, which is what keeps this incapable of stranding work.
func FairShare(queued, workers, max int) int {
	if workers < 1 {
		workers = 1
	}
	share := queued/workers + 1
	if share > max {
		share = max
	}
	if share < 1 {
		share = 1
	}
	return share
}

// Drain coalesces work from ch into dst, starting with an item the caller has
// already received.
//
// The shape matters and is the whole point of this helper. The caller blocks
// receiving the first item — a worker with nothing to do should sleep, not
// spin — and everything after that is a non-blocking peek. So Drain NEVER waits
// for a batch to fill. There is no timer and no minimum size.
//
// That is what makes batching safe to bolt onto latency-sensitive paths: when
// work is scarce, a batch is whatever happened to be queued (often one item)
// and latency is unchanged; when work is abundant, batches fill naturally and
// the accelerated path pays off. A batching timer would trade the first case
// away to improve the second, and the first case is the tip of the chain.
//
// dst is reused across calls; pass the same worker-local slice every time and
// Drain will not allocate after it has grown once.
func Drain[T any](dst []T, first T, ch <-chan T, max int) []T {
	if max < 1 {
		max = 1
	}
	dst = append(dst[:0], first)
	for len(dst) < max {
		select {
		case item, open := <-ch:
			if !open {
				return dst
			}
			dst = append(dst, item)
		default:
			return dst
		}
	}
	return dst
}
