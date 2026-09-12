package node

import (
	"context"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sigverify"
)

// sigverifyReportInterval is deliberately coarse. Batch width is a property of
// the workload's shape, not of any one block, so a short interval would report
// noise; the question this answers is "are we filling groups over a stretch of
// replay", and that changes on the scale of minutes.
const sigverifyReportInterval = 5 * time.Minute

// startSigverifyReporter periodically records verification behaviour to the
// per-run log directory.
//
// It writes only to <run-dir>/sigverify.log via NamedFilef, never to the
// terminal. Operator-facing stderr already carries replay progress, and batch
// width is diagnostic rather than something to watch live; adding it to the
// console would cost attention from the output that matters and change the
// startup display that people read at a glance.
//
// Three things go in, and each answers a question the others cannot:
//
//   - the resolved backend, because with backend=auto the name is the only way
//     to learn whether this machine got the accelerated path;
//   - InternalFaultFallbacks, which should be zero forever -- a nonzero value
//     is a bug in the accelerated backend rather than an input-dependent
//     condition, so it is worth alerting on rather than merely recording;
//   - the batch-width distribution, because a signature total cannot show
//     whether the drain policy is filling groups, and filling them is worth a
//     ~3.7x factor per signature.
func startSigverifyReporter(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(sigverifyReportInterval)
		defer ticker.Stop()

		// Baseline at startup so the first interval has something to difference
		// against, and so the resolved backend is recorded even on a node that
		// exits before the first tick.
		previous := sigverify.Stats()
		mlog.NamedFilef("sigverify", "startup: %s", previous)

		for {
			select {
			case <-ctx.Done():
				// A final snapshot: a short run would otherwise leave no record
				// of what it did, and a run that ends because of a verification
				// problem is exactly when this is worth having.
				mlog.NamedFilef("sigverify", "shutdown: %s", sigverify.Stats())
				return

			case <-ticker.C:
				current := sigverify.Stats()

				// Counters are monotonic for the process lifetime, so an
				// interval line needs the delta. Reporting only cumulative
				// values would let a long-healthy run hide a recent collapse in
				// batch width.
				interval := sigverify.Snapshot{
					Backend:                current.Backend,
					InternalFaultFallbacks: current.InternalFaultFallbacks,
					Batches:                current.Batches - previous.Batches,
					Signatures:             current.Signatures - previous.Signatures,
					EmptyBatches:           current.EmptyBatches - previous.EmptyBatches,
					Widths:                 diffWidths(previous.Widths, current.Widths),
				}
				mlog.NamedFilef("sigverify", "interval: %s", interval)

				if current.InternalFaultFallbacks > previous.InternalFaultFallbacks {
					// This one does reach the operator log, because it means the
					// accelerated backend produced a result it could not trust
					// and recomputed on the portable path. It should never
					// happen; if it does, silence is the wrong default.
					mlog.Log.Warnf("sigverify: accelerated backend fell back on an internal fault %d time(s) total; this is a backend bug, not an input condition",
						current.InternalFaultFallbacks)
				}

				previous = current
			}
		}
	}()
}

// diffWidths subtracts an earlier histogram from a later one. Buckets are
// matched by upper bound rather than by position, because Stats omits empty
// buckets and the two snapshots need not have the same shape.
func diffWidths(before, after []sigverify.WidthBucket) []sigverify.WidthBucket {
	earlier := make(map[int]uint64, len(before))
	for _, b := range before {
		earlier[b.Upper] = b.Batches
	}

	delta := make([]sigverify.WidthBucket, 0, len(after))
	for _, b := range after {
		if n := b.Batches - earlier[b.Upper]; n > 0 {
			delta = append(delta, sigverify.WidthBucket{Upper: b.Upper, Batches: n})
		}
	}
	return delta
}
