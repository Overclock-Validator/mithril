package node

import (
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockprod/scheduler"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/tpu"
)

// pipelineMeasurement copies the existing service observations. The caller
// binds StartedAt to this TPU/scheduler pair and samples outside viewer paths.
// A negative queue observation is unknown, not zero or an unsigned conversion.
func pipelineMeasurement(now, startedAt time.Time, stats tpu.Stats, schedulerStats scheduler.Stats, queued int) *dashboard.Pipeline {
	p := stats.Pipeline
	evicted, expired := schedulerStats.EvictedCapacity, schedulerStats.DroppedExpired
	m := &dashboard.Pipeline{
		SampledAt: now.UTC(), StartedAt: startedAt.UTC(),
		Ingress: dashboard.PipelineIngress{EnqueuedPackets: p.Ingress.EnqueuedPackets, EnqueuedBytes: p.Ingress.EnqueuedBytes, DroppedFull: p.Ingress.DroppedFull},
		Dedup: dashboard.PipelineDedup{
			InPackets: p.Dedup.InPackets, InBytes: p.Dedup.InBytes, OutPackets: p.Dedup.OutPackets, OutBytes: p.Dedup.OutBytes,
			DroppedSanitize: p.Dedup.DroppedSanitize, DroppedUnsupportedVersion: p.Dedup.DroppedUnsupportedVersion, DroppedDedup: p.Dedup.DroppedDedup,
		},
		Sigverify: dashboard.PipelineSigverify{
			InPackets: p.Sigverify.InPackets, InBytes: p.Sigverify.InBytes, VerifiedPackets: p.Sigverify.VerifiedPackets,
			VerifiedBytes: p.Sigverify.VerifiedBytes, DroppedSigverify: p.Sigverify.DroppedSigverify,
		},
		Scheduler: dashboard.PipelineScheduler{
			InPackets: schedulerStats.InPackets, InBytes: schedulerStats.InBytes, BufferedTotal: schedulerStats.Buffered, Accepted: schedulerStats.Accepted,
			DroppedParse: schedulerStats.DroppedParse, DroppedVote: schedulerStats.DroppedVote, DroppedDuplicate: schedulerStats.DroppedDuplicate,
			DroppedCapacity: schedulerStats.DroppedCapacity, EvictedCapacity: &evicted, DroppedExpired: &expired,
			DroppedAlreadyProcessed: schedulerStats.DroppedAlreadyProcessed, DroppedAccountLocks: schedulerStats.DroppedAccountLocks,
			DroppedLookup: schedulerStats.DroppedLookup, DroppedNoBank: schedulerStats.DroppedNoBank,
			DroppedCost: schedulerStats.DroppedCost, DroppedExecution: schedulerStats.DroppedExecution,
		},
	}
	if queued >= 0 {
		depth := uint64(queued)
		m.Scheduler.QueueDepth = &depth
	}
	if q := stats.QUIC; q != nil {
		m.QUIC = &dashboard.PipelineQUIC{
			AcceptedConnections: q.TotalNewConnections.Load(), OpenConnections: q.OpenConnections.Load(),
			AcceptedStreams: q.TotalNewStreams.Load(), ActiveStreams: q.ActiveStreams.Load(),
			InvalidStreamSize: q.InvalidStreamSize.Load(), ReadErrors: q.TotalStreamReadErrors.Load(), ReadTimeouts: q.TotalStreamReadTimeout.Load(),
			AcceptErrors: q.ConnectionSetupError.Load(), RefusedTooManyOpen: q.RefusedTooManyOpen.Load(),
			RefusedPerIP: q.RefusedPerIP.Load(), StreamBufferDrops: q.ConnectionAddFailed.Load(),
		}
	}
	return m
}
