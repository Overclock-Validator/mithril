package node

import (
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockprod/scheduler"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/tpu"
)

// The feed owns this detached copy. Attachment time is not counter-zero time;
// replacement can lower the counts and no continuity or rate is inferred.
type schedulerBudgetObservation struct {
	service                *tpu.TPU
	owner                  *scheduler.Scheduler
	runID, genesis         string
	nodeStarted, sampledAt time.Time
	value                  dashboard.ProductionSchedulerDetail
}

func (d *nodeDashboard) observeSchedulerBudget(service *tpu.TPU, owner *scheduler.Scheduler, node dashboard.Snapshot, stats scheduler.Stats, sampledAt time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.productionDetails || service == nil || owner == nil || d.tpu != service || d.scheduler != owner ||
		node.Mode != "validator" || !node.Alpenglow || d.state.Mode != node.Mode || d.state.Alpenglow != node.Alpenglow ||
		node.RunID != d.state.RunID || !node.StartedAt.Equal(d.state.StartedAt) ||
		node.GenesisHash == nil || d.state.GenesisHash == nil || *node.GenesisHash != *d.state.GenesisHash {
		return
	}
	if previous := d.schedulerBudget; previous != nil && previous.owner == owner && previous.service == service && previous.sampledAt.After(sampledAt) {
		return
	}
	d.schedulerBudget = &schedulerBudgetObservation{service: service, owner: owner, runID: node.RunID,
		genesis: *node.GenesisHash, nodeStarted: node.StartedAt, sampledAt: sampledAt,
		value: dashboard.ProductionSchedulerDetail{Scope: "attached_scheduler_snapshot", SampledAt: sampledAt.UTC(),
			BlockCost: stats.DroppedBlockCost, AccountCost: stats.DroppedAccountCost,
			AllocatedData: stats.DroppedAllocCost, EntryBytes: stats.DroppedBatchBytes}}
}

// Requires d.mu. Return the original observation, never a redated record.
func (d *nodeDashboard) schedulerBudgetFor(binding dashboard.ProductionDetailBinding, now time.Time) *schedulerBudgetObservation {
	o := d.schedulerBudget
	if o == nil || o.service != d.tpu || o.owner != d.scheduler || o.runID != binding.RunID ||
		!o.nodeStarted.Equal(binding.NodeStartedAt) || o.genesis != binding.GenesisHash ||
		now.Before(o.sampledAt) || !o.sampledAt.Add(dashboard.ProductionDetailFreshFor).After(now) {
		return nil
	}
	return o
}

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
