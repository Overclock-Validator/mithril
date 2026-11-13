package node

import (
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/progress"
)

type nodeSnapshotShardFlush struct {
	value       *dashboard.SnapshotShardFlush
	attempt     uint64
	runID       string
	nodeStarted time.Time
	genesis     string
}

func (d *nodeDashboard) newSnapshotShardFlush(root *progress.DualProgress, parent nodeSnapshotArchive) *progress.IndexingProgress {
	p := progress.NewIndexingProgress("Flush (shard logs)")
	if d == nil {
		return p
	}
	d.mu.Lock()
	a := d.snapshotArchive
	if root == nil || d.snapshotRoot != root || d.state.Phase == "stopping" || d.snapshotAttempt == ^uint64(0) ||
		d.state.RunID != parent.runID || !d.state.StartedAt.Equal(parent.nodeStarted) || d.state.GenesisHash == nil || *d.state.GenesisHash != parent.genesis ||
		a == nil || a.attempt != d.snapshotAttempt || a.runID != parent.runID || !a.nodeStarted.Equal(parent.nodeStarted) || a.genesis != parent.genesis ||
		a.value == nil || a.value.Phase != "complete" {
		d.mu.Unlock()
		return p
	}
	fullSlot := a.value.FullSnapshotSlot
	var incremental *uint64
	if a.incrementalSlot != nil {
		x := *a.incrementalSlot
		incremental = &x
	}
	d.snapshotAttempt++
	owner := &nodeSnapshotShardFlush{attempt: d.snapshotAttempt, runID: parent.runID, nodeStarted: parent.nodeStarted, genesis: parent.genesis}
	d.snapshotShardFlush = owner
	d.mu.Unlock()
	p.SetObserver(func(o progress.IndexingProgressObservation) {
		if o.StartedAt.IsZero() || o.StartedAt.Before(owner.nodeStarted) || o.CapturedAt.Before(o.StartedAt) {
			return
		}
		v := &dashboard.SnapshotShardFlush{Attempt: owner.attempt, StartedAt: o.StartedAt.UTC(), SampledAt: o.CapturedAt.UTC(), Phase: o.State,
			FullSnapshotSlot: fullSlot, CompletedShards: o.Completed, TotalShards: o.Total}
		if incremental != nil {
			x := *incremental
			v.IncrementalSlot = &x
		}
		if !o.SSTIngestReturnedAt.IsZero() {
			if o.State == "active" || o.SSTIngestReturnedAt.Before(o.CapturedAt) {
				return
			}
			at := o.SSTIngestReturnedAt.UTC()
			v.SSTIngestReturnedAt = &at
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		if d.snapshotRoot != root || d.snapshotShardFlush != owner || d.snapshotAttempt != owner.attempt || d.state.Phase == "stopping" ||
			d.state.RunID != owner.runID || !d.state.StartedAt.Equal(owner.nodeStarted) || d.state.GenesisHash == nil || *d.state.GenesisHash != owner.genesis {
			return
		}
		if before := owner.value; before != nil {
			if v.SampledAt.Before(before.SampledAt) || v.TotalShards != before.TotalShards || v.CompletedShards < before.CompletedShards ||
				v.Phase == "active" && v.CompletedShards == before.CompletedShards {
				return
			}
			if before.Phase != "active" && (v.Phase != before.Phase || !v.SampledAt.Equal(before.SampledAt) || v.CompletedShards != before.CompletedShards ||
				before.SSTIngestReturnedAt != nil || v.SSTIngestReturnedAt == nil) {
				return
			}
		}
		owner.value = v
	})
	return p
}
