package node

import (
	"strings"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/progress"
)

type nodeSnapshotArchive struct {
	value           *dashboard.SnapshotArchive
	attempt         uint64
	runID           string
	nodeStarted     time.Time
	genesis         string
	incrementalSlot *uint64
}

type nodeSnapshotManifests struct {
	value       dashboard.SnapshotManifests
	root        *progress.DualProgress
	runID       string
	nodeStarted time.Time
	genesis     string
}

// snapshotReadyMarker records the already-saved state marker for the active
// snapshot attempt. An AccountsDB resume has no snapshot-attempt marker.
func (d *nodeDashboard) snapshotReadyMarker(slot uint64) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	m := d.snapshotManifests
	if m == nil || m.root != d.snapshotRoot || m.runID != d.state.RunID ||
		!m.nodeStarted.Equal(d.state.StartedAt) || d.state.GenesisHash == nil ||
		*d.state.GenesisHash != m.genesis || d.state.Phase != "bootstrap" || m.value.ReadyMarker != nil {
		return
	}
	selected := m.value.Full
	if m.value.Incremental != nil {
		selected = m.value.Incremental
	}
	at := time.Now().UTC()
	if selected == nil || selected.Slot != slot || at.Before(selected.ParsedAt) {
		return
	}
	next := *m
	next.value.ReadyMarker = &dashboard.SnapshotReadyMarker{Slot: slot, ObservedAt: at}
	d.snapshotManifests = &next
}

// newSnapshotProgress binds one full-archive owner to this node run. AccountsDB
// resume has no such owner; a constructed but unstarted attempt stays absent.
func (d *nodeDashboard) newSnapshotProgress(slot uint64, kind string) *progress.DualProgress {
	return d.newArchiveProgress(slot, nil, kind, nil, nodeSnapshotArchive{})
}

func (d *nodeDashboard) newExplicitSnapshotProgress(slot uint64, path string) *progress.DualProgress {
	kind := "local"
	if strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://") {
		kind = "remote"
	}
	return d.newSnapshotProgress(slot, kind)
}

// newArchiveProgress creates one archive attempt under a full-bootstrap lineage.
// A retired full owner's constructor cannot attach an incremental to a new run.
func (d *nodeDashboard) newArchiveProgress(slot uint64, incrementalSlot *uint64, kind string, root *progress.DualProgress, parent nodeSnapshotArchive) *progress.DualProgress {
	dp := progress.NewDualProgress()
	if d == nil || (kind != "local" && kind != "remote") {
		if incrementalSlot != nil {
			return nil
		}
		return dp
	}
	d.mu.Lock()
	if incrementalSlot != nil && (*incrementalSlot <= slot || root == nil || d.snapshotRoot != root ||
		d.state.RunID != parent.runID || !d.state.StartedAt.Equal(parent.nodeStarted) ||
		d.state.GenesisHash == nil || *d.state.GenesisHash != parent.genesis) {
		d.mu.Unlock()
		return nil
	}
	if incrementalSlot == nil {
		d.snapshotRoot = nil
		d.snapshotManifests = nil
	}
	d.snapshotArchive = nil
	if d.snapshotAttempt == ^uint64(0) || d.state.RunID == "" || d.state.StartedAt.IsZero() || d.state.GenesisHash == nil {
		d.snapshotRoot = nil
		d.mu.Unlock()
		if incrementalSlot != nil {
			return nil
		}
		return dp
	}
	d.snapshotAttempt++
	attempt, run, started, genesis := d.snapshotAttempt, d.state.RunID, d.state.StartedAt, *d.state.GenesisHash
	if incrementalSlot == nil {
		root = dp
		d.snapshotRoot = dp
	}
	var incremental *uint64
	if incrementalSlot != nil {
		value := *incrementalSlot
		incremental = &value
	}
	d.mu.Unlock()
	dp.SetObserver(func(o progress.DualProgressObservation) {
		if incremental != nil {
			// Reuse the common phase mapper; the wire child stays incremental-only.
			switch o.State {
			case "active":
			case "incremental_archive_complete":
				o.State = "full_archive_complete"
			case "incremental_archive_interrupted":
				o.State = "full_archive_interrupted"
			default:
				return
			}
		}
		value := snapshotArchiveMeasurement(attempt, slot, kind, started, o)
		if value == nil {
			return
		}
		d.mu.Lock()
		if d.snapshotRoot == root && d.snapshotAttempt == attempt && d.state.RunID == run && d.state.StartedAt.Equal(started) &&
			d.state.GenesisHash != nil && *d.state.GenesisHash == genesis {
			d.snapshotArchive = &nodeSnapshotArchive{value: value, attempt: attempt, runID: run, nodeStarted: started, genesis: genesis, incrementalSlot: incremental}
		}
		d.mu.Unlock()
	})
	if incremental == nil {
		binding := nodeSnapshotArchive{attempt: attempt, runID: run, nodeStarted: started, genesis: genesis}
		dp.SetManifestObserver(func(incremental bool, slot uint64, at time.Time) {
			d.mu.Lock()
			defer d.mu.Unlock()
			if d.snapshotRoot != root || d.state.RunID != run || !d.state.StartedAt.Equal(started) ||
				d.state.GenesisHash == nil || *d.state.GenesisHash != genesis || d.state.Phase == "stopping" {
				return
			}
			next := nodeSnapshotManifests{root: root, runID: run, nodeStarted: started, genesis: genesis}
			if d.snapshotManifests != nil {
				next = *d.snapshotManifests
			} else {
				next.value.Attempt = attempt
			}
			parsed := &dashboard.SnapshotManifestParse{Slot: slot, ParsedAt: at.UTC()}
			if incremental {
				next.value.Incremental = parsed
			} else {
				next.value.Full = parsed
			}
			d.snapshotManifests = &next
		})
		dp.SetIncrementalProgress(func(end uint64, inputKind string) *progress.DualProgress {
			return d.newArchiveProgress(slot, &end, inputKind, root, binding)
		})
	}
	return dp
}

func snapshotArchiveMeasurement(attempt, slot uint64, kind string, nodeStarted time.Time, o progress.DualProgressObservation) *dashboard.SnapshotArchive {
	if attempt == 0 || (kind != "local" && kind != "remote") || o.StartedAt.IsZero() || o.CapturedAt.IsZero() ||
		o.StartedAt.Before(nodeStarted) || o.CapturedAt.Before(o.StartedAt) ||
		(o.CompressedTotalKnown && o.CompressedReportedTotal == 0) {
		return nil
	}
	phase := ""
	switch o.State {
	case "active":
		phase = "active"
	case "full_archive_complete":
		phase = "complete"
	case "full_archive_interrupted":
		phase = "interrupted"
	default:
		return nil
	}
	v := &dashboard.SnapshotArchive{Attempt: attempt, StartedAt: o.StartedAt.UTC(), SampledAt: o.CapturedAt.UTC(),
		Phase: phase, FullSnapshotSlot: slot, InputKind: kind,
		CompressedInputBytes: o.CompressedInputBytes, AccountPayloadBytes: o.AccountPayloadBytes}
	if o.CompressedTotalKnown {
		total := o.CompressedReportedTotal
		v.CompressedReportedTotal = &total
	}
	return v
}
