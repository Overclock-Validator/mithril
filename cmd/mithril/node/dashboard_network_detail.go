package node

import (
	"errors"
	"strings"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
)

func validateDashboardNetworkDetails(enabled bool, socket string) error {
	if enabled && strings.TrimSpace(socket) == "" {
		return errors.New("dashboard.network_details requires dashboard.socket")
	}
	return nil
}

func sameReplayDetailGenesis(a, b *string) bool {
	return (a == nil) == (b == nil) && (a == nil || *a == *b)
}

// sampleNetworkDetail copies the current source cache without calling a socket
// owner. Two source/context observations fence replacement during the copy.
func (d *nodeDashboard) sampleNetworkDetail(_ time.Time) (dashboard.NetworkDetailCapture, *repairGateObservation, error) {
	if d == nil {
		return dashboard.NetworkDetailCapture{}, nil, errors.New("source_unavailable")
	}
	d.mu.Lock()
	binding := dashboard.NetworkDetailBinding{RunID: d.state.RunID, NodeStartedAt: d.state.StartedAt}
	if d.state.GenesisHash != nil {
		value := *d.state.GenesisHash
		binding.GenesisHash = &value
	}
	d.mu.Unlock()
	before, ok := d.progress.NetworkSnapshot()
	if !ok {
		return dashboard.NetworkDetailCapture{}, nil, errors.New("waiting_source")
	}
	// A new owner observation may arrive during NetworkSnapshot. Its original
	// deadline is checked against the post-copy clock, not Feed's earlier tick.
	capture, err := networkDetailMeasurement(binding, before, time.Now())
	if err != nil {
		return dashboard.NetworkDetailCapture{}, nil, err
	}
	after, ok := d.progress.NetworkSnapshot()
	if !ok || !networkDetailSourceStable(before, after) {
		return dashboard.NetworkDetailCapture{}, nil, errors.New("binding_changed")
	}
	d.mu.Lock()
	unchanged := d.state.RunID == binding.RunID && d.state.StartedAt.Equal(binding.NodeStartedAt) && sameReplayDetailGenesis(d.state.GenesisHash, binding.GenesisHash)
	d.mu.Unlock()
	if !unchanged {
		return dashboard.NetworkDetailCapture{}, nil, errors.New("binding_changed")
	}
	if gate, ok := repairGateMeasurement(before, capture.Detail.Binding, time.Now()); ok {
		return capture, &gate, nil
	}
	return capture, nil, nil
}

func networkDetailSourceStable(a, b blockstream.NetworkSnapshot) bool {
	return a.ConfiguredSource == b.ConfiguredSource && a.SourceStartedAt.Equal(b.SourceStartedAt) && a.LiveConnected && b.LiveConnected &&
		a.Turbine != nil && b.Turbine != nil && a.Turbine.Session == b.Turbine.Session && a.Turbine.StartedAt.Equal(b.Turbine.StartedAt) &&
		a.Turbine.SocketMemoryObserved && b.Turbine.SocketMemoryObserved && a.Turbine.SocketMemory.Generation == b.Turbine.SocketMemory.Generation
}

func networkDetailMeasurement(binding dashboard.NetworkDetailBinding, s blockstream.NetworkSnapshot, now time.Time) (dashboard.NetworkDetailCapture, error) {
	if s.ConfiguredSource != "turbine" {
		return dashboard.NetworkDetailCapture{}, errors.New("unsupported_source")
	}
	t := s.Turbine
	if !s.LiveConnected || t == nil || !t.SocketMemoryObserved {
		return dashboard.NetworkDetailCapture{}, errors.New("source_unavailable")
	}
	if s.SourceStartedAt.IsZero() || t.StartedAt.IsZero() || t.SampledAt.IsZero() || t.Session == 0 || t.SocketMemory.Generation == 0 {
		return dashboard.NetworkDetailCapture{}, errors.New("source_invalid")
	}
	binding.SourceStartedAt, binding.ReceiverStartedAt = s.SourceStartedAt.UTC(), t.StartedAt.UTC()
	binding.ReceiverSession, binding.SocketGeneration = t.Session, t.SocketMemory.Generation
	d := dashboard.NetworkDetail{SchemaVersion: 1, Family: "network", Scope: "turbine_receiver_session", State: "fresh",
		Binding: binding, UpdatedAt: t.SampledAt.UTC()}
	if f := t.Frontiers; f.HasReconstructed && !f.ReconstructedAt.IsZero() {
		d.Reconstruction = &dashboard.ReconstructionDetail{Scope: "assembler_session_high_water", Slot: f.ReconstructedSlot, ObservedAt: f.ReconstructedAt.UTC()}
	}
	live, deadline, err := socketDetailMeasurement(t.SocketMemory.Live, true, t.SocketMemoryObservedAt, now)
	if err != nil {
		return dashboard.NetworkDetailCapture{}, err
	}
	repair, repairDeadline, err := socketDetailMeasurement(t.SocketMemory.Repair, t.SocketMemory.RepairEnabled, t.SocketMemoryObservedAt, now)
	if err != nil {
		return dashboard.NetworkDetailCapture{}, err
	}
	d.Live, d.Repair = live, repair
	if t.SocketMemory.RepairEnabled && repairDeadline.Before(deadline) {
		deadline = repairDeadline
	}
	if a := t.SocketMemory.ReadActivity; a.Enabled {
		if a.StartedAt.IsZero() || a.SampledAt.IsZero() || now.Before(a.SampledAt) ||
			!a.SampledAt.Add(dashboard.NetworkDetailFreshFor).After(now) ||
			!a.LiveKnown && a.LiveBytes != 0 || !a.RepairKnown && a.RepairBytes != 0 ||
			!a.LiveKnown && a.LiveWindow != (turbine.SocketReadWindow{}) || !a.RepairKnown && a.RepairWindow != (turbine.SocketReadWindow{}) ||
			!t.SocketMemory.RepairEnabled && a.RepairKnown {
			return dashboard.NetworkDetailCapture{}, errors.New("source_invalid")
		}
		if a.LiveKnown {
			d.Live.ReadBytes = &dashboard.SocketReadBytes{StartedAt: a.StartedAt.UTC(), SampledAt: a.SampledAt.UTC(), Bytes: a.LiveBytes}
			if err := attachSocketReadWindow(d.Live.ReadBytes, a.LiveWindow, a.SampledAt); err != nil {
				return dashboard.NetworkDetailCapture{}, err
			}
		}
		if a.RepairKnown {
			d.Repair.ReadBytes = &dashboard.SocketReadBytes{StartedAt: a.StartedAt.UTC(), SampledAt: a.SampledAt.UTC(), Bytes: a.RepairBytes}
			if err := attachSocketReadWindow(d.Repair.ReadBytes, a.RepairWindow, a.SampledAt); err != nil {
				return dashboard.NetworkDetailCapture{}, err
			}
		}
		if end := a.SampledAt.Add(dashboard.NetworkDetailFreshFor); end.Before(deadline) {
			deadline = end
		}
	} else if a != (turbine.SocketReadActivity{}) {
		return dashboard.NetworkDetailCapture{}, errors.New("source_invalid")
	}
	if a := t.SocketMemory.RepairRequests; a.Enabled {
		if !t.SocketMemory.RepairEnabled || a.StartedAt.IsZero() || a.SampledAt.IsZero() ||
			a.StartedAt.Before(t.StartedAt) || a.SampledAt.Before(a.StartedAt) || now.Before(a.SampledAt) ||
			!a.SampledAt.Add(dashboard.NetworkDetailFreshFor).After(now) || !a.Known && a.Bytes != 0 || a.Known && a.Bytes == ^uint64(0) {
			return dashboard.NetworkDetailCapture{}, errors.New("source_invalid")
		}
		d.RepairRequests = &dashboard.ApplicationPayloadCounter{StartedAt: a.StartedAt.UTC(), SampledAt: a.SampledAt.UTC()}
		if a.Known {
			value := a.Bytes
			d.RepairRequests.Bytes = &value
		}
		if w := a.Window; w.Known {
			if !a.Known || w.StartedAt.IsZero() || !w.SampledAt.Equal(a.SampledAt) || w.Duration <= 0 ||
				w.Duration > dashboard.NetworkDetailFreshFor || w.Bytes > a.Bytes {
				return dashboard.NetworkDetailCapture{}, errors.New("source_invalid")
			}
			d.RepairRequests.Window = &dashboard.ApplicationPayloadInterval{StartedAt: w.StartedAt.UTC(), SampledAt: w.SampledAt.UTC(),
				DurationNanoseconds: uint64(w.Duration), Bytes: w.Bytes}
		} else if w != (turbine.SocketReadWindow{}) {
			return dashboard.NetworkDetailCapture{}, errors.New("source_invalid")
		}
		if end := a.SampledAt.Add(dashboard.NetworkDetailFreshFor); end.Before(deadline) {
			deadline = end
		}
	} else if a != (turbine.RepairRequestActivity{}) {
		return dashboard.NetworkDetailCapture{}, errors.New("source_invalid")
	}
	if r := s.Reception; r != nil && r.Session == t.Session && r.StartedAt.Equal(t.StartedAt) {
		a := r.Progress.ArrivalTiming
		if a.AdjacentPairs > 0 && now != now.Round(0) && a.SampledAt != a.SampledAt.Round(0) &&
			!now.Before(a.SampledAt) && now.Sub(a.SampledAt) < 3*time.Second {
			d.ArrivalTiming = &dashboard.ArrivalTimingDetail{Scope: "retained_adjacent_tvu_arrivals", SampledAt: a.SampledAt.UTC(),
				LatestReceiptAt: a.LatestReceiptAt.UTC(), RetainedSlots: a.RetainedSlots, AdjacentPairs: a.AdjacentPairs,
				ReorderedPairs: a.ReorderedPairs, TotalIntervalNanoseconds: uint64(a.TotalInterval)}
		}
	}
	return dashboard.NetworkDetailCapture{Detail: d, FreshUntil: deadline}, nil
}

func attachSocketReadWindow(d *dashboard.SocketReadBytes, w turbine.SocketReadWindow, sampledAt time.Time) error {
	if !w.Known {
		if w != (turbine.SocketReadWindow{}) {
			return errors.New("source_invalid")
		}
		return nil
	}
	if w.StartedAt.IsZero() || !w.SampledAt.Equal(sampledAt) || w.Duration <= 0 || w.Duration > dashboard.NetworkDetailFreshFor || w.Bytes > d.Bytes {
		return errors.New("source_invalid")
	}
	d.Window = &dashboard.SocketReadInterval{StartedAt: w.StartedAt.UTC(), SampledAt: w.SampledAt.UTC(), DurationNanoseconds: uint64(w.Duration), Bytes: w.Bytes}
	return nil
}

func socketDetailMeasurement(s turbine.SocketMemorySample, enabled bool, ownerAt, now time.Time) (dashboard.SocketDetail, time.Time, error) {
	d := dashboard.SocketDetail{Enabled: enabled, State: "not_provided"}
	if !enabled {
		if s != (turbine.SocketMemorySample{}) {
			return d, time.Time{}, errors.New("source_invalid")
		}
		return d, time.Time{}, nil
	}
	o := s.Observation
	switch o.Reason {
	case metrics.SocketAvailable:
		d.State = "available"
	case metrics.SocketUnsupported:
		d.State = "unsupported"
	case metrics.SocketClosed:
		d.State = "closed"
	case metrics.SocketReadFailed:
		d.State = "read_failed"
	case metrics.SocketInvalidResponse:
		d.State = "invalid_response"
	case metrics.SocketCaptureExpired:
		d.State = "capture_expired"
	case metrics.SocketClockInvalid:
		d.State = "clock_invalid"
	default:
		return d, time.Time{}, errors.New("source_invalid")
	}
	if o.Known != (o.Reason == metrics.SocketAvailable) || !o.Known && (o.Memory != (metrics.SocketMemory{}) || s.DropDelta.Known) {
		return d, time.Time{}, errors.New("source_invalid")
	}
	observedAt := o.SampledAt
	if o.Reason == metrics.SocketUnsupported && o.StartedAt.IsZero() && observedAt.IsZero() {
		observedAt = ownerAt
	} else {
		if o.StartedAt.IsZero() || observedAt.IsZero() || observedAt.Before(o.StartedAt) {
			return d, time.Time{}, errors.New("source_time_invalid")
		}
		started, sampled := o.StartedAt.UTC(), observedAt.UTC()
		d.StartedAt, d.SampledAt = &started, &sampled
	}
	if observedAt.IsZero() || now.Before(observedAt) || !observedAt.Add(dashboard.NetworkDetailFreshFor).After(now) {
		return d, time.Time{}, errors.New("source_time_invalid")
	}
	if o.Known {
		m := o.Memory
		d.Memory = &dashboard.SocketMemory{ReceiveAllocatedBytes: m.ReceiveAllocated, ReceiveLimitBytes: m.ReceiveLimit,
			SendAllocatedBytes: m.SendAllocated, SendLimitBytes: m.SendLimit, ForwardAllocatedBytes: m.ForwardAllocated,
			SendQueuedBytes: m.SendQueued, OptionsAllocatedBytes: m.OptionsAllocated, BacklogBytes: m.Backlog, Drops: m.Drops}
	}
	if w := s.DropDelta; w.Known {
		duration := w.SampledAt.Sub(w.StartedAt)
		if w.StartedAt.IsZero() || !w.SampledAt.Equal(o.SampledAt) || duration <= 0 || duration > dashboard.NetworkDetailFreshFor || w.Drops > uint64(o.Memory.Drops) {
			return d, time.Time{}, errors.New("source_invalid")
		}
		d.DropWindow = &dashboard.SocketDropWindow{StartedAt: w.StartedAt.UTC(), SampledAt: w.SampledAt.UTC(),
			DurationMilliseconds: uint64(duration / time.Millisecond), Drops: w.Drops}
	}
	return d, observedAt.Add(dashboard.NetworkDetailFreshFor), nil
}
