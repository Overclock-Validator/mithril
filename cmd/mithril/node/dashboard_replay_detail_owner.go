package node

import (
	"errors"
	"math"
	"strings"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/replay"
)

func validateDashboardReplayDetails(enabled bool, socket string) error {
	if enabled && strings.TrimSpace(socket) == "" {
		return errors.New("dashboard.replay_details requires dashboard.socket")
	}
	return nil
}

// sampleReplayDetail copies the bounded replay owner between two progress
// observations. It performs no OS, account, storage or live-metrics read.
func (d *nodeDashboard) sampleReplayDetail(capturedAt time.Time) (dashboard.ReplayDetailCore, error) {
	if d == nil {
		return dashboard.ReplayDetailCore{}, errors.New("source_unavailable")
	}
	d.mu.Lock()
	run, started := d.state.RunID, d.state.StartedAt
	var genesis *string
	if d.state.GenesisHash != nil {
		value := *d.state.GenesisHash
		genesis = &value
	}
	d.mu.Unlock()
	before, ok := d.progress.Snapshot()
	if !ok {
		return dashboard.ReplayDetailCore{}, errors.New("waiting_source")
	}
	source, ok := d.progress.ReplayDiagnosticsSnapshot()
	if !ok {
		return dashboard.ReplayDetailCore{}, errors.New("waiting_source")
	}
	schedule := heldScheduleMeasurement(before)
	selected, selectedKnown := d.progress.SelectedSlotsSnapshot()
	after, ok := d.progress.Snapshot()
	if !ok || source.RunID != run || !replayDetailProgressStable(before, after, source) {
		return dashboard.ReplayDetailCore{}, errors.New("binding_changed")
	}
	if schedule != nil && before.NextKnownAssignment.Identity != after.NextKnownAssignment.Identity {
		schedule.OwnAssignments = nil
	}
	d.mu.Lock()
	unchanged := d.state.RunID == run && d.state.StartedAt.Equal(started) && sameReplayDetailGenesis(d.state.GenesisHash, genesis)
	d.mu.Unlock()
	if !unchanged {
		return dashboard.ReplayDetailCore{}, errors.New("binding_changed")
	}
	core, err := replayDetailCore(source, started, capturedAt, genesis)
	if err != nil {
		return dashboard.ReplayDetailCore{}, err
	}
	core.ScheduleWindow = schedule
	if selectedKnown {
		core.LatestSelected = latestSelectedMeasurement(selected, source)
		core.RecentSelected = recentSelectedMeasurement(selected, source)
	}
	return core, nil
}

// sampleSelectedWindow uses the same run and replay-attempt fences as replay
// details. It reads only detached in-memory observations on the detail worker.
func (d *nodeDashboard) sampleSelectedWindow(capturedAt time.Time) (dashboard.SelectedWindow, error) {
	if d == nil {
		return dashboard.SelectedWindow{}, errors.New("source_unavailable")
	}
	d.mu.Lock()
	run, started := d.state.RunID, d.state.StartedAt
	var genesis *string
	if d.state.GenesisHash != nil {
		value := *d.state.GenesisHash
		genesis = &value
	}
	d.mu.Unlock()
	before, ok := d.progress.Snapshot()
	if !ok {
		return dashboard.SelectedWindow{}, errors.New("waiting_source")
	}
	selected, ok := d.progress.SelectedSlotsSnapshot()
	if !ok {
		return dashboard.SelectedWindow{}, errors.New("waiting_source")
	}
	// SelectedSlotsSnapshot already fences its generation and attempt against
	// the diagnostics owner. Its context avoids copying the unrelated replay
	// row history a second time on every selected-window capture.
	source := replay.ReplayDiagnosticSnapshot{RunID: selected.RunID, Generation: selected.Generation,
		Attempt: selected.Attempt, Rewinds: selected.Rewinds, StartedAt: selected.StartedAt, ResetAt: selected.ResetAt}
	after, ok := d.progress.Snapshot()
	if !ok || source.RunID != run || !replayDetailProgressStable(before, after, source) {
		return dashboard.SelectedWindow{}, errors.New("binding_changed")
	}
	d.mu.Lock()
	unchanged := d.state.RunID == run && d.state.StartedAt.Equal(started) && sameReplayDetailGenesis(d.state.GenesisHash, genesis)
	d.mu.Unlock()
	if !unchanged {
		return dashboard.SelectedWindow{}, errors.New("binding_changed")
	}
	binding := dashboard.ReplayDetailBinding{RunID: run, NodeStartedAt: started.UTC(), GenesisHash: genesis,
		SourceGeneration: source.Generation, Attempt: source.Attempt, Rewinds: source.Rewinds,
		AttemptStartedAt: source.StartedAt.UTC(), ResetAt: source.ResetAt.UTC()}
	w := selectedWindowMeasurement(selected, source, binding, capturedAt)
	if w == nil {
		return dashboard.SelectedWindow{}, errors.New("source_invalid")
	}
	return *w, nil
}

// recentSelectedMeasurement reuses the already-copied source and the same
// exact-binding record mapper. Source order is preserved, not inferred from
// numeric slot, hash or UTC ordering. This is a display subset, not an archive.
func recentSelectedMeasurement(s replay.SelectedSlotSnapshot, source replay.ReplayDiagnosticSnapshot) *dashboard.ReplayRecentSelected {
	count := len(s.Records)
	if count == 0 || s.WindowEntries > 512 || uint64(count) != min(s.WindowEntries, replay.SelectedSlotCapacity) ||
		s.OmittedFromWindow != s.WindowEntries-uint64(count) {
		return nil
	}
	v := &dashboard.ReplayRecentSelected{Scope: "recent_selected_and_folded", ObservedAt: s.UpdatedAt.UTC(),
		WindowEntries: s.WindowEntries, SourceRecords: uint64(count), CoverageIncomplete: s.CoverageIncomplete,
	}
	v.Records = selectedWindowRows(s, source, max(0, count-dashboard.ReplayRecentSelectedCapacity))
	if v.Records == nil {
		return nil
	}
	for _, row := range v.Records {
		v.CoverageIncomplete = v.CoverageIncomplete || row.Gap
	}
	return v
}

// selectedWindowRows maps only values already captured by the replay owner.
// Each row keeps its own IDs and original replay-result time.
func selectedWindowRows(s replay.SelectedSlotSnapshot, source replay.ReplayDiagnosticSnapshot, start int) []dashboard.ReplaySelectedRecord {
	rows := make([]dashboard.ReplaySelectedRecord, 0, len(s.Records)-start)
	for i := start; i < len(s.Records); i++ {
		one := s
		one.Records = s.Records[i : i+1]
		r := latestSelectedMeasurement(one, source)
		if r == nil {
			return nil
		}
		rows = append(rows, dashboard.ReplaySelectedRecord{Slot: r.Slot, Bankhash: r.Bankhash,
			BlockID: r.BlockID, Gap: r.Gap, Facts: r.Facts})
	}
	return rows
}

// selectedWindowMeasurement prepares the complete retained source projection;
// it does not infer omitted or skipped slots outside that projection.
func selectedWindowMeasurement(s replay.SelectedSlotSnapshot, source replay.ReplayDiagnosticSnapshot, binding dashboard.ReplayDetailBinding, capturedAt time.Time) *dashboard.SelectedWindow {
	count := len(s.Records)
	if s.RunID != source.RunID || s.Generation != source.Generation || s.Attempt != source.Attempt ||
		s.Rewinds != source.Rewinds || !s.StartedAt.Equal(source.StartedAt) || !s.ResetAt.Equal(source.ResetAt) ||
		binding.RunID != source.RunID || binding.SourceGeneration != source.Generation || binding.Attempt != source.Attempt ||
		binding.Rewinds != source.Rewinds || !binding.AttemptStartedAt.Equal(source.StartedAt) || !binding.ResetAt.Equal(source.ResetAt) ||
		s.WindowEntries > 512 || uint64(count) != min(s.WindowEntries, replay.SelectedSlotCapacity) ||
		s.OmittedFromWindow != s.WindowEntries-uint64(count) {
		return nil
	}
	rows := selectedWindowRows(s, source, 0)
	if rows == nil {
		return nil
	}
	w := &dashboard.SelectedWindow{SchemaVersion: 1, Family: "selected_slots", Scope: "recent_selected_and_folded", Binding: binding,
		CapturedAt: capturedAt.UTC(), ObservedAt: s.UpdatedAt.UTC(), WindowEntries: s.WindowEntries,
		CoverageIncomplete: s.CoverageIncomplete, Records: rows}
	if binding.GenesisHash != nil {
		value := *binding.GenesisHash
		w.Binding.GenesisHash = &value
	}
	for _, row := range rows {
		w.CoverageIncomplete = w.CoverageIncomplete || row.Gap
	}
	return w
}

// latestSelectedMeasurement selects only the newest applied fold. It never
// searches past a gap or joins it to an unrelated completed replay row.
func latestSelectedMeasurement(s replay.SelectedSlotSnapshot, source replay.ReplayDiagnosticSnapshot) *dashboard.ReplayLatestSelected {
	if s.RunID != source.RunID || s.Generation != source.Generation || s.Attempt != source.Attempt ||
		s.Rewinds != source.Rewinds || !s.StartedAt.Equal(source.StartedAt) || !s.ResetAt.Equal(source.ResetAt) ||
		len(s.Records) == 0 || len(s.Records) > replay.SelectedSlotCapacity {
		return nil
	}
	r := s.Records[len(s.Records)-1]
	value := &dashboard.ReplayLatestSelected{Scope: "latest_selected_and_folded", ObservedAt: s.UpdatedAt.UTC(), Slot: r.Slot, Gap: r.Gap}
	if !r.Bankhash.IsZero() {
		hash := r.Bankhash.String()
		value.Bankhash = &hash
	}
	if r.HasBlockID && !r.BlockID.IsZero() {
		id := r.BlockID.String()
		value.BlockID = &id
	}
	if !r.Gap {
		computeUnits := r.Transactions.ComputeUnits
		value.Facts = &dashboard.ReplaySelectedFacts{ReplayResultAt: r.Facts.ReplayResultAt.UTC(), Epoch: r.Facts.Epoch,
			FromLocalProduction: r.Facts.FromLocalProduction, Transactions: dashboard.ReplaySelectedCounts{
				Total: r.Transactions.Total, NonVote: r.Transactions.NonVote, Failed: r.Transactions.Failed,
				NonVoteFailed: r.Transactions.NonVoteFailed, Saturated: r.Transactions.Saturated, ComputeUnits: &computeUnits}}
		if !r.Facts.ExecutionLeader.IsZero() {
			leader := r.Facts.ExecutionLeader.String()
			value.Facts.ExecutionLeader = &leader
		}
		e := r.Facts.Economics
		if f := e.Fees; e.FeesKnown && f.ExecutionLamports <= math.MaxUint64-f.PriorityLamports &&
			f.ExecutionLamports+f.PriorityLamports == f.TotalLamports {
			value.Facts.Fees = &dashboard.ReplaySelectedFees{ExecutionLamports: f.ExecutionLamports,
				PriorityLamports: f.PriorityLamports, TotalLamports: f.TotalLamports}
			if e.FeeBurnKnown && e.FeeBurnLamports <= f.TotalLamports {
				burned := e.FeeBurnLamports
				value.Facts.Fees.BurnedLamports = &burned
			}
		}
		if e.NativeCostKnown && r.Facts.FromLocalProduction {
			value.Facts.LocalCost = &dashboard.ReplaySelectedCost{BlockCost: e.NativeCost.BlockCost, BlockCostLimit: e.NativeCost.BlockCostLimit}
		}
	}
	return value
}

func replayDetailProgressStable(before, after replay.ProgressSnapshot, source replay.ReplayDiagnosticSnapshot) bool {
	return source.Generation != 0 && source.Attempt != 0 &&
		before.Attempt == after.Attempt && before.Rewinds == after.Rewinds && before.StartedAt.Equal(after.StartedAt) &&
		source.Attempt == before.Attempt && source.Rewinds == before.Rewinds && source.StartedAt.Equal(before.StartedAt)
}
