package node

import (
	"errors"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
)

// sampleRootedHistory copies the successful fold owner's immutable history,
// fenced by the same replay lifetime as status. It performs no account scan.
func (d *nodeDashboard) sampleRootedHistory(now time.Time) (dashboard.RootedHistoryCapture, error) {
	if d == nil {
		return dashboard.RootedHistoryCapture{}, errors.New("source_unavailable")
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
		return dashboard.RootedHistoryCapture{}, errors.New("waiting_source")
	}
	source, ok := d.progress.RootedHistorySnapshot()
	if !ok {
		return dashboard.RootedHistoryCapture{}, errors.New("waiting_source")
	}
	after, ok := d.progress.Snapshot()
	if !ok || source.RunID != run || source.Attempt == 0 || source.Generation == 0 ||
		before.Attempt != after.Attempt || before.Rewinds != after.Rewinds || !before.StartedAt.Equal(after.StartedAt) ||
		source.Attempt != before.Attempt || source.Rewinds != before.Rewinds || !source.StartedAt.Equal(before.StartedAt) {
		return dashboard.RootedHistoryCapture{}, errors.New("binding_changed")
	}
	d.mu.Lock()
	unchanged := d.state.RunID == run && d.state.StartedAt.Equal(started) && sameReplayDetailGenesis(d.state.GenesisHash, genesis)
	d.mu.Unlock()
	if !unchanged {
		return dashboard.RootedHistoryCapture{}, errors.New("binding_changed")
	}
	h := source.History
	bitmap, ok := h.BitmapBase64()
	if !ok {
		return dashboard.RootedHistoryCapture{}, errors.New("source_invalid")
	}
	var epoch *dashboard.RootedHistoryEpoch
	if h.Epoch.Known {
		epoch = &dashboard.RootedHistoryEpoch{Number: h.Epoch.Number, FirstSlot: h.Epoch.FirstSlot, Slots: h.Epoch.Slots}
	}
	return dashboard.RootedHistoryCapture{FreshUntil: now.Add(dashboard.RootedHistoryFreshFor), Detail: dashboard.RootedHistoryDetail{
		SchemaVersion: 1, Family: "rooted_history", Scope: "rooted_bank_slot_history", State: "fresh",
		Binding: dashboard.ReplayDetailBinding{RunID: run, NodeStartedAt: started.UTC(), GenesisHash: genesis,
			SourceGeneration: source.Generation, Attempt: source.Attempt, Rewinds: source.Rewinds,
			AttemptStartedAt: source.StartedAt.UTC(), ResetAt: source.ResetAt.UTC()},
		CapturedAt: now.UTC(), ObservedAt: h.ObservedAt.UTC(), BankSlot: h.BankSlot, Bankhash: h.Bankhash.String(),
		OldestSlot: h.OldestSlot, NextSlot: h.NextSlot, Capacity: dashboard.RootedHistoryCapacity,
		Encoding: "base64_le_lsb0", Bitmap: bitmap, Epoch: epoch,
	}}, nil
}
