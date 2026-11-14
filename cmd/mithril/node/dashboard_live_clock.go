package node

import (
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
)

// captureLiveClock uses the already attached engine lifetime without reading a
// socket or requiring a receiver. Only the existing Feed cycle calls it.
func (d *nodeDashboard) captureLiveClock(binding dashboard.NodeNetworkBinding) (*dashboard.ConsensusClockDetail, time.Time) {
	if d == nil {
		return nil, time.Time{}
	}
	d.mu.Lock()
	owner := d.votorSocket
	matched := d.networkDetails && !d.networkRetired && d.state.Phase != "stopping" && d.state.Alpenglow &&
		owner != nil && owner.engine == d.consensus && owner.binding.RunID == binding.RunID &&
		owner.binding.NodeStartedAt.Equal(binding.NodeStartedAt) && sameReplayDetailGenesis(owner.binding.GenesisHash, binding.GenesisHash) &&
		d.state.RunID == binding.RunID && d.state.StartedAt.Equal(binding.NodeStartedAt) && sameReplayDetailGenesis(d.state.GenesisHash, binding.GenesisHash)
	d.mu.Unlock()
	if !matched {
		return nil, time.Time{}
	}
	started := time.Now()
	o, known := owner.engine.ObserveLiveClock()
	completed := time.Now()
	if !known || completed.Before(started) || completed.Sub(started) > time.Second || o.SampledAt.Before(started) || o.SampledAt.After(completed) {
		return nil, time.Time{}
	}
	d.mu.Lock()
	matched = d.votorSocket == owner && d.consensus == owner.engine && d.networkDetails && !d.networkRetired && d.state.Phase != "stopping" &&
		d.state.Alpenglow && d.state.RunID == binding.RunID && d.state.StartedAt.Equal(binding.NodeStartedAt) && sameReplayDetailGenesis(d.state.GenesisHash, binding.GenesisHash)
	d.mu.Unlock()
	if !matched {
		return nil, time.Time{}
	}
	return &dashboard.ConsensusClockDetail{Scope: "consensus_bounded_live_clock", Generation: owner.generation, SampledAt: o.SampledAt.UTC(), AnchorAt: o.AnchorAt.UTC(),
		AnchorSlot: o.AnchorSlot, AnchorAgeNanoseconds: uint64(o.AnchorAge), EstimatedSlot: o.EstimatedSlot, AddedSlots: o.AddedSlots}, started.Add(dashboard.NetworkDetailFreshFor)
}
