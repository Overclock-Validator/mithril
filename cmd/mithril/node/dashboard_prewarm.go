package node

import (
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/blockstream"
	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/gagliardetto/solana-go"
)

// prewarmHandoverMonitor retains one terminal input summary, not a live
// receiver sample. Retired callbacks can write only their own instance.
type prewarmHandoverMonitor struct {
	binding    dashboard.NodeNetworkBinding
	generation uint64
	latest     atomic.Pointer[blockstream.TurbinePrewarmHandoverObservation]
}

func (d *nodeDashboard) prewarmHandoverObserver() func(blockstream.TurbinePrewarmHandoverObservation) {
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.prewarmHandover = nil
	if !d.networkDetails || d.networkRetired || d.state.Phase == "stopping" || d.state.RunID == "" ||
		d.state.StartedAt.IsZero() || d.state.GenesisHash == nil || d.prewarmGeneration == ^uint64(0) {
		return nil
	}
	genesis, err := solana.HashFromBase58(*d.state.GenesisHash)
	if err != nil || genesis.IsZero() || genesis.String() != *d.state.GenesisHash {
		return nil
	}
	d.prewarmGeneration++
	if d.prewarmGeneration == ^uint64(0) {
		return nil
	}
	hash := *d.state.GenesisHash
	m := &prewarmHandoverMonitor{binding: dashboard.NodeNetworkBinding{RunID: d.state.RunID,
		NodeStartedAt: d.state.StartedAt, GenesisHash: &hash}, generation: d.prewarmGeneration}
	d.prewarmHandover = m
	return m.observe
}

func (m *prewarmHandoverMonitor) observe(o blockstream.TurbinePrewarmHandoverObservation) {
	if m == nil || o.HandedOverAt.IsZero() || o.Capacity == 0 || o.ReturnedBlocks > o.Capacity ||
		o.ReturnedBlocks == 0 && (o.FirstSlot != 0 || o.LastSlot != 0) ||
		o.ReturnedBlocks > 0 && (o.FirstSlot < o.FloorSlot || o.LastSlot < o.FirstSlot || o.LastSlot-o.FirstSlot < o.ReturnedBlocks-1 ||
			o.ReturnedBlocks == 1 && o.FirstSlot != o.LastSlot) ||
		o.RepairKnown && o.Repair.SampledAt.IsZero() || !o.RepairKnown && o.Repair != (blockstream.TurbinePrewarmRepairObservation{}) {
		return
	}
	m.latest.CompareAndSwap(nil, &o)
}

func (m *prewarmHandoverMonitor) capture(binding dashboard.NodeNetworkBinding) *dashboard.PrewarmHandoverDetail {
	if m == nil || m.binding.RunID != binding.RunID || !m.binding.NodeStartedAt.Equal(binding.NodeStartedAt) ||
		!sameReplayDetailGenesis(m.binding.GenesisHash, binding.GenesisHash) {
		return nil
	}
	o := m.latest.Load()
	if o == nil {
		return nil
	}
	v := &dashboard.PrewarmHandoverDetail{Generation: m.generation, HandedOverAt: o.HandedOverAt.UTC(), FloorSlot: o.FloorSlot,
		Capacity: o.Capacity, ReturnedBlocks: o.ReturnedBlocks, OverflowEvictions: o.OverflowEvictions}
	if o.ReturnedBlocks > 0 {
		first, last := o.FirstSlot, o.LastSlot
		v.FirstSlot, v.LastSlot = &first, &last
	}
	if o.RepairKnown {
		r := o.Repair
		v.Repair = &dashboard.PrewarmRepairDetail{SampledAt: r.SampledAt.UTC(), Requests: r.Requests,
			Responses: r.Responses, LateResponses: r.LateResponses, Timeouts: r.Timeouts}
	}
	return v
}
