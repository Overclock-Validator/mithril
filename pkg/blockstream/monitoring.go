package blockstream

import (
	"time"

	"github.com/Overclock-Validator/mithril/pkg/gossip"
	"github.com/Overclock-Validator/mithril/pkg/turbine"
)

// NetworkSnapshot contains source state and cached receiver/gossip observations.
// The fields are independently sampled, not an atomic replay or bank view.
type NetworkSnapshot struct {
	SampledAt            time.Time
	SourceStartedAt      time.Time
	ConfiguredSource     string
	NextSlot             uint64
	LiveConnected        bool
	LiveActive           bool
	RPCBlockFetchAllowed bool
	RepairCatchupPending bool
	RepairCatchupFrom    uint64
	RepairCatchupUntil   uint64
	Turbine              *TurbineObservation
	Reception            *ReceptionObservation
	// Gossip is the actual stream client's cached aggregate observation. Its
	// lifetime may span receiver sessions when the validator shares a client.
	Gossip *gossip.MonitoringSnapshot
}

// ReceptionObservation binds the monitoring-only recent TVU sample to one
// receiver session. It is independent of the ten-second receiver counters.
type ReceptionObservation struct {
	StartedAt          time.Time
	Session            uint64
	WindowMilliseconds uint64
	Progress           turbine.ReceptionSnapshot
}

// TurbineObservation is cumulative within (SourceStartedAt, Session), not a
// node-run total. Data/coding shred counts precede signature verification.
// Repair responses and late responses are separate matched-response counters.
type TurbineObservation struct {
	SampledAt            time.Time
	StartedAt            time.Time
	Session              uint64
	Packets              uint64
	DataShreds           uint64
	CodingShreds         uint64
	ParseErrors          uint64
	SignatureErrors      uint64
	MissingLeaders       uint64
	ShredVersionMismatch uint64
	UsefulRepairShreds   uint64
	SpooledRepairShreds  uint64
	LastPacketAt         time.Time
	RepairEnabled        bool
	RepairRequests       uint64
	RepairResponses      uint64
	RepairLateResponses  uint64
	RepairTimeouts       uint64
	RepairErrors         uint64
	Frontiers            turbine.ShredFrontiers
	BlocksEmitted        uint64
	RecoveredData        uint64
	AssemblyErrors       uint64
	ActiveSlots          int
	EvictedSlots         uint64
	IgnoredOldShreds     uint64
	SigVerifies          uint64
	SigVerifyCached      uint64
	RetransmitEnabled    bool
	PeerPayloadBytes     uint64
	RepairOutstanding    int
	RepairPeers          int
	RepairResponding     int
}

// EnableMonitoring opts into bounded publication by the stream owner.
// Call only before Start; readers never initialize or sample the receiver.
func (bs *BlockSource) EnableMonitoring() { bs.monitoringEnabled = true }

// NetworkSnapshot copies bounded scalars without receiver calls, I/O, map scans
// or reset-on-read counters. A stopped or unmonitored source is unavailable.
func (bs *BlockSource) NetworkSnapshot() (NetworkSnapshot, bool) {
	if bs == nil || !bs.monitoringEnabled || bs.stopped.Load() {
		return NetworkSnapshot{}, false
	}
	s := NetworkSnapshot{
		SampledAt: time.Now().UTC(), SourceStartedAt: bs.startedAt.UTC(),
		LiveConnected: bs.liveStreamConnected.Load(), LiveActive: bs.liveStreamActive.Load(),
		RPCBlockFetchAllowed: bs.rpcBlockFetchAllowed(), RepairCatchupPending: bs.repairCatchupPending.Load(),
		RepairCatchupFrom: bs.repairCatchupFrom.Load(), RepairCatchupUntil: bs.repairCatchupUntil.Load(),
	}
	switch bs.sourceType {
	case BlockSourceRpc:
		s.ConfiguredSource = "rpc"
	case BlockSourceFile:
		s.ConfiguredSource = "file"
	case BlockSourceLightbringer:
		s.ConfiguredSource = "lightbringer"
	case BlockSourceTurbine:
		s.ConfiguredSource = "turbine"
	}
	bs.reorderMu.Lock()
	s.NextSlot = bs.nextSlotToSend
	bs.reorderMu.Unlock()
	if s.LiveConnected {
		if cached := bs.turbineObservation.Load(); cached != nil {
			observation := *cached
			s.Turbine = &observation
		}
		if cached := bs.receptionObservation.Load(); cached != nil {
			observation := *cached
			s.Reception = &observation
		}
		if observation, ok := bs.monitoredGossip.Load().MonitoringSnapshot(); ok {
			s.Gossip = &observation
		}
	}
	return s, true
}

func (bs *BlockSource) publishReceptionObservation(started time.Time, session uint64, progress turbine.ReceptionSnapshot) {
	if !bs.monitoringEnabled {
		return
	}
	bs.receptionObservation.Store(&ReceptionObservation{StartedAt: started.UTC(), Session: session,
		WindowMilliseconds: uint64(turbine.ReceptionWindow / time.Millisecond), Progress: progress})
}

// CachedReceptionObservation copies the connected source's existing TVU cache.
// It does not sample the receiver or read the clock. The consumer must check
// the original monotonic ValidUntil; availability alone does not mean fresh.
func (bs *BlockSource) CachedReceptionObservation() (time.Time, ReceptionObservation, bool) {
	if bs == nil || !bs.monitoringEnabled || bs.stopped.Load() || !bs.liveStreamConnected.Load() {
		return time.Time{}, ReceptionObservation{}, false
	}
	cached := bs.receptionObservation.Load()
	if cached == nil {
		return time.Time{}, ReceptionObservation{}, false
	}
	value := *cached
	if bs.receptionObservation.Load() != cached || bs.stopped.Load() || !bs.liveStreamConnected.Load() {
		return time.Time{}, ReceptionObservation{}, false
	}
	return bs.startedAt.UTC(), value, true
}

// setMonitoredGossip attaches the chosen shared or stream-owned client without
// sampling it. Enabling a running shared client only opts into its next cycle.
func (bs *BlockSource) setMonitoredGossip(client *gossip.Client) {
	if !bs.monitoringEnabled || client == nil {
		bs.monitoredGossip.Store(nil)
		return
	}
	client.EnableMonitoring()
	bs.monitoredGossip.Store(client)
}

// publishTurbineObservation reuses the receiver owner's existing stats read.
func (bs *BlockSource) publishTurbineObservation(now, started time.Time, session uint64, repair bool, stats turbine.ReceiverStats, frontiers turbine.ShredFrontiers) {
	if !bs.monitoringEnabled {
		return
	}
	s := &TurbineObservation{
		SampledAt: now.UTC(), StartedAt: started.UTC(), Session: session,
		Packets: stats.Packets, DataShreds: stats.DataShreds, CodingShreds: stats.CodingShreds,
		ParseErrors: stats.ParseErrors, SignatureErrors: stats.SignatureErrors,
		MissingLeaders: stats.MissingLeaders, ShredVersionMismatch: stats.ShredVersionMismatch,
		UsefulRepairShreds: stats.UsefulRepairShreds, SpooledRepairShreds: stats.SpooledRepairShreds,
		RepairEnabled: repair, RepairRequests: stats.Repair.Requests, RepairResponses: stats.Repair.Responses,
		RepairLateResponses: stats.Repair.LateResponses, RepairTimeouts: stats.Repair.Timeouts, RepairErrors: stats.Repair.Errors,
		Frontiers: frontiers, BlocksEmitted: stats.BlocksEmitted, RecoveredData: stats.RecoveredData,
		AssemblyErrors: stats.AssemblyErrors, ActiveSlots: stats.ActiveSlots, EvictedSlots: stats.EvictedSlots,
		IgnoredOldShreds: stats.IgnoredOldShreds, SigVerifies: stats.SigVerifies, SigVerifyCached: stats.SigVerifyCached,
		RetransmitEnabled: stats.RetransmitEnabled, PeerPayloadBytes: stats.Retransmit.SentPayloadBytes,
		RepairOutstanding: stats.Repair.Outstanding, RepairPeers: stats.Repair.Peers, RepairResponding: stats.Repair.RespondingPeers,
	}
	if stats.LastPacketUnix != 0 {
		s.LastPacketAt = time.Unix(stats.LastPacketUnix, 0).UTC()
	}
	bs.turbineObservation.Store(s)
}
