package consensus

import (
	"math"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/gagliardetto/solana-go"
)

// MonitoringSnapshot contains bounded observations, not a promotion/readiness
// decision. Call after Start has returned; it never initializes components.
type MonitoringSnapshot struct {
	ObservedBlocks   uint64
	ReplayedSlots    uint64
	SafetyFault      bool
	NotarizedSlot    uint64
	HasNotarizedSlot bool
	Chain            alpenglow.ChainSnapshot
	Receiver         *alpenglow.ReceiverStats
	VotingEnabled    bool
	Voting           *VotingObservation
	Lifecycle        *LifecycleSnapshot
}

// ObserveReceiverSocketMemory is an explicitly opted-in diagnostics read of the
// inbound Votor listener, not outbound broadcaster connections. Like
// MonitoringSnapshot, call only after Start has returned; the node starts each
// engine once. Ordinary monitoring snapshots perform no OS reads.
func (e *AlpenglowObserverEngine) ObserveReceiverSocketMemory() metrics.SocketObservation {
	if e == nil {
		return metrics.SocketObservation{Reason: metrics.SocketNotProvided}
	}
	return e.receiver.ObserveSocketMemory()
}

// VotorApplicationPayloadObservation keeps receiver and broadcaster lifetimes
// separate. SendAccepted counts local QUIC queue acceptance, not delivery.
type VotorApplicationPayloadObservation struct {
	Received, SendAccepted       alpenglow.ApplicationPayloadObservation
	HasReceived, HasSendAccepted bool
}

// ObserveVotorApplicationPayload is a low-rate diagnostics read after Start.
// RX copies one scalar under the receiver's existing lock; TX is read only from
// the voter owner's immutable cache, never from broadcaster queues or Stats.
func (e *AlpenglowObserverEngine) ObserveVotorApplicationPayload() VotorApplicationPayloadObservation {
	var o VotorApplicationPayloadObservation
	if e == nil || !e.votorPayloadMonitoring {
		return o
	}
	o.Received, o.HasReceived = e.receiver.ApplicationPayloadObservation()
	e.voterMu.RLock()
	defer e.voterMu.RUnlock()
	if e.voter != nil {
		if s := e.voter.monitoring.Load(); s != nil && s.HasApplicationPayload {
			o.SendAccepted, o.HasSendAccepted = s.ApplicationPayload, true
		}
	}
	return o
}

// VotingObservation is published by the voter loop, which owns signing state.
// Eligibility checks the root epoch's identity, vote account and public BLS key;
// it does not bypass live-window, reservation, safety or per-vote guards.
type VotingObservation struct {
	At                    time.Time
	Identity              solana.PublicKey
	VoteAccount           solana.PublicKey
	RootSlot              uint64
	Epoch                 uint64
	HasEpochSet           bool
	Eligible              bool
	Rank                  uint16
	Stake                 uint64
	TotalStake            uint64
	HasNextEpoch          bool
	NextEpoch             InstalledSignerObservation
	JoinedLive            bool
	VotesCast             uint64
	NetworkLanded         uint64
	LastNetworkLandedSlot uint64
	LastNetworkLandedAt   time.Time
	MessagesQueued        uint64
	MessagesDropped       uint64
	PeerSendErrors        uint64
	PeerQueueDrops        uint64
	SendTimeouts          uint64
	DesiredPeers          int
	Connections           int
	HasApplicationPayload bool
	ApplicationPayload    alpenglow.ApplicationPayloadObservation
}

// InstalledSignerObservation checks one already-installed ranked set against
// the running voter's node, vote account and derived public BLS key. It is not
// a prediction of admission, a payment receipt or permission to sign.
type InstalledSignerObservation struct {
	Epoch       uint64
	HasEpochSet bool
	Eligible    bool
	Rank        uint16
	Stake       uint64
	TotalStake  uint64
}

// MonitoringSnapshot copies scalar consensus state and the voter's last sample.
func (e *AlpenglowObserverEngine) MonitoringSnapshot() MonitoringSnapshot {
	s := MonitoringSnapshot{ObservedBlocks: e.observedBlocks.Load(), ReplayedSlots: e.replayedSlots.Load(), SafetyFault: e.safetyError() != nil}
	if e.chain != nil {
		s.Chain = e.chain.Snapshot()
	}
	if e.observer != nil {
		s.NotarizedSlot, s.HasNotarizedSlot = e.observer.NotarizedSlot()
	}
	if e.receiver != nil {
		r := e.receiver.Stats()
		s.Receiver = &r
	}
	if e.lifecycle != nil {
		lifecycle := e.lifecycle.snapshot()
		s.Lifecycle = &lifecycle
	}
	e.voterMu.RLock()
	voter := e.voter
	e.voterMu.RUnlock()
	if voter != nil {
		s.VotingEnabled = true
		if p := voter.monitoring.Load(); p != nil {
			observation := *p
			s.Voting = &observation
		}
	}
	return s
}

// Called only on the voter goroutine, after its existing periodic stats read.
func (v *alpenglowVoter) publishMonitoring(stats VotingStats) {
	if !v.engine.monitoringEnabled {
		return
	}
	s := &VotingObservation{
		At: time.Now().UTC(), Identity: v.node, VoteAccount: v.voteAccount, JoinedLive: v.votingStarted,
		VotesCast: stats.VotesCastThisRun, NetworkLanded: stats.NetworkLandedVotes,
		LastNetworkLandedSlot: stats.LastNetworkLandedSlot, LastNetworkLandedAt: stats.LastNetworkLandedAt,
		MessagesQueued: stats.BroadcastMessagesQueued, MessagesDropped: stats.BroadcastMessagesDropped,
		PeerSendErrors: stats.BroadcastPeerSendErrors, PeerQueueDrops: stats.BroadcastPeerQueueDrops,
		SendTimeouts: stats.BroadcastPeerSendTimeouts, DesiredPeers: stats.BroadcastDesiredPeers, Connections: stats.BroadcastActiveConnections,
	}
	if v.engine.votorPayloadMonitoring && stats.BroadcastApplicationPayload != nil {
		s.HasApplicationPayload, s.ApplicationPayload = true, *stats.BroadcastApplicationPayload
	}
	if v.engine.votorPeerRootSet.Load() {
		s.RootSlot = v.engine.votorPeerRoot.Load()
		s.Epoch = v.epochForSlot(s.RootSlot)
		set, ok := v.validatorSet(s.RootSlot)
		if ok {
			s.HasEpochSet, s.TotalStake = true, set.TotalStake
			public := v.signer.PublicKeyCompressed()
			for _, validator := range set.Validators {
				if validator.VoteAccount == v.voteAccount {
					s.Eligible = validator.NodePubkey == v.node && validator.BlsPubkeyCompressed == public
					if s.Eligible {
						s.Rank, s.Stake = validator.Rank, validator.Stake
					}
					break
				}
			}
		}
		if s.Epoch != math.MaxUint64 {
			s.HasNextEpoch = true
			s.NextEpoch = v.installedSignerObservation(s.Epoch + 1)
		}
	}
	v.monitoring.Store(s)
}

// Called on the voter owner after validatorSet has released the voter set lock.
// Installed sets and their identity indexes share this engine lock.
func (v *alpenglowVoter) installedSignerObservation(epoch uint64) InstalledSignerObservation {
	s := InstalledSignerObservation{Epoch: epoch}
	v.engine.validatorSetsMu.RLock()
	defer v.engine.validatorSetsMu.RUnlock()
	set, known := v.engine.validatorSets[epoch]
	if !known {
		return s
	}
	s.HasEpochSet, s.TotalStake = true, set.TotalStake
	rank, found := v.engine.validatorRanks[epoch][v.node]
	if !found || int(rank) >= len(set.Validators) || set.Epoch != epoch {
		return s
	}
	validator := set.Validators[rank]
	if validator.Rank != rank || validator.NodePubkey != v.node || validator.VoteAccount != v.voteAccount || validator.BlsPubkeyCompressed != v.signer.PublicKeyCompressed() {
		return s
	}
	s.Eligible, s.Rank, s.Stake = true, rank, validator.Stake
	return s
}
