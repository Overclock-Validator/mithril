package consensus

import (
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/gagliardetto/solana-go"
)

func TestMonitoringApplicationPayloadRequiresExplicitOptInAndCopiesCache(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		e, err := NewEngine(Config{MonitoringEnabled: true, VotorPayloadMonitoringEnabled: enabled})
		if err != nil {
			t.Fatal(err)
		}
		v := &alpenglowVoter{engine: e}
		e.voter = v
		at := time.Now()
		raw := &alpenglow.ApplicationPayloadObservation{StartedAt: at, SampledAt: at, Bytes: 9007199254740993, Known: true}
		v.publishMonitoring(VotingStats{BroadcastApplicationPayload: raw})
		raw.Bytes = 0
		o := e.ObserveVotorApplicationPayload()
		if o.HasReceived || o.HasSendAccepted != enabled || enabled && (o.SendAccepted.Bytes != 9007199254740993 || !o.SendAccepted.SampledAt.Equal(at)) {
			t.Fatal("payload availability, exact value or original date changed")
		}
		if enabled {
			o.SendAccepted.Bytes = 1
			if e.ObserveVotorApplicationPayload().SendAccepted.Bytes != 9007199254740993 {
				t.Fatal("cached payload aliases its caller")
			}
		}
		e.voter = nil
		if e.ObserveVotorApplicationPayload().HasSendAccepted {
			t.Fatal("detached voter remained available")
		}
	}
	e, err := NewEngine(Config{VotorPayloadMonitoringEnabled: true})
	if err != nil || !e.monitoringEnabled {
		t.Fatal("explicit opt-in did not enable its existing cached publication path")
	}
}

func TestMonitoringEligibilityUsesAllPublicBindings(t *testing.T) {
	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := alpenglow.DeriveBLSSigner(key)
	if err != nil {
		t.Fatal(err)
	}
	node, vote := solana.PublicKey{1}, solana.PublicKey{2}
	e, err := NewEngine(Config{MonitoringEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	v := &alpenglowVoter{engine: e, node: node, voteAccount: vote, signer: signer,
		epochForSlot: func(slot uint64) uint64 { return slot / 100 }, sets: make(map[uint64]alpenglow.ValidatorSet)}
	e.voter = v
	v.publishMonitoring(VotingStats{VotesCastThisRun: 4})
	if s := e.MonitoringSnapshot(); !s.VotingEnabled || s.Voting == nil || s.Voting.HasEpochSet {
		t.Fatal("missing epoch set must be unknown")
	}
	e.votorPeerRoot.Store(201)
	e.votorPeerRootSet.Store(true)
	member := alpenglow.ValidatorStake{NodePubkey: node, VoteAccount: vote, Rank: 0, Stake: 100, BlsPubkeyCompressed: signer.PublicKeyCompressed()}
	for _, mismatch := range []string{"none", "node", "vote", "bls"} {
		entry := member
		switch mismatch {
		case "node":
			entry.NodePubkey[0]++
		case "vote":
			entry.VoteAccount[0]++
		case "bls":
			entry.BlsPubkeyCompressed[0]++
		}
		v.sets[2] = alpenglow.ValidatorSet{Epoch: 2, TotalStake: 200, Validators: []alpenglow.ValidatorStake{entry}}
		v.publishMonitoring(VotingStats{VotesCastThisRun: 4})
		s := e.MonitoringSnapshot().Voting
		if !s.HasEpochSet || s.Epoch != 2 || s.Eligible != (mismatch == "none") || s.VotesCast != 4 {
			t.Fatalf("%s: %+v", mismatch, s)
		}
		if mismatch != "none" && s.Stake != 0 {
			t.Fatal("ineligible identity attributed stake")
		}
	}
	e.monitoringEnabled = false
	before := v.monitoring.Load()
	v.publishMonitoring(VotingStats{VotesCastThisRun: 100})
	if before != v.monitoring.Load() {
		t.Fatal("disabled publication performed work")
	}
}
