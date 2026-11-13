package consensus

import (
	"crypto/ed25519"
	"math"
	"reflect"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/gagliardetto/solana-go"
)

func nextSignerFixture(t *testing.T, current, next bool, mutate func(*alpenglow.ValidatorSet)) (*AlpenglowObserverEngine, *alpenglowVoter, alpenglow.ValidatorSet) {
	t.Helper()
	identity, authorized := voterTestKey(81), voterTestKey(82)
	vote := solana.PublicKey{83}
	signer, err := alpenglow.DeriveBLSSigner(authorized)
	if err != nil {
		t.Fatal(err)
	}
	e, err := NewEngine(Config{MonitoringEnabled: true})
	if err != nil {
		t.Fatal(err)
	}
	set := voterTestValidatorSet(t, identity, authorized, vote)
	if current {
		if err := e.SetAlpenglowValidatorSet(set); err != nil {
			t.Fatal(err)
		}
	}
	set.Epoch++
	if mutate != nil {
		mutate(&set)
	}
	if next {
		if err := e.SetAlpenglowValidatorSet(set); err != nil {
			t.Fatal(err)
		}
	}
	v := &alpenglowVoter{engine: e, node: solana.PublicKey(identity.Public().(ed25519.PublicKey)), voteAccount: vote,
		signer: signer, epochForSlot: func(slot uint64) uint64 { return slot / 100 }, sets: make(map[uint64]alpenglow.ValidatorSet)}
	for epoch, installed := range e.validatorSets {
		v.sets[epoch] = cloneValidatorSet(installed)
	}
	e.voter = v
	e.votorPeerRoot.Store(701)
	e.votorPeerRootSet.Store(true)
	return e, v, set
}

func TestMonitoringInstalledNextSetAvailability(t *testing.T) {
	for _, test := range []struct {
		name          string
		current, next bool
	}{
		{"missing", false, false}, {"current only", true, false}, {"next only", false, true}, {"both", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			e, v, _ := nextSignerFixture(t, test.current, test.next, nil)
			v.publishMonitoring(VotingStats{VotesCastThisRun: 9})
			o := e.MonitoringSnapshot().Voting
			if o == nil || !o.HasNextEpoch || o.RootSlot != 701 || o.Epoch != 7 || o.HasEpochSet != test.current || o.Eligible != test.current || o.VotesCast != 9 {
				t.Fatalf("current observation = %+v", o)
			}
			if o.NextEpoch.Epoch != 8 || o.NextEpoch.HasEpochSet != test.next || o.NextEpoch.Eligible != test.next {
				t.Fatalf("next observation = %+v", o.NextEpoch)
			}
			if test.next && (o.NextEpoch.Rank != 0 || o.NextEpoch.Stake != 30 || o.NextEpoch.TotalStake != 100) {
				t.Fatalf("next attribution = %+v", o.NextEpoch)
			}
			if !test.next && o.NextEpoch != (InstalledSignerObservation{Epoch: 8}) {
				t.Fatalf("unknown set gained values: %+v", o.NextEpoch)
			}
		})
	}
}

func TestMonitoringInstalledNextBindings(t *testing.T) {
	for _, mismatch := range []string{"none", "node", "vote", "bls"} {
		t.Run(mismatch, func(t *testing.T) {
			e, v, _ := nextSignerFixture(t, true, true, func(set *alpenglow.ValidatorSet) {
				switch mismatch {
				case "node":
					set.Validators[0].NodePubkey = solana.PublicKey{91}
				case "vote":
					set.Validators[0].VoteAccount = solana.PublicKey{92}
				case "bls":
					other, err := alpenglow.DeriveBLSSigner(voterTestKey(93))
					if err != nil {
						t.Fatal(err)
					}
					set.Validators[0].BlsPubkeyCompressed = other.PublicKeyCompressed()
				}
			})
			v.publishMonitoring(VotingStats{})
			o := e.MonitoringSnapshot().Voting
			if !o.Eligible || !o.HasEpochSet {
				t.Fatalf("next mismatch changed current eligibility: %+v", o)
			}
			if !o.NextEpoch.HasEpochSet || o.NextEpoch.Eligible != (mismatch == "none") || o.NextEpoch.TotalStake != 100 {
				t.Fatalf("next binding = %+v", o.NextEpoch)
			}
			if mismatch != "none" && (o.NextEpoch.Stake != 0 || o.NextEpoch.Rank != 0) {
				t.Fatalf("ineligible signer attributed rank/stake: %+v", o.NextEpoch)
			}
		})
	}
}

func TestMonitoringInstalledNextRankGuards(t *testing.T) {
	for _, defect := range []string{"missing index", "missing node", "outside", "wrong row", "wrong rank", "wrong epoch"} {
		t.Run(defect, func(t *testing.T) {
			e, v, _ := nextSignerFixture(t, true, true, nil)
			e.validatorSetsMu.Lock()
			switch defect {
			case "missing index":
				delete(e.validatorRanks, 8)
			case "missing node":
				delete(e.validatorRanks[8], v.node)
			case "outside":
				e.validatorRanks[8][v.node] = math.MaxUint16
			case "wrong row":
				e.validatorRanks[8][v.node] = 1
			case "wrong rank":
				e.validatorSets[8].Validators[0].Rank = 1
			case "wrong epoch":
				set := e.validatorSets[8]
				set.Epoch = 9
				e.validatorSets[8] = set
			}
			e.validatorSetsMu.Unlock()
			v.publishMonitoring(VotingStats{})
			o := e.MonitoringSnapshot().Voting.NextEpoch
			if !o.HasEpochSet || o.Eligible || o.Stake != 0 || o.Rank != 0 {
				t.Fatalf("invalid index accepted: %+v", o)
			}
		})
	}
}

func TestMonitoringNextRequiresRootAndRepresentableEpoch(t *testing.T) {
	e, v, _ := nextSignerFixture(t, true, true, nil)
	e.votorPeerRootSet.Store(false)
	v.publishMonitoring(VotingStats{})
	if o := e.MonitoringSnapshot().Voting; o.HasNextEpoch || o.NextEpoch != (InstalledSignerObservation{}) {
		t.Fatalf("unknown root inferred next epoch: %+v", o)
	}
	e.votorPeerRootSet.Store(true)
	v.epochForSlot = func(uint64) uint64 { return math.MaxUint64 }
	v.publishMonitoring(VotingStats{})
	if o := e.MonitoringSnapshot().Voting; o.Epoch != math.MaxUint64 || o.HasNextEpoch || o.NextEpoch != (InstalledSignerObservation{}) {
		t.Fatalf("next epoch wrapped: %+v", o)
	}
	v.epochForSlot = func(uint64) uint64 { return math.MaxUint64 - 1 }
	v.publishMonitoring(VotingStats{})
	if o := e.MonitoringSnapshot().Voting; !o.HasNextEpoch || o.NextEpoch.Epoch != math.MaxUint64 || o.NextEpoch.HasEpochSet {
		t.Fatalf("representable maximum lost: %+v", o)
	}
	v.epochForSlot = func(uint64) uint64 { return 0 }
	e.votorPeerRoot.Store(0)
	v.publishMonitoring(VotingStats{})
	if o := e.MonitoringSnapshot().Voting; !o.HasNextEpoch || o.NextEpoch.Epoch != 1 {
		t.Fatalf("known zero root lost: %+v", o)
	}
	e.voterMu.Lock()
	e.voter = nil
	e.voterMu.Unlock()
	if o := e.MonitoringSnapshot(); o.VotingEnabled || o.Voting != nil {
		t.Fatalf("full node acquired signer observation: %+v", o)
	}
}

func TestMonitoringNextDetachedAndDisabled(t *testing.T) {
	e, v, original := nextSignerFixture(t, true, true, nil)
	v.publishMonitoring(VotingStats{})
	want := e.MonitoringSnapshot().Voting.NextEpoch
	original.Validators[0].Stake = 999
	copy := e.MonitoringSnapshot().Voting
	copy.NextEpoch.Stake = 888
	if got := e.MonitoringSnapshot().Voting.NextEpoch; got != want {
		t.Fatalf("snapshot alias: %+v want %+v", got, want)
	}
	v.publishMonitoring(VotingStats{})
	if got := e.MonitoringSnapshot().Voting.NextEpoch; got != want {
		t.Fatalf("caller set mutated installed source: %+v", got)
	}
	typeOf := reflect.TypeFor[InstalledSignerObservation]()
	for i := 0; i < typeOf.NumField(); i++ {
		switch typeOf.Field(i).Type.Kind() {
		case reflect.Bool, reflect.Uint16, reflect.Uint64:
		default:
			t.Fatalf("non-scalar field %s", typeOf.Field(i).Name)
		}
	}
	e.monitoringEnabled = false
	before := v.monitoring.Load()
	v.signer = nil
	v.epochForSlot = func(uint64) uint64 { panic("disabled monitoring queried epoch") }
	if n := testing.AllocsPerRun(100, func() { v.publishMonitoring(VotingStats{}) }); n != 0 {
		t.Fatalf("disabled allocations = %v", n)
	}
	if v.monitoring.Load() != before {
		t.Fatal("disabled publication replaced sample")
	}
}

func TestMonitoringNextConcurrentInstalledSetRead(t *testing.T) {
	e, v, set := nextSignerFixture(t, true, false, nil)
	v.events, v.done = make(chan voterEvent, 1), make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			v.publishMonitoring(VotingStats{})
		}
	}()
	if err := e.SetAlpenglowValidatorSet(set); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	v.publishMonitoring(VotingStats{})
	if got := e.MonitoringSnapshot().Voting.NextEpoch; !got.HasEpochSet || !got.Eligible || got.Stake != 30 {
		t.Fatalf("installed result = %+v", got)
	}
}
