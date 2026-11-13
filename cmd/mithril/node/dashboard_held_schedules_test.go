package node

import (
	"math"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/leaderschedule"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

func heldScheduleNodeBank(slot uint64, configuration sealevel.SysvarEpochSchedule) replay.CompletedBlock {
	epoch, index := configuration.GetEpochAndSlotIndex(slot)
	return replay.CompletedBlock{At: time.Now().UTC(), Slot: slot, Bankhash: solana.Hash{1}, Epoch: epoch,
		SlotIndex: index, SlotsInEpoch: configuration.SlotsInEpoch(epoch), EpochSchedule: configuration}
}

func heldScheduleNodeSource(first, count uint64, identity solana.PublicKey) *leaderschedule.LeaderSchedule {
	indices := make([]uint64, count)
	for i := range indices {
		indices[i] = uint64(i)
	}
	return leaderschedule.NewLeaderScheduleFromKeyedSlots(map[solana.PublicKey][]uint64{identity: indices}, first)
}

func heldScheduleNodeStakes(epoch, generation uint64, key solana.PublicKey) nodeHeldStakeMaterial {
	return nodeHeldStakeMaterial{known: true, snapshot: epochstakes.Snapshot{
		Epoch: epoch, Generation: generation, Selection: epochstakes.SelectionVATFiltered,
		Stakes: map[solana.PublicKey]uint64{key: 1},
		VoteAccounts: map[solana.PublicKey]*epochstakes.VoteAccount{
			key: {NodePubkey: key},
		},
		TotalStake: 1,
	}}
}

func TestNodeHeldScheduleCacheReuseReplacementAndInvalidation(t *testing.T) {
	now := time.Now()
	context := nodeHeldScheduleContext{runID: "held-node-test", nodeStarted: now.Add(-time.Minute).UTC(), attemptStarted: now.Add(-time.Second).UTC(), attempt: 1}
	configuration := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128, Warmup: true, FirstNormalEpoch: 2, FirstNormalSlot: 96}
	bank := heldScheduleNodeBank(20, configuration)
	current := heldScheduleNodeSource(0, 32, solana.PublicKey{1})
	next := heldScheduleNodeSource(32, 64, solana.PublicKey{2})
	var cache nodeHeldScheduleCache
	var stakes [2]nodeHeldStakeMaterial
	first, err := cache.capture(now, context, bank, current, next, stakes)
	if err != nil || first.Data == nil || cache.generation != 1 {
		t.Fatalf("initial capture: %v", err)
	}
	bank = heldScheduleNodeBank(21, configuration)
	second, err := cache.capture(time.Now(), context, bank, current, next, stakes)
	if err != nil || second.Data != first.Data || cache.generation != 1 || second.ReferenceSlot != 21 {
		t.Fatal("unchanged held pointers rebuilt data instead of revalidating")
	}
	// Legal same-epoch replacement is not a same-bank immutability violation.
	current = heldScheduleNodeSource(0, 32, solana.PublicKey{3})
	replaced, err := cache.capture(time.Now(), context, bank, current, next, stakes)
	if err != nil || replaced.Data == first.Data || cache.generation != 2 {
		t.Fatal("same-epoch replacement did not replace data")
	}
	missingNext, err := cache.capture(time.Now(), context, bank, current, nil, stakes)
	if err != nil || missingNext.Data == replaced.Data || cache.generation != 3 {
		t.Fatal("missing next source retained its old dataset")
	}
	_, err = cache.capture(time.Now(), context, bank, nil, nil, stakes)
	if err == nil || cache.data != nil || cache.key.current != nil {
		t.Fatal("missing current source did not release old availability/pointers")
	}
	restored, err := cache.capture(time.Now(), context, bank, current, next, stakes)
	if err != nil || restored.Data == missingNext.Data || cache.generation != 4 {
		t.Fatal("source return revived an old dataset")
	}
	context.rewinds++
	rewound, err := cache.capture(time.Now(), context, bank, current, next, stakes)
	if err != nil || rewound.Data == restored.Data || cache.generation != 5 {
		t.Fatal("replay context change reused another lifetime")
	}
	context.runID = "replacement-run"
	_, err = cache.capture(time.Now(), context, bank, current, next, stakes)
	if err != nil || cache.generation != 6 {
		t.Fatal("node run change did not bind new data")
	}
	cache.generation = math.MaxUint64
	current = heldScheduleNodeSource(0, 32, solana.PublicKey{4})
	_, err = cache.capture(time.Now(), context, bank, current, next, stakes)
	if err == nil || cache.data != nil || cache.generation != math.MaxUint64 {
		t.Fatal("revision exhaustion wrapped or retained old availability")
	}
}

func TestNodeHeldScheduleCacheTracksStakeGenerationAndAvailability(t *testing.T) {
	now := time.Now()
	context := nodeHeldScheduleContext{runID: "held-stake-test", nodeStarted: now.Add(-time.Minute).UTC(), attemptStarted: now.Add(-time.Second).UTC(), attempt: 1}
	configuration := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 128, Warmup: true, FirstNormalEpoch: 2, FirstNormalSlot: 96}
	bank := heldScheduleNodeBank(20, configuration)
	currentRange, nextRange, nextReason, err := heldEpochRanges(bank)
	if err != nil || nextReason != "" {
		t.Fatalf("fixture ranges: %s, %v", nextReason, err)
	}
	current := heldScheduleNodeSource(currentRange.first, currentRange.count, solana.PublicKey{1})
	next := heldScheduleNodeSource(nextRange.first, nextRange.count, solana.PublicKey{2})
	stakes := [2]nodeHeldStakeMaterial{
		heldScheduleNodeStakes(currentRange.epoch, 1, solana.PublicKey{11}),
		heldScheduleNodeStakes(nextRange.epoch, 1, solana.PublicKey{12}),
	}
	var cache nodeHeldScheduleCache
	first, err := cache.capture(now, context, bank, current, next, stakes)
	if err != nil || first.Data == nil || cache.generation != 1 {
		t.Fatalf("initial stake capture: %v", err)
	}
	reused, err := cache.capture(now.Add(time.Second), context, bank, current, next, stakes)
	if err != nil || reused.Data != first.Data || cache.generation != 1 {
		t.Fatal("same immutable stake generations rebuilt the held dataset")
	}

	stakes[0] = heldScheduleNodeStakes(currentRange.epoch, 2, solana.PublicKey{13})
	replaced, err := cache.capture(now.Add(2*time.Second), context, bank, current, next, stakes)
	if err != nil || replaced.Data == reused.Data || cache.generation != 2 {
		t.Fatal("new stake generation reused the prior held dataset")
	}

	stakes[0] = nodeHeldStakeMaterial{}
	missing, err := cache.capture(now.Add(3*time.Second), context, bank, current, next, stakes)
	if err != nil || missing.Data == replaced.Data || cache.generation != 3 || cache.key.stakes[0].known {
		t.Fatal("known-to-missing stake material retained the prior held dataset")
	}

	stakes[0] = heldScheduleNodeStakes(currentRange.epoch, 2, solana.PublicKey{13})
	restored, err := cache.capture(now.Add(4*time.Second), context, bank, current, next, stakes)
	if err != nil || restored.Data == missing.Data || cache.generation != 4 || !cache.key.stakes[0].known {
		t.Fatal("absent-to-known stake material reused the missing-source dataset")
	}

	wrong := heldScheduleNodeStakes(nextRange.epoch, 3, solana.PublicKey{14})
	stakes[0] = wrong
	invalid, err := cache.capture(now.Add(5*time.Second), context, bank, current, next, stakes)
	if err != nil || invalid.Data == restored.Data || cache.generation != 5 {
		t.Fatal("wrong-epoch stake material reused the prior held dataset")
	}
	epoch, err := heldEpochData(currentRange, current, wrong)
	if err != nil || epoch.StakeSet != nil || epoch.StakeSetReason != "source_invalid" {
		t.Fatalf("wrong-epoch stake material was not marked invalid: %+v, %v", epoch.StakeSet, err)
	}
}

func TestNodeHeldScheduleAdapterWaitsBeforeReplay(t *testing.T) {
	var absent *nodeDashboard
	if _, err := absent.sampleHeldSchedules(time.Now()); err == nil || err.Error() != "source_unavailable" {
		t.Fatal("nil node invented a source")
	}
	d := &nodeDashboard{}
	if _, err := d.sampleHeldSchedules(time.Now()); err == nil || err.Error() != "waiting_source" {
		t.Fatal("unstarted replay invented schedules")
	}
}

func TestNodeHeldScheduleLocalReplayDatesMatchGeometryAndReplaceCache(t *testing.T) {
	now := time.Now().UTC()
	context := nodeHeldScheduleContext{runID: "held-epoch-time", nodeStarted: now.Add(-time.Minute), attemptStarted: now, attempt: 1}
	configuration := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 8}
	bank := heldScheduleNodeBank(3, configuration)
	current := heldScheduleNodeSource(0, 8, solana.PublicKey{1})
	next := heldScheduleNodeSource(8, 8, solana.PublicKey{2})
	var cache nodeHeldScheduleCache
	var stakes [2]nodeHeldStakeMaterial
	first, err := cache.capture(now, context, bank, current, next, stakes)
	if err != nil {
		t.Fatal(err)
	}
	row := replay.LocalEpochReplayBoundary{Epoch: 0, FirstSlot: 0, SlotCount: 8, EndedAt: now}
	context.localReplay = replay.LocalEpochReplayBoundaries{Count: 1, Rows: [2]replay.LocalEpochReplayBoundary{row}}
	replaced, err := cache.capture(now.Add(time.Second), context, bank, current, next, stakes)
	if err != nil || replaced.Data == first.Data || cache.generation != 2 {
		t.Fatalf("new boundary reused unchanged held pointers: %v", err)
	}
	reused, err := cache.capture(now.Add(2*time.Second), context, bank, current, next, stakes)
	if err != nil || reused.Data != replaced.Data {
		t.Fatalf("same boundary rebuilt data: %v", err)
	}
	r := nodeHeldEpochRange{epoch: 0, first: 0, count: 8}
	times := heldEpochReplayTimes(r, context.localReplay)
	if times == nil || times.StartedAt != nil || times.EndedAt == nil || !times.EndedAt.Equal(now) {
		t.Fatal("known epoch zero end or unknown start was lost")
	}
	*times.EndedAt = now.Add(time.Hour)
	if !context.localReplay.Rows[0].EndedAt.Equal(now) {
		t.Fatal("date pointer aliases replay state")
	}
	for _, r := range []nodeHeldEpochRange{{epoch: 1, first: 0, count: 8}, {epoch: 0, first: 1, count: 8}, {epoch: 0, first: 0, count: 7}} {
		if heldEpochReplayTimes(r, context.localReplay) != nil {
			t.Fatal("mismatched epoch geometry inherited a date")
		}
	}
	context.localReplay.Count = 3
	if heldEpochReplayTimes(r, context.localReplay) != nil {
		t.Fatal("invalid row count accepted")
	}
}

func TestNodeHeldScheduleCacheTracksEpochTargets(t *testing.T) {
	now := time.Now().UTC()
	context := nodeHeldScheduleContext{runID: "held-target-test", nodeStarted: now, attemptStarted: now, attempt: 1}
	configuration := sealevel.SysvarEpochSchedule{SlotsPerEpoch: 8}
	bank := heldScheduleNodeBank(3, configuration)
	current := heldScheduleNodeSource(0, 8, solana.PublicKey{1})
	next := heldScheduleNodeSource(8, 8, solana.PublicKey{2})
	var cache nodeHeldScheduleCache
	var stakes [2]nodeHeldStakeMaterial
	first, err := cache.capture(now, context, bank, current, next, stakes)
	if err != nil {
		t.Fatal(err)
	}
	bank.EpochTargetTiming = replay.EpochTargetTiming{CurrentNanoseconds: 400_000_000, NextNanoseconds: 350_000_000}
	known, err := cache.capture(now.Add(time.Second), context, bank, current, next, stakes)
	if err != nil || known.Data == first.Data {
		t.Fatalf("known target retained an unknown dataset: %v", err)
	}
	reused, err := cache.capture(now.Add(2*time.Second), context, bank, current, next, stakes)
	if err != nil || reused.Data != known.Data {
		t.Fatalf("unchanged target rebuilt data: %v", err)
	}
	bank.EpochTargetTiming.NextNanoseconds = 300_000_000
	replaced, err := cache.capture(now.Add(3*time.Second), context, bank, current, next, stakes)
	if err != nil || replaced.Data == known.Data {
		t.Fatalf("next target changed without replacing data: %v", err)
	}
	bank.EpochTargetTiming = replay.EpochTargetTiming{}
	unknown, err := cache.capture(now.Add(4*time.Second), context, bank, current, next, stakes)
	if err != nil || unknown.Data == replaced.Data || heldEpochTargetDuration(0) != nil {
		t.Fatalf("unknown target revived old values: %v", err)
	}
	full := heldEpochTargetDuration(math.MaxUint64)
	if full == nil || full.Nanoseconds != math.MaxUint64 || full.Provenance != "snapshot_seed_and_bank_features" {
		t.Fatal("target mapping lost exact unsigned duration or provenance")
	}
}
