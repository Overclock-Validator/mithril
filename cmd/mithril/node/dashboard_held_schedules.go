package node

import (
	"encoding/base64"
	"errors"
	"math"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/leaderschedule"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
)

type nodeHeldScheduleContext struct {
	runID, genesis              string
	hasGenesis                  bool
	nodeStarted, attemptStarted time.Time
	attempt, rewinds            uint64
	localReplay                 replay.LocalEpochReplayBoundaries
}

type nodeHeldEpochRange struct{ epoch, first, count uint64 }

type nodeHeldStakeMaterial struct {
	snapshot epochstakes.Snapshot
	known    bool
}

type nodeHeldStakeKey struct {
	generation uint64
	known      bool
}

type nodeHeldScheduleKey struct {
	context       nodeHeldScheduleContext
	configuration sealevel.SysvarEpochSchedule
	current, next *leaderschedule.LeaderSchedule
	epoch         uint64
	stakes        [2]nodeHeldStakeKey
	timing        replay.EpochTargetTiming
}

// nodeHeldScheduleCache belongs to the joined detail worker. It keeps only
// the latest prepared body and source pointers, not a cache on every protocol
// schedule. Copy/encoding never holds node, global or consensus locks.
type nodeHeldScheduleCache struct {
	key        nodeHeldScheduleKey
	generation uint64
	data       *dashboard.PreparedHeldScheduleData
}

func heldEpochSchedule(configuration sealevel.SysvarEpochSchedule) *dashboard.HeldEpochSchedule {
	return &dashboard.HeldEpochSchedule{SlotsPerEpoch: configuration.SlotsPerEpoch, FirstNormalEpoch: configuration.FirstNormalEpoch,
		FirstNormalSlot: configuration.FirstNormalSlot, Warmup: configuration.Warmup}
}

func (c *nodeHeldScheduleCache) invalidate() {
	c.data = nil
	c.key = nodeHeldScheduleKey{}
}

func heldEpochRanges(b replay.CompletedBlock) (current, next nodeHeldEpochRange, nextReason string, err error) {
	configuration, ok := b.CheckedEpochSchedule()
	if !ok {
		return current, next, "", errors.New("waiting_source")
	}
	current = nodeHeldEpochRange{epoch: b.Epoch, first: b.Slot - b.SlotIndex, count: b.SlotsInEpoch}
	if current.count > leaderschedule.MaxHeldScheduleSlots || current.first > math.MaxUint64-(current.count-1) {
		return current, next, "", errors.New("response_limit")
	}
	last := current.first + current.count - 1
	if last == math.MaxUint64 || current.epoch == math.MaxUint64 {
		return current, next, "range_unavailable", nil
	}
	first := last + 1
	epoch, index := configuration.GetEpochAndSlotIndex(first)
	count := configuration.SlotsInEpoch(epoch)
	if epoch != current.epoch+1 || index != 0 || count == 0 || first > math.MaxUint64-(count-1) {
		return current, next, "range_unavailable", nil
	}
	if count > leaderschedule.MaxHeldScheduleSlots {
		return current, next, "response_limit", nil
	}
	return current, nodeHeldEpochRange{epoch: epoch, first: first, count: count}, "", nil
}

func heldEpochData(r nodeHeldEpochRange, source *leaderschedule.LeaderSchedule, stakes nodeHeldStakeMaterial) (dashboard.HeldEpochAssignments, error) {
	copy, err := source.CaptureHeldSchedule(r.first, r.count)
	if err != nil {
		return dashboard.HeldEpochAssignments{}, errors.New("response_limit")
	}
	leaders := make([]string, len(copy.Leaders))
	for i, key := range copy.Leaders {
		leaders[i] = key.String()
	}
	result := dashboard.HeldEpochAssignments{Epoch: r.epoch, FirstSlot: r.first, SlotCount: r.count, KnownSlots: copy.KnownSlots,
		Encoding: "base64_u16_le", Stride: copy.Stride, Leaders: leaders, Indices: base64.StdEncoding.EncodeToString([]byte(copy.Indices))}
	if stakes.known && stakes.snapshot.Epoch != r.epoch {
		result.StakeSetReason = "source_invalid"
	} else {
		result.StakeSet, err = captureVATEpochStakes(stakes.snapshot, stakes.known)
		if err != nil {
			result.StakeSetReason = err.Error()
		}
	}
	return result, nil
}

func heldEpochReplayTimes(r nodeHeldEpochRange, source replay.LocalEpochReplayBoundaries) *dashboard.HeldEpochReplayTimes {
	if int(source.Count) > len(source.Rows) {
		return nil
	}
	for _, row := range source.Rows[:source.Count] {
		if row.Epoch != r.epoch || row.FirstSlot != r.first || row.SlotCount != r.count || row.StartedAt.IsZero() && row.EndedAt.IsZero() {
			continue
		}
		times := &dashboard.HeldEpochReplayTimes{}
		if !row.StartedAt.IsZero() {
			value := row.StartedAt.UTC()
			times.StartedAt = &value
		}
		if !row.EndedAt.IsZero() {
			value := row.EndedAt.UTC()
			times.EndedAt = &value
		}
		return times
	}
	return nil
}

func heldEpochTargetDuration(nanoseconds uint64) *dashboard.HeldEpochTargetDuration {
	if nanoseconds == 0 {
		return nil
	}
	return &dashboard.HeldEpochTargetDuration{Nanoseconds: nanoseconds, Provenance: "snapshot_seed_and_bank_features"}
}

func (c *nodeHeldScheduleCache) capture(now time.Time, context nodeHeldScheduleContext, bank replay.CompletedBlock, currentSource, nextSource *leaderschedule.LeaderSchedule, stakes [2]nodeHeldStakeMaterial) (dashboard.HeldScheduleCapture, error) {
	currentRange, nextRange, nextReason, err := heldEpochRanges(bank)
	if err != nil || currentSource == nil || bank.At.IsZero() || bank.Bankhash.IsZero() {
		c.invalidate()
		if err != nil {
			return dashboard.HeldScheduleCapture{}, err
		}
		return dashboard.HeldScheduleCapture{}, errors.New("waiting_source")
	}
	if nextReason != "" {
		nextSource = nil
	}
	key := nodeHeldScheduleKey{context: context, configuration: bank.EpochSchedule, current: currentSource, next: nextSource, epoch: bank.Epoch}
	key.timing = bank.EpochTargetTiming
	key.stakes[0] = nodeHeldStakeKey{generation: stakes[0].snapshot.Generation, known: stakes[0].known}
	if nextSource != nil {
		key.stakes[1] = nodeHeldStakeKey{generation: stakes[1].snapshot.Generation, known: stakes[1].known}
	}
	if c.data == nil || c.key != key {
		c.invalidate()
		if c.generation == math.MaxUint64 {
			return dashboard.HeldScheduleCapture{}, errors.New("source_unavailable")
		}
		current, err := heldEpochData(currentRange, currentSource, stakes[0])
		if err != nil {
			return dashboard.HeldScheduleCapture{}, err
		}
		current.LocalReplay = heldEpochReplayTimes(currentRange, context.localReplay)
		current.TargetSlotDuration = heldEpochTargetDuration(bank.EpochTargetTiming.CurrentNanoseconds)
		var next *dashboard.HeldEpochAssignments
		if nextReason == "" {
			if nextSource == nil {
				nextReason = "waiting_source"
			} else {
				value, err := heldEpochData(nextRange, nextSource, stakes[1])
				if err != nil {
					nextReason = "response_limit"
				} else {
					value.LocalReplay = heldEpochReplayTimes(nextRange, context.localReplay)
					value.TargetSlotDuration = heldEpochTargetDuration(bank.EpochTargetTiming.NextNanoseconds)
					next = &value
				}
			}
		}
		var genesis *string
		if context.hasGenesis {
			value := context.genesis
			genesis = &value
		}
		data, err := dashboard.PrepareHeldScheduleData(dashboard.HeldScheduleData{
			SchemaVersion: 1, Family: "held_schedule", Scope: "locally_held_epoch_assignments",
			Binding: dashboard.ReplayDetailBinding{RunID: context.runID, NodeStartedAt: context.nodeStarted, GenesisHash: genesis,
				SourceGeneration: c.generation + 1, Attempt: context.attempt, Rewinds: context.rewinds,
				AttemptStartedAt: context.attemptStarted, ResetAt: now.UTC()},
			ObservedAt: now.UTC(), GeometryBankSlot: bank.Slot, GeometryBankhash: bank.Bankhash.String(), GeometryBankCompletedAt: bank.At.UTC(),
			EpochSchedule: heldEpochSchedule(bank.EpochSchedule),
			Current:       current, Next: next, NextReason: nextReason,
		})
		if err != nil {
			return dashboard.HeldScheduleCapture{}, err
		}
		c.generation++
		c.data, c.key = data, key
	}
	return dashboard.HeldScheduleCapture{Data: c.data, CapturedAt: now.UTC(), FreshUntil: now.Add(dashboard.HeldScheduleFreshFor),
		ReferenceSlot: bank.Slot, ReferenceBankhash: bank.Bankhash.String(), ReferenceCompletedAt: bank.At.UTC()}, nil
}

// sampleHeldSchedules fences both replay geometry and the retained pointer pair
// around a bounded copy. It is not used by the ordinary status sampler or HTTP.
func (d *nodeDashboard) sampleHeldSchedules(now time.Time) (dashboard.HeldScheduleCapture, error) {
	if d == nil {
		return dashboard.HeldScheduleCapture{}, errors.New("source_unavailable")
	}
	d.mu.Lock()
	context := nodeHeldScheduleContext{runID: d.state.RunID, nodeStarted: d.state.StartedAt.UTC()}
	if d.state.GenesisHash != nil {
		context.hasGenesis, context.genesis = true, *d.state.GenesisHash
	}
	d.mu.Unlock()
	before, ok := d.progress.Snapshot()
	if !ok || !before.HasCompleted {
		d.heldSchedules.invalidate()
		return dashboard.HeldScheduleCapture{}, errors.New("waiting_source")
	}
	current, next, nextReason, err := heldEpochRanges(before.Completed)
	if err != nil {
		d.heldSchedules.invalidate()
		return dashboard.HeldScheduleCapture{}, err
	}
	context.attempt, context.rewinds, context.attemptStarted = before.Attempt, before.Rewinds, before.StartedAt.UTC()
	context.localReplay = before.LocalEpochReplay
	epochs := []uint64{current.epoch}
	if nextReason == "" {
		epochs = append(epochs, next.epoch)
	}
	pointers := global.LeaderSchedulesForEpochs(epochs)
	var stakes [2]nodeHeldStakeMaterial
	for i, epoch := range epochs {
		stakes[i].snapshot, stakes[i].known = global.EpochStakesSnapshot(epoch)
	}
	var nextSource *leaderschedule.LeaderSchedule
	if len(pointers) == 2 {
		nextSource = pointers[1]
	}
	capture, err := d.heldSchedules.capture(now, context, before.Completed, pointers[0], nextSource, stakes)
	if err != nil {
		return dashboard.HeldScheduleCapture{}, err
	}
	after, ok := d.progress.Snapshot()
	currentPointers := global.LeaderSchedulesForEpochs(epochs)
	_, checkedAfter := after.Completed.CheckedEpochSchedule()
	stable := ok && after.HasCompleted && before.Attempt == after.Attempt && before.Rewinds == after.Rewinds &&
		checkedAfter && before.StartedAt.Equal(after.StartedAt) && before.Completed.Epoch == after.Completed.Epoch && before.Completed.EpochSchedule == after.Completed.EpochSchedule &&
		before.LocalEpochReplay == after.LocalEpochReplay
	stable = stable && before.Completed.EpochTargetTiming == after.Completed.EpochTargetTiming
	for i, pointer := range pointers {
		stable = stable && pointer == currentPointers[i]
		currentStake, known := global.EpochStakesSnapshot(epochs[i])
		stable = stable && known == stakes[i].known && currentStake.Generation == stakes[i].snapshot.Generation
	}
	d.mu.Lock()
	stable = stable && d.state.RunID == context.runID && d.state.StartedAt.Equal(context.nodeStarted) &&
		(d.state.GenesisHash != nil) == context.hasGenesis && (!context.hasGenesis || *d.state.GenesisHash == context.genesis)
	d.mu.Unlock()
	if !stable {
		d.heldSchedules.invalidate()
		return dashboard.HeldScheduleCapture{}, errors.New("binding_changed")
	}
	return capture, nil
}
