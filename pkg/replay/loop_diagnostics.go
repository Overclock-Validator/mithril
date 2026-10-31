package replay

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
)

// Budgets for replay time no phase timer would otherwise show: the loop's own
// work outside execution and source waits (the next-leader scan and the
// fork-switch stake scan lived there), and the part of whole-block execution
// outside every watchdog stage (where the parameter-arena wait lived).
var (
	replayLoopOverhead   = metrics.Background("replay.loop_outside_execution", 25*time.Millisecond)
	processBlockUnstaged = metrics.Background("replay.process_block_unstaged", 10*time.Millisecond)

	// Fold backpressure: replay waiting for folds because the unfolded tail hit
	// its memory or slot cap. Any wait warns. Forced settlement folds
	// synchronously on the loop thread before account-wide epoch scans.
	foldBackpressure      = metrics.Background("replay.fold_backpressure", time.Millisecond)
	forcedFoldSettlement  = metrics.Background("replay.forced_fold_settlement", 500*time.Millisecond)
	unrootedRetainedLevel = metrics.NewLevel("replay.unrooted_retained_bytes")
	unrootedHeldLevel     = metrics.NewLevel("replay.unrooted_held_slots")
)

// recordUnrootedTail records the unfolded tail after a block against its caps.
func recordUnrootedTail(tail *unrootedTail) {
	if tail == nil {
		return
	}
	bytes, slots := tail.overlay.RetainedBytes(), uint64(tail.overlay.HeldSlots())
	metrics.GlobalBlockReplay.UnrootedRetainedBytes = bytes
	metrics.GlobalBlockReplay.UnrootedHeldSlots = slots
	unrootedRetainedLevel.Observe(bytes, tail.maxBytes)
	unrootedHeldLevel.Observe(slots, tail.slotLimit())
}

// recordReplayLoopClosure writes the loop timings for the row about to be
// written and checks the time spent outside execution and source waits.
func recordReplayLoopClosure(rowAt, lastRowAt, iterationStart time.Time, sourceWait time.Duration, slotReplayEnd time.Time, slot uint64) {
	r := &metrics.GlobalBlockReplay
	sinceLastRow := rowAt.Sub(lastRowAt)
	iteration := rowAt.Sub(iterationStart)
	afterReplay := rowAt.Sub(slotReplayEnd)
	slotReplay := time.Duration(r.SlotReplay.SumNanoseconds)
	// Include loop setup before the source call, not only work after admission.
	beforeReplay := iteration - sourceWait - slotReplay - afterReplay
	r.SinceLastRow.AddTiming(sinceLastRow)
	r.LoopIteration.AddTiming(iteration)
	r.LoopSourceWait.AddTiming(sourceWait)
	r.LoopBeforeReplay.AddTiming(beforeReplay)
	r.LoopAfterReplay.AddTiming(afterReplay)
	outside := sinceLastRow - sourceWait - slotReplay
	if outside < 0 {
		outside = 0
	}
	replayLoopOverhead.Observe(outside, func() string {
		return fmt.Sprintf("slot %d: before replay %s, after replay %s, before this iteration %s (this iteration's source call %s and replay %s excluded)",
			slot, beforeReplay.Round(time.Microsecond), afterReplay.Round(time.Microsecond),
			(sinceLastRow - iteration).Round(time.Microsecond), sourceWait.Round(time.Millisecond), slotReplay.Round(time.Millisecond))
	})
}

// recordProcessBlockUnstaged records whole-block ProcessBlock time outside
// every stage and names the largest stages when it is over budget.
func recordProcessBlockUnstaged(processBlock time.Duration, slot uint64) {
	r := &metrics.GlobalBlockReplay
	var staged time.Duration
	for _, t := range r.ReplayStages {
		staged += time.Duration(t.SumNanoseconds)
	}
	unstaged := processBlock - staged
	if unstaged < 0 {
		unstaged = 0
	}
	r.ProcessBlockUnstaged.AddTiming(unstaged)
	processBlockUnstaged.Observe(unstaged, func() string {
		return fmt.Sprintf("slot %d: ProcessBlock %s, stages %s", slot, processBlock.Round(time.Microsecond), formatStages(r.ReplayStages))
	})
}

func formatStages(stages map[string]metrics.Timing) string {
	names := make([]string, 0, len(stages))
	for name := range stages {
		names = append(names, name)
	}
	sort.Slice(names, func(i, j int) bool { return stages[names[i]].SumNanoseconds > stages[names[j]].SumNanoseconds })
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, fmt.Sprintf("%s=%s", name, time.Duration(stages[name].SumNanoseconds).Round(time.Microsecond)))
	}
	return strings.Join(parts, " ")
}
