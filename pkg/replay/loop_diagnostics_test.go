package replay

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var replayWarningCaptureID atomic.Uint64

func captureReplayWarnings(t *testing.T) *[]string {
	t.Helper()
	var got []string
	previous := metrics.DiagnosticsWarn
	previousLoop, previousUnstaged := replayLoopOverhead, processBlockUnstaged
	name := fmt.Sprintf("test.%s.%d", t.Name(), replayWarningCaptureID.Add(1))
	// Production limiters intentionally persist; each test needs a fresh window,
	// including repeated -count runs within the same process.
	replayLoopOverhead = metrics.Background(name+".loop", 25*time.Millisecond)
	processBlockUnstaged = metrics.Background(name+".unstaged", 10*time.Millisecond)
	metrics.DiagnosticsWarn = func(format string, args ...any) { got = append(got, fmt.Sprintf(format, args...)) }
	t.Cleanup(func() {
		metrics.DiagnosticsWarn = previous
		replayLoopOverhead, processBlockUnstaged = previousLoop, previousUnstaged
	})
	return &got
}

// Whole-block execution records non-overlapping stages, including the
// parameter-arena barrier, whose sum fits inside ProcessBlock; streamed
// execution records its stages too.
func TestReplayStagesTileBlockExecution(t *testing.T) {
	txs := transferTransactions(t, 12, 999_000)
	for _, parallelism := range []int{0, 4} {
		metrics.GlobalBlockReplay = metrics.BlockReplay{}
		started := time.Now()
		lifecycleWholeBlock(t, newLifecycleEnv(t), txs, parallelism)
		wall := time.Since(started)
		stages := metrics.GlobalBlockReplay.ReplayStages
		for _, name := range []string{"parameter_arena", "prepare_dependency_planner", "load_accounts", "tx_loop", "bankhash", "done"} {
			_, ok := stages[name]
			assert.True(t, ok, "parallelism %d: stage %s recorded", parallelism, name)
		}
		var staged time.Duration
		for name, timing := range stages {
			assert.Equal(t, uint64(1), timing.Count, "stage %s recorded once", name)
			staged += time.Duration(timing.SumNanoseconds)
		}
		assert.LessOrEqual(t, staged, wall, "stages do not overlap")
	}

	StreamingExecutionCfg = StreamingExecutionConfig{Enabled: true}
	defer func() { StreamingExecutionCfg = StreamingExecutionConfig{} }()
	metrics.GlobalBlockReplay = metrics.BlockReplay{}
	lifecycleStream(t, newLifecycleEnv(t), txs, []int{4}, 3, 4)
	_, ok := metrics.GlobalBlockReplay.ReplayStages["tx_loop"]
	assert.True(t, ok, "a streamed bank records its stages at finalize")
	metrics.GlobalBlockReplay = metrics.BlockReplay{}
}

// The loop closure tiles the time since the previous row and flags time
// spent outside execution and source waits.
func TestReplayLoopClosure(t *testing.T) {
	warnings := captureReplayWarnings(t)
	metrics.GlobalBlockReplay = metrics.BlockReplay{}
	base := time.Unix(1000, 0)
	lastRow := base
	iterationStart := base.Add(40 * time.Millisecond) // 40 ms before this iteration
	setup := 3 * time.Millisecond                     // loop setup before asking the source
	sourceWait := 5 * time.Millisecond
	beforeReplay := 2 * time.Millisecond
	slotReplay := 20 * time.Millisecond
	metrics.GlobalBlockReplay.SlotReplay.AddTiming(slotReplay)
	slotReplayEnd := iterationStart.Add(setup + sourceWait + beforeReplay + slotReplay)
	rowAt := slotReplayEnd.Add(3 * time.Millisecond)

	recordReplayLoopClosure(rowAt, lastRow, iterationStart, sourceWait, slotReplayEnd, 42)
	r := metrics.GlobalBlockReplay
	assert.Equal(t, uint64(73*time.Millisecond), r.SinceLastRow.SumNanoseconds)
	assert.Equal(t, uint64(33*time.Millisecond), r.LoopIteration.SumNanoseconds)
	assert.Equal(t, uint64(setup+beforeReplay), r.LoopBeforeReplay.SumNanoseconds)
	assert.Equal(t, uint64(3*time.Millisecond), r.LoopAfterReplay.SumNanoseconds)
	assert.Equal(t, r.LoopIteration.SumNanoseconds,
		r.LoopSourceWait.SumNanoseconds+r.LoopBeforeReplay.SumNanoseconds+r.SlotReplay.SumNanoseconds+r.LoopAfterReplay.SumNanoseconds,
		"the iteration tiles exactly")
	require.Len(t, *warnings, 1, "48 ms outside execution and source wait exceeds the 25 ms budget")
	assert.Contains(t, (*warnings)[0], "slot 42")
	assert.Contains(t, (*warnings)[0], "before this iteration 40ms")
	metrics.GlobalBlockReplay = metrics.BlockReplay{}
}

// Unstaged ProcessBlock time is ProcessBlock minus every stage.
func TestProcessBlockUnstaged(t *testing.T) {
	warnings := captureReplayWarnings(t)
	metrics.GlobalBlockReplay = metrics.BlockReplay{ReplayStages: map[string]metrics.Timing{
		"load_accounts": {Count: 1, SumNanoseconds: uint64(4 * time.Millisecond)},
		"tx_loop":       {Count: 1, SumNanoseconds: uint64(10 * time.Millisecond)},
	}}
	recordProcessBlockUnstaged(15*time.Millisecond, 7)
	assert.Equal(t, uint64(time.Millisecond), metrics.GlobalBlockReplay.ProcessBlockUnstaged.SumNanoseconds)
	assert.Empty(t, *warnings)
	recordProcessBlockUnstaged(40*time.Millisecond, 8)
	require.Len(t, *warnings, 1)
	assert.Contains(t, (*warnings)[0], "tx_loop=10ms load_accounts=4ms")
	metrics.GlobalBlockReplay = metrics.BlockReplay{}
}

// The fold worker's busy time covers finished jobs and the running one, so a
// block's overlap with folding is the difference across its replay.
func TestPromoterBusyThrough(t *testing.T) {
	var nilPromoter *asyncPromoter
	assert.Zero(t, nilPromoter.busyThrough(time.Now()))
	p := &asyncPromoter{}
	p.setBusy(true)
	time.Sleep(20 * time.Millisecond)
	mid := p.busyThrough(time.Now())
	assert.GreaterOrEqual(t, mid, 20*time.Millisecond, "a running job counts")
	p.setBusy(false)
	done := p.busyThrough(time.Now())
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, done, p.busyThrough(time.Now()), "idle time does not count")
	assert.GreaterOrEqual(t, done, mid)
}
