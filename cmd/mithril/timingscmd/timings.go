// Package timingscmd reads the per-block rows a node writes to
// replay_timings.jsonl and reports on them.
package timingscmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"sort"

	"github.com/spf13/cobra"
)

var (
	topBlocks int

	TimingsCmd = cobra.Command{
		Use:   "timings",
		Short: "Reports on replay_timings.jsonl",
	}

	tipCmd = cobra.Command{
		Use:   "tip TIMINGS.jsonl",
		Short: "Break down live replay latency from each block's last shred to its replay result",
		Long: `Reads the per-block rows of a live run (replay_timings.jsonl) and reports,
for blocks that arrived as shreds, where the time went between the kernel
receiving the shred that completed the block and the replay result reaching
consensus. The intervals are consecutive and add up to FullToReplayed, plus
ReceiveToFull before it. Only WaitParentArrival is set by the network; the
rest is Mithril's own work or queueing. Runtime interference (GC, scheduler
latency, kernel socket drops) covers each row's window.`,
		Args: cobra.ExactArgs(1),
		RunE: runTip,
	}
)

func init() {
	tipCmd.Flags().IntVar(&topBlocks, "top", 10, "How many of the slowest blocks to list")
	TimingsCmd.AddCommand(&tipCmd)
}

type timing struct {
	Count          uint64
	SumNanoseconds uint64
}

func (t timing) ms() float64 { return float64(t.SumNanoseconds) / 1e6 }

// row is the part of a replay_timings.jsonl row the tip report reads.
type row struct {
	Slot                 uint64
	ComputeUnitsConsumed uint64
	ReplayWork           timing
	SlotReplay           timing
	TxLoop               timing
	FullToReplayed       timing
	TipPath              struct {
		ShredReceivedNanos  int64
		ReceiveToFull       timing
		FullToReady         timing
		WaitParentArrival   timing
		WaitParentReplay    timing
		LoopUntilAdmitted   timing
		WaitUnclassified    timing
		AdmittedToReplay    timing
		ReplayToHandoff     timing
		Unattributed        timing
		HandedOffNanos      int64
		LastShredToReplayed timing
		DataShreds          int
		RepairedShreds      int
		RecoveredShreds     int
		TipLagSlots         uint64
		ForkSwitch          bool
	}
	RuntimeInterference struct {
		GCCycles                   uint64
		GCPauseCPU                 timing
		GCAssistCPU                timing
		GCPauseMax                 timing
		SchedLatencyMax            timing
		SchedLatencyOver1ms        uint64
		KernelShredDrops           uint64
		InvoluntaryContextSwitches uint64
		MajorPageFaults            uint64
		PressureCPU                timing
		PressureIO                 timing
		PressureMemory             timing
	}
	VoteEvents []voteEvent

	// vote is this node's notarize vote for this slot, joined from any
	// row's VoteEvents; zero instants when it did not vote.
	vote notarizeVote
}

type voteEvent struct {
	Slot    uint64
	Type    string
	Stage   string
	Peers   int
	AtNanos int64
}

// notarizeVote is the stages one slot's notarize vote reached.
type notarizeVote struct {
	queued, firstSend, fanoutDone int64
	peers                         int
}

func (v *notarizeVote) note(e voteEvent) {
	earliest := func(dst *int64) {
		if *dst == 0 || e.AtNanos < *dst {
			*dst = e.AtNanos
		}
	}
	switch e.Stage {
	case "queued":
		earliest(&v.queued)
	case "first_send":
		earliest(&v.firstSend)
	case "fanout_done":
		earliest(&v.fanoutDone)
		v.peers = e.Peers
	}
}

// lastShredNanos is the block's last shred: its kernel receive time when
// known, otherwise full assembly, as LastShredToReplayed measures from.
func lastShredNanos(r *row) int64 {
	if r.TipPath.HandedOffNanos == 0 || r.TipPath.LastShredToReplayed.Count == 0 {
		return 0
	}
	return r.TipPath.HandedOffNanos - int64(r.TipPath.LastShredToReplayed.SumNanoseconds)
}

// betweenMs is end minus start in milliseconds, or -1 when either is unknown
// or they are out of order.
func betweenMs(start, end int64) float64 {
	if start == 0 || end == 0 || end < start {
		return -1
	}
	return float64(end-start) / 1e6
}

// handoffToVoteMs is the time from the replay result reaching consensus to
// the notarize vote being queued for broadcast, or -1 without a vote.
func handoffToVoteMs(r *row) float64 { return betweenMs(r.TipPath.HandedOffNanos, r.vote.queued) }

// component is one interval of the tip path, in order.
type component struct {
	name    string
	network bool // set by the network rather than by Mithril
	get     func(*row) timing
}

var components = []component{
	{"ReceiveToFull", false, func(r *row) timing { return r.TipPath.ReceiveToFull }},
	{"FullToReady", false, func(r *row) timing { return r.TipPath.FullToReady }},
	{"WaitParentArrival", true, func(r *row) timing { return r.TipPath.WaitParentArrival }},
	{"WaitParentReplay", false, func(r *row) timing { return r.TipPath.WaitParentReplay }},
	{"LoopUntilAdmitted", false, func(r *row) timing { return r.TipPath.LoopUntilAdmitted }},
	{"WaitUnclassified", false, func(r *row) timing { return r.TipPath.WaitUnclassified }},
	{"AdmittedToReplay", false, func(r *row) timing { return r.TipPath.AdmittedToReplay }},
	{"ReplayToHandoff", false, func(r *row) timing { return r.TipPath.ReplayToHandoff }},
	{"Unattributed", false, func(r *row) timing { return r.TipPath.Unattributed }},
}

// controlledMs is the block's time on the vote path that Mithril controls:
// every interval except waiting for the parent's shreds. A block without a
// kernel receive time has no ReceiveToFull, so it starts at full.
func controlledMs(r *row) float64 {
	var sum float64
	for _, c := range components {
		if !c.network {
			sum += c.get(r).ms()
		}
	}
	return sum
}

func readRows(path string) ([]row, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	var withPath []row
	last := map[uint64]int{} // a slot replayed again after a fork switch counts once, by its last row
	notarized := map[uint64]*notarizeVote{}
	total := 0
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var r row
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return nil, 0, fmt.Errorf("%s: %w", path, err)
		}
		total++
		for _, e := range r.VoteEvents {
			if e.Type != "notarize" {
				continue
			}
			v := notarized[e.Slot]
			if v == nil {
				v = &notarizeVote{}
				notarized[e.Slot] = v
			}
			v.note(e)
		}
		if r.FullToReplayed.Count == 0 {
			continue
		}
		if i, ok := last[r.Slot]; ok {
			withPath[i] = r
			continue
		}
		last[r.Slot] = len(withPath)
		withPath = append(withPath, r)
	}
	for i := range withPath {
		if v := notarized[withPath[i].Slot]; v != nil {
			withPath[i].vote = *v
		}
	}
	return withPath, total, sc.Err()
}

func runTip(cmd *cobra.Command, args []string) error {
	cmd.SilenceUsage = true
	rows, total, err := readRows(args[0])
	if err != nil {
		return err
	}
	writeTipReport(cmd.OutOrStdout(), rows, total, topBlocks)
	return nil
}

// quantiles returns the nearest-rank p50, p90, p99 and the max of values.
func quantiles(values []float64) (p50, p90, p99, maxValue float64) {
	sorted := slices.Clone(values)
	slices.Sort(sorted)
	at := func(p float64) float64 {
		idx := int(math.Ceil(p*float64(len(sorted)))) - 1
		return sorted[min(max0(idx), len(sorted)-1)]
	}
	return at(0.50), at(0.90), at(0.99), sorted[len(sorted)-1]
}

func max0(i int) int {
	if i < 0 {
		return 0
	}
	return i
}

func writeTipReport(w io.Writer, rows []row, total, top int) {
	if len(rows) == 0 {
		fmt.Fprintf(w, "no blocks with shred timing among %d rows (FullToReplayed is recorded only for blocks that arrived as shreds)\n", total)
		return
	}
	var cu, fullToReplayedNs, replayWorkNs uint64
	withReceive := 0
	for i := range rows {
		cu += rows[i].ComputeUnitsConsumed
		fullToReplayedNs += rows[i].FullToReplayed.SumNanoseconds
		replayWorkNs += rows[i].ReplayWork.SumNanoseconds
		if rows[i].TipPath.ShredReceivedNanos != 0 {
			withReceive++
		}
	}
	n := float64(len(rows))
	fmt.Fprintf(w, "Tip replay: %d blocks with shred timing of %d rows, slots %d..%d\n", len(rows), total, rows[0].Slot, rows[len(rows)-1].Slot)
	fmt.Fprintf(w, "  avg consumed CU/block %.2f M; ReplayWork mean %.2f ms (%.3f CU/ns); kernel receive time known for %d blocks\n",
		float64(cu)/n/1e6, float64(replayWorkNs)/n/1e6, ratio(cu, replayWorkNs), withReceive)
	fmt.Fprintf(w, "  per block, ms; share is of total FullToReplayed (ReceiveToFull lies before it)\n")
	fmt.Fprintf(w, "  %-20s %-8s %7s %8s %8s %8s %8s %8s\n", "interval", "owner", "share", "mean", "p50", "p90", "p99", "max")
	values := make([]float64, len(rows))
	line := func(name, owner string, share float64, get func(*row) float64) {
		var sum float64
		for i := range rows {
			values[i] = get(&rows[i])
			sum += values[i]
		}
		p50, p90, p99, mx := quantiles(values)
		shareText := "-"
		if !math.IsNaN(share) {
			shareText = fmt.Sprintf("%.1f%%", share*100)
		}
		fmt.Fprintf(w, "  %-20s %-8s %7s %8.2f %8.2f %8.2f %8.2f %8.2f\n", name, owner, shareText, sum/n, p50, p90, p99, mx)
	}
	for _, c := range components {
		owner := "mithril"
		if c.network {
			owner = "network"
		}
		share := math.NaN()
		if c.name != "ReceiveToFull" && fullToReplayedNs > 0 {
			var ns uint64
			for i := range rows {
				ns += c.get(&rows[i]).SumNanoseconds
			}
			share = float64(ns) / float64(fullToReplayedNs)
		}
		line(c.name, owner, share, func(r *row) float64 { return c.get(r).ms() })
	}
	line("FullToReplayed", "total", 1, func(r *row) float64 { return r.FullToReplayed.ms() })
	line("controlled", "mithril", math.NaN(), controlledMs)
	line("ReplayWork", "mithril", math.NaN(), func(r *row) float64 { return r.ReplayWork.ms() })

	// From the last shred to replay done, and in validator mode on to the
	// notarize vote leaving: each over the blocks where both ends are known.
	partial := func(name, owner string, get func(*row) float64) {
		var vals []float64
		for i := range rows {
			if ms := get(&rows[i]); ms >= 0 {
				vals = append(vals, ms)
			}
		}
		if len(vals) == 0 {
			return
		}
		var sum float64
		for _, v := range vals {
			sum += v
		}
		p50, p90, p99, mx := quantiles(vals)
		fmt.Fprintf(w, "  %-20s %-8s %7s %8.2f %8.2f %8.2f %8.2f %8.2f  (%d blocks)\n", name, owner, "-", sum/float64(len(vals)), p50, p90, p99, mx, len(vals))
	}
	fmt.Fprintf(w, "  last shred to replay done and to the vote (last shred: kernel receive time when known, else full assembly)\n")
	partial("LastShredToReplayed", "mithril", func(r *row) float64 {
		if r.TipPath.LastShredToReplayed.Count == 0 {
			return -1
		}
		return r.TipPath.LastShredToReplayed.ms()
	})
	partial("HandoffToVote", "mithril", handoffToVoteMs)
	partial("VoteQueuedToSend", "mithril", func(r *row) float64 { return betweenMs(r.vote.queued, r.vote.firstSend) })
	partial("LastShredToVoteSent", "mithril", func(r *row) float64 { return betweenMs(lastShredNanos(r), r.vote.firstSend) })
	partial("LastShredToFanout", "mithril", func(r *row) float64 { return betweenMs(lastShredNanos(r), r.vote.fanoutDone) })
	voted, noPeers := 0, 0
	for i := range rows {
		if rows[i].vote.queued != 0 {
			voted++
		}
		if rows[i].vote.fanoutDone != 0 && rows[i].vote.peers == 0 {
			noPeers++
		}
	}
	if voted == 0 {
		fmt.Fprintf(w, "  no notarize votes in these rows (not voting, or the votes were recorded outside them)\n")
	} else {
		fmt.Fprintf(w, "  %d of %d blocks voted; %d votes reached no connected peer\n", voted, len(rows), noPeers)
	}

	var dataShreds, repaired, recovered, forkSwitches int
	repairedBlocks, recoveredBlocks := 0, 0
	lags := make([]float64, 0, len(rows))
	for i := range rows {
		tp := &rows[i].TipPath
		dataShreds += tp.DataShreds
		repaired += tp.RepairedShreds
		recovered += tp.RecoveredShreds
		if tp.RepairedShreds > 0 {
			repairedBlocks++
		}
		if tp.RecoveredShreds > 0 {
			recoveredBlocks++
		}
		if tp.ForkSwitch {
			forkSwitches++
		}
		lags = append(lags, float64(tp.TipLagSlots))
	}
	fmt.Fprintf(w, "  shreds: %d data shreds; repair delivered %d in %d blocks, recovery rebuilt %d in %d blocks; %d fork switches\n",
		dataShreds, repaired, repairedBlocks, recovered, recoveredBlocks, forkSwitches)
	if p50, _, p99, mx := quantiles(lags); mx > 0 {
		fmt.Fprintf(w, "  tip lag at hand-off (slots behind the newest data shred): p50 %.0f, p99 %.0f, max %.0f\n", p50, p99, mx)
	}

	var cycles, drops, over1ms uint64
	gcBlocks := 0
	for i := range rows {
		ri := &rows[i].RuntimeInterference
		cycles += ri.GCCycles
		drops += ri.KernelShredDrops
		over1ms += ri.SchedLatencyOver1ms
		if ri.GCPauseMax.Count > 0 {
			gcBlocks++
		}
	}
	var involuntary, majorFaults uint64
	for i := range rows {
		involuntary += rows[i].RuntimeInterference.InvoluntaryContextSwitches
		majorFaults += rows[i].RuntimeInterference.MajorPageFaults
	}
	fmt.Fprintf(w, "  runtime: %d GC cycles, a GC pause in %d of %d row windows, %d scheduler waits over 1 ms, %d kernel shred drops, %d involuntary context switches, %d major page faults\n",
		cycles, gcBlocks, len(rows), over1ms, drops, involuntary, majorFaults)
	line("GCPauseMax (bound)", "mithril", math.NaN(), func(r *row) float64 { return r.RuntimeInterference.GCPauseMax.ms() })
	line("SchedLatencyMax", "mithril", math.NaN(), func(r *row) float64 { return r.RuntimeInterference.SchedLatencyMax.ms() })
	line("PressureCPU (host)", "host", math.NaN(), func(r *row) float64 { return r.RuntimeInterference.PressureCPU.ms() })
	line("PressureIO (host)", "host", math.NaN(), func(r *row) float64 { return r.RuntimeInterference.PressureIO.ms() })
	line("PressureMemory (host)", "host", math.NaN(), func(r *row) float64 { return r.RuntimeInterference.PressureMemory.ms() })

	if top <= 0 {
		return
	}
	order := make([]int, len(rows))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return controlledMs(&rows[order[a]]) > controlledMs(&rows[order[b]]) })
	fmt.Fprintf(w, "  slowest %d blocks by controlled time:\n", min(top, len(rows)))
	fmt.Fprintf(w, "  %-12s %8s %10s %10s %10s %8s  %s\n", "slot", "M CU", "controlled", "full->done", "ReplayWork", "CU/ns", "largest intervals; context")
	for _, i := range order[:min(top, len(rows))] {
		r := &rows[i]
		fmt.Fprintf(w, "  %-12d %8.2f %10.2f %10.2f %10.2f %8.3f  %s%s\n", r.Slot, float64(r.ComputeUnitsConsumed)/1e6,
			controlledMs(r), r.FullToReplayed.ms(), r.ReplayWork.ms(), ratio(r.ComputeUnitsConsumed, r.ReplayWork.SumNanoseconds), largest(r, 3), context(r))
	}
}

// context lists what else was unusual about a block.
func context(r *row) string {
	out := ""
	add := func(format string, args ...any) { out += "; " + fmt.Sprintf(format, args...) }
	if r.TipPath.RepairedShreds > 0 {
		add("%d repaired shreds", r.TipPath.RepairedShreds)
	}
	if r.TipPath.RecoveredShreds > 0 {
		add("%d recovered shreds", r.TipPath.RecoveredShreds)
	}
	if r.TipPath.ForkSwitch {
		add("fork switch")
	}
	if r.RuntimeInterference.GCCycles > 0 {
		add("%d GC cycles", r.RuntimeInterference.GCCycles)
	}
	if r.RuntimeInterference.MajorPageFaults > 0 {
		add("%d major faults", r.RuntimeInterference.MajorPageFaults)
	}
	if r.RuntimeInterference.KernelShredDrops > 0 {
		add("%d kernel drops", r.RuntimeInterference.KernelShredDrops)
	}
	return out
}

func ratio(cu, ns uint64) float64 {
	if ns == 0 {
		return 0
	}
	return float64(cu) / float64(ns)
}

// largest names a row's k biggest intervals with their milliseconds.
func largest(r *row, k int) string {
	type named struct {
		name string
		ms   float64
	}
	var all []named
	for _, c := range components {
		if ms := c.get(r).ms(); ms > 0 {
			all = append(all, named{c.name, ms})
		}
	}
	if gc := r.RuntimeInterference.GCPauseMax.ms(); gc > 0 {
		all = append(all, named{"GCPauseMax", gc})
	}
	sort.SliceStable(all, func(a, b int) bool { return all[a].ms > all[b].ms })
	out := ""
	for i, it := range all[:min(k, len(all))] {
		if i > 0 {
			out += ", "
		}
		out += fmt.Sprintf("%s %.2f", it.name, it.ms)
	}
	return out
}
