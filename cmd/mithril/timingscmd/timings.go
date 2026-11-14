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
		ShredReceivedNanos int64
		ReceiveToFull      timing
		FullToReady        timing
		WaitParentArrival  timing
		WaitParentReplay   timing
		LoopUntilAdmitted  timing
		WaitUnclassified   timing
		AdmittedToReplay   timing
		ReplayToHandoff    timing
		Unattributed       timing
	}
	RuntimeInterference struct {
		GCCycles            uint64
		GCPauseCPU          timing
		GCAssistCPU         timing
		GCPauseMax          timing
		SchedLatencyMax     timing
		SchedLatencyOver1ms uint64
		KernelShredDrops    uint64
	}
}

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
	fmt.Fprintf(w, "  runtime: %d GC cycles, a GC pause in %d of %d row windows, %d scheduler waits over 1 ms, %d kernel shred drops\n",
		cycles, gcBlocks, len(rows), over1ms, drops)
	line("GCPauseMax (bound)", "mithril", math.NaN(), func(r *row) float64 { return r.RuntimeInterference.GCPauseMax.ms() })
	line("SchedLatencyMax", "mithril", math.NaN(), func(r *row) float64 { return r.RuntimeInterference.SchedLatencyMax.ms() })

	if top <= 0 {
		return
	}
	order := make([]int, len(rows))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return controlledMs(&rows[order[a]]) > controlledMs(&rows[order[b]]) })
	fmt.Fprintf(w, "  slowest %d blocks by controlled time:\n", min(top, len(rows)))
	fmt.Fprintf(w, "  %-12s %8s %10s %10s %10s  %s\n", "slot", "M CU", "controlled", "full->done", "ReplayWork", "largest intervals")
	for _, i := range order[:min(top, len(rows))] {
		r := &rows[i]
		fmt.Fprintf(w, "  %-12d %8.2f %10.2f %10.2f %10.2f  %s\n", r.Slot, float64(r.ComputeUnitsConsumed)/1e6,
			controlledMs(r), r.FullToReplayed.ms(), r.ReplayWork.ms(), largest(r, 3))
	}
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
