package timingscmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"

	"github.com/spf13/cobra"
)

var (
	spanFrom, spanTo uint64
	spanWarmup       int

	spanCmd = cobra.Command{
		Use:   "span TIMINGS.jsonl",
		Short: "Blocks replayed per second over a stretch of a run, with per-block times that add up to it",
		Long: `Reads a run's replay_timings.jsonl (backtest, offline, catch-up or live) and
reports, over the chosen blocks, how many were replayed in how much wall time,
and each block's completion-to-completion interval. The intervals add up to the
span, so their mean is exactly the span's time per block. LoopSourceWait shows
how much of the span replay spent waiting for the next block (the network,
repair or the spool) rather than replaying.`,
		Args: cobra.ExactArgs(1),
		RunE: runSpan,
	}
)

func init() {
	spanCmd.Flags().Uint64Var(&spanFrom, "from-slot", 0, "First slot of the span (0 = the first row)")
	spanCmd.Flags().Uint64Var(&spanTo, "to-slot", 0, "Last slot of the span (0 = the last row)")
	spanCmd.Flags().IntVar(&spanWarmup, "warmup", 0, "Leading blocks of the span left out (their time still opens the span)")
	TimingsCmd.AddCommand(&spanCmd)
}

// spanRow is the part of a timing row the span report reads.
type spanRow struct {
	Slot                     uint64
	ComputeUnitsConsumed     uint64
	LongestChainCU           uint64
	LongestChainTransactions uint32
	CompletedUnixNano        int64
	SlotReplay               timing
	ReplayWork               timing
	LoopSourceWait           timing
}

// chainTotals sums the longest dependency chain over the blocks that recorded
// one. Rows written before the field existed, and blocks replay did not
// execute itself, have none (LongestChainTransactions is zero) and are left
// out rather than counted as zero.
type chainTotals struct {
	blocks       int
	chainCU      uint64
	transactions uint64
	consumedCU   uint64 // of the same blocks
}

func (c *chainTotals) add(consumedCU, chainCU uint64, transactions uint32) {
	if transactions == 0 {
		return
	}
	c.blocks++
	c.chainCU += chainCU
	c.transactions += uint64(transactions)
	c.consumedCU += consumedCU
}

// avgM is the average chain in millions of CU, "-" when no block recorded one.
func (c chainTotals) avgM() string {
	if c.blocks == 0 {
		return "-"
	}
	return fmt.Sprintf("%.2f", float64(c.chainCU)/float64(c.blocks)/1e6)
}

// writeSummary prints the chain line that goes under the consumed CU line,
// and nothing when no block recorded a chain. The share is total chain CU over
// the total consumed CU of the same blocks.
func (c chainTotals) writeSummary(w io.Writer, of int) {
	if c.blocks == 0 {
		return
	}
	fmt.Fprintf(w, "  avg longest chain CU/block %s M (%.1f%% of consumed CU), %.1f transactions; recorded for %d of %d blocks\n",
		c.avgM(), 100*float64(c.chainCU)/float64(max(c.consumedCU, 1)), float64(c.transactions)/float64(c.blocks), c.blocks, of)
}

func readSpanRows(path string) ([]spanRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	var rows []spanRow
	last := map[uint64]int{}
	for sc.Scan() {
		if len(sc.Bytes()) == 0 {
			continue
		}
		var r spanRow
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		// A slot replayed again after a fork switch counts once, at its last
		// completion; the discarded replay's time stays inside the span.
		if i, ok := last[r.Slot]; ok {
			rows = append(rows[:i], rows[i+1:]...)
			for s, j := range last {
				if j > i {
					last[s] = j - 1
				}
			}
		}
		last[r.Slot] = len(rows)
		rows = append(rows, r)
	}
	return rows, sc.Err()
}

func runSpan(cmd *cobra.Command, args []string) error {
	cmd.SilenceUsage = true
	rows, err := readSpanRows(args[0])
	if err != nil {
		return err
	}
	return writeSpanReport(cmd.OutOrStdout(), rows, spanFrom, spanTo, spanWarmup)
}

// writeSpanReport measures the blocks in [from, to] after warmup. The span
// opens at the completion of the block before the first measured one, so
// every measured block has a full interval; the first row of the file can
// therefore never be measured.
func writeSpanReport(w io.Writer, rows []spanRow, from, to uint64, warmup int) error {
	first := -1
	for i, r := range rows {
		if r.Slot >= from {
			first = i
			break
		}
	}
	if first < 0 {
		return fmt.Errorf("no rows at or after slot %d", from)
	}
	if first == 0 {
		first = 1 // the file's first row has no previous completion to open its interval
	}
	first += warmup
	end := len(rows)
	if to != 0 {
		for end > 0 && rows[end-1].Slot > to {
			end--
		}
	}
	if first >= end {
		return fmt.Errorf("no blocks to measure between slots %d and %d after %d warm-up blocks", from, to, warmup)
	}
	measured := rows[first:end]
	open := rows[first-1]
	intervals := make([]float64, 0, len(measured))
	prev := open.CompletedUnixNano
	var cu, replayWorkNs, sourceWaitNs uint64
	var chain chainTotals
	for _, r := range measured {
		if r.CompletedUnixNano == 0 || prev == 0 || r.CompletedUnixNano < prev {
			return fmt.Errorf("slot %d has no usable completion time; rows written before CompletedUnixNano existed cannot be measured", r.Slot)
		}
		intervals = append(intervals, float64(r.CompletedUnixNano-prev)/1e6)
		prev = r.CompletedUnixNano
		cu += r.ComputeUnitsConsumed
		chain.add(r.ComputeUnitsConsumed, r.LongestChainCU, r.LongestChainTransactions)
		replayWorkNs += r.ReplayWork.SumNanoseconds
		sourceWaitNs += r.LoopSourceWait.SumNanoseconds
	}
	spanNs := measured[len(measured)-1].CompletedUnixNano - open.CompletedUnixNano
	seconds := float64(spanNs) / 1e9
	n := len(measured)
	slots := measured[n-1].Slot - open.Slot
	fmt.Fprintf(w, "span: slots %d..%d (opened by slot %d's completion)\n", measured[0].Slot, measured[n-1].Slot, open.Slot)
	fmt.Fprintf(w, "  %d blocks in %.3f s: %.2f blocks/s, %.3f ms/block; %d slots advanced: %.2f slots/s\n",
		n, seconds, float64(n)/seconds, seconds*1e3/float64(n), slots, float64(slots)/seconds)
	fmt.Fprintf(w, "  avg consumed CU/block %.2f M; CU/ns over the span %.3f, over ReplayWork %.3f\n",
		float64(cu)/float64(n)/1e6, ratio(cu, uint64(spanNs)), ratio(cu, replayWorkNs))
	chain.writeSummary(w, n)
	fmt.Fprintf(w, "  waiting for the next block (LoopSourceWait): %.1f%% of the span\n", 100*float64(sourceWaitNs)/float64(spanNs))
	p50, p90, p99, mx := quantiles(intervals)
	fmt.Fprintf(w, "  %-14s %8s %8s %8s %8s %8s\n", "interval", "mean ms", "p50", "p90", "p99", "max")
	fmt.Fprintf(w, "  %-14s %8.3f %8.3f %8.3f %8.3f %8.3f\n", "BlockInterval", seconds*1e3/float64(n), p50, p90, p99, mx)
	fmt.Fprintf(w, "  BlockInterval is completion to completion; the intervals add up to the span.\n")
	writeSizeBuckets(w, intervals, measured)
	return nil
}

// sizeBucketsM are the lower bounds, in millions of consumed CU, of the block
// size groups; the last group is open-ended.
var sizeBucketsM = []uint64{0, 10, 20, 30, 40}

// writeSizeBuckets groups the measured blocks by consumed CU and reports each
// group's BlockInterval; intervals[i] is blocks[i]'s. CU/ns is the group's
// total CU over its total interval time. chain M CU is the group's average
// longest dependency chain, over the blocks that recorded one.
func writeSizeBuckets(w io.Writer, intervals []float64, blocks []spanRow) {
	fmt.Fprintf(w, "  by block size (BlockInterval per group):\n")
	fmt.Fprintf(w, "  %-12s %7s %10s %8s %8s %8s %8s %8s %11s\n", "M CU", "blocks", "avg M CU", "mean ms", "p50", "p99", "max", "CU/ns", "chain M CU")
	for i, lo := range sizeBucketsM {
		hi := uint64(math.MaxUint64)
		label := fmt.Sprintf("%d+", lo)
		if i+1 < len(sizeBucketsM) {
			hi = sizeBucketsM[i+1]
			label = fmt.Sprintf("%d-%d", lo, hi)
		}
		var vals []float64
		var cu uint64
		var ms float64
		var chain chainTotals
		for j := range blocks {
			c := blocks[j].ComputeUnitsConsumed
			if c >= lo*1_000_000 && (hi == math.MaxUint64 || c < hi*1_000_000) {
				vals = append(vals, intervals[j])
				cu += c
				ms += intervals[j]
				chain.add(c, blocks[j].LongestChainCU, blocks[j].LongestChainTransactions)
			}
		}
		if len(vals) == 0 {
			fmt.Fprintf(w, "  %-12s %7d\n", label, 0)
			continue
		}
		p50, _, p99, mx := quantiles(vals)
		n := float64(len(vals))
		fmt.Fprintf(w, "  %-12s %7d %10.2f %8.3f %8.3f %8.3f %8.3f %8.3f %11s\n", label, len(vals), float64(cu)/n/1e6, ms/n, p50, p99, mx, float64(cu)/(ms*1e6), chain.avgM())
	}
}
