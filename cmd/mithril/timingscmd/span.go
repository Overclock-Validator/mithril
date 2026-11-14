package timingscmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
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
	Slot                 uint64
	ComputeUnitsConsumed uint64
	CompletedUnixNano    int64
	SlotReplay           timing
	ReplayWork           timing
	LoopSourceWait       timing
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
	for _, r := range measured {
		if r.CompletedUnixNano == 0 || prev == 0 || r.CompletedUnixNano < prev {
			return fmt.Errorf("slot %d has no usable completion time; rows written before CompletedUnixNano existed cannot be measured", r.Slot)
		}
		intervals = append(intervals, float64(r.CompletedUnixNano-prev)/1e6)
		prev = r.CompletedUnixNano
		cu += r.ComputeUnitsConsumed
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
	fmt.Fprintf(w, "  waiting for the next block (LoopSourceWait): %.1f%% of the span\n", 100*float64(sourceWaitNs)/float64(spanNs))
	p50, p90, p99, mx := quantiles(intervals)
	fmt.Fprintf(w, "  %-14s %8s %8s %8s %8s %8s\n", "interval", "mean ms", "p50", "p90", "p99", "max")
	fmt.Fprintf(w, "  %-14s %8.3f %8.3f %8.3f %8.3f %8.3f\n", "BlockInterval", seconds*1e3/float64(n), p50, p90, p99, mx)
	fmt.Fprintf(w, "  BlockInterval is completion to completion; the intervals add up to the span.\n")
	return nil
}
