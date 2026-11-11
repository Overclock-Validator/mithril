package node

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"slices"
	"time"
)

// timingRow is the part of one replay_timings.jsonl row a backtest summary
// reads. Intervals are total nanoseconds for the block.
type timingRow struct {
	Slot         uint64
	CU           uint64
	Transactions uint64
	SlotReplay   int64
	TxLoop       int64
	SinceLastRow int64
}

func readTimingRows(path string) ([]timingRow, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	type timing struct {
		SumNanoseconds int64
	}
	var rows []timingRow
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for sc.Scan() {
		var r struct {
			Slot                 uint64
			ComputeUnitsConsumed uint64
			Transactions         uint64
			SlotReplay           timing
			TxLoop               timing
			SinceLastRow         timing
		}
		if len(sc.Bytes()) == 0 {
			continue
		}
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		rows = append(rows, timingRow{Slot: r.Slot, CU: r.ComputeUnitsConsumed, Transactions: r.Transactions,
			SlotReplay: r.SlotReplay.SumNanoseconds, TxLoop: r.TxLoop.SumNanoseconds, SinceLastRow: r.SinceLastRow.SumNanoseconds})
	}
	return rows, sc.Err()
}

// intervalStats summarizes one timing interval over the measured blocks.
// CUPerNs is total consumed CU over total nanoseconds, not a mean of ratios.
type intervalStats struct {
	MeanMs, P50Ms, P90Ms, P99Ms float64
	CUPerNs                     float64
}

type backtestSummary struct {
	Replayed   int // blocks replayed this run
	Warmup     int // leading blocks excluded from the measured set
	Measured   int
	FirstSlot  uint64
	LastSlot   uint64
	AvgCU      float64 // consumed CU per measured block
	SlotReplay intervalStats
	TxLoop     intervalStats

	// Firedancer-comparable quantities over every replayed block (Firedancer's
	// backtest does not exclude warm-up). SlotExecSeconds runs from the first
	// block's start to the last block's completion: the first block's
	// SlotReplay plus the block-to-block time of every later block, the span
	// Firedancer reports as slot_exec_s.
	SlotExecSeconds float64
	SecPerSlot      float64
	TPS             float64
}

func summarizeBacktest(rows []timingRow, warmup int, measureFrom uint64) backtestSummary {
	s := backtestSummary{Replayed: len(rows)}
	if len(rows) > 0 {
		replay := time.Duration(rows[0].SlotReplay)
		var txns uint64
		for i, r := range rows {
			if i > 0 {
				replay += time.Duration(r.SinceLastRow)
			}
			txns += r.Transactions
		}
		s.SlotExecSeconds = replay.Seconds()
		if s.SlotExecSeconds > 0 {
			s.SecPerSlot = s.SlotExecSeconds / float64(len(rows))
			s.TPS = float64(txns) / s.SlotExecSeconds
		}
	}
	measured := rows
	for len(measured) > 0 && measured[0].Slot < measureFrom {
		measured = measured[1:]
	}
	s.Warmup = min(warmup, len(measured))
	measured = measured[s.Warmup:]
	s.Measured = len(measured)
	if len(measured) == 0 {
		return s
	}
	s.FirstSlot, s.LastSlot = measured[0].Slot, measured[len(measured)-1].Slot
	var cu uint64
	slotReplay := make([]int64, len(measured))
	txLoop := make([]int64, len(measured))
	for i, r := range measured {
		cu += r.CU
		slotReplay[i], txLoop[i] = r.SlotReplay, r.TxLoop
	}
	s.AvgCU = float64(cu) / float64(len(measured))
	s.SlotReplay = statsOf(slotReplay, cu)
	s.TxLoop = statsOf(txLoop, cu)
	return s
}

func statsOf(ns []int64, totalCU uint64) intervalStats {
	sorted := slices.Clone(ns)
	slices.Sort(sorted)
	var sum int64
	for _, v := range sorted {
		sum += v
	}
	pct := func(p float64) float64 {
		// Nearest rank.
		idx := int(math.Ceil(p*float64(len(sorted)))) - 1
		return float64(sorted[max(idx, 0)]) / 1e6
	}
	st := intervalStats{MeanMs: float64(sum) / float64(len(sorted)) / 1e6, P50Ms: pct(0.50), P90Ms: pct(0.90), P99Ms: pct(0.99)}
	if sum > 0 {
		st.CUPerNs = float64(totalCU) / float64(sum)
	}
	return st
}

// write prints the summary. settings lists what affects speed, in order.
func (s backtestSummary) write(w io.Writer, settings [][2]string, totalSeconds float64) {
	fmt.Fprintf(w, "\nBacktest summary\n")
	for _, kv := range settings {
		fmt.Fprintf(w, "  %-22s %s\n", kv[0], kv[1])
	}
	fmt.Fprintf(w, "  %-22s %d replayed, %d warm-up excluded, %d measured (slots %d..%d)\n", "blocks", s.Replayed, s.Warmup, s.Measured, s.FirstSlot, s.LastSlot)
	if s.Measured == 0 {
		fmt.Fprintf(w, "  no measured blocks\n")
		return
	}
	fmt.Fprintf(w, "  %-22s %.2f M\n", "avg consumed CU/block", s.AvgCU/1e6)
	fmt.Fprintf(w, "  %-12s %9s %9s %9s %9s %8s\n", "interval", "mean ms", "p50 ms", "p90 ms", "p99 ms", "CU/ns")
	for _, iv := range []struct {
		name string
		st   intervalStats
	}{{"SlotReplay", s.SlotReplay}, {"TxLoop", s.TxLoop}} {
		fmt.Fprintf(w, "  %-12s %9.2f %9.2f %9.2f %9.2f %8.3f\n", iv.name, iv.st.MeanMs, iv.st.P50Ms, iv.st.P90Ms, iv.st.P99Ms, iv.st.CUPerNs)
	}
	fmt.Fprintf(w, "FD_COMPARABLE slots=%d slot_exec_s=%.3f sec_per_slot=%.6f tps=%.0f outside_s=%.3f\n",
		s.Replayed, s.SlotExecSeconds, s.SecPerSlot, s.TPS, max(totalSeconds-s.SlotExecSeconds, 0))
}
