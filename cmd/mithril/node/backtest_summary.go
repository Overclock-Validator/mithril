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
	Completed    int64 // wall clock when SlotReplay ended, Unix nanoseconds
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
			CompletedUnixNano    int64
		}
		if len(sc.Bytes()) == 0 {
			continue
		}
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		rows = append(rows, timingRow{Slot: r.Slot, CU: r.ComputeUnitsConsumed, Transactions: r.Transactions,
			SlotReplay: r.SlotReplay.SumNanoseconds, TxLoop: r.TxLoop.SumNanoseconds, SinceLastRow: r.SinceLastRow.SumNanoseconds,
			Completed: r.CompletedUnixNano})
	}
	return rows, sc.Err()
}

// intervalStats summarizes one timing interval over the measured blocks.
// CUPerNs is total consumed CU over total nanoseconds, not a mean of ratios.
type intervalStats struct {
	MeanMs, P50Ms, P90Ms, P99Ms float64
	CUPerNs                     float64
}

// backtestClock holds the wall-clock moments a run passes through. Boot is
// process start; ImportDone ends the capture import (zero work when an
// earlier import is reused); StateReady is the AccountsDB built or opened and
// rewound (Firedancer's snapshot_done_time); ReplayReady is the replay loop
// about to fetch its first block (Firedancer's replay_ready_time).
type backtestClock struct {
	Boot, ImportDone, StateReady, ReplayReady time.Time
}

// firedancerSummary is Firedancer's BACKTEST_SUMMARY line, each field measured
// over the span fd_backtest_tile.c measures it over, covering every replayed
// block with no warm-up excluded. SnapBytesS keeps Firedancer's name although
// it is a duration (boot or here import end, to state ready).
type firedancerSummary struct {
	SnapshotSlot, EndSlot      uint64
	Slots                      int
	Txns                       uint64
	SnapBytesS, ReplayInitS    float64
	LoadTotalS, ReplayS        float64
	SlotExecS, SecPerSlot, TPS float64
	TotalS                     float64
	ImportS                    float64 // Mithril only: Firedancer reads the capture while replaying
	Complete                   bool    // every timestamp was recorded
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
	FD         firedancerSummary
}

func secondsBetween(a, b time.Time) float64 { return b.Sub(a).Seconds() }

// firedancerFields computes the BACKTEST_SUMMARY quantities. startSlot is the
// slot replay started from (the snapshot or checkpoint slot); rows are every
// replayed block, never including that slot.
func firedancerFields(rows []timingRow, startSlot uint64, clock backtestClock) firedancerSummary {
	fd := firedancerSummary{SnapshotSlot: startSlot, Slots: len(rows)}
	if len(rows) == 0 {
		return fd
	}
	for _, r := range rows {
		fd.Txns += r.Transactions
	}
	first, last := rows[0], rows[len(rows)-1]
	fd.EndSlot = last.Slot
	lastDone := time.Unix(0, last.Completed)
	firstBegin := time.Unix(0, first.Completed-first.SlotReplay)
	fd.SlotExecS = secondsBetween(firstBegin, lastDone)
	fd.Complete = first.Completed != 0 && last.Completed != 0 &&
		!clock.Boot.IsZero() && !clock.ImportDone.IsZero() && !clock.StateReady.IsZero() && !clock.ReplayReady.IsZero()
	if !fd.Complete {
		return fd
	}
	fd.ImportS = secondsBetween(clock.Boot, clock.ImportDone)
	fd.SnapBytesS = secondsBetween(clock.ImportDone, clock.StateReady)
	fd.ReplayInitS = secondsBetween(clock.StateReady, clock.ReplayReady)
	fd.LoadTotalS = secondsBetween(clock.ImportDone, clock.ReplayReady)
	fd.ReplayS = secondsBetween(clock.ReplayReady, lastDone)
	fd.TotalS = secondsBetween(clock.Boot, lastDone)
	if fd.ReplayS > 0 {
		fd.SecPerSlot = fd.ReplayS / float64(fd.Slots)
		fd.TPS = float64(fd.Txns) / fd.ReplayS
	}
	return fd
}

func summarizeBacktest(rows []timingRow, warmup int, measureFrom uint64, startSlot uint64, clock backtestClock) backtestSummary {
	rows = lastRowPerSlot(rows)
	s := backtestSummary{Replayed: len(rows), FD: firedancerFields(rows, startSlot, clock)}
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
		fmt.Fprintf(w, "  no measured blocks (fewer replayed blocks than the warm-up)\n")
	} else {
		s.writeIntervals(w)
	}
	s.writeFiredancer(w, totalSeconds)
}

func (s backtestSummary) writeIntervals(w io.Writer) {
	fmt.Fprintf(w, "  %-22s %.2f M\n", "avg consumed CU/block", s.AvgCU/1e6)
	fmt.Fprintf(w, "  %-12s %9s %9s %9s %9s %8s\n", "interval", "mean ms", "p50 ms", "p90 ms", "p99 ms", "CU/ns")
	for _, iv := range []struct {
		name string
		st   intervalStats
	}{{"SlotReplay", s.SlotReplay}, {"TxLoop", s.TxLoop}} {
		fmt.Fprintf(w, "  %-12s %9.2f %9.2f %9.2f %9.2f %8.3f\n", iv.name, iv.st.MeanMs, iv.st.P50Ms, iv.st.P90Ms, iv.st.P99Ms, iv.st.CUPerNs)
	}
}

func (s backtestSummary) writeFiredancer(w io.Writer, totalSeconds float64) {
	fd := s.FD
	if !fd.Complete {
		fmt.Fprintf(w, "BACKTEST_SUMMARY unavailable: a run timestamp was not recorded (slots=%d txns=%d slot_exec_s=%.3f)\n", fd.Slots, fd.Txns, fd.SlotExecS)
		return
	}
	// Firedancer's field names and spans (fd_backtest_tile.c); every replayed
	// block counts, as in Firedancer.
	fmt.Fprintf(w, "BACKTEST_SUMMARY snapshot_slot=%d end_slot=%d slots=%d txns=%d snap_bytes_s=%.3f replay_init_s=%.3f load_total_s=%.3f replay_s=%.3f slot_exec_s=%.3f sec_per_slot=%.6f tps=%.0f total_s=%.3f\n",
		fd.SnapshotSlot, fd.EndSlot, fd.Slots, fd.Txns, fd.SnapBytesS, fd.ReplayInitS, fd.LoadTotalS, fd.ReplayS, fd.SlotExecS, fd.SecPerSlot, fd.TPS, fd.TotalS)
	// Mithril only: Firedancer has no import step (it reads the capture while
	// replaying), so total_s = import_s + load_total_s + replay_s.
	fmt.Fprintf(w, "MITHRIL_EXTRA import_s=%.3f shutdown_s=%.3f\n", fd.ImportS, max(totalSeconds-fd.TotalS, 0))
}

// lastRowPerSlot keeps each slot's last row, in the order those rows were
// written. A fork switch can make replay execute a slot again; only the
// execution that stood counts, once.
func lastRowPerSlot(rows []timingRow) []timingRow {
	last := make(map[uint64]int, len(rows))
	for i, r := range rows {
		last[r.Slot] = i
	}
	if len(last) == len(rows) {
		return rows
	}
	out := make([]timingRow, 0, len(last))
	for i, r := range rows {
		if last[r.Slot] == i {
			out = append(out, r)
		}
	}
	return out
}

// checkBacktestCoverage verifies that the run replayed exactly the blocks it
// planned: the chain's slots from firstSlot through head, each once, in chain
// order. Ending on the last block is not enough: a store that startup left
// ahead of the plan would replay only the tail and still end there. chain is
// the corpus manifest's slot list; a slot replayed again after a fork switch
// counts once, by its last row.
func checkBacktestCoverage(chain []uint64, firstSlot, head uint64, rows []timingRow) error {
	var planned []uint64
	for _, slot := range chain {
		if slot >= firstSlot && slot <= head {
			planned = append(planned, slot)
		}
	}
	rows = lastRowPerSlot(rows)
	replayed := make([]uint64, len(rows))
	for i, r := range rows {
		replayed[i] = r.Slot
	}
	describe := func(slots []uint64) string {
		if len(slots) == 0 {
			return "no blocks"
		}
		return fmt.Sprintf("%d blocks, slots %d..%d", len(slots), slots[0], slots[len(slots)-1])
	}
	fail := func(format string, args ...any) error {
		return fmt.Errorf("replay did not cover the planned blocks: %s (planned %s; replayed %s)",
			fmt.Sprintf(format, args...), describe(planned), describe(replayed))
	}
	for i, want := range planned {
		if i >= len(replayed) {
			return fail("slot %d was not replayed", want)
		}
		switch got := replayed[i]; {
		case got == want:
		case got < want || !slices.Contains(planned, got):
			return fail("slot %d was replayed but is not one of the planned blocks", got)
		case slices.Contains(replayed, want):
			return fail("slot %d was replayed out of chain order", want)
		case i == 0:
			return fail("the first replayed block is slot %d, after the planned first slot %d", got, want)
		default:
			return fail("slot %d was not replayed", want)
		}
	}
	if len(replayed) > len(planned) {
		return fail("slot %d was replayed but is not one of the planned blocks", replayed[len(planned)])
	}
	return nil
}
