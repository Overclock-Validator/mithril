package replay

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Terminal replay stats for the Alpenglow/native-shred path. Terminology
// follows Agave: a slot is "full" when all its data shreds are present and the
// block is reconstructable (SlotMeta/is_full) — NOT finalized/consensus-safe.
//
// Shred timings are replay-relative, the only clock the operator actually
// cares about:
//
//	ready = when the block finished assembling MINUS when replay asked for
//	        it. Negative: it was ready that long BEFORE replay needed it
//	        (the pipeline is ahead). Positive: replay sat idle that long
//	        waiting for shreds (the pipeline is the bottleneck).
//	asm   = first shred seen -> fully assembled. How long the slot took to
//	        collect, i.e. the repair grind for holes and pre-join slots;
//	        near-live slots read a few hundred ms (one broadcast pass).
//
// Both come from two timestamps the receiver already stamps per block plus
// one time.Now() replay already takes — no added tracking cost.

// shredSample is one executed slot's shred timing record for the summary
// window.
type shredSample struct {
	readySecs float64
	asmSecs   float64
}

// medianF / percentileF / maxF operate on a copy; empty input returns 0.
func medianF(vals []float64) float64 { return percentileF(vals, 50) }

func percentileF(vals []float64, pct int) float64 {
	if len(vals) == 0 {
		return 0
	}
	sorted := append([]float64(nil), vals...)
	sort.Float64s(sorted)
	if pct <= 0 {
		return sorted[0]
	}
	if pct >= 100 {
		return sorted[len(sorted)-1]
	}
	idx := (len(sorted) - 1) * pct / 100
	return sorted[idx]
}

func maxF(vals []float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	m := vals[0]
	for _, v := range vals[1:] {
		if v > m {
			m = v
		}
	}
	return m
}

// fmtMcu renders compute units as millions: 31_000_000 -> "31.0M".
func fmtMcu(cu uint64) string {
	return fmt.Sprintf("%.1fM", float64(cu)/1e6)
}

// fmtK renders a count in thousands: 38_000 -> "38k"; below 1000 verbatim.
func fmtK(v float64) string {
	if v >= 1000 {
		return fmt.Sprintf("%.0fk", v/1000)
	}
	return fmt.Sprintf("%.0f", v)
}

// shortPubkey renders "7abc...Q9x" (first 4 + last 3) for terminal alignment.
func shortPubkey(s string) string {
	if len(s) <= 10 {
		return s
	}
	return s[:4] + "..." + s[len(s)-3:]
}

// Dash cells for fields with no value, padded to the exact content width of
// their populated counterparts (txns %5d, cu %5s, exec %4.0f+"ms",
// eff %5.1f+"ms/Mcu") so the pipe separators land in the same terminal
// columns down a mixed stream of executed, zero-txn, and skipped lines. The
// dashes right-align under the digits.
const (
	dashTxns = "   --"
	dashCU   = "   --"
	dashExec = "  --  "
	dashEff  = "   --      "
)

// buildSlotStatsLine renders the per-slot terminal line. The shreds segment is
// omitted for blocks that did not come from shreds (never fabricated). Value
// fields use fixed cell widths (see dash constants); an extreme value
// overflows its cell and shifts only its own line's tail.
//
// ready < 0: block was assembled |ready| before replay asked for it (good —
// the pipeline runs ahead). ready > 0: replay waited that long for shreds.
// asm: first shred seen -> fully assembled (the collection/repair grind).
func buildSlotStatsLine(slot uint64, leader string, txns int, cu uint64, execMs float64, hasShreds bool, readySecs, asmSecs float64, repaired int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "slot %d | leader %s | txns %5d | cu %5s | exec %4.0fms", slot, shortPubkey(leader), txns, fmtMcu(cu), execMs)
	if cu > 0 {
		fmt.Fprintf(&b, " | eff %5.1fms/Mcu", execMs/(float64(cu)/1e6))
	} else {
		b.WriteString(" | eff " + dashEff)
	}
	if hasShreds {
		fmt.Fprintf(&b, " | shreds(ready %+6.1fs, asm %5.1fs, repair %d)", readySecs, asmSecs, repaired)
	}
	return strings.TrimRight(b.String(), " ")
}

// buildSkippedStatsLine renders the skipped-slot terminal line with the SAME
// field order as executed lines (slot | leader | txns | cu | exec | eff) so
// the columns read straight down a mixed stream; "skipped" trails as the
// status. A shreds segment appears ONLY when partial shreds actually arrived
// — "the leader sent 12 shreds then stopped" is a different operator story
// from "the leader never transmitted" — matching executed lines, which also
// omit the segment when there is no shred data. Nothing is ever fabricated.
func buildSkippedStatsLine(slot uint64, leader string, partialShreds, repairedShreds int) string {
	line := fmt.Sprintf("slot %d | leader %s | txns %s | cu %s | exec %s | eff %s | skipped",
		slot, shortPubkey(leader), dashTxns, dashCU, dashExec, dashEff)
	if partialShreds > 0 {
		// Turbine's independent observation of the skipped slot: the leader
		// DID transmit (we hold this many distinct data shreds, this many of
		// them repair-fetched) but the block never completed and consensus
		// skipped it. Plain words — "seen/repaired" — because "partial" read
		// as jargon.
		line += fmt.Sprintf(" | shreds seen %d (repaired %d) — block never completed", partialShreds, repairedShreds)
	}
	return line
}

// processRSSBytes reads the resident set size on Linux (/proc/self/statm,
// second field, pages). Returns 0 (omit from output) where unavailable —
// stats must not fabricate on other platforms.
func processRSSBytes() uint64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * uint64(os.Getpagesize())
}
