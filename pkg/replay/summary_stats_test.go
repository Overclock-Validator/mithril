package replay

import (
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
)

func TestPercentileHelpers(t *testing.T) {
	vals := []float64{5, 1, 3, 2, 4}
	assert.Equal(t, 3.0, medianF(vals))
	assert.Equal(t, 1.0, percentileF(vals, 0))
	assert.Equal(t, 5.0, percentileF(vals, 100))
	assert.Equal(t, 4.0, percentileF(vals, 90)) // index (5-1)*90/100 = 3 of sorted
	assert.Equal(t, 5.0, maxF(vals))
	// ready values can be all-negative (pipeline ahead everywhere): max must
	// return the true (least-negative) maximum, not a zero floor.
	assert.Equal(t, -1.5, maxF([]float64{-4.2, -1.5, -9.0}))
	// Input order untouched (helpers sort a copy).
	assert.Equal(t, []float64{5, 1, 3, 2, 4}, vals)
	// Empty inputs are zeros, never a panic.
	assert.Equal(t, 0.0, medianF(nil))
	assert.Equal(t, 0.0, maxF(nil))
}

func TestFormatSlotETA(t *testing.T) {
	assert.Equal(t, "200ms", formatSlotETA(1))
	assert.Equal(t, "1.0s", formatSlotETA(5))
	assert.Equal(t, "23s", formatSlotETA(117))
	assert.Equal(t, "1m00s", formatSlotETA(300))
	assert.Equal(t, "1m30s", formatSlotETA(450))
}

func TestNextLeaderHint(t *testing.T) {
	var identity solana.PublicKey
	identity[0] = 7
	lookup := func(_ solana.PublicKey, from uint64) (uint64, bool) {
		if from <= 110 {
			return 110, true
		}
		if from <= 200 {
			return 200, true
		}
		return 0, false
	}
	c := &nextLeaderCursor{identity: identity, lookup: lookup}

	assert.Equal(t, "next 110 ~1.0s", c.hint(105))
	assert.Equal(t, "ours", c.hint(110))
	assert.Equal(t, "next 200 ~18s", c.hint(111))
	assert.Equal(t, "", c.hint(201))
	assert.Equal(t, "", newNextLeaderCursor(solana.PublicKey{}).hint(1))
	assert.Equal(t,
		"slot 1 | leader abcd...xyz | ours",
		appendNextLeaderHint("slot 1 | leader abcd...xyz", "ours"))
}

func TestFormatHelpers(t *testing.T) {
	assert.Equal(t, "31.0M", fmtMcu(31_000_000))
	assert.Equal(t, "0.5M", fmtMcu(480_000))
	assert.Equal(t, "38k", fmtK(38_000))
	assert.Equal(t, "850", fmtK(850))
	assert.Equal(t, "7abc...Q9x", shortPubkey("7abcDEFGHIJKLMNOPQ9x"))
	assert.Equal(t, "short", shortPubkey("short"))
}

// Per-slot lines match the handoff spec shape; the shreds segment is omitted
// (not dashed, not fabricated) for non-shred-sourced blocks. Value fields are
// fixed-width cells and dashes are padded to the same widths, so the pipe
// separators land in identical columns on every line of a mixed stream —
// these assertions lock the exact padding.
func TestBuildSlotStatsLines(t *testing.T) {
	// ready < 0: the block was fully assembled 45.2s before replay asked for
	// it (pipeline ahead). asm: 69.6s from first shred to full.
	line := buildSlotStatsLine(123456789, "7abcDEFGHIJKLMNOPQ9x", 862, 31_000_000, 170, true, -45.2, 69.6, 3168)
	assert.Equal(t, "slot 123456789 | leader 7abc...Q9x | txns   862 | cu 31.0M | exec  170ms | eff   5.5ms/Mcu | shreds(ready  -45.2s, asm  69.6s, repair 3168)", line)

	// ready > 0: replay sat waiting 12.4s for the slot to finish assembling.
	waited := buildSlotStatsLine(123456790, "7abcDEFGHIJKLMNOPQ9x", 100, 1_000_000, 30, true, 12.4, 0.3, 96)
	assert.Contains(t, waited, "shreds(ready  +12.4s, asm   0.3s, repair 96)")

	rpcLine := buildSlotStatsLine(42, "7abcDEFGHIJKLMNOPQ9x", 10, 2_000_000, 30, false, 0, 0, 0)
	assert.NotContains(t, rpcLine, "shreds(", "RPC-sourced blocks must not fabricate shred stats")

	zeroCU := buildSlotStatsLine(43, "7abcDEFGHIJKLMNOPQ9x", 0, 0, 5, false, 0, 0, 0)
	assert.Equal(t, "slot 43 | leader 7abc...Q9x | txns     0 | cu  0.0M | exec    5ms | eff    --", zeroCU,
		"no efficiency without compute units; dash padded to the eff cell, trailing spaces trimmed")

	// Skipped with nothing observed: identical field order to executed lines,
	// dashes padded to the executed cells' widths so the columns align,
	// "skipped" as the trailing status, and NO shreds segment — same omission
	// rule as executed lines.
	skipped := buildSkippedStatsLine(123456790, "4defGHIJKLMNOPQRK2p", 0, 0)
	assert.Equal(t, "slot 123456790 | leader 4def...K2p | txns    -- | cu    -- | exec   --   | eff    --       | skipped", skipped)

	// Every pipe — including the one before the status/shreds tail — must sit
	// in the same column as the executed line's: the alignment property
	// itself, not just the text.
	assert.Equal(t, pipeColumns(line), pipeColumns(skipped), "executed and skipped separators must align")

	// Skipped but the leader DID send shreds before dying: report the partial
	// arrivals — the signal separating "leader started then stopped" from
	// "leader never transmitted".
	partial := buildSkippedStatsLine(123456791, "4defGHIJKLMNOPQRK2p", 12, 3)
	assert.Equal(t, "slot 123456791 | leader 4def...K2p | txns    -- | cu    -- | exec   --   | eff    --       | skipped | shreds seen 12 (repaired 3) — block never completed", partial)
}

// pipeColumns returns the byte offsets of every '|' in a line.
func pipeColumns(s string) []int {
	var cols []int
	for i, r := range s {
		if r == '|' {
			cols = append(cols, i)
		}
	}
	return cols
}
