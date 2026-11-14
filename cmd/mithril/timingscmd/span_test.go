package timingscmd

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSpanReport(t *testing.T) {
	ms := int64(1_000_000)
	row := func(slot uint64, done int64, wait int64) string {
		return `{"Slot":` + itoa(slot) + `,"ComputeUnitsConsumed":10000000,"CompletedUnixNano":` + itoa(uint64(done)) +
			`,"ReplayWork":{"Count":1,"SumNanoseconds":20000000},"LoopSourceWait":{"Count":1,"SumNanoseconds":` + itoa(uint64(wait)) + `}}`
	}
	lines := []string{
		row(100, 1000*ms, 0),
		row(101, 1040*ms, 10*ms),
		row(102, 1060*ms, 0),
		row(102, 1070*ms, 0), // replayed again after a fork switch: the last counts
		row(104, 1100*ms, 20*ms),
	}
	path := filepath.Join(t.TempDir(), "replay_timings.jsonl")
	require.NoError(t, os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644))
	rows, err := readSpanRows(path)
	require.NoError(t, err)
	require.Len(t, rows, 4)

	var out bytes.Buffer
	require.NoError(t, writeSpanReport(&out, rows, 0, 0, 0))
	text := out.String()
	// Slots 101, 102, 104 measured from slot 100's completion: 100 ms.
	require.Contains(t, text, "span: slots 101..104 (opened by slot 100's completion)")
	require.Contains(t, text, "3 blocks in 0.100 s: 30.00 blocks/s, 33.333 ms/block; 4 slots advanced: 40.00 slots/s")
	require.Contains(t, text, "waiting for the next block (LoopSourceWait): 30.0% of the span")
	// Intervals 40, 30, 30 ms: mean is the span's ms/block.
	require.Regexp(t, `BlockInterval\s+33\.333\s+30\.000\s+40\.000\s+40\.000\s+40\.000`, text)
	// Every block here consumed 10 M CU: all three fall in the 10-20 group.
	require.Regexp(t, `10-20\s+3\s+10\.00\s+33\.333\s+30\.000\s+40\.000\s+40\.000\s+0\.300`, text)
	require.Regexp(t, `30-40\s+0\n`, text)

	out.Reset()
	require.NoError(t, writeSpanReport(&out, rows, 0, 102, 1))
	require.Contains(t, out.String(), "1 blocks in 0.030 s")

	require.Error(t, writeSpanReport(&out, rows, 0, 0, 10))
	require.Error(t, writeSpanReport(&out, rows, 200, 0, 0))
}

func TestSizeBuckets(t *testing.T) {
	var out bytes.Buffer
	// Two 35 M CU blocks (60 and 80 ms), one 5 M, one 45 M.
	writeSizeBuckets(&out, []float64{60, 10, 80, 100}, []uint64{35e6, 5e6, 35e6, 45e6})
	text := out.String()
	require.Regexp(t, `0-10\s+1\s+5\.00\s+10\.000`, text)
	require.Regexp(t, `30-40\s+2\s+35\.00\s+70\.000\s+60\.000\s+80\.000\s+80\.000\s+0\.500`, text)
	require.Regexp(t, `40\+\s+1\s+45\.00\s+100\.000`, text)
	require.Regexp(t, `20-30\s+0\n`, text)
}
