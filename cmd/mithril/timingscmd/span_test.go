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

	out.Reset()
	require.NoError(t, writeSpanReport(&out, rows, 0, 102, 1))
	require.Contains(t, out.String(), "1 blocks in 0.030 s")

	require.Error(t, writeSpanReport(&out, rows, 0, 0, 10))
	require.Error(t, writeSpanReport(&out, rows, 200, 0, 0))
}
