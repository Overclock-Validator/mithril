package timingscmd

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTipReport(t *testing.T) {
	ms := uint64(1_000_000)
	line := func(slot, cu, work, full, arrival, replay, handoff uint64, received int64, gcPause uint64) string {
		gc := ""
		if gcPause > 0 {
			gc = `,"GCCycles":1,"GCPauseMax":{"Count":1,"SumNanoseconds":` + itoa(gcPause) + `}`
		}
		return `{"Slot":` + itoa(slot) + `,"ComputeUnitsConsumed":` + itoa(cu) +
			`,"ReplayWork":{"Count":1,"SumNanoseconds":` + itoa(work) + `}` +
			`,"FullToReplayed":{"Count":1,"SumNanoseconds":` + itoa(full) + `}` +
			`,"TipPath":{"ShredReceivedNanos":` + itoa(uint64(received)) +
			`,"ReceiveToFull":{"Count":1,"SumNanoseconds":` + itoa(ms) + `}` +
			`,"WaitParentArrival":{"Count":1,"SumNanoseconds":` + itoa(arrival) + `}` +
			`,"WaitParentReplay":{"Count":1,"SumNanoseconds":` + itoa(replay) + `}` +
			`,"ReplayToHandoff":{"Count":1,"SumNanoseconds":` + itoa(handoff) + `}}` +
			`,"RuntimeInterference":{"KernelShredDrops":2` + gc + `}}`
	}
	rows := []string{
		`{"Slot":9,"ComputeUnitsConsumed":5}`, // not a shred block
		line(10, 10e6, 20*ms, 30*ms, 0, 5*ms, 25*ms, 1, 0),
		line(11, 20e6, 40*ms, 60*ms, 10*ms, 10*ms, 40*ms, 1, 3*ms),
		line(11, 20e6, 41*ms, 62*ms, 12*ms, 10*ms, 40*ms, 1, 3*ms), // replayed again: the last row counts
	}
	path := filepath.Join(t.TempDir(), "replay_timings.jsonl")
	require.NoError(t, os.WriteFile(path, []byte(strings.Join(rows, "\n")+"\n"), 0o644))
	got, total, err := readRows(path)
	require.NoError(t, err)
	require.Equal(t, 4, total)
	require.Len(t, got, 2)
	require.InDelta(t, 62.0, got[1].FullToReplayed.ms(), 1e-9, "slot 11's last row")
	// Controlled excludes only the parent's missing shreds: 1 + 10 + 40.
	require.InDelta(t, 51.0, controlledMs(&got[1]), 1e-9)

	var out bytes.Buffer
	writeTipReport(&out, got, total, 5)
	text := out.String()
	require.Contains(t, text, "Tip replay: 2 blocks with shred timing of 4 rows, slots 10..11")
	require.Contains(t, text, "avg consumed CU/block 15.00 M")
	require.Contains(t, text, "4 kernel shred drops")
	require.Contains(t, text, "a GC pause in 1 of 2 row windows")
	// WaitParentArrival is 12 of 92 ms of FullToReplayed.
	require.Regexp(t, `WaitParentArrival\s+network\s+13\.0%`, text)
	slowest := text[strings.Index(text, "slowest"):]
	require.Less(t, strings.Index(slowest, "\n  11 "), strings.Index(slowest, "\n  10 "), "slot 11 is slower")
	require.Contains(t, slowest, "ReplayToHandoff 40.00, WaitParentArrival 12.00, WaitParentReplay 10.00")
}

func TestTipReportWithoutShredBlocks(t *testing.T) {
	var out bytes.Buffer
	writeTipReport(&out, nil, 3, 5)
	require.Contains(t, out.String(), "no blocks with shred timing among 3 rows")
}

func itoa(v uint64) string { return strconv.FormatUint(v, 10) }
