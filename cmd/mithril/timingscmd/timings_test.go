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

// A block's notarize vote is joined from whichever rows carried its stages,
// usually the next block's, and the report runs from the last shred to replay
// done and on to the vote leaving.
func TestTipReportJoinsVotes(t *testing.T) {
	const ms = int64(1_000_000)
	block := func(slot uint64, handedOff int64, repaired int) string {
		return `{"Slot":` + itoa(slot) + `,"ComputeUnitsConsumed":1000000,"ReplayWork":{"Count":1,"SumNanoseconds":1000000}` +
			`,"FullToReplayed":{"Count":1,"SumNanoseconds":5000000}` +
			`,"TipPath":{"HandedOffNanos":` + itoa(uint64(handedOff)) +
			`,"LastShredToReplayed":{"Count":1,"SumNanoseconds":6000000}` +
			`,"DataShreds":64,"RepairedShreds":` + itoa(uint64(repaired)) +
			`,"RecoveredShreds":1,"TipLagSlots":2,"ForkSwitch":` + strconv.FormatBool(repaired > 0) +
			`,"ReplayToHandoff":{"Count":1,"SumNanoseconds":5000000}}`
	}
	event := func(slot uint64, typ, stage string, peers int, at int64) string {
		return `{"Slot":` + itoa(slot) + `,"Type":"` + typ + `","Stage":"` + stage + `","Peers":` + itoa(uint64(peers)) + `,"AtNanos":` + itoa(uint64(at)) + `}`
	}
	h := 1000 * ms // slot 20's hand-off; its last shred was 6 ms earlier
	rows := []string{
		block(20, h, 0) + `,"VoteEvents":[` + event(20, "notarize", "queued", 0, h+ms/2) + `]}`,
		// The send stages land in the next row, beside a skip vote that must not count.
		block(21, 2000*ms, 3) + `,"VoteEvents":[` + event(20, "skip", "queued", 0, h) + `,` +
			event(20, "notarize", "first_send", 0, h+ms) + `,` + event(20, "notarize", "fanout_done", 7, h+3*ms) + `]}`,
	}
	path := filepath.Join(t.TempDir(), "replay_timings.jsonl")
	require.NoError(t, os.WriteFile(path, []byte(strings.Join(rows, "\n")+"\n"), 0o644))
	got, _, err := readRows(path)
	require.NoError(t, err)
	require.InDelta(t, 0.5, handoffToVoteMs(&got[0]), 1e-9)
	require.Equal(t, -1.0, handoffToVoteMs(&got[1]), "slot 21 has no vote yet")
	require.Equal(t, 7, got[0].vote.peers)

	var out bytes.Buffer
	writeTipReport(&out, got, len(rows), 5)
	text := out.String()
	require.Regexp(t, `LastShredToReplayed\s+mithril\s+-\s+6\.00 .*\(2 blocks\)`, text)
	require.Regexp(t, `HandoffToVote\s+mithril\s+-\s+0\.50 .*\(1 blocks\)`, text)
	require.Regexp(t, `VoteQueuedToSend\s+mithril\s+-\s+0\.50 `, text)
	require.Regexp(t, `LastShredToVoteSent\s+mithril\s+-\s+7\.00 `, text, "6 ms to replay done, 1 ms more to the first send")
	require.Regexp(t, `LastShredToFanout\s+mithril\s+-\s+9\.00 `, text)
	require.Contains(t, text, "1 of 2 blocks voted; 0 votes reached no connected peer")
	require.Contains(t, text, "shreds: 128 data shreds; repair delivered 3 in 1 blocks, recovery rebuilt 2 in 2 blocks; 1 fork switches")
	require.Contains(t, text, "tip lag at hand-off (slots behind the newest data shred): p50 2, p99 2, max 2")
	require.Contains(t, text, "3 repaired shreds; 1 recovered shreds; fork switch")
}
