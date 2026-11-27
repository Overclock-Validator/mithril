package rpcserver

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/stretchr/testify/require"
)

func TestCheapMethods(t *testing.T) {
	server, endpoint := getBlocksFixture(t) // blocks 10 and 12, slot 11 skipped, rooted at 12
	call := func(method, params string) map[string]json.RawMessage {
		return getBlockHistoryHTTPCall(t, endpoint, method, params)
	}
	result := func(method, params string) string {
		body := call(method, params)
		require.NotContains(t, body, "error", "%s %s: %s", method, params, body["error"])
		return string(body["result"])
	}
	code := func(method, params string) int {
		body := call(method, params)
		require.NotContains(t, body, "result", "%s %s", method, params)
		var failure struct{ Code int }
		require.NoError(t, json.Unmarshal(body["error"], &failure))
		return failure.Code
	}

	for params, want := range map[string]string{
		`[10,1]`: `[10]`, `[10,2]`: `[10,12]`, `[10,5]`: `[10,12]`, `[11,1]`: `[12]`, `[10,0]`: `[]`,
		`[13,3]`: `[]`, `[10,2,{"commitment":"finalized"}]`: `[10,12]`,
	} {
		require.JSONEq(t, want, result("getBlocksWithLimit", params), params)
	}
	for _, params := range []string{`[10]`, `[10,500001]`, `[10,2,{"commitment":"processed"}]`, `["10",2]`} {
		require.Equal(t, -32602, code("getBlocksWithLimit", params), params)
	}

	require.JSONEq(t, `null`, result("getBlockTime", `[12]`), "recorded without a time")
	require.Equal(t, -32007, code("getBlockTime", `[11]`))
	require.Equal(t, -32004, code("getBlockTime", `[9]`))

	require.JSONEq(t, `{"slotsPerEpoch":100,"leaderScheduleSlotOffset":0,"warmup":false,"firstNormalEpoch":0,"firstNormalSlot":0}`, result("getEpochSchedule", `[]`))
	require.Equal(t, -32602, code("getEpochSchedule", `[1]`))
	require.JSONEq(t, `0`, result("getTransactionCount", `[]`))
	require.JSONEq(t, `0`, result("getTransactionCount", `[{"commitment":"finalized"}]`))
	require.Equal(t, -32008, code("getHighestSnapshotSlot", `[]`))
	result("getMaxShredInsertSlot", `[]`)
	result("getMaxRetransmitSlot", `[]`)
	require.JSONEq(t, `[]`, result("getRecentPerformanceSamples", `[]`))
	require.Equal(t, -32602, code("getRecentPerformanceSamples", `[721]`))
	require.Equal(t, -32602, code("getSlotLeaders", `[10,5001]`))

	_, err := server.GetInflationGovernor(t.Context(), nil)
	require.Error(t, err, "no settings yet")
	server.SetInflation(rewards.Inflation{Initial: 0.08, Terminal: 0.015, Taper: 0.15, FoundationVal: 0.05, FoundationTerm: 7}, 78892314.98)
	require.JSONEq(t, `{"initial":0.08,"terminal":0.015,"taper":0.15,"foundation":0.05,"foundationTerm":7}`, result("getInflationGovernor", `[]`))
}

func TestPerfSampler(t *testing.T) {
	var s perfSampler
	at := time.Unix(1000, 0)
	s.observe(at, 100, 5000)
	s.observe(at.Add(30*time.Second), 175, 9000) // within the period
	s.observe(at.Add(61*time.Second), 250, 14000)
	s.observe(at.Add(122*time.Second), 400, 20000)
	got := s.newest(10)
	require.Len(t, got, 2)
	require.Equal(t, PerfSample{Slot: 400, NumTransactions: 6000, NumSlots: 150, SamplePeriodSecs: 61}, got[0])
	require.Equal(t, PerfSample{Slot: 250, NumTransactions: 9000, NumSlots: 150, SamplePeriodSecs: 61}, got[1])
	require.Len(t, s.newest(1), 1)
	// A rewind restarts the baseline instead of reporting negative counts.
	s.observe(at.Add(200*time.Second), 300, 19000)
	s.observe(at.Add(262*time.Second), 450, 25000)
	require.Equal(t, uint64(150), s.newest(1)[0].NumSlots)
	for i := 0; i < maxPerfSamples+10; i++ {
		s.observe(at.Add(time.Duration(300+61*i)*time.Second), uint64(500+i), uint64(30000+i))
	}
	require.Len(t, s.newest(maxPerfSamples+5), maxPerfSamples)
}
