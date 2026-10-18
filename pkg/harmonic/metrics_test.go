package harmonic

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/blockprod"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// metricValue reads a counter or gauge, or a histogram's sample count, from
// the default registry. labels are name, value pairs. A series not yet
// created reads as zero.
func metricValue(t *testing.T, name string, labels ...string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, m := range family.GetMetric() {
			if !labelsMatch(m.GetLabel(), labels) {
				continue
			}
			switch {
			case m.Counter != nil:
				return m.Counter.GetValue()
			case m.Gauge != nil:
				return m.Gauge.GetValue()
			case m.Histogram != nil:
				return float64(m.Histogram.GetSampleCount())
			}
		}
	}
	return 0
}

func labelsMatch(have []*dto.LabelPair, want []string) bool {
	if len(have)*2 != len(want) {
		return false
	}
	for i := 0; i < len(want); i += 2 {
		if !slices.ContainsFunc(have, func(l *dto.LabelPair) bool { return l.GetName() == want[i] && l.GetValue() == want[i+1] }) {
			return false
		}
	}
	return true
}

// metricDelta returns how much a metric has grown since the call.
func metricDelta(t *testing.T, name string, labels ...string) func() float64 {
	t.Helper()
	before := metricValue(t, name, labels...)
	return func() float64 { return metricValue(t, name, labels...) - before }
}

func TestReportSlotExportsTheSlot(t *testing.T) {
	slots := metricDelta(t, "harmonic_slots_total", "mode", "harmonic")
	reverted := metricDelta(t, "harmonic_slot_end_flags_total", "flag", "reverted")
	timeout := metricDelta(t, "harmonic_slot_end_flags_total", "flag", "timeout")
	failed := metricDelta(t, "harmonic_slot_end_flags_total", "flag", "failed")
	executed := metricDelta(t, "harmonic_slot_bundles_total", "result", "executed")
	revertedBundles := metricDelta(t, "harmonic_slot_bundles_total", "result", "reverted")
	cranks := metricDelta(t, "harmonic_tip_cranks_total", "result", "landed")
	arrivals := metricDelta(t, "harmonic_first_bundle_delay_seconds")

	reportSlot(SlotReport{Mode: ModeHarmonic, Flags: FlagReverted | FlagTimeout, Executed: 3, Reverted: 1,
		Crank: CrankLanded, Arrived: true, FirstArrival: 30 * time.Millisecond})
	assert.Equal(t, 1.0, slots())
	assert.Equal(t, 1.0, reverted())
	assert.Equal(t, 1.0, timeout())
	assert.Zero(t, failed())
	assert.Equal(t, 3.0, executed())
	assert.Equal(t, 1.0, revertedBundles())
	assert.Equal(t, 1.0, cranks())
	assert.Equal(t, 1.0, arrivals())

	// A slot that fell back before any bundle has no crank or arrival.
	reportSlot(SlotReport{Mode: ModeFallback, Flags: FlagUnavailable})
	assert.Equal(t, 1.0, cranks())
	assert.Equal(t, 1.0, arrivals())
}

func TestServiceExportsMetrics(t *testing.T) {
	sessions := metricDelta(t, "harmonic_sessions_total", "session", "block_engine")
	relayerSessions := metricDelta(t, "harmonic_sessions_total", "session", "relayer")
	notices := metricDelta(t, "harmonic_leader_notices_total", "result", "sent")
	rtts := metricDelta(t, "harmonic_leader_notice_rtt_seconds")
	messages := metricDelta(t, "harmonic_block_messages_received_total")
	bundles := metricDelta(t, "harmonic_block_bundles_received_total", "result", "valid")
	txs := metricDelta(t, "harmonic_block_transactions_received_total")
	slots := metricDelta(t, "harmonic_slots_total", "mode", "harmonic")
	cranks := metricDelta(t, "harmonic_tip_cranks_total", "result", "not_needed")
	arrivals := metricDelta(t, "harmonic_first_bundle_delay_seconds")

	h := startService(t)
	gaugeOn := func(name string) func() bool {
		return func() bool { return metricValue(t, name) == 1 }
	}
	eventually(t, gaugeOn("harmonic_block_engine_healthy"), "block engine healthy")
	eventually(t, gaugeOn("harmonic_relayer_connected"), "relayer connected")
	eventually(t, gaugeOn("harmonic_relayer_tpu_advertised"), "relayer TPU advertised")
	eventually(t, gaugeOn("harmonic_client_id_advertised"), "Harmonic client ID advertised")
	// Gauges are global, so they may hold an earlier service's values.
	eventually(t, func() bool { return sessions() == 1 && relayerSessions() == 1 }, "sessions exported")

	env := gatedBank(t, costmodel.Limits{})
	h.routeTipsHere(t, env)
	now := time.Now()
	h.service.Hooks().SlotStarted(blockprod.LeaderSlotInfo{Slot: 10, Bank: env.Bank, Start: now, End: now.Add(time.Hour)})
	eventually(t, func() bool { return slices.Contains(h.be.noticeSlots(), 10) }, "notice sent")
	wire, err := transfer(t, 1).MarshalBinary()
	require.NoError(t, err)
	h.be.blocks <- blockMsg(10, blockTx(wire, 0))
	eventually(t, func() bool { return len(env.Bank.ForgedTransactions()) == 1 }, "block executed")
	h.service.Hooks().SlotEnding(10)

	eventually(t, func() bool { return slots() == 1 }, "slot reported")
	assert.Equal(t, 1.0, cranks())
	assert.Equal(t, 1.0, arrivals())
	eventually(t, func() bool { return notices() == 1 && rtts() == 1 }, "notice exported")
	eventually(t, func() bool { return messages() == 1 && bundles() == 1 && txs() == 1 }, "block stream exported")

	h.be.endBlocks <- errors.New("block engine restarting")
	eventually(t, func() bool { return metricValue(t, "harmonic_block_engine_healthy") == 0 }, "block engine down")
	eventually(t, func() bool { return metricValue(t, "harmonic_relayer_tpu_advertised") == 0 }, "local TPU advertised")
}
