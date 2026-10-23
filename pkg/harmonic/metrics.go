package harmonic

import (
	"context"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/statsd"
)

const metricsInterval = time.Second

// statsSnapshot is the cumulative counters last exported.
type statsSnapshot struct {
	blockEngine BlockEngineStats
	relayer     RelayerStats
	pipeline    BlockPipelineStats
}

// exportMetrics publishes the sessions' and the pipeline's counters, and the
// state gauges, every interval until ctx ends.
func (s *Service) exportMetrics(ctx context.Context) {
	tick := time.NewTicker(s.metricsInterval)
	defer tick.Stop()
	var last statsSnapshot
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			last = s.publishStats(last)
		}
	}
}

// publishStats exports what changed since last and returns the new snapshot.
func (s *Service) publishStats(last statsSnapshot) statsSnapshot {
	now := statsSnapshot{blockEngine: s.blockEngine.Stats(), pipeline: s.pipeline.Stats()}
	if s.relayer != nil {
		now.relayer = s.relayer.Stats()
	}

	be, prevBE := now.blockEngine, last.blockEngine
	countDelta(statsd.HarmonicSessions, be.Sessions, prevBE.Sessions, "block_engine")
	countDelta(statsd.HarmonicSessionResets, be.Resets, prevBE.Resets, "block_engine")
	countDelta(statsd.HarmonicPackets, be.Packets, prevBE.Packets, "block_engine")
	countDelta(statsd.HarmonicPacketsDropped, be.DroppedSize, prevBE.DroppedSize, "block_engine", "size")
	countDelta(statsd.HarmonicPacketsDropped, be.DroppedFull, prevBE.DroppedFull, "block_engine", "full")
	countDelta(statsd.HarmonicBlockEngineErrors, be.StrategyErrors, prevBE.StrategyErrors, "set_strategy")
	countDelta(statsd.HarmonicBlockEngineErrors, be.InvalidFeeInfo, prevBE.InvalidFeeInfo, "fee_info")
	countDelta(statsd.HarmonicLeaderNotices, be.NoticesSent, prevBE.NoticesSent, "sent")
	countDelta(statsd.HarmonicLeaderNotices, be.NoticeErrors, prevBE.NoticeErrors, "failed")
	countDelta(statsd.HarmonicLeaderNotices, be.NoticesExpired, prevBE.NoticesExpired, "expired")
	countDelta(statsd.HarmonicBlockStreamsLost, be.StreamsLost, prevBE.StreamsLost)

	r, prevR := now.relayer, last.relayer
	countDelta(statsd.HarmonicSessions, r.Sessions, prevR.Sessions, "relayer")
	countDelta(statsd.HarmonicSessionResets, r.Resets, prevR.Resets, "relayer")
	countDelta(statsd.HarmonicPackets, r.Packets, prevR.Packets, "relayer")
	countDelta(statsd.HarmonicPacketsDropped, r.DroppedSize, prevR.DroppedSize, "relayer", "size")
	countDelta(statsd.HarmonicPacketsDropped, r.DroppedFull, prevR.DroppedFull, "relayer", "full")
	countDelta(statsd.HarmonicRelayerHeartbeats, r.Heartbeats, prevR.Heartbeats)
	countDelta(statsd.HarmonicRelayerInvalidTPUConfigs, r.InvalidTPUConfigs, prevR.InvalidTPUConfigs)

	p, prevP := now.pipeline, last.pipeline
	countDelta(statsd.HarmonicBlockMessages, p.Messages, prevP.Messages)
	countDelta(statsd.HarmonicBlockMessagesDropped, p.WrongSlot, prevP.WrongSlot, "wrong_slot")
	countDelta(statsd.HarmonicBlockMessagesDropped, p.LostSlot, prevP.LostSlot, "lost_slot")
	countDelta(statsd.HarmonicBlockBundles, p.Bundles, prevP.Bundles, "valid")
	countDelta(statsd.HarmonicBlockBundles, p.InvalidBundles, prevP.InvalidBundles, "invalid")
	countDelta(statsd.HarmonicBlockTransactions, p.Transactions, prevP.Transactions)

	setGauge(statsd.HarmonicBlockEngineHealthy, s.blockEngine.Healthy())
	setGauge(statsd.HarmonicRelayerConnected, s.relayer != nil && s.relayer.Status() != nil)
	setGauge(statsd.HarmonicRelayerTPUAdvertised, s.tpu.RelayerAdvertised())
	setGauge(statsd.HarmonicClientIDAdvertised, s.clientIDAdvertised.Load())
	return now
}

func countDelta(m statsd.Metric, now, last uint64, labels ...string) {
	if now > last {
		_ = statsd.Count(m, int64(now-last), labels)
	}
}

func setGauge(m statsd.Metric, on bool) {
	value := 0.0
	if on {
		value = 1
	}
	_ = statsd.Gauge(m, value, nil)
}

// reportSlot exports a leader slot's report.
func reportSlot(r SlotReport) {
	_ = statsd.Count(statsd.HarmonicSlots, 1, []string{r.Mode.String()})
	for _, flag := range []EndFlags{FlagTimeout, FlagFailed, FlagReverted, FlagUnavailable} {
		if r.Flags&flag != 0 {
			_ = statsd.Count(statsd.HarmonicSlotEndFlags, 1, []string{flag.String()})
		}
	}
	countDelta(statsd.HarmonicSlotBundles, r.Executed, 0, "executed")
	countDelta(statsd.HarmonicSlotBundles, r.Reverted, 0, "reverted")
	countDelta(statsd.HarmonicSlotBundles, r.Rejected, 0, "rejected")
	if r.Crank != CrankNotRun {
		_ = statsd.Count(statsd.HarmonicTipCranks, 1, []string{r.Crank.String()})
	}
	if r.Arrived {
		_ = statsd.Duration(statsd.HarmonicFirstBundleDelay, r.FirstArrival, nil)
	}
	if r.MaxParallel > 0 {
		_ = statsd.Timing(statsd.HarmonicSlotMaxParallel, uint64(r.MaxParallel), nil)
	}
}
