package blockprod

import (
	"errors"
	"testing"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/Overclock-Validator/mithril/pkg/statsd"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func leaderHistogram(t *testing.T, name string) *dto.Histogram {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() == name && len(family.GetMetric()) != 0 {
			return family.GetMetric()[0].GetHistogram()
		}
	}
	return &dto.Histogram{}
}

func TestLeaderCompletionMetricsExcludeHandoffAndRetainSubmillisecondMiss(t *testing.T) {
	for _, margin := range []time.Duration{8 * time.Millisecond, 0, -500 * time.Microsecond} {
		t.Run(margin.String(), func(t *testing.T) {
			const slot = uint64(120)
			readyAt := time.Unix(1_700_000_000, 0)
			start := readyAt.Add(185 * time.Millisecond)
			completed := readyAt.Add(200*time.Millisecond - margin)
			now := start
			loop, _, _, _ := newFinishingLeaderLoop(t, slot, &recordingHooks{},
				func(*ParentContext, replay.CommitLeaderInput) error { now = completed; return nil })
			loop.productionWindow = leaderProductionWindow{active: true, startSlot: slot, endSlot: slot + 3, nextSlot: slot, readyAt: readyAt}
			loop.now = func() time.Time { return now }
			loop.onBlock = func(*b.Block) { now = now.Add(time.Second) }
			beforeCompletion := leaderHistogram(t, statsd.BlockProductionCompletion.String())
			beforeMargin := leaderHistogram(t, statsd.BlockProductionDeadlineMargin.String())
			beforeMisses := prometheusCounterValue(t, statsd.BlockProductionDeadlineMisses.String(), nil)
			loop.finishActiveSlotLocked()
			afterCompletion := leaderHistogram(t, statsd.BlockProductionCompletion.String())
			afterMargin := leaderHistogram(t, statsd.BlockProductionDeadlineMargin.String())
			require.Equal(t, beforeCompletion.GetSampleCount()+1, afterCompletion.GetSampleCount())
			require.InDelta(t, completed.Sub(start).Seconds(), afterCompletion.GetSampleSum()-beforeCompletion.GetSampleSum(), 1e-12)
			require.Equal(t, beforeMargin.GetSampleCount()+1, afterMargin.GetSampleCount())
			require.InDelta(t, margin.Seconds(), afterMargin.GetSampleSum()-beforeMargin.GetSampleSum(), 1e-12)
			wantMisses := beforeMisses
			if margin < 0 {
				wantMisses++
			}
			require.Equal(t, wantMisses, prometheusCounterValue(t, statsd.BlockProductionDeadlineMisses.String(), nil))
		})
	}
}

func TestLeaderAbortedFinalizationDoesNotCountAsNetworkCompletion(t *testing.T) {
	loop, _, broadcaster, delivered := newFinishingLeaderLoop(t, 120, &recordingHooks{},
		func(*ParentContext, replay.CommitLeaderInput) error { return errors.New("test bank commit failure") })
	beforeCompletion := leaderHistogram(t, statsd.BlockProductionCompletion.String()).GetSampleCount()
	beforeQuiescence := leaderHistogram(t, statsd.BlockProductionExecutionQuiescence.String()).GetSampleCount()
	loop.finishActiveSlotLocked()
	require.False(t, *delivered)
	require.Zero(t, broadcaster.count())
	require.Equal(t, beforeCompletion, leaderHistogram(t, statsd.BlockProductionCompletion.String()).GetSampleCount())
	require.Equal(t, beforeQuiescence+1, leaderHistogram(t, statsd.BlockProductionExecutionQuiescence.String()).GetSampleCount())
}

type emptyReadyRewardPreparer struct{}

func (emptyReadyRewardPreparer) BuildForLeaderSlot(uint64) rewardcerts.RewardCertificates {
	panic("prepared reward consumer must not run a synchronous fallback")
}

func (emptyReadyRewardPreparer) TakeForLeaderSlot(uint64) (rewardcerts.RewardCertificates, rewardcerts.PreparationStatus) {
	return rewardcerts.RewardCertificates{}, rewardcerts.PreparationStatus{Ready: true}
}

func TestLeaderEmptyReadyRewardsAreNotMissing(t *testing.T) {
	loop, _, _, delivered := newFinishingLeaderLoop(t, 120, &recordingHooks{},
		func(*ParentContext, replay.CommitLeaderInput) error { return nil })
	loop.rewardCerts = emptyReadyRewardPreparer{}
	metric := statsd.BlockProductionRewardPreparation.String()
	beforeReady := prometheusCounterValue(t, metric, map[string]string{"status": "ready"})
	beforeMissing := prometheusCounterValue(t, metric, map[string]string{"status": "missing"})
	loop.finishActiveSlotLocked()
	require.True(t, *delivered)
	require.Equal(t, beforeReady+1, prometheusCounterValue(t, metric, map[string]string{"status": "ready"}))
	require.Equal(t, beforeMissing, prometheusCounterValue(t, metric, map[string]string{"status": "missing"}))
}
