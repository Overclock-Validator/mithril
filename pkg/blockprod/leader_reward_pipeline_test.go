package blockprod

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type recordingRewardPreparer struct{ slots []uint64 }

func (p *recordingRewardPreparer) PrepareForLeaderSlot(slot uint64) {
	p.slots = append(p.slots, slot)
}

func (*recordingRewardPreparer) BuildForLeaderSlot(uint64) rewardcerts.RewardCertificates {
	return rewardcerts.RewardCertificates{}
}

func TestLeaderDoesNotPrepareRewardsBeforeParentReady(t *testing.T) {
	for _, wallSlot := range []uint64{208, 212} {
		t.Run(fmt.Sprint(wallSlot), func(t *testing.T) {
			preparer := &recordingRewardPreparer{}
			loop := NewLeaderLoop(LeaderLoopConfig{
				Identity:    identityHolder(txfixture.PayerPrivateKey()),
				CurrentSlot: func() uint64 { return wallSlot },
				LeaderForSlot: func(slot uint64) (solana.PublicKey, bool) {
					if slot >= 212 && slot <= 215 {
						return txfixture.PayerPubkey(), true
					}
					return solana.PublicKey{99}, true
				},
				ProductionParent: func(uint64) alpenglow.BlockProductionParent {
					require.Empty(t, preparer.slots)
					return alpenglow.BlockProductionParent{Kind: alpenglow.BlockProductionParentNotReady}
				},
				RewardCerts: preparer,
			})
			loop.tick()
			require.Empty(t, preparer.slots, "early preparation must not consume votes for a future window")
			require.Nil(t, loop.activeBank, "preparation must not bypass production gates")
		})
	}
}

func TestLeaderPreparesOnlySelectedSlotBeforeReplayGate(t *testing.T) {
	previousFrontier := global.ReplayFrontier()
	t.Cleanup(func() { global.SetReplayFrontier(previousFrontier) })
	global.SetReplayFrontier(1)
	readyAt := time.Now()
	preparer := &recordingRewardPreparer{}
	loop := NewLeaderLoop(LeaderLoopConfig{
		Identity:    identityHolder(txfixture.PayerPrivateKey()),
		CurrentSlot: func() uint64 { return 212 },
		Now:         func() time.Time { return readyAt },
		LeaderForSlot: func(uint64) (solana.PublicKey, bool) {
			return txfixture.PayerPubkey(), true
		},
		ProductionParent: func(uint64) alpenglow.BlockProductionParent {
			return alpenglow.BlockProductionParent{
				Kind:    alpenglow.BlockProductionParentReady,
				Parent:  alpenglow.BlockID{Slot: 208, Hash: solana.Hash{1}},
				ReadyAt: readyAt,
			}
		},
		RewardCerts: preparer,
	})
	loop.tick()
	require.Equal(t, []uint64{212}, preparer.slots, "future window slots must continue collecting late votes")
	require.Nil(t, loop.activeBank, "preparation must precede the closed replay gate")
	require.Contains(t, loop.pendingFailures, uint64(212))
	preparer.slots = nil
	loop.productionWindow.nextSlot = 213
	loop.tick()
	require.Equal(t, []uint64{213}, preparer.slots)
}

func TestLeaderPreparesRewardsBeforeReplayGate(t *testing.T) {
	previousFrontier := global.ReplayFrontier()
	t.Cleanup(func() { global.SetReplayFrontier(previousFrontier) })
	global.SetReplayFrontier(1)
	preparer := &recordingRewardPreparer{}
	loop := NewLeaderLoop(LeaderLoopConfig{
		Identity:    identityHolder(txfixture.PayerPrivateKey()),
		CurrentSlot: func() uint64 { return 212 },
		LeaderForSlot: func(uint64) (solana.PublicKey, bool) {
			return txfixture.PayerPubkey(), true
		},
		RewardCerts: preparer,
	})
	loop.tick()
	require.Equal(t, []uint64{212}, preparer.slots)
	require.Nil(t, loop.activeBank)
	require.Contains(t, loop.pendingFailures, uint64(212))
}

type blockedRewardBuilder struct {
	started chan struct{}
	release chan struct{}
}

func (b *blockedRewardBuilder) BuildForLeaderSlot(uint64) rewardcerts.RewardCertificates {
	close(b.started)
	<-b.release
	return rewardcerts.RewardCertificates{Skip: []byte{1}}
}

func TestLeaderFooterCompletesWhileRewardBuilderBlocked(t *testing.T) {
	const slot = uint64(106)
	builder := &blockedRewardBuilder{started: make(chan struct{}), release: make(chan struct{})}
	pipeline := rewardcerts.NewPipeline(builder)
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(builder.release) }); pipeline.Close() })
	pipeline.PrepareForLeaderSlot(slot)
	select {
	case <-builder.started:
	case <-time.After(time.Second):
		t.Fatal("reward worker did not start")
	}
	hooks := &recordingHooks{}
	loop, _, broadcaster, delivered := newFinishingLeaderLoop(t, slot, hooks,
		func(_ *ParentContext, in replay.CommitLeaderInput) error {
			if len(in.Block.SkipRewardCert) != 0 || len(in.Block.NotarRewardCert) != 0 {
				return errParentNotReady
			}
			return nil
		})
	loop.rewardCerts = pipeline
	finished := make(chan struct{})
	go func() { loop.finishActiveSlotLocked(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(time.Second):
		release.Do(func() { close(builder.release) })
		<-finished
		t.Fatal("block completion waited for reward preparation")
	}
	require.True(t, *delivered)
	require.Greater(t, broadcaster.count(), 0)
	require.Nil(t, loop.activeBank)
	release.Do(func() { close(builder.release) })
	pipeline.Close()
	require.Empty(t, pipeline.BuildForLeaderSlot(slot).Skip)
}
