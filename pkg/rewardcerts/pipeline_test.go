package rewardcerts

import (
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type pipelineBuilderFunc func(uint64) RewardCertificates

func (f pipelineBuilderFunc) BuildForLeaderSlot(slot uint64) RewardCertificates { return f(slot) }

func awaitPipelineSlot(t *testing.T, started <-chan uint64) uint64 {
	t.Helper()
	select {
	case slot := <-started:
		return slot
	case <-time.After(5 * time.Second):
		t.Fatal("reward worker did not start")
		return 0
	}
}

func awaitPipelineReady(t *testing.T, p *Pipeline, slot uint64) {
	t.Helper()
	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		work := p.work[slot]
		return work != nil && work.status.Ready
	}, 5*time.Second, time.Millisecond)
}

func TestPipelineFooterDoesNotWaitAndDiscardsLateResult(t *testing.T) {
	started := make(chan uint64, 8)
	blocked := make(chan struct{})
	var unblock sync.Once
	p := NewPipeline(pipelineBuilderFunc(func(slot uint64) RewardCertificates {
		started <- slot
		if slot == 100 {
			<-blocked
		}
		return RewardCertificates{Skip: []byte{byte(slot)}}
	}))
	t.Cleanup(func() { unblock.Do(func() { close(blocked) }); p.Close() })
	p.PrepareForLeaderSlot(100)
	require.Equal(t, uint64(100), awaitPipelineSlot(t, started))
	for range 20 {
		p.PrepareForLeaderSlot(100)
	}
	consumed := make(chan RewardCertificates, 1)
	go func() { consumed <- p.BuildForLeaderSlot(100) }()
	select {
	case certs := <-consumed:
		require.Empty(t, certs.Skip)
	case <-time.After(time.Second):
		t.Fatal("footer waited for the blocked builder")
	}
	p.PrepareForLeaderSlot(100) // An expired request must not consume votes again.
	p.PrepareForLeaderSlot(101)
	unblock.Do(func() { close(blocked) })
	require.Equal(t, uint64(101), awaitPipelineSlot(t, started))
	awaitPipelineReady(t, p, 101)
	certs, status := p.TakeForLeaderSlot(100)
	require.False(t, status.Ready, "late slot 100 result must be discarded")
	require.Empty(t, certs.Skip)
	certs, status = p.TakeForLeaderSlot(101)
	require.True(t, status.Ready)
	require.Equal(t, []byte{101}, certs.Skip, "never reuse another slot's result")
	require.Positive(t, status.BuildDuration)
	require.Empty(t, p.BuildForLeaderSlot(101).Skip, "results transfer only once")
	p.Close()
	require.Empty(t, started, "duplicate requests must never rebuild")
}

func TestPipelineBoundsAbandonedWorkAndCloseDropsQueue(t *testing.T) {
	started := make(chan uint64, 32)
	blocked := make(chan struct{})
	var unblock sync.Once
	p := NewPipeline(pipelineBuilderFunc(func(slot uint64) RewardCertificates {
		started <- slot
		<-blocked
		return RewardCertificates{Notar: []byte{1}}
	}))
	t.Cleanup(func() { unblock.Do(func() { close(blocked) }); p.Close() })
	p.PrepareForLeaderSlot(100)
	require.Equal(t, uint64(100), awaitPipelineSlot(t, started))
	for slot := uint64(101); slot <= 10_000; slot++ {
		p.PrepareForLeaderSlot(slot)
		p.mu.Lock()
		retained := len(p.work)
		p.mu.Unlock()
		require.LessOrEqual(t, retained, pipelineRetainedSlots)
	}
	p.mu.Lock()
	require.Len(t, p.work, pipelineRetainedSlots)
	require.NotContains(t, p.work, uint64(100))
	p.mu.Unlock()
	closed := make(chan struct{})
	go func() { p.Close(); close(closed) }()
	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.closed && len(p.work) == 0
	}, time.Second, time.Millisecond)
	select {
	case <-closed:
		t.Fatal("Close returned while the builder was still running")
	default:
	}
	p.PrepareForLeaderSlot(10_001)
	unblock.Do(func() { close(blocked) })
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not join the completed worker")
	}
	require.Empty(t, started, "Close must discard queued builds")
	require.Empty(t, p.BuildForLeaderSlot(10_000).Notar)
	p.Close() // Idempotent, including a worker already joined.
}

func TestPipelineOwnsPreparedBuffersAndBuildsInOrder(t *testing.T) {
	started := make(chan uint64, 8)
	blocked := make(chan struct{})
	var unblock sync.Once
	skip, notar := []byte{1}, []byte{2}
	p := NewPipeline(pipelineBuilderFunc(func(slot uint64) RewardCertificates {
		started <- slot
		if slot == 100 {
			<-blocked
		}
		return RewardCertificates{Skip: skip, Notar: notar}
	}))
	t.Cleanup(func() { unblock.Do(func() { close(blocked) }); p.Close() })
	p.PrepareForLeaderSlot(100)
	require.Equal(t, uint64(100), awaitPipelineSlot(t, started))
	// Requests that have not started are sorted, even if queued out of order.
	p.PrepareForLeaderSlot(103)
	p.PrepareForLeaderSlot(102)
	p.PrepareForLeaderSlot(101)
	unblock.Do(func() { close(blocked) })
	for _, slot := range []uint64{101, 102, 103} {
		require.Equal(t, slot, awaitPipelineSlot(t, started))
	}
	awaitPipelineReady(t, p, 103)
	skip[0], notar[0] = 9, 9 // Worker has finished copying all returned buffers.
	for _, slot := range []uint64{100, 101, 102, 103} {
		certs, status := p.TakeForLeaderSlot(slot)
		require.True(t, status.Ready)
		require.Equal(t, []byte{1}, certs.Skip)
		require.Equal(t, []byte{2}, certs.Notar)
		certs.Skip[0] = 5 // Caller mutation cannot affect another prepared result.
	}
	p.PrepareForLeaderSlot(101)
	p.Close()
	require.Empty(t, started)
}

func TestPipelinePreparedEmptyResultIsReady(t *testing.T) {
	p := NewPipeline(pipelineBuilderFunc(func(uint64) RewardCertificates { return RewardCertificates{} }))
	t.Cleanup(p.Close)
	p.PrepareForLeaderSlot(100)
	awaitPipelineReady(t, p, 100)
	certs, status := p.TakeForLeaderSlot(100)
	require.True(t, status.Ready)
	require.Empty(t, certs.Skip)
	require.Empty(t, certs.Notar)
	_, status = p.TakeForLeaderSlot(101)
	require.False(t, status.Ready)
}

func TestPipelineFlushesVerifiedVotesIntoValidRewardCertificates(t *testing.T) {
	const rewardSlot = uint64(123)
	const leaderSlot = rewardSlot + SlotsForReward
	keys := testBLSKeys(t, 4)
	blockID := solana.Hash{9}
	skipVotes := []alpenglow.VoteMessage{
		testSignedVote(t, alpenglow.NewSkipVote(rewardSlot), 0, keys[0]),
		testSignedVote(t, alpenglow.NewSkipVote(rewardSlot), 1, keys[1]),
	}
	notarVotes := []alpenglow.VoteMessage{
		testSignedVote(t, alpenglow.NewNotarizationVote(rewardSlot, blockID), 2, keys[2]),
		testSignedVote(t, alpenglow.NewNotarizationVote(rewardSlot, blockID), 3, keys[3]),
	}
	votes := append(append([]alpenglow.VoteMessage(nil), skipVotes...), notarVotes...)
	set, _ := testValidatorSetForVotes(t, votes)
	verifier := alpenglow.NewCertificateVerifier()
	require.NoError(t, verifier.SetValidatorSet(set))
	verifier.SetShredVersion(0)
	verified := make([]alpenglow.VerifiedVote, len(votes))
	for index, vote := range votes {
		result, err := verifier.VerifyVoteMessageForEpoch(set.Epoch, vote)
		require.NoError(t, err)
		verified[index] = alpenglow.VerifiedVote{Message: vote, Result: result}
		_, trusted := verified[index].VerifiedSignature()
		require.True(t, trusted)
	}
	flushed := make(chan uint64, 2)
	var builder *Builder
	builder = NewBuilder(BuilderConfig{MaxValidators: 4, BeforeBuild: func(slot uint64) {
		flushed <- slot
		for _, vote := range verified {
			builder.AddVerifiedVote(vote)
		}
	}})
	pipeline := NewPipeline(builder)
	t.Cleanup(pipeline.Close)
	pipeline.PrepareForLeaderSlot(leaderSlot)
	awaitPipelineReady(t, pipeline, leaderSlot)
	require.Equal(t, rewardSlot, awaitPipelineSlot(t, flushed), "lazy flush must target exactly N-8")
	certs, status := pipeline.TakeForLeaderSlot(leaderSlot)
	require.True(t, status.Ready)
	skip, err := DecodeSkipRewardCertificate(certs.Skip)
	require.NoError(t, err)
	require.Equal(t, rewardSlot, skip.Slot)
	assertBitmapRanks(t, skip.Bitmap, 4, 0, 1)
	assertAggregateMatchesVotes(t, skip.Signature[:], skipVotes)
	notar, err := DecodeNotarRewardCertificate(certs.Notar)
	require.NoError(t, err)
	require.Equal(t, rewardSlot, notar.Slot)
	require.Equal(t, blockID, notar.BlockID)
	assertBitmapRanks(t, notar.Bitmap, 4, 2, 3)
	assertAggregateMatchesVotes(t, notar.Signature[:], notarVotes)
	pipeline.PrepareForLeaderSlot(leaderSlot)
	pipeline.Close()
	require.Empty(t, flushed, "consuming the footer must not trigger another lazy flush")
	require.Empty(t, pipeline.BuildForLeaderSlot(leaderSlot).Skip)
}

func TestPipelineConcurrentPreparationConsumptionAndClose(t *testing.T) {
	var callsMu sync.Mutex
	calls := make(map[uint64]int)
	p := NewPipeline(pipelineBuilderFunc(func(slot uint64) RewardCertificates {
		callsMu.Lock()
		calls[slot]++
		callsMu.Unlock()
		return RewardCertificates{Skip: []byte{byte(slot)}}
	}))
	var workers sync.WaitGroup
	for worker := range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for slot := uint64(100); slot < 500; slot++ {
				p.PrepareForLeaderSlot(slot)
				if worker%2 == 0 {
					p.BuildForLeaderSlot(slot)
				}
			}
			p.Close()
		}()
	}
	workers.Wait()
	for slot, count := range calls {
		require.Equalf(t, 1, count, "slot %d was built twice", slot)
	}
	p.mu.Lock()
	require.Empty(t, p.work)
	p.mu.Unlock()
}
