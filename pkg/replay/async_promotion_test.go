package replay

import (
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// slowCommitter delays each CommitBatch so tests can observe the in-flight
// window; it also records call times to prove the loop was never blocked.
type slowCommitter struct {
	fakeCommitter
	mu    sync.Mutex
	delay time.Duration
}

func (c *slowCommitter) CommitBatch(deltas []accounts.SlotDelta, throughSlot uint64, bankhashes map[uint64][32]byte, resumeCtx []byte) (accountsdb.BatchCommitResult, error) {
	time.Sleep(c.delay)
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fakeCommitter.CommitBatch(deltas, throughSlot, bankhashes, resumeCtx)
}

func asyncTestTail(committer batchCommitter, slots ...uint64) *unrootedTail {
	tail := newUnrootedTail(&fakeDurable{}, committer, 512, 2, "")
	for i, s := range slots {
		tail.Add(s, []*accounts.Account{testAccount(byte(i+1), s)}, testHashBytes(byte(s)))
		tail.SetContext(s, &state.ResumeContext{Slot: s})
	}
	return tail
}

// The build/run/apply split preserves the sync path's semantics: the overlay
// retains the chunk while the fold is in flight (reads stay correct) and
// drops it only at apply.
func TestAsyncFoldBuildRunApply(t *testing.T) {
	fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
	tail := asyncTestTail(fc, 5, 6, 7)

	job, err := tail.buildFoldJob(7, false)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, uint64(6), job.through, "one K=2 chunk: slots 5..6")

	// In-flight: overlay still holds everything (reads unaffected).
	assert.Equal(t, 3, tail.overlay.HeldSlots())

	require.NoError(t, runFoldJob(fc, job))
	assert.Equal(t, []uint64{5, 6}, fc.committed)
	// Committed but not applied: overlay STILL holds the chunk.
	assert.Equal(t, 3, tail.overlay.HeldSlots())

	ctx := tail.applyFoldJob(job)
	require.NotNil(t, ctx)
	assert.Equal(t, uint64(6), ctx.Slot)
	assert.Equal(t, 1, tail.overlay.HeldSlots(), "only slot 7 remains")
	assert.Empty(t, tail.bankhashes[uint64(5)])
	_, has5 := tail.contexts[5]
	assert.False(t, has5, "contexts pruned through the fold")
}

// A partial trailing chunk builds only under force (the shutdown flush).
func TestBuildFoldJobPartialChunkOnlyWhenForced(t *testing.T) {
	fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
	tail := asyncTestTail(fc, 5) // one slot < K=2

	job, err := tail.buildFoldJob(5, false)
	require.NoError(t, err)
	assert.Nil(t, job, "partial chunk must stay in RAM without force")

	job, err = tail.buildFoldJob(5, true)
	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, uint64(5), job.through)
}

// A context-less chunk-top refuses to build: committing it would produce a
// manifest recovery cannot resume from.
func TestBuildFoldJobRefusesMissingContext(t *testing.T) {
	fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
	tail := newUnrootedTail(&fakeDurable{}, fc, 512, 2, "")
	tail.Add(5, []*accounts.Account{testAccount(1, 5)}, testHashBytes(5))
	tail.Add(6, []*accounts.Account{testAccount(2, 6)}, testHashBytes(6))
	tail.SetContext(5, &state.ResumeContext{Slot: 5}) // 6 (chunk top) missing

	job, err := tail.buildFoldJob(6, false)
	require.Error(t, err)
	assert.Nil(t, job)
	assert.Equal(t, 2, tail.overlay.HeldSlots(), "nothing folds on a refused build")
}

// The promoter runs jobs off-thread: enqueue returns immediately, poll is
// non-blocking during the commit, and drain settles the in-flight job.
func TestAsyncPromoterOffLoopAndDrain(t *testing.T) {
	sc := &slowCommitter{fakeCommitter: fakeCommitter{durable: accounts.NewMemAccounts()}, delay: 60 * time.Millisecond}
	tail := asyncTestTail(sc, 5, 6, 7)
	p := newAsyncPromoter(sc)
	defer p.stop()

	job, err := tail.buildFoldJob(7, false)
	require.NoError(t, err)
	require.NotNil(t, job)

	start := time.Now()
	p.enqueue(job)
	require.Less(t, time.Since(start), 20*time.Millisecond, "enqueue must not block on the commit")

	// While the worker commits, the loop keeps going: poll stays nil.
	assert.Nil(t, p.poll(), "poll must be non-blocking while the fold runs")
	assert.True(t, p.inFlight)

	// Drain blocks until completion — the fork-unwind / shutdown barrier.
	res := p.drain()
	require.NotNil(t, res)
	require.NoError(t, res.err)
	assert.Equal(t, uint64(6), res.job.through)
	assert.False(t, p.inFlight)
	assert.GreaterOrEqual(t, time.Since(start), sc.delay, "drain waited for the worker")

	ctx := tail.applyFoldJob(res.job)
	require.NotNil(t, ctx)
	assert.Equal(t, 1, tail.overlay.HeldSlots())
}

// A failed fold leaves the tail untouched (natural retry: the same chunk
// rebuilds because nothing advanced) and reports the error via the result.
func TestAsyncPromoterFailedFoldRetries(t *testing.T) {
	fc := &fakeCommitter{durable: accounts.NewMemAccounts(), failOn: 5}
	tail := asyncTestTail(fc, 5, 6, 7)
	p := newAsyncPromoter(fc)
	defer p.stop()

	job, err := tail.buildFoldJob(7, false)
	require.NoError(t, err)
	p.enqueue(job)
	res := p.drain()
	require.NotNil(t, res)
	require.Error(t, res.err)
	assert.Equal(t, 3, tail.overlay.HeldSlots(), "failed fold must not touch the overlay")

	// Retry after the fault clears: identical chunk rebuilds and succeeds.
	fc.failOn = 0
	job2, err := tail.buildFoldJob(7, false)
	require.NoError(t, err)
	require.Equal(t, job.through, job2.through, "same chunk rebuilds after a failed fold")
	p.enqueue(job2)
	res = p.drain()
	require.NoError(t, res.err)
	tail.applyFoldJob(res.job)
	assert.Equal(t, 1, tail.overlay.HeldSlots())
}

// Codex item 5 — the shutdown gate proof: the flush path folds THROUGH THE
// GATE-DERIVED TARGET AND NO FURTHER, even under force. Slots above
// min(finality, verified) stay in RAM no matter how the process exits.
func TestShutdownFlushCannotFoldPastGateTarget(t *testing.T) {
	fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
	tail := asyncTestTail(fc, 5, 6, 7, 8, 9)

	// The shared gate: finality says 9, the verifier has only reached 7.
	target := safePromoteTarget(9, true, 7, 0)
	require.Equal(t, uint64(7), target)

	promoted, ctx, err := tail.flush(target)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), promoted, "force-flush stops exactly at the gate target")
	require.NotNil(t, ctx)
	assert.Equal(t, uint64(7), ctx.Slot)
	assert.Equal(t, []uint64{5, 6, 7}, fc.committed)
	assert.Equal(t, 2, tail.overlay.HeldSlots(), "unverified slots 8,9 must survive shutdown in RAM")

	// And the divergence floor clamps even harder than the verifier.
	target = safePromoteTarget(9, true, 7, 6)
	assert.Equal(t, uint64(5), target, "persisted-divergence floor holds promotion below the disputed slot")
}
