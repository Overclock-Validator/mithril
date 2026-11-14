package replay

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/fixtures"
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/progtrace"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func readinessJobs(t *testing.T, buf *bytes.Buffer) map[uint64]progtrace.Job {
	t.Helper()
	out := make(map[uint64]progtrace.Job)
	sc := bufio.NewScanner(buf)
	for sc.Scan() {
		var job progtrace.Job
		require.NoError(t, json.Unmarshal(sc.Bytes(), &job))
		if job.Kind != "job" {
			continue
		}
		_, twice := out[job.Slot]
		require.False(t, twice, "slot %d recorded twice", job.Slot)
		out[job.Slot] = job
	}
	return out
}

// Every whole-block job is recorded with its outcome, including the ones that
// never ran: offered late, overtaken while queued, or still waiting at Close.
func TestReadinessRecordsEveryJobOutcome(t *testing.T) {
	var buf bytes.Buffer
	stop := progtrace.Start(&buf, 256, 1000)
	defer stop()
	src := &prefetchRecordingSource{accts: map[solana.PublicKey]*accounts.Account{}}
	p := newAccountPrefetcher(src, nil, 2, 0, false)
	tx := lookupTestTx(t)
	offer := func(slot uint64) { p.Offer(&b.Block{Slot: slot, Transactions: []*solana.Transaction{tx}}) }

	p.Started(10)
	offer(10) // replay already began it
	offer(12) // inside the window
	require.Eventually(t, func() bool { return p.warmed.Load() == 1 }, time.Second, time.Millisecond)
	offer(15) // waits for replay to reach 13
	time.Sleep(10 * time.Millisecond)
	waited := progtrace.Now()
	p.Started(13)
	require.Eventually(t, func() bool { return p.warmed.Load() == 2 }, time.Second, time.Millisecond)
	offer(20)
	p.Started(21) // overtaken while queued
	require.Eventually(t, func() bool { return p.late.Load() == 2 }, time.Second, time.Millisecond)
	offer(30) // beyond the window until Close
	time.Sleep(10 * time.Millisecond)
	p.Close()
	stop()

	jobs := readinessJobs(t, &buf)
	require.Len(t, jobs, 5)
	assert.Equal(t, "late_at_offer", jobs[10].Outcome)
	assert.Equal(t, "late_in_queue", jobs[20].Outcome)
	assert.Equal(t, "never_started", jobs[30].Outcome)
	for _, slot := range []uint64{10, 20, 30} {
		job := jobs[slot]
		assert.Zero(t, job.Started, slot)
		assert.Zero(t, job.ReadDone, slot)
		assert.Equal(t, "not_reached", job.Preload, slot)
		assert.NotZero(t, job.Requested, slot)
		assert.NotZero(t, job.ID, slot)
		assert.True(t, job.Whole, slot)
	}
	for _, slot := range []uint64{12, 15} {
		job := jobs[slot]
		assert.Equal(t, "finished", job.Outcome, slot)
		assert.Equal(t, "no_program_cache", job.Preload, slot)
		assert.True(t, 0 < job.Requested && job.Requested <= job.Started && job.Started <= job.ReadDone, "slot %d: %+v", slot, job)
		assert.Zero(t, job.PreloadDone, slot)
		assert.NotZero(t, job.Keys, slot)
	}
	assert.Equal(t, uint64(10), jobs[12].ReplayAt)
	assert.Equal(t, uint64(13), jobs[15].ReplayAt)
	assert.Greater(t, jobs[15].Started, waited, "the job began only once replay reached its window")
	ids := map[uint64]bool{}
	for _, job := range jobs {
		ids[job.ID] = true
	}
	assert.Len(t, ids, 5, "job ids are distinct")
}

// gatedSource holds every batch read until released.
type gatedSource struct {
	prefetchRecordingSource
	entered, release chan struct{}
}

func (s *gatedSource) GetAccountsBatch(ctx context.Context, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.prefetchRecordingSource.GetAccountsBatch(ctx, slot, pks)
}

// A job's read can finish on either side of replay beginning its slot; the
// timestamps place it, and a read cancelled by Close is recorded as cancelled.
func TestReadinessPlacesCompletionAroundTheTurn(t *testing.T) {
	run := func(turnDuringRead bool) (job progtrace.Job, turn int64) {
		var buf bytes.Buffer
		stop := progtrace.Start(&buf, 64, 1000)
		defer stop()
		src := &gatedSource{prefetchRecordingSource: prefetchRecordingSource{accts: map[solana.PublicKey]*accounts.Account{}}, entered: make(chan struct{}, 1), release: make(chan struct{})}
		p := newAccountPrefetcher(src, nil, 1, 0, false)
		p.Started(99)
		p.Offer(&b.Block{Slot: 100, Transactions: []*solana.Transaction{lookupTestTx(t)}})
		<-src.entered
		if turnDuringRead {
			p.Started(100)
			turn = progtrace.Now()
			time.Sleep(time.Millisecond)
		}
		close(src.release)
		require.Eventually(t, func() bool { return p.warmed.Load() == 1 }, time.Second, time.Millisecond)
		if !turnDuringRead {
			time.Sleep(time.Millisecond)
			p.Started(100)
			turn = progtrace.Now()
		}
		p.Close()
		stop()
		return readinessJobs(t, &buf)[100], turn
	}
	before, turn := run(false)
	assert.Equal(t, "finished", before.Outcome)
	assert.Less(t, before.ReadDone, turn, "read done before replay began the slot")
	crossed, turn := run(true)
	assert.Equal(t, "finished", crossed.Outcome)
	assert.Less(t, crossed.Started, turn)
	assert.Greater(t, crossed.ReadDone, turn, "read done after replay began the slot")

	var buf bytes.Buffer
	stop := progtrace.Start(&buf, 64, 1000)
	defer stop()
	src := &gatedSource{prefetchRecordingSource: prefetchRecordingSource{accts: map[solana.PublicKey]*accounts.Account{}}, entered: make(chan struct{}, 1), release: make(chan struct{})}
	p := newAccountPrefetcher(src, nil, 1, 0, false)
	p.Started(99)
	p.Offer(&b.Block{Slot: 100, Transactions: []*solana.Transaction{lookupTestTx(t)}})
	<-src.entered
	p.Close()
	stop()
	cancelled := readinessJobs(t, &buf)[100]
	assert.Equal(t, "cancelled", cancelled.Outcome)
	assert.NotZero(t, cancelled.Started)
	assert.Zero(t, cancelled.ReadDone)
}

// A job with a program cache records what preloading did: how many programs
// resolved, how many were already cached, and how many compiled and were
// admitted; the second job for the same block finds them cached.
func TestReadinessRecordsPreloadCounts(t *testing.T) {
	elf := fixtures.Load(t, "sbpf", "noop_aligned.so")
	w := newPreloadAccounts(t, elf)
	p, _ := preloadPrefetcher(&prefetchRecordingSource{accts: w.accts})
	var buf bytes.Buffer
	stop := progtrace.Start(&buf, 256, 1000)
	defer stop()
	p.warmJob(snapshotAccountPrefetchJob(&b.Block{Slot: 50, Transactions: []*solana.Transaction{w.tx(t)}}), true)
	p.warmJob(snapshotAccountPrefetchJob(&b.Block{Slot: 51, Transactions: []*solana.Transaction{w.tx(t)}}), true)
	stop()

	jobs := readinessJobs(t, &buf)
	first, second := jobs[50], jobs[51]
	assert.Equal(t, "done", first.Preload)
	assert.Positive(t, first.Compiled)
	assert.Equal(t, first.Compiled, first.Admitted)
	assert.Zero(t, first.Refused)
	assert.Equal(t, first.Programs, first.Compiled+first.Cached)
	assert.True(t, first.ReadDone <= first.PreloadDone && first.PreloadDone > 0)
	assert.Zero(t, second.Compiled)
	assert.Equal(t, first.Programs, second.Cached, "the second job compiles nothing")
	assert.NotEqual(t, first.ID, second.ID)
	assert.Equal(t, int(p.preloaded.Load()), first.Compiled, "the counter still counts compiles, not admissions")
}
