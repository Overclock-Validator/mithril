package replay

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/stretchr/testify/require"
)

var rewardCertPrefetchTestSchedule = &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, LeaderScheduleSlotOffset: 432000}

func rewardCertCandidate(slot uint64, skip, notar byte) *b.Block {
	return &b.Block{Slot: slot, SkipRewardCert: []byte{skip}, NotarRewardCert: []byte{notar}}
}

// testRewardCertPrefetcher validates with result, counting calls, and blocks
// each validation until release is closed when release is not nil.
func testRewardCertPrefetcher(material *voteRewardVerifierMaterial, err error, release chan struct{}) (*rewardCertPrefetcher, *atomic.Int32, chan uint64) {
	c := newRewardCertPrefetcher()
	calls := new(atomic.Int32)
	started := make(chan uint64, rewardCertPrefetchKeep)
	c.validate = func(p *rewardCertPrefetch) {
		calls.Add(1)
		started <- p.slot
		if release != nil {
			<-release
		}
		p.material, p.err = material, err
		if err == nil {
			p.validated = &rewardcerts.ValidatedRewardCert{RewardSlot: p.slot}
		}
	}
	return c, calls, started
}

// The preflight may use a background validation only for exactly its own
// inputs and only when it succeeded; it is consumed once.
func TestRewardCertPrefetchMatchesOnlyIdenticalInputs(t *testing.T) {
	material, otherMaterial := &voteRewardVerifierMaterial{}, &voteRewardVerifierMaterial{}
	c, _, started := testRewardCertPrefetcher(material, nil, nil)

	offerAndWait := func(slot uint64) {
		c.offer(rewardCertCandidate(slot, 1, 2), rewardCertPrefetchTestSchedule)
		require.Equal(t, slot, <-started)
		c.mu.Lock()
		p := c.bySlot[slot]
		c.mu.Unlock()
		<-p.done
	}

	offerAndWait(1000)
	got, ok := c.take(1000, []byte{1}, []byte{2}, material)
	require.True(t, ok)
	require.EqualValues(t, 1000, got.RewardSlot)
	_, ok = c.take(1000, []byte{1}, []byte{2}, material)
	require.False(t, ok, "a prefetch is consumed once")

	for name, take := range map[string]func() bool{
		"slot":     func() bool { _, ok := c.take(1002, []byte{1}, []byte{2}, material); return ok },
		"skip":     func() bool { _, ok := c.take(1001, []byte{9}, []byte{2}, material); return ok },
		"notar":    func() bool { _, ok := c.take(1001, []byte{1}, []byte{9}, material); return ok },
		"material": func() bool { _, ok := c.take(1001, []byte{1}, []byte{2}, otherMaterial); return ok },
	} {
		offerAndWait(1001)
		require.False(t, take(), "different %s must validate synchronously", name)
	}

	// A failed background validation is never reused: the caller validates
	// itself and reports its own error.
	failing, _, failedStarted := testRewardCertPrefetcher(material, errors.New("invalid signature"), nil)
	failing.offer(rewardCertCandidate(1000, 1, 2), rewardCertPrefetchTestSchedule)
	<-failedStarted
	_, ok = failing.take(1000, []byte{1}, []byte{2}, material)
	require.False(t, ok)
}

// A caller waits for a validation that is already running and never for one
// still queued behind other blocks: that one is abandoned and never runs.
func TestRewardCertPrefetchWaitsOnlyForRunningValidation(t *testing.T) {
	material := &voteRewardVerifierMaterial{}
	release := make(chan struct{})
	c, calls, started := testRewardCertPrefetcher(material, nil, release)

	c.offer(rewardCertCandidate(1000, 1, 2), rewardCertPrefetchTestSchedule)
	c.offer(rewardCertCandidate(1001, 1, 2), rewardCertPrefetchTestSchedule)
	require.EqualValues(t, 1000, <-started)

	_, ok := c.take(1001, []byte{1}, []byte{2}, material)
	require.False(t, ok, "a queued validation is not waited for")

	taken := make(chan bool, 1)
	go func() {
		_, ok := c.take(1000, []byte{1}, []byte{2}, material)
		taken <- ok
	}()
	select {
	case <-taken:
		t.Fatal("take returned before the running validation finished")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	require.True(t, <-taken)

	// The abandoned entry leaves the queue without being validated.
	c.offer(rewardCertCandidate(1002, 1, 2), rewardCertPrefetchTestSchedule)
	require.EqualValues(t, 1002, <-started)
	require.EqualValues(t, 2, calls.Load())
}

// Blocks without reward certificates are ignored, a later candidate for a slot
// replaces the earlier one, and the table and queue stay bounded without ever
// blocking the caller.
func TestRewardCertPrefetchOffersAreBounded(t *testing.T) {
	material := &voteRewardVerifierMaterial{}
	release := make(chan struct{})
	c, _, started := testRewardCertPrefetcher(material, nil, release)
	defer close(release)

	c.offer(nil, rewardCertPrefetchTestSchedule)
	c.offer(&b.Block{Slot: 1000}, rewardCertPrefetchTestSchedule)
	c.offer(&b.Block{Slot: 1000, IsSkipped: true, SkipRewardCert: []byte{1}}, rewardCertPrefetchTestSchedule)
	c.offer(rewardCertCandidate(1000, 1, 2), nil)
	require.Empty(t, c.bySlot)

	c.offer(rewardCertCandidate(2000, 1, 2), rewardCertPrefetchTestSchedule)
	require.EqualValues(t, 2000, <-started) // the worker is now held in this validation
	c.offer(rewardCertCandidate(2001, 1, 2), rewardCertPrefetchTestSchedule)
	c.offer(rewardCertCandidate(2001, 7, 2), rewardCertPrefetchTestSchedule)
	_, ok := c.take(2001, []byte{1}, []byte{2}, material)
	require.False(t, ok, "the replaced candidate is gone")
	require.Contains(t, c.bySlot, uint64(2001), "a mismatch leaves the entry for its own candidate")

	done := make(chan struct{})
	go func() {
		defer close(done)
		for slot := uint64(3000); slot < 3000+4*rewardCertPrefetchKeep; slot++ {
			c.offer(rewardCertCandidate(slot, 1, 2), rewardCertPrefetchTestSchedule)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("offer blocked while the worker was busy")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	require.LessOrEqual(t, len(c.bySlot), rewardCertPrefetchKeep)
	require.Equal(t, len(c.bySlot), len(c.order))
}
