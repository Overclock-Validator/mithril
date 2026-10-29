package replay

import (
	"errors"
	"testing"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/rewardcerts"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/stretchr/testify/require"
)

func injectFinalCertPrefetch(slot uint64, raw []byte, epoch uint64, material *voteRewardVerifierMaterial, validated *rewardcerts.ValidatedFinalCert, err error) {
	p := &finalCertPrefetch{slot: slot, raw: raw, epoch: epoch, done: make(chan struct{}), material: material, validated: validated, err: err}
	close(p.done)
	pendingFinalCert.Store(p)
}

// The reward step may use a background validation only for exactly its own
// inputs and only when it succeeded; it is consumed once.
func TestFinalCertPrefetchMatchesOnlyIdenticalInputs(t *testing.T) {
	t.Cleanup(func() { pendingFinalCert.Store(nil) })
	material, otherMaterial := &voteRewardVerifierMaterial{}, &voteRewardVerifierMaterial{}
	raw := []byte{1, 2, 3}
	validated := &rewardcerts.ValidatedFinalCert{FinalSlot: 99}

	injectFinalCertPrefetch(100, raw, 7, material, validated, nil)
	got, ok := takeFinalCertPrefetch(100, []byte{1, 2, 3}, 7, material)
	require.True(t, ok)
	require.Same(t, validated, got)
	_, ok = takeFinalCertPrefetch(100, raw, 7, material)
	require.False(t, ok, "a prefetch is consumed once")

	for name, take := range map[string]func() bool{
		"slot":     func() bool { _, ok := takeFinalCertPrefetch(101, raw, 7, material); return ok },
		"bytes":    func() bool { _, ok := takeFinalCertPrefetch(100, []byte{1, 2, 4}, 7, material); return ok },
		"epoch":    func() bool { _, ok := takeFinalCertPrefetch(100, raw, 8, material); return ok },
		"material": func() bool { _, ok := takeFinalCertPrefetch(100, raw, 7, otherMaterial); return ok },
	} {
		injectFinalCertPrefetch(100, raw, 7, material, validated, nil)
		require.False(t, take(), "different %s must validate synchronously", name)
	}

	// A failed background validation is never reused: the reward step validates
	// itself and reports its own error.
	injectFinalCertPrefetch(100, raw, 7, material, nil, errors.New("invalid signature"))
	_, ok = takeFinalCertPrefetch(100, raw, 7, material)
	require.False(t, ok)
	require.Nil(t, pendingFinalCert.Load())

	pendingFinalCert.Store(nil)
	_, ok = takeFinalCertPrefetch(100, raw, 7, material)
	require.False(t, ok)
}

// A flood of candidates must not start one background BLS validation each:
// while one runs, later candidates get no prefetch (the reward step validates
// synchronously), and the next candidate after it finishes starts one again.
func TestFinalCertPrefetchRunsOneValidationAtATime(t *testing.T) {
	t.Cleanup(func() {
		if p := pendingFinalCert.Load(); p != nil {
			<-p.done
		}
		pendingFinalCert.Store(nil)
		finalCertPrefetchRunning.Store(false)
	})
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, LeaderScheduleSlotOffset: 432000}
	candidate := func(slot uint64) *b.Block {
		// The smallest decodable final certificate: zero signature, empty bitmap, no notar aggregate.
		return &b.Block{Slot: slot, BlockFinalCert: make([]byte, 8+32+96+2+1)}
	}

	finalCertPrefetchRunning.Store(true) // the validation for slot 99 is still running
	material := &voteRewardVerifierMaterial{}
	running := &rewardcerts.ValidatedFinalCert{FinalSlot: 99}
	injectFinalCertPrefetch(99, []byte{1}, 0, material, running, nil)
	startFinalCertPrefetch(candidate(100), schedule, 0)
	require.True(t, finalCertPrefetchRunning.Load())
	_, ok := takeFinalCertPrefetch(100, candidate(100).BlockFinalCert, 0, material)
	require.False(t, ok, "a candidate arriving while busy gets no prefetch")
	got, ok := takeFinalCertPrefetch(99, []byte{1}, 0, material)
	require.True(t, ok, "the running validation stays available to its own block")
	require.Same(t, running, got)

	finalCertPrefetchRunning.Store(false)
	startFinalCertPrefetch(candidate(101), schedule, 0)
	p := pendingFinalCert.Load()
	require.NotNil(t, p)
	require.EqualValues(t, 101, p.slot)
	<-p.done
	require.Eventually(t, func() bool { return !finalCertPrefetchRunning.Load() }, 5*time.Second, time.Millisecond,
		"a finished validation frees the slot for the next candidate")
}
