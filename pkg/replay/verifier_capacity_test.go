package replay

import (
	"context"
	"encoding/base64"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/stretchr/testify/require"
)

func TestVerifierCapacityWaitProgressRelievesRealTail(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
		tail := newUnrootedTail(&fakeDurable{}, fc, 32, 32, "")
		for slot := uint64(1); slot <= 33; slot++ {
			tail.Add(slot, nil, testHashBytes(byte(slot)))
			tail.SetContext(slot, &state.ResumeContext{Slot: slot})
		}
		p := newAsyncPromoter(fc)
		defer p.stop()
		started := time.Now()
		verified := uint64(0)
		over, err := waitForVerifierProgress(context.Background(), 30*time.Second, 120*time.Second, func() uint64 { return verified }, func() (bool, error) {
			verified = uint64(time.Since(started) / (2 * time.Second))
			return settleFoldsAtCapacity(tail, p, 33, func(res *foldResult) {
				if res != nil && res.err == nil {
					tail.applyFoldJob(res.job)
				}
			}, func() bool {
				job, err := tail.buildFoldJob(verified, false)
				require.NoError(t, err)
				if job != nil {
					require.NoError(t, p.enqueue(job))
				}
				return false
			}), p.failure
		})
		require.NoError(t, err)
		require.False(t, over)
		require.Equal(t, 1, tail.overlay.HeldSlots())
		require.NotNil(t, tail.contexts[33])
		require.NotEmpty(t, fc.committed)
		require.GreaterOrEqual(t, time.Since(started), 64*time.Second)
	})
}

func TestVerifierCapacityWaitRelievesRealTail(t *testing.T) {
	fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
	tail := newUnrootedTail(&fakeDurable{}, fc, 2, 2, "")
	for slot := uint64(1); slot <= 3; slot++ {
		tail.Add(slot, nil, testHashBytes(byte(slot)))
		tail.SetContext(slot, &state.ResumeContext{Slot: slot})
	}
	p := newAsyncPromoter(fc)
	defer p.stop()
	verified := uint64(0)
	admit := func() bool {
		job, err := tail.buildFoldJob(verified, false)
		require.NoError(t, err)
		if job != nil {
			require.NoError(t, p.enqueue(job))
		}
		return false
	}
	apply := func(res *foldResult) {
		if res != nil && res.err == nil {
			tail.applyFoldJob(res.job)
		}
	}
	require.True(t, settleFoldsAtCapacity(tail, p, 3, apply, admit), "upstream seam stops before verification advances")
	calls := 0
	over, err := waitForVerifierProgress(context.Background(), time.Second, 4*time.Second, func() uint64 { return verified }, func() (bool, error) {
		calls++
		if calls == 2 {
			verified = 2
		}
		return settleFoldsAtCapacity(tail, p, 3, apply, admit), p.failure
	})
	require.NoError(t, err)
	require.False(t, over)
	require.Equal(t, 1, tail.overlay.HeldSlots())
	require.NotNil(t, tail.contexts[3])
	require.NotEmpty(t, fc.committed)
}

func TestVerifierCapacityWaitStops(t *testing.T) {
	t.Run("timeout", func(t *testing.T) {
		over, err := waitForVerifierProgress(context.Background(), time.Millisecond, time.Second, func() uint64 { return 0 }, func() (bool, error) { return true, nil })
		require.True(t, over)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
	t.Run("cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		over, err := waitForVerifierProgress(ctx, time.Second, 4*time.Second, func() uint64 { return 0 }, func() (bool, error) { t.Fatal("settled after cancellation"); return false, nil })
		require.True(t, over)
		require.ErrorIs(t, err, context.Canceled)
	})
	t.Run("safety or storage failure wins", func(t *testing.T) {
		failure := errors.New("fold rejected")
		_, err := waitForVerifierProgress(context.Background(), time.Second, 4*time.Second, func() uint64 { return 0 }, func() (bool, error) { return false, failure })
		require.ErrorIs(t, err, failure)
	})
}

func TestVerifierCapacityWaitProgressBounds(t *testing.T) {
	t.Run("blocked watermark cannot revive expired idle", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			started := time.Now()
			calls := 0
			over, err := waitForVerifierProgress(context.Background(), 30*time.Second, 120*time.Second,
				func() uint64 {
					calls++
					if calls == 1 {
						return 0
					}
					time.Sleep(31 * time.Second)
					return 1
				}, func() (bool, error) { return true, nil })
			require.True(t, over)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Equal(t, 31*time.Second, time.Since(started))
		})
	})
	for _, relieved := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			started := time.Now()
			verified := uint64(0)
			over, err := waitForVerifierProgress(context.Background(), 30*time.Second, 120*time.Second,
				func() uint64 { return verified }, func() (bool, error) {
					time.Sleep(31 * time.Second)
					verified++
					return !relieved, nil
				})
			require.Equal(t, !relieved, over)
			if relieved {
				require.NoError(t, err, "finished durable write may relieve pressure after idle expiry")
			} else {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			}
			require.Equal(t, 31*time.Second, time.Since(started))
		})
	}
	for _, progressAt := range []time.Duration{30 * time.Second, 30*time.Second + 25*time.Millisecond} {
		t.Run("expired idle cannot be revived/"+progressAt.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				started := time.Now()
				over, err := waitForVerifierProgress(context.Background(), 30*time.Second, 120*time.Second,
					func() uint64 {
						if time.Since(started) >= progressAt {
							return 1
						}
						return 0
					}, func() (bool, error) { return true, nil })
				require.True(t, over)
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Equal(t, 30*time.Second, time.Since(started))
			})
		})
	}
	t.Run("absolute deadline despite progress", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			started := time.Now()
			over, err := waitForVerifierProgress(context.Background(), 30*time.Second, 120*time.Second,
				func() uint64 { return uint64(time.Since(started) / time.Second) },
				func() (bool, error) { return true, nil })
			require.True(t, over)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Equal(t, 120*time.Second, time.Since(started))
		})
	})
	t.Run("out of order completions do not extend idle deadline", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			v := &TrailingVerifier{pending: make(map[uint64]*pendingDigest)}
			for slot := uint64(1); slot <= 40; slot++ {
				v.order = append(v.order, slot)
				v.pending[slot] = &pendingDigest{}
			}
			started := time.Now()
			over, err := waitForVerifierProgress(context.Background(), 30*time.Second, 120*time.Second,
				v.VerifiedWatermark, func() (bool, error) {
					v.mu.Lock()
					v.markVerifiedLocked(2 + uint64(time.Since(started)/time.Second))
					v.mu.Unlock()
					return true, nil
				})
			require.True(t, over)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Zero(t, v.VerifiedWatermark())
			require.Equal(t, 30*time.Second, time.Since(started))
		})
	})
}

func TestVerifierCapacityBatchUsesRetainedBanks(t *testing.T) {
	fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
	tail := newUnrootedTail(&fakeDurable{}, fc, 2, 2, "")
	tail.Add(1, nil, testHashBytes(1))
	tail.SetContext(1, &state.ResumeContext{Slot: 1})
	require.False(t, verifierCapacityBatch(tail, 100), "skipped slot distance cannot form a full batch")
	tail.Add(100, nil, testHashBytes(2))
	require.False(t, verifierCapacityBatch(tail, 100), "missing chunk-top context must refuse waiting")
	tail.SetContext(100, &state.ResumeContext{Slot: 100})
	require.False(t, verifierCapacityBatch(tail, 99), "finality or lag bound cannot include newer bank")
	require.True(t, verifierCapacityBatch(tail, 100))
}

func TestVerifierCapacityWaitPreservesCommitFailure(t *testing.T) {
	fc := &fakeCommitter{durable: accounts.NewMemAccounts(), failOn: 1}
	tail := newUnrootedTail(&fakeDurable{}, fc, 2, 2, "")
	for slot := uint64(1); slot <= 3; slot++ {
		tail.Add(slot, nil, testHashBytes(byte(slot)))
		tail.SetContext(slot, &state.ResumeContext{Slot: slot})
	}
	p := newAsyncPromoter(fc)
	defer p.stop()
	over, err := waitForVerifierProgress(context.Background(), time.Second, 4*time.Second, func() uint64 { return 2 }, func() (bool, error) {
		over := settleFoldsAtCapacity(tail, p, 3, func(res *foldResult) {
			if res != nil && res.err == nil {
				tail.applyFoldJob(res.job)
			}
		}, func() bool {
			job, err := tail.buildFoldJob(2, false)
			require.NoError(t, err)
			require.NoError(t, p.enqueue(job))
			return false
		})
		return over, p.failure
	})
	require.Error(t, err)
	require.True(t, over)
	require.Equal(t, 3, tail.overlay.HeldSlots())
	require.Empty(t, fc.committed)
}

func TestVerifierCapacityProgressBeforeEligibility(t *testing.T) {
	fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
	tail := newUnrootedTail(&fakeDurable{}, fc, 2, 2, "")
	for slot := uint64(1); slot <= 3; slot++ {
		tail.Add(slot, nil, testHashBytes(byte(slot)))
		tail.SetContext(slot, &state.ResumeContext{Slot: slot})
	}
	p := newAsyncPromoter(fc)
	defer p.stop()
	verified := uint64(0)
	settle := func() (bool, error) {
		over := settleFoldsAtCapacity(tail, p, 3, func(res *foldResult) {
			if res != nil && res.err == nil {
				tail.applyFoldJob(res.job)
			}
		}, func() bool {
			job, err := tail.buildFoldJob(verified, false)
			require.NoError(t, err)
			if job != nil {
				require.NoError(t, p.enqueue(job))
			}
			return false
		})
		return over, p.failure
	}
	over, err := settle()
	require.NoError(t, err)
	require.True(t, over)
	// A verifier worker completes the batch between the initial settlement
	// attempt and the eligibility check on the replay goroutine.
	verified = 2
	require.True(t, verifierCapacityBatch(tail, 2))
	over, err = waitForVerifierProgress(context.Background(), time.Second, 4*time.Second, func() uint64 { return verified }, settle)
	require.NoError(t, err)
	require.False(t, over)
	require.Equal(t, 1, tail.overlay.HeldSlots())
}

func TestVerifierCapacityWaitRechecksSafety(t *testing.T) {
	for _, kind := range []string{"certificate conflict", "execution divergence"} {
		t.Run(kind, func(t *testing.T) {
			fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
			tail := newUnrootedTail(&fakeDurable{}, fc, 2, 2, "")
			for slot := uint64(1); slot <= 3; slot++ {
				tail.Add(slot, nil, testHashBytes(byte(slot)))
				tail.SetContext(slot, &state.ResumeContext{Slot: slot, Bankhash: vhash(byte(slot)).String(),
					AcctsLtHash: base64.StdEncoding.EncodeToString(make([]byte, 2048))}, testUnwindBankSysvars(t, slot, slot*10))
			}
			tip := cloneResumeContextForFold(tail.contexts[3])
			p := newAsyncPromoter(fc)
			defer p.stop()
			src := &fakeVerificationSource{blocks: make(map[uint64]*verifiedBlock)}
			v := newTestVerifier(src)
			for slot := uint64(1); slot <= 2; slot++ {
				digest, block := digestFor(slot, vhash(byte(slot)), vsig(byte(slot)), 5000, false, []uint64{10}, []uint64{5}, []byte{0})
				v.Record(digest)
				src.blocks[slot] = block
			}
			v.SetExecutedTip(10)
			engine := &stubFinalityEngine{conflicts: make(map[uint64]bool)}
			saved := &state.MithrilState{}
			calls := 0
			var failure error
			over, err := waitForVerifierProgress(context.Background(), time.Second, 4*time.Second, v.VerifiedWatermark, func() (bool, error) {
				calls++
				over := settleFoldsAtCapacity(tail, p, 3, func(res *foldResult) {
					if res != nil && res.err == nil {
						tail.applyFoldJob(res.job)
					}
				}, func() bool {
					if divergence := v.Failure(); divergence != nil {
						recordReplayDivergenceEvidence(saved, divergence)
						failure = divergence
						return true
					}
					through, gateErr := alpenglowPromotionGate(engine, nil, nil, nil, 0,
						safePromoteTarget(3, true, v.VerifiedWatermark(), 0), 3, nil)
					if gateErr != nil {
						var mismatch *AlpenglowFinalityMismatch
						require.ErrorAs(t, gateErr, &mismatch)
						recordAlpenglowEvidence(saved, mismatch)
						failure = gateErr
						return true
					}
					job, buildErr := tail.buildFoldJob(through, false)
					require.NoError(t, buildErr)
					if job != nil {
						require.NoError(t, p.enqueue(job))
					}
					return false
				})
				if calls == 1 {
					require.True(t, over)
					require.NoError(t, failure)
					if kind == "certificate conflict" {
						engine.conflicts[1] = true
					} else {
						src.blocks[1].Txs[0].Fee++
					}
					v.verifyNext()
					v.verifyNext()
				}
				return over, failure
			})
			require.True(t, over)
			require.Equal(t, 2, calls, "safety changed only after the first settlement")
			if kind == "certificate conflict" {
				var mismatch *AlpenglowFinalityMismatch
				require.ErrorAs(t, err, &mismatch)
				require.True(t, mismatch.Conflict)
				require.Len(t, saved.AlpenglowEvidence, 1)
			} else {
				var divergence *ReplayDivergence
				require.ErrorAs(t, err, &divergence)
				require.Equal(t, "tx_record", divergence.Kind)
				require.Len(t, saved.ReplayDivergenceEvidence, 1)
			}
			require.Empty(t, fc.committed)
			require.False(t, p.inFlight)
			require.Equal(t, 3, tail.overlay.HeldSlots())
			require.Equal(t, tip, tail.contexts[3])
			resume, resumeErr := ResumeStateFromRootedContext(tail.contexts[3], nil)
			require.NoError(t, resumeErr)
			require.Equal(t, uint64(3), resume.ParentSlot)
		})
	}
}

func TestVerifierCapacityWaitCancellationAfterSettlement(t *testing.T) {
	fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
	tail := newUnrootedTail(&fakeDurable{}, fc, 2, 2, "")
	for slot := uint64(1); slot <= 3; slot++ {
		tail.Add(slot, nil, testHashBytes(byte(slot)))
		tail.SetContext(slot, &state.ResumeContext{Slot: slot, Bankhash: vhash(byte(slot)).String(),
			AcctsLtHash: base64.StdEncoding.EncodeToString(make([]byte, 2048))}, testUnwindBankSysvars(t, slot, slot*10))
	}
	tip := cloneResumeContextForFold(tail.contexts[3])
	p := newAsyncPromoter(fc)
	defer p.stop()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	over, err := waitForVerifierProgress(ctx, time.Second, 4*time.Second, func() uint64 { return 0 }, func() (bool, error) {
		calls++
		over := settleFoldsAtCapacity(tail, p, 3, nil, nil)
		cancel()
		return over, nil
	})
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, over)
	require.Equal(t, 1, calls, "cancellation must prevent another settlement")
	require.Empty(t, fc.committed)
	require.False(t, p.inFlight)
	require.Equal(t, 3, tail.overlay.HeldSlots())
	require.Equal(t, tip, tail.contexts[3])
	resume, resumeErr := ResumeStateFromRootedContext(tail.contexts[3], nil)
	require.NoError(t, resumeErr)
	require.Equal(t, uint64(3), resume.ParentSlot)
}
