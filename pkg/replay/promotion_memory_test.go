package replay

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/stretchr/testify/require"
)

func TestByteTriggeredFoldsRecoverAndRewind(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "accounts"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0o644))
	db, err := accountsdb.OpenDb(dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		if db != nil {
			db.CloseDb()
		}
	})
	db.InitCaches()
	tail := newUnrootedTail(db, db, 512, 128, "")
	tail.foldMaxBytes = 16 << 10
	for s := uint64(1); s <= 5; s++ {
		addMemorySlot(tail, s, 8<<10)
	}
	for _, through := range []uint64{2, 4} {
		job, err := tail.buildFoldJob(5, false)
		require.NoError(t, err)
		require.Equal(t, through, job.through)
		require.NoError(t, runFoldJob(db, job))
		if through == 2 {
			tail.applyFoldJob(job)
		}
		// Deliberately leave the second result unapplied, as at a hard exit.
	}
	db.CloseDb()
	db = nil
	db, err = accountsdb.OpenDb(dir)
	require.NoError(t, err)
	db.InitCaches()
	recovered, err := db.RecoverFoldState()
	require.NoError(t, err)
	require.Equal(t, uint64(4), recovered.DurableThrough)
	key := testAccount(1, 1).Key
	a, err := db.GetAccount(4, key)
	require.NoError(t, err)
	require.Equal(t, byte(4), a.Data[0])
	_, err = db.RewindToBatchBoundary(2)
	require.NoError(t, err)
	a, err = db.GetAccount(2, key)
	require.NoError(t, err)
	require.Equal(t, byte(2), a.Data[0], "early folds must retain exact undo history")
}

func addMemorySlot(tail *unrootedTail, slot uint64, bytes int) {
	a := testAccount(1, slot)
	a.Data = make([]byte, bytes)
	a.Data[0] = byte(slot)
	tail.Add(slot, []*accounts.Account{a}, testHashBytes(byte(slot)))
	tail.SetContext(slot, &state.ResumeContext{Slot: slot})
}

func TestFoldMemoryBudgetAndCompletedJobRelease(t *testing.T) {
	fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
	tail := newUnrootedTail(&fakeDurable{}, fc, 512, 128, "")
	tail.foldMaxBytes = 16 << 10
	for s := uint64(1); s <= 5; s++ {
		addMemorySlot(tail, s, 8<<10)
	}
	job, err := tail.buildFoldJob(1, false)
	require.NoError(t, err)
	require.Nil(t, job)
	job, err = tail.buildFoldJob(5, false)
	require.NoError(t, err)
	require.Equal(t, uint64(2), job.through)
	require.Equal(t, 2, job.slotCount)
	require.NoError(t, runFoldJob(fc, job))
	require.Nil(t, job.chunk, "idle worker/result must not pin completed account versions")
	require.Equal(t, 5, tail.overlay.HeldSlots(), "durability does not prune before loop apply")
	tail.applyFoldJob(job)
	require.Equal(t, 3, tail.overlay.HeldSlots())
	require.NoError(t, tail.overlay.CheckInvariants())
}

func TestFoldMemoryPressureDrainsMultipleSafeChunks(t *testing.T) {
	fc := &slowCommitter{fakeCommitter: fakeCommitter{durable: accounts.NewMemAccounts()}, delay: 20 * time.Millisecond}
	tail := newUnrootedTail(&fakeDurable{}, fc, 512, 128, "")
	tail.foldMaxBytes, tail.maxBytes = 16<<10, 24<<10
	for s := uint64(1); s <= 6; s++ {
		addMemorySlot(tail, s, 8<<10)
	}
	p := newAsyncPromoter(fc)
	defer p.stop()
	admit := func() bool {
		if p.inFlight {
			return false
		}
		job, err := tail.buildFoldJob(6, false)
		require.NoError(t, err)
		if job != nil {
			require.NoError(t, p.enqueue(job))
		}
		return false
	}
	start := time.Now()
	stillOver := settleFoldsAtCapacity(tail, p, 6, func(res *foldResult) {
		require.NoError(t, res.err)
		tail.applyFoldJob(res.job)
	}, admit)
	require.False(t, stillOver)
	require.GreaterOrEqual(t, time.Since(start), 40*time.Millisecond)
	require.Equal(t, []uint64{1, 2, 3, 4}, fc.committed)
	require.Equal(t, 2, tail.overlay.HeldSlots())
	require.Less(t, tail.overlay.RetainedBytes(), tail.maxBytes)
}

func TestFoldMemoryPressureNeverCrossesVerificationBound(t *testing.T) {
	fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
	tail := newUnrootedTail(&fakeDurable{}, fc, 512, 128, "")
	tail.foldMaxBytes, tail.maxBytes = 16<<10, 24<<10
	for s := uint64(1); s <= 5; s++ {
		addMemorySlot(tail, s, 8<<10)
	}
	p := newAsyncPromoter(fc)
	defer p.stop()
	admit := func() bool {
		job, err := tail.buildFoldJob(safePromoteTarget(5, true, 1, 0), false)
		require.NoError(t, err)
		if job != nil {
			require.NoError(t, p.enqueue(job))
		}
		return false
	}
	require.True(t, settleFoldsAtCapacity(tail, p, 5, func(res *foldResult) {
		require.NoError(t, res.err)
		tail.applyFoldJob(res.job)
	}, admit), "must halt if the remaining bytes cannot be safely folded")
	require.Equal(t, []uint64{1}, fc.committed)
	require.Equal(t, 4, tail.overlay.HeldSlots())
}

func TestRewardsCompletionIgnoresNormalFoldByteTarget(t *testing.T) {
	fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
	tail := newUnrootedTail(&fakeDurable{}, fc, 512, 128, "")
	tail.foldMaxBytes = 16 << 10
	for s := uint64(1); s <= 5; s++ {
		addMemorySlot(tail, s, 8<<10)
	}
	job, err := tail.buildRewardsCompletionFoldJob(5)
	require.NoError(t, err)
	require.Len(t, job.chunk, 5, "never persist a partial rewards distribution")
}

// Repeated large rewrites of one key used to fill all 128 slot layers before
// folding. Scale bytes down so CI can exercise the same accumulation pattern.
func TestFoldMemoryPressureSoak(t *testing.T) {
	fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
	tail := newUnrootedTail(&fakeDurable{}, fc, 512, 128, "")
	tail.foldMaxBytes, tail.maxBytes = 2<<20, 8<<20
	p := newAsyncPromoter(fc)
	defer p.stop()
	apply := func(res *foldResult) {
		if res != nil {
			require.NoError(t, res.err)
			tail.applyFoldJob(res.job)
		}
	}
	var peak uint64
	for s := uint64(1); s <= 512; s++ {
		addMemorySlot(tail, s, 256<<10)
		peak = max(peak, tail.overlay.RetainedBytes())
		admit := func() bool {
			apply(p.poll())
			if !p.inFlight {
				job, err := tail.buildFoldJob(s, false)
				require.NoError(t, err)
				if job != nil {
					require.NoError(t, p.enqueue(job))
				}
			}
			return false
		}
		admit()
		require.False(t, settleFoldsAtCapacity(tail, p, s, apply, admit))
		require.Less(t, tail.overlay.RetainedBytes(), tail.maxBytes)
	}
	apply(p.drain())
	require.Less(t, peak, tail.maxBytes+(512<<10))
	t.Logf("512 rewrites, peak retained bytes %d, budget %d, durable through %d", peak, tail.maxBytes, fc.throughs[len(fc.throughs)-1])
}
