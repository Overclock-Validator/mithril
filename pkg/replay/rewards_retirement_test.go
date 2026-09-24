package replay

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	bin "github.com/gagliardetto/binary"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
)

func TestRewardsRetirementRequiresCompletedBankAndDurability(t *testing.T) {
	active := &sealevel.SysvarEpochRewards{Active: true}
	var raw bytes.Buffer
	require.NoError(t, active.MarshalWithEncoder(bin.NewBinEncoder(&raw)))
	activeBank, err := sealevel.NewBankSysvars(5, &accounts.Account{Key: sealevel.SysvarEpochRewardsAddr, Data: raw.Bytes()})
	require.NoError(t, err)
	missingBank, err := sealevel.NewBankSysvars(5)
	require.NoError(t, err)
	for _, tc := range []struct {
		name      string
		remaining uint64
		bank      *sealevel.BankSysvars
	}{
		{"active distribution", 1, testUnwindBankSysvars(t, 5, 50)},
		{"active bank", 0, activeBank},
		{"missing bank", 0, nil},
		{"missing rewards", 0, missingBank},
		{"unknown slot", 0, testUnwindBankSysvars(t, 0, 50)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			info := &rewards.PartitionedRewardDistributionInfo{NumRewardPartitionsRemaining: tc.remaining}
			var completed partitionedRewardsCompletion
			completed.observeBank(info, tc.bank)
			require.False(t, completed.retire(&info, 100))
			require.NotNil(t, info)
		})
	}

	info := &rewards.PartitionedRewardDistributionInfo{}
	var completed partitionedRewardsCompletion
	require.False(t, completed.retire(&info, 100), "zero remaining without observed completion is insufficient")
	completed.observeBank(info, testUnwindBankSysvars(t, 5, 50))
	completed.observeBank(info, testUnwindBankSysvars(t, 7, 50))
	require.False(t, completed.retire(&info, 4), "uncommitted completion must retain the guard")
	require.True(t, completed.retire(&info, 5), "later observations must not postpone recorded completion")
	require.Nil(t, info)
	require.False(t, completed.retire(&info, 100), "retirement is one-shot")
}

func TestRewardsPromotionRetainsBoundaryAfterFailedDistribution(t *testing.T) {
	const boundary = uint64(6264000)
	info := &rewards.PartitionedRewardDistributionInfo{NumRewardPartitionsRemaining: 1}
	var completed partitionedRewardsCompletion
	require.Equal(t, boundary-1, completed.limitPromotion(info, boundary, boundary+1))

	// Distribution consumes the final spool before ProcessBlock checks the
	// footer. The epoch-116 failure took this path; no successful bank was
	// observed, so forced shutdown must not persist the boundary bank.
	info.NumRewardPartitionsRemaining = 0
	require.Equal(t, boundary-1, completed.limitPromotion(info, boundary, boundary+1))
	completed.observeBank(info, nil)
	require.Equal(t, boundary-1, completed.limitPromotion(info, boundary, boundary+1))

	completed.observeBank(info, testUnwindBankSysvars(t, boundary+1, 50))
	require.Equal(t, boundary-2, completed.limitPromotion(info, boundary, boundary-2))
	require.Equal(t, boundary-1, completed.limitPromotion(info, boundary, boundary), "finality stopped inside rewards window")
	require.Equal(t, boundary+1, completed.limitPromotion(info, boundary, boundary+1))

	next := &rewards.PartitionedRewardDistributionInfo{}
	require.Equal(t, boundary-1, completed.limitPromotion(next, boundary, boundary+2), "completion belongs to another distribution")
	require.Equal(t, boundary+2, completed.limitPromotion(nil, boundary, boundary+2))
	require.Equal(t, boundary+2, completed.limitPromotion(info, 0, boundary+2))
}

func TestRewardsCompletionFoldCannotCheckpointInsideWindow(t *testing.T) {
	for _, batchSize := range []int{1, 2, 128} {
		t.Run(fmt.Sprintf("batch_%d", batchSize), func(t *testing.T) {
			fc := &fakeCommitter{durable: accounts.NewMemAccounts(), failOn: 7}
			tail := asyncTestTail(fc, 5, 6, 7, 8)
			tail.batchSlots = batchSize
			info := &rewards.PartitionedRewardDistributionInfo{}
			var completed partitionedRewardsCompletion
			completed.observeBank(info, testUnwindBankSysvars(t, 7, 50))
			through := completed.limitPromotion(info, 5, 8)
			require.Equal(t, uint64(8), through)

			job, err := tail.buildRewardsCompletionFoldJob(completed.slot)
			require.NoError(t, err)
			require.NotNil(t, job)
			require.Equal(t, uint64(7), job.through)
			require.Len(t, job.chunk, 3, "the entire distribution must share one commit")
			require.Equal(t, batchSize, tail.batchSlots, "normal batching is unchanged")
			require.Error(t, runFoldJob(fc, job))
			require.Empty(t, fc.throughs)
			require.False(t, completed.retire(&info, 4))

			fc.failOn = 0
			job, err = tail.buildRewardsCompletionFoldJob(completed.slot)
			require.NoError(t, err)
			require.NoError(t, runFoldJob(fc, job))
			require.Equal(t, []uint64{7}, fc.throughs)
			tail.applyFoldJob(job)
			require.True(t, completed.retire(&info, 7))
			require.Equal(t, 1, tail.overlay.HeldSlots(), "later banks remain buffered")
		})
	}
	tail := asyncTestTail(&fakeCommitter{durable: accounts.NewMemAccounts()}, 5, 6)
	_, err := tail.buildRewardsCompletionFoldJob(7)
	require.ErrorContains(t, err, "absent from retained fold prefix")
}

func TestRewardsRetirementDoesNotCrossGenerations(t *testing.T) {
	old := &rewards.PartitionedRewardDistributionInfo{SpoolSlot: 1}
	next := &rewards.PartitionedRewardDistributionInfo{SpoolSlot: 10}
	var completed partitionedRewardsCompletion
	completed.observeBank(old, testUnwindBankSysvars(t, 5, 50))
	require.False(t, completed.retire(&next, 100), "old completion cannot retire new bookkeeping")
	completed.observeBank(next, testUnwindBankSysvars(t, 11, 60))
	require.False(t, completed.retire(&next, 10))
	require.True(t, completed.retire(&next, 11))
}

func TestRewardsRetirementWaitsForSuccessfulFold(t *testing.T) {
	fc := &fakeCommitter{durable: accounts.NewMemAccounts(), failOn: 5}
	tail := asyncTestTail(fc, 5, 6)
	info := &rewards.PartitionedRewardDistributionInfo{}
	var completed partitionedRewardsCompletion
	completed.observeBank(info, testUnwindBankSysvars(t, 5, 50))
	job, err := tail.buildFoldJob(6, true)
	require.NoError(t, err)
	require.NotNil(t, job)
	root := uint64(4)
	require.False(t, completed.retire(&info, root), "capturing a job does not make its bank durable")
	require.Error(t, runFoldJob(fc, job))
	require.False(t, completed.retire(&info, root), "a failed fold leaves the old durable root")
	fc.failOn = 0
	require.NoError(t, runFoldJob(fc, job))
	ctx := tail.applyFoldJob(job)
	require.NotNil(t, ctx)
	root = job.through
	require.True(t, completed.retire(&info, root))
}

func TestRewardsRetirementAllowsExactParentUnwind(t *testing.T) {
	resetVoteStakeDirty()
	t.Cleanup(resetVoteStakeDirty)
	info := &rewards.PartitionedRewardDistributionInfo{}
	var completed partitionedRewardsCompletion
	completed.observeBank(info, testUnwindBankSysvars(t, 5, 50))
	tail := newUnrootedTail(&fakeDurable{}, &fakeCommitter{durable: accounts.NewMemAccounts()}, 512, 1, "")
	parent := &state.ResumeContext{Slot: 7, Bankhash: base58.Encode(make([]byte, 32)), AcctsLtHash: base64.StdEncoding.EncodeToString(make([]byte, 2048)), Capitalization: 700}
	bank := testUnwindBankSysvars(t, 7, 50)
	tail.Add(7, []*accounts.Account{testAccount(1, 71)}, testHashBytes(7))
	tail.SetContext(7, parent, bank)
	tail.Add(8, []*accounts.Account{testAccount(1, 81)}, testHashBytes(8))
	tail.SetContext(8, &state.ResumeContext{Slot: 8}, testUnwindBankSysvars(t, 8, 999))
	sw := &CertifiedSwitch{Slot: 8}
	ms := &state.MithrilState{LastRootedSlot: 4}
	sched := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000}
	rs, _, reason := tryInLoopUnwind(sw, tail, ms, sched, 0, info)
	require.Nil(t, rs)
	require.Equal(t, unwindFallbackRewardsWindow, reason)
	ms.LastRootedSlot = 5
	markVoteStakeDirty(5) // completed reward writes are also below the durable root
	require.True(t, completed.retire(&info, ms.LastRootedSlot))
	rs, restored, reason := tryInLoopUnwind(sw, tail, ms, sched, 0, info)
	require.Empty(t, reason)
	require.Same(t, bank, restored, "use the surviving bank, never abandoned reward sysvars")
	want, err := ResumeStateFromRootedContext(parent, nil)
	require.NoError(t, err)
	require.Equal(t, want, rs, "resume state must match rebuilding the exact retained parent")
	acct, err := tail.GetAccount(8, testAccount(1, 0).Key)
	require.NoError(t, err)
	require.Equal(t, uint64(71), acct.Lamports, "abandoned account writes must be removed")
}
