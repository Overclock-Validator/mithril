package replay

import (
	"encoding/base64"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/require"
)

func forkTestContext(slot uint64) *state.ResumeContext {
	return &state.ResumeContext{Slot: slot, Bankhash: base58.Encode(make([]byte, 32)),
		AcctsLtHash: base64.StdEncoding.EncodeToString(make([]byte, 2048))}
}

func TestForkCacheUnwindMatchesCleanParentClock(t *testing.T) {
	resetVoteStakeDirty()
	t.Cleanup(resetVoteStakeDirty)
	oldEpochs := global.CaptureEpochCaches()
	t.Cleanup(func() { global.RestoreEpochCaches(oldEpochs) })
	key := solana.PublicKey{0xE9, 1}
	oldVote := global.VoteCacheItem(key)
	t.Cleanup(func() {
		if oldVote == nil {
			global.DeleteVoteCacheItem(key)
		} else {
			global.PutVoteCacheItem(key, oldVote)
		}
	})
	global.PutEpochStakes(0, map[solana.PublicKey]uint64{key: 100}, nil, 100)
	makeVote := func(slot uint64, timestamp int64) *sealevel.VoteStateVersions {
		return &sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionCurrent,
			Current: sealevel.VoteState{LastTimestamp: sealevel.BlockTimestamp{Slot: slot, Timestamp: timestamp}}}
	}
	global.PutVoteCacheItem(key, makeVote(4, 1000))
	tail := newUnrootedTail(&fakeDurable{}, &fakeCommitter{durable: accounts.NewMemAccounts()}, 512, 1, "")
	addBank := func(slot uint64) {
		tail.Add(slot, []*accounts.Account{testAccount(1, slot)}, testHashBytes(byte(slot)))
		tail.SetContext(slot, forkTestContext(slot), testUnwindBankSysvars(t, slot, 0))
	}
	putVoteCacheItem(&sealevel.SlotCtx{Slot: 7}, key, makeVote(7, 1001))
	addBank(7)
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, LeaderScheduleSlotOffset: 432000}
	calcBefore := global.CalcUnixTimeForClockSysvar()
	global.SetCalcUnixTimeForClockSysvar(true)
	t.Cleanup(func() { global.SetCalcUnixTimeForClockSysvar(calcBefore) })
	clean := &sealevel.SysvarClock{Slot: 7, EpochStartTimestamp: 1000, UnixTimestamp: 1000}
	require.NoError(t, updateClockSysvarForMode(clean, &block.Block{Slot: 8}, schedule, false))

	// An accepted wrong sibling changes the timestamp oracle; a later slot
	// deletes the same account. Both must disappear without losing slot 7.
	putVoteCacheItem(&sealevel.SlotCtx{Slot: 8}, key, makeVote(8, 9000))
	addBank(8)
	deleteVoteCacheItem(&sealevel.SlotCtx{Slot: 9}, key)
	addBank(9)
	// Concurrent future production owns its pending updates, not the journal.
	future := &sealevel.SlotCtx{Slot: 12, DeferVoteCachePublication: true}
	putVoteCacheItem(future, key, makeVote(12, 9500))
	rs, sysvars, reason := tryInLoopUnwind(&CertifiedSwitch{Slot: 8}, tail, &state.MithrilState{}, schedule, 0, nil)
	require.Empty(t, reason)
	require.Equal(t, uint64(7), rs.ParentSlot)
	require.Equal(t, uint64(7), sysvars.Slot())
	require.Equal(t, int64(1001), global.VoteCacheItem(key).LastTimestamp().Timestamp)
	require.Contains(t, future.PendingVoteCache, key)
	replayed := &sealevel.SysvarClock{Slot: 7, EpochStartTimestamp: 1000, UnixTimestamp: 1000}
	require.NoError(t, updateClockSysvarForMode(replayed, &block.Block{Slot: 8}, schedule, false))
	require.Equal(t, clean.MustMarshal(), replayed.MustMarshal(), "clock account bytes feeding LtHash match clean replay")

	putVoteCacheItem(&sealevel.SlotCtx{Slot: 8}, key, makeVote(8, 1100))
	addBank(8)
	_, _, reason = tryInLoopUnwind(&CertifiedSwitch{Slot: 8}, tail, &state.MithrilState{}, schedule, 0, nil)
	require.Empty(t, reason)
	require.Equal(t, int64(1001), global.VoteCacheItem(key).LastTimestamp().Timestamp)
}

func TestForkUnwindRetainsDurableParentSnapshot(t *testing.T) {
	resetVoteStakeDirty()
	t.Cleanup(resetVoteStakeDirty)
	tail := newUnrootedTail(&fakeDurable{}, &fakeCommitter{durable: accounts.NewMemAccounts()}, 512, 1, "")
	ctx := forkTestContext(7)
	sysvars := testUnwindBankSysvars(t, 7, 700)
	tail.Add(7, []*accounts.Account{testAccount(1, 7)}, testHashBytes(7))
	tail.SetContext(7, ctx, sysvars)
	through, _, err := tail.flush(7)
	require.NoError(t, err)
	require.Equal(t, uint64(7), through)
	require.Empty(t, tail.contexts, "folded per-slot metadata is pruned")
	require.Equal(t, ctx, tail.rootContext)
	tail.Add(8, []*accounts.Account{testAccount(1, 8)}, testHashBytes(8))
	tail.SetContext(8, forkTestContext(8), testUnwindBankSysvars(t, 8, 800))
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, LeaderScheduleSlotOffset: 432000}
	rs, actualSysvars, reason := tryInLoopUnwind(&CertifiedSwitch{Slot: 8}, tail, &state.MithrilState{LastRootedSlot: 7}, schedule, 0, nil)
	require.Empty(t, reason)
	require.Equal(t, uint64(7), rs.ParentSlot)
	require.Same(t, sysvars, actualSysvars)
}

func TestRejectedForkUnwindDoesNotMutateLiveBranch(t *testing.T) {
	resetVoteStakeDirty()
	t.Cleanup(resetVoteStakeDirty)
	tail := newUnrootedTail(&fakeDurable{}, &fakeCommitter{durable: accounts.NewMemAccounts()}, 512, 1, "")
	tail.Add(7, []*accounts.Account{testAccount(1, 7)}, testHashBytes(7))
	tail.SetContext(7, forkTestContext(7)) // missing immutable sysvars
	tail.Add(8, []*accounts.Account{testAccount(1, 8)}, testHashBytes(8))
	schedule := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, LeaderScheduleSlotOffset: 432000}
	_, _, reason := tryInLoopUnwind(&CertifiedSwitch{Slot: 8}, tail, &state.MithrilState{}, schedule, 0, nil)
	require.Equal(t, unwindFallbackMissingSysvars, reason)
	require.Contains(t, tail.bankhashes, uint64(8))
	acct, err := tail.GetAccount(8, testAccount(1, 8).Key)
	require.NoError(t, err)
	require.Equal(t, uint64(8), acct.Lamports)
}
