package replay

import (
	"bytes"
	"encoding/base64"
	"reflect"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	bin "github.com/gagliardetto/binary"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testUnwindBankSysvars(t *testing.T, slot uint64, epochRewardsMarker uint64) *sealevel.BankSysvars {
	t.Helper()
	clock := &sealevel.SysvarClock{Slot: slot}
	rewardsSysvar := &sealevel.SysvarEpochRewards{DistributedRewards: epochRewardsMarker}
	var rewardsData bytes.Buffer
	require.NoError(t, rewardsSysvar.MarshalWithEncoder(bin.NewBinEncoder(&rewardsData)))
	snapshot, err := sealevel.NewBankSysvars(slot,
		&accounts.Account{Key: sealevel.SysvarClockAddr, Lamports: 1, Data: clock.MustMarshal()},
		&accounts.Account{Key: sealevel.SysvarEpochRewardsAddr, Lamports: 1, Data: rewardsData.Bytes()},
	)
	require.NoError(t, err)
	return snapshot
}

// Tripwire: the fork-switch unwind and checkpoint resume are only correct if
// EVERY runtime side effect a slot produces is carried in ResumeContext and
// restored. If you add a field to state.ResumeContext, this test forces you to
// (1) map it in ResumeStateFromRootedContext, (2) restore it on resume
// (configureInitialBlockFromResume or the ReplayBlocks seed path), (3) restore
// it on the in-loop unwind, and (4) bump the count below.
func TestResumeContextFieldTripwire(t *testing.T) {
	// TransactionStatusCheckpoint is the one durability-only field: startup and
	// rewind consume it before ResumeState is built. It intentionally does not
	// enter bank execution state, but remains part of this conscious field count.
	const wired = 24 // 23 execution fields + 1 durability-only checkpoint ref
	if n := reflect.TypeOf(state.ResumeContext{}).NumField(); n != wired {
		t.Fatalf("state.ResumeContext has %d fields but %d are wired through the resume/unwind restoration path — wire the new field(s) end-to-end, then update this count", n, wired)
	}
}

func TestParentSwitchNeedsStateUnwind(t *testing.T) {
	assert.False(t, parentSwitchNeedsStateUnwind(101, 100), "fork discovered ahead of replay is source-only")
	assert.True(t, parentSwitchNeedsStateUnwind(100, 100), "executed divergence slot must unwind")
	assert.True(t, parentSwitchNeedsStateUnwind(99, 100), "executed suffix beyond divergence must unwind")
}

// Full-fidelity mapping: every restorable ResumeContext field survives into
// ResumeState exactly (the input to both checkpoint resume and the in-loop
// fork-switch unwind).
func TestResumeStateFromRootedContextRoundTrip(t *testing.T) {
	bankhash := make([]byte, 32)
	bankhash[0] = 0xAA
	lt := make([]byte, 2048)
	lt[7] = 3
	evicted := make([]byte, 32)
	evicted[1] = 0xBB
	lastBH := make([]byte, 32)
	lastBH[2] = 0xCC
	alpenglowID := make([]byte, 32)
	alpenglowID[4] = 0xAE
	alpenglowChainedRoot := make([]byte, 32)
	alpenglowChainedRoot[5] = 0xAF
	entryBH := make([]byte, 32)
	entryBH[3] = 0xDD
	clock := []byte{9, 8, 7, 6}
	txc := uint64(123456)

	rc := &state.ResumeContext{
		Slot:                       900,
		Bankhash:                   base58.Encode(bankhash),
		AlpenglowBlockID:           base58.Encode(alpenglowID),
		AlpenglowChainedMerkleRoot: base58.Encode(alpenglowChainedRoot),
		BlockHeight:                880,
		Epoch:                      2,
		AcctsLtHash:                base64.StdEncoding.EncodeToString(lt),
		LamportsPerSignature:       5000,
		PrevLamportsPerSig:         4500,
		NumSignatures:              777,
		RecentBlockhashes:          []state.BlockhashEntry{{Blockhash: base58.Encode(entryBH), LamportsPerSignature: 5000}},
		EvictedBlockhash:           base58.Encode(evicted),
		Blockhash:                  base58.Encode(lastBH),
		SlotHashes:                 []state.SlotHashEntry{{Slot: 899, Hash: base58.Encode(entryBH)}},
		Clock:                      base64.StdEncoding.EncodeToString(clock),
		Capitalization:             42_000_000,
		SlotsPerYear:               78840000,
		InflationInitial:           0.08,
		InflationTerminal:          0.015,
		InflationTaper:             0.15,
		InflationFoundation:        0.05,
		InflationFoundationTerm:    7,
		TransactionCount:           &txc,
	}

	rs, err := ResumeStateFromRootedContext(rc, map[uint64]string{2: "c3Rha2Vz"})
	require.NoError(t, err)

	assert.Equal(t, uint64(900), rs.ParentSlot)
	assert.Equal(t, uint64(880), rs.ParentBlockHeight)
	assert.Equal(t, bankhash, rs.ParentBankhash)
	assert.True(t, rs.HasParentAlpenglowBlockID)
	assert.Equal(t, alpenglowID, rs.ParentAlpenglowBlockID[:])
	assert.True(t, rs.HasParentAlpenglowChainedMerkleRoot)
	assert.Equal(t, alpenglowChainedRoot, rs.ParentAlpenglowChainedMerkleRoot[:])
	require.NotNil(t, rs.AcctsLtHash)
	assert.Equal(t, lt, rs.AcctsLtHash.Hash(), "lt-hash restored byte-exact")
	assert.Equal(t, uint64(5000), rs.LamportsPerSignature)
	assert.Equal(t, uint64(4500), rs.PrevLamportsPerSignature)
	assert.Equal(t, uint64(777), rs.NumSignatures)
	require.NotNil(t, rs.RecentBlockhashes)
	require.Len(t, *rs.RecentBlockhashes, 1)
	assert.Equal(t, entryBH, (*rs.RecentBlockhashes)[0].Blockhash[:])
	assert.Equal(t, evicted, rs.EvictedBlockhash[:])
	assert.Equal(t, lastBH, rs.LastBlockhash[:])
	require.NotNil(t, rs.SlotHashes)
	require.Len(t, *rs.SlotHashes, 1)
	assert.Equal(t, uint64(899), (*rs.SlotHashes)[0].Slot)
	assert.Equal(t, clock, rs.Clock)
	assert.Equal(t, uint64(42_000_000), rs.Capitalization)
	assert.Equal(t, float64(78840000), rs.SlotsPerYear)
	assert.Equal(t, 0.08, rs.InflationInitial)
	assert.Equal(t, 0.015, rs.InflationTerminal)
	assert.Equal(t, 0.15, rs.InflationTaper)
	assert.Equal(t, 0.05, rs.InflationFoundation)
	assert.Equal(t, float64(7), rs.InflationFoundationTerm)
	require.NotNil(t, rs.TransactionCount)
	assert.Equal(t, uint64(123456), *rs.TransactionCount)
	assert.Equal(t, []byte("c3Rha2Vz"), rs.ComputedEpochStakes[2])
}

// unwind returns the ACTUAL executed parent context, which need not be
// numerically fromSlot-1 when the slots in between were skipped (P2).
func TestUnwindReturnsExecutedParentAcrossSkips(t *testing.T) {
	tail := newUnrootedTail(&fakeDurable{}, &fakeCommitter{durable: accounts.NewMemAccounts()}, 512, 1, "")
	// Executed slots 5 and 8 (6, 7 skipped -> no context), then 9.
	bank5 := testUnwindBankSysvars(t, 5, 50)
	bank8 := testUnwindBankSysvars(t, 8, 80)
	bank9 := testUnwindBankSysvars(t, 9, 90)
	tail.Add(5, []*accounts.Account{testAccount(1, 51)}, testHashBytes(5))
	tail.SetContext(5, &state.ResumeContext{Slot: 5, Bankhash: "bh5"}, bank5)
	tail.Add(8, []*accounts.Account{testAccount(2, 82)}, testHashBytes(8))
	tail.SetContext(8, &state.ResumeContext{Slot: 8, Bankhash: "bh8"}, bank8)
	tail.Add(9, []*accounts.Account{testAccount(3, 93)}, testHashBytes(9))
	tail.SetContext(9, &state.ResumeContext{Slot: 9, Bankhash: "bh9"}, bank9)

	// Switch at slot 9: the parent is the executed slot 8.
	ctx, bankSysvars := tail.unwind(9)
	require.NotNil(t, ctx)
	assert.Equal(t, uint64(8), ctx.Slot)
	assert.Same(t, bank8, bankSysvars)
	assert.NotContains(t, tail.bankSysvars, uint64(9), "discarded suffix snapshot must be evicted")

	// Switch at slot 8: slots 6,7 were skipped, so the executed parent is slot 5
	// — returned even though it is not numerically 8-1=7 (the old code rejected
	// this and forced a rooted re-replay).
	ctx, bankSysvars = tail.unwind(8)
	require.NotNil(t, ctx, "parent across skipped slots must be returned")
	assert.Equal(t, uint64(5), ctx.Slot)
	assert.Same(t, bank5, bankSysvars, "exact parent snapshot survives skipped slots")
	assert.NotContains(t, tail.bankSysvars, uint64(8), "second discarded suffix snapshot must be evicted")
}

// The seed for the running transaction count: exact from a checkpoint that
// recorded one (including a genuine zero — dev genesis), approximate from the
// snapshot manifest when the checkpoint predates the field (nil pointer).
func TestResolveInitialTransactionCount(t *testing.T) {
	// Pre-field checkpoint (nil pointer): manifest fallback, flagged approximate.
	count, exact := resolveInitialTransactionCount(&ResumeState{}, 5000)
	assert.Equal(t, uint64(5000), count)
	assert.False(t, exact, "nil TransactionCount must be treated as absent, not zero")

	// Fresh start (no resume state at all): manifest is exact enough by definition.
	count, exact = resolveInitialTransactionCount(nil, 5000)
	assert.Equal(t, uint64(5000), count)
	assert.False(t, exact)

	// Checkpoint with a recorded count: exact, overrides the manifest.
	txc := uint64(777777)
	count, exact = resolveInitialTransactionCount(&ResumeState{TransactionCount: &txc}, 5000)
	assert.Equal(t, uint64(777777), count)
	assert.True(t, exact)

	// Present-but-zero is EXACT zero (dev genesis), not "unset".
	zero := uint64(0)
	count, exact = resolveInitialTransactionCount(&ResumeState{TransactionCount: &zero}, 5000)
	assert.Equal(t, uint64(0), count)
	assert.True(t, exact, "explicit zero must not fall back to the manifest")
}

// The vote/stake dirty watermark is a monotonic max, reset per run.
func TestVoteStakeDirtyWatermark(t *testing.T) {
	resetVoteStakeDirty()
	assert.Equal(t, uint64(0), voteStakeDirtySlot.Load())
	markVoteStakeDirty(100)
	markVoteStakeDirty(50) // lower — monotonic max holds
	assert.Equal(t, uint64(100), voteStakeDirtySlot.Load())
	markVoteStakeDirty(150)
	assert.Equal(t, uint64(150), voteStakeDirtySlot.Load())
	resetVoteStakeDirty()
	assert.Equal(t, uint64(0), voteStakeDirtySlot.Load())
}

// The in-loop unwind proceeds when the unwound suffix left the global vote/stake
// caches untouched, but falls back to the rooted-checkpoint re-replay when a
// slot in that suffix mutated them (P1) — the unwind can only roll back account
// state, not those process-global caches.
func TestTryInLoopUnwindFallsBackWhenVoteStakeDirty(t *testing.T) {
	previousEpochRewards := sealevel.SysvarCache.EpochRewards
	t.Cleanup(func() {
		sealevel.SysvarCache.EpochRewards.Sysvar = previousEpochRewards.Sysvar
		sealevel.SysvarCache.EpochRewards.Acct = previousEpochRewards.Acct
	})
	staleEpochRewards := &sealevel.SysvarEpochRewards{DistributedRewards: 999}
	sealevel.SysvarCache.EpochRewards.Sysvar = staleEpochRewards

	// A resume context the rebuild accepts: base58 bankhash + base64 lt-hash
	// (1024 uint16 elements = 2048 bytes).
	ctxTxCount := uint64(999)
	validCtx := &state.ResumeContext{
		Slot:             7,
		Bankhash:         base58.Encode(make([]byte, 32)),
		AcctsLtHash:      base64.StdEncoding.EncodeToString(make([]byte, 2048)),
		TransactionCount: &ctxTxCount,
	}
	newTail := func() *unrootedTail {
		tail := newUnrootedTail(&fakeDurable{}, &fakeCommitter{durable: accounts.NewMemAccounts()}, 512, 1, "")
		tail.Add(7, []*accounts.Account{testAccount(1, 71)}, testHashBytes(7))
		tail.SetContext(7, validCtx, testUnwindBankSysvars(t, 7, 700))
		return tail
	}
	sched := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, LeaderScheduleSlotOffset: 432000}
	sw := &CertifiedSwitch{Slot: 8, Executed: swHash(1), Certified: swHash(2)}
	epoch := sched.GetEpoch(8) // 0 with these params
	mithrilState := &state.MithrilState{}

	// Clean: the in-loop unwind succeeds (evict slot 8+, resume from slot 7) and
	// carries the parent's transaction count so the discarded fork's txs can be
	// dropped from the running count.
	resetVoteStakeDirty()
	rs, bankSysvars, _ := tryInLoopUnwind(sw, newTail(), mithrilState, sched, epoch, nil)
	require.NotNil(t, rs, "clean unwind should succeed in-loop")
	require.NotNil(t, bankSysvars, "clean unwind returns its exact parent sysvars")
	assert.Equal(t, uint64(7), bankSysvars.Slot())
	retainedEpochRewards, ok := bankSysvars.EpochRewards()
	require.True(t, ok)
	assert.Equal(t, uint64(700), retainedEpochRewards.DistributedRewards,
		"retained parent snapshot wins over the abandoned process-global EpochRewards generation")
	require.NotNil(t, rs.TransactionCount, "unwind carries the parent's tx count for restore")
	assert.Equal(t, uint64(999), *rs.TransactionCount)

	// A global cache was mutated in the UNWOUND suffix (at the switch slot):
	// the unwind cannot roll the caches back -> must fall back (nil).
	markVoteStakeDirty(8)
	rs, _, reason := tryInLoopUnwind(sw, newTail(), mithrilState, sched, epoch, nil)
	assert.Nil(t, rs, "dirty cache in the unwound suffix must force the rooted re-replay fallback")
	assert.Equal(t, unwindFallbackVoteStakeDirty, reason)
	resetVoteStakeDirty()

	// A cache write in the RETAINED suffix (below the switch slot but above the
	// rooted watermark) is just as unsafe: the resume path reloads the vote
	// cache from durable, which cannot see the retained suffix's writes — the
	// reload would REGRESS the cache below live account state.
	markVoteStakeDirty(7) // retained slot; rooted watermark is 0
	rs, _, reason = tryInLoopUnwind(sw, newTail(), mithrilState, sched, epoch, nil)
	assert.Nil(t, rs, "dirty cache in the retained suffix must also force the fallback")
	assert.Equal(t, unwindFallbackVoteStakeDirty, reason)
	resetVoteStakeDirty()

	// Once the rooted watermark passes the dirty slot, the write is folded into
	// durable and reload-from-durable is exact again -> fast path allowed.
	markVoteStakeDirty(7)
	rootedPast := &state.MithrilState{LastRootedSlot: 7}
	rs, _, _ = tryInLoopUnwind(sw, newTail(), rootedPast, sched, epoch, nil)
	require.NotNil(t, rs, "dirtiness at/below the rooted watermark is durably folded — fast path is safe")
	resetVoteStakeDirty()
}

// Every unsafe-unwind guard falls back to the rooted-checkpoint re-replay
// (returns nil) instead of proceeding: cross-epoch spans, the partitioned-
// rewards window, and a missing parent context.
func TestTryInLoopUnwindGuardMatrix(t *testing.T) {
	validCtx := &state.ResumeContext{
		Slot:        7,
		Bankhash:    base58.Encode(make([]byte, 32)),
		AcctsLtHash: base64.StdEncoding.EncodeToString(make([]byte, 2048)),
	}
	newTail := func(withCtx bool) *unrootedTail {
		tail := newUnrootedTail(&fakeDurable{}, &fakeCommitter{durable: accounts.NewMemAccounts()}, 512, 1, "")
		tail.Add(7, []*accounts.Account{testAccount(1, 71)}, testHashBytes(7))
		if withCtx {
			tail.SetContext(7, validCtx, testUnwindBankSysvars(t, 7, 700))
		}
		return tail
	}
	sched := &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 432000, LeaderScheduleSlotOffset: 432000}
	mithrilState := &state.MithrilState{}
	resetVoteStakeDirty()

	// Nil tail / zero slot are inert.
	assertUnwindFallback(t, &CertifiedSwitch{Slot: 8}, nil, mithrilState, sched, 0, nil)
	assertUnwindFallback(t, &CertifiedSwitch{Slot: 0}, newTail(true), mithrilState, sched, 0, nil)

	// Cross-epoch span: the switch slot's parent sits in the previous epoch —
	// epoch-scoped caches (stakes, leader schedule, stake history) would be
	// stale for the re-execution. Slot 432000 is the first slot of epoch 1.
	sw := &CertifiedSwitch{Slot: 432000, Executed: swHash(1), Certified: swHash(2)}
	assertUnwindFallbackReason(t, unwindFallbackCrossEpoch, sw, newTail(true), mithrilState, sched, 1, nil)

	// Partitioned-rewards window: re-execution would double-apply distribution
	// bookkeeping.
	sw = &CertifiedSwitch{Slot: 8, Executed: swHash(1), Certified: swHash(2)}
	rewardsActive := &rewards.PartitionedRewardDistributionInfo{NumRewardPartitionsRemaining: 3}
	assertUnwindFallbackReason(t, unwindFallbackRewardsWindow, sw, newTail(true), mithrilState, sched, 0, rewardsActive)
	rewardsCompleted := &rewards.PartitionedRewardDistributionInfo{}
	assertUnwindFallbackReason(t, unwindFallbackRewardsWindow, sw, newTail(true), mithrilState, sched, 0, rewardsCompleted)

	// Missing parent context: nothing retained to rebuild execution state from.
	assertUnwindFallbackReason(t, unwindFallbackMissingContext, sw, newTail(false), mithrilState, sched, 0, nil)

	// A context without its in-memory-only bank snapshot cannot use the fast
	// path: process globals may describe the discarded child generation.
	missingSysvars := newTail(false)
	missingSysvars.SetContext(7, validCtx)
	assertUnwindFallbackReason(t, unwindFallbackMissingSysvars, sw, missingSysvars, mithrilState, sched, 0, nil)

	// The persisted durable-boundary context deliberately carries no pointer;
	// it must re-enter through rooted recovery, which reconstructs all sysvars.
	durableBoundary := newTail(false)
	durableState := &state.MithrilState{LastRootedSlot: 7, LastRootedContext: validCtx}
	assertUnwindFallbackReason(t, unwindFallbackMissingSysvars, sw, durableBoundary, durableState, sched, 0, nil)

	// Snapshot/context slot mismatches fail closed rather than deriving a child
	// from the wrong bank generation.
	mismatched := newTail(false)
	mismatched.SetContext(7, validCtx, testUnwindBankSysvars(t, 6, 600))
	assertUnwindFallbackReason(t, unwindFallbackSysvarSlot, sw, mismatched, mithrilState, sched, 0, nil)

	// Control: with every guard clear, the unwind proceeds.
	assertUnwindOK(t, sw, newTail(true), mithrilState, sched, 0, nil)
}

// assertUnwindOK asserts the in-RAM unwind proceeds (no fallback reason).
func assertUnwindOK(t *testing.T, sw *CertifiedSwitch, tail *unrootedTail, ms *state.MithrilState, sched *sealevel.SysvarEpochSchedule, epoch uint64, ri *rewards.PartitionedRewardDistributionInfo) {
	t.Helper()
	rs, bankSysvars, reason := tryInLoopUnwind(sw, tail, ms, sched, epoch, ri)
	require.NotNil(t, rs, "expected in-RAM unwind to proceed, got fallback %q", reason)
	require.NotNil(t, bankSysvars)
	assert.Empty(t, reason)
}

// assertUnwindFallback asserts the unwind falls back (any reason).
func assertUnwindFallback(t *testing.T, sw *CertifiedSwitch, tail *unrootedTail, ms *state.MithrilState, sched *sealevel.SysvarEpochSchedule, epoch uint64, ri *rewards.PartitionedRewardDistributionInfo) {
	t.Helper()
	rs, bankSysvars, reason := tryInLoopUnwind(sw, tail, ms, sched, epoch, ri)
	assert.Nil(t, rs)
	assert.Nil(t, bankSysvars)
	assert.NotEmpty(t, reason, "fallback must carry a reason for the instrumentation")
}

// assertUnwindFallbackReason asserts the unwind falls back with the exact
// instrumented reason operators will see.
func assertUnwindFallbackReason(t *testing.T, want string, sw *CertifiedSwitch, tail *unrootedTail, ms *state.MithrilState, sched *sealevel.SysvarEpochSchedule, epoch uint64, ri *rewards.PartitionedRewardDistributionInfo) {
	t.Helper()
	rs, bankSysvars, reason := tryInLoopUnwind(sw, tail, ms, sched, epoch, ri)
	assert.Nil(t, rs)
	assert.Nil(t, bankSysvars)
	assert.Equal(t, want, reason)
}
