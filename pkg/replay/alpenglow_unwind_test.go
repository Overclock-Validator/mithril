package replay

import (
	"encoding/base64"
	"reflect"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/mr-tron/base58"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tripwire: the fork-switch unwind and checkpoint resume are only correct if
// EVERY runtime side effect a slot produces is carried in ResumeContext and
// restored. If you add a field to state.ResumeContext, this test forces you to
// (1) map it in ResumeStateFromRootedContext, (2) restore it on resume
// (configureInitialBlockFromResume or the ReplayBlocks seed path), (3) restore
// it on the in-loop unwind, and (4) bump the count below.
func TestResumeContextFieldTripwire(t *testing.T) {
	const wired = 21 // fields consciously wired through resume + unwind
	if n := reflect.TypeOf(state.ResumeContext{}).NumField(); n != wired {
		t.Fatalf("state.ResumeContext has %d fields but %d are wired through the resume/unwind restoration path — wire the new field(s) end-to-end, then update this count", n, wired)
	}
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
	entryBH := make([]byte, 32)
	entryBH[3] = 0xDD
	clock := []byte{9, 8, 7, 6}
	txc := uint64(123456)

	rc := &state.ResumeContext{
		Slot:                    900,
		Bankhash:                base58.Encode(bankhash),
		BlockHeight:             880,
		Epoch:                   2,
		AcctsLtHash:             base64.StdEncoding.EncodeToString(lt),
		LamportsPerSignature:    5000,
		PrevLamportsPerSig:      4500,
		NumSignatures:           777,
		RecentBlockhashes:       []state.BlockhashEntry{{Blockhash: base58.Encode(entryBH), LamportsPerSignature: 5000}},
		EvictedBlockhash:        base58.Encode(evicted),
		Blockhash:               base58.Encode(lastBH),
		SlotHashes:              []state.SlotHashEntry{{Slot: 899, Hash: base58.Encode(entryBH)}},
		Clock:                   base64.StdEncoding.EncodeToString(clock),
		Capitalization:          42_000_000,
		SlotsPerYear:            78840000,
		InflationInitial:        0.08,
		InflationTerminal:       0.015,
		InflationTaper:          0.15,
		InflationFoundation:     0.05,
		InflationFoundationTerm: 7,
		TransactionCount:        &txc,
	}

	rs, err := ResumeStateFromRootedContext(rc, map[uint64]string{2: "c3Rha2Vz"})
	require.NoError(t, err)

	assert.Equal(t, uint64(900), rs.ParentSlot)
	assert.Equal(t, uint64(880), rs.ParentBlockHeight)
	assert.Equal(t, bankhash, rs.ParentBankhash)
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
	tail.Add(5, []*accounts.Account{testAccount(1, 51)}, testHashBytes(5))
	tail.SetContext(5, &state.ResumeContext{Slot: 5, Bankhash: "bh5"})
	tail.Add(8, []*accounts.Account{testAccount(2, 82)}, testHashBytes(8))
	tail.SetContext(8, &state.ResumeContext{Slot: 8, Bankhash: "bh8"})
	tail.Add(9, []*accounts.Account{testAccount(3, 93)}, testHashBytes(9))
	tail.SetContext(9, &state.ResumeContext{Slot: 9, Bankhash: "bh9"})

	// Switch at slot 9: the parent is the executed slot 8.
	ctx := tail.unwind(9)
	require.NotNil(t, ctx)
	assert.Equal(t, uint64(8), ctx.Slot)

	// Switch at slot 8: slots 6,7 were skipped, so the executed parent is slot 5
	// — returned even though it is not numerically 8-1=7 (the old code rejected
	// this and forced a rooted re-replay).
	ctx = tail.unwind(8)
	require.NotNil(t, ctx, "parent across skipped slots must be returned")
	assert.Equal(t, uint64(5), ctx.Slot)
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
		tail.SetContext(7, validCtx)
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
	rs, _ := tryInLoopUnwind(sw, newTail(), mithrilState, sched, epoch, nil)
	require.NotNil(t, rs, "clean unwind should succeed in-loop")
	require.NotNil(t, rs.TransactionCount, "unwind carries the parent's tx count for restore")
	assert.Equal(t, uint64(999), *rs.TransactionCount)

	// A global cache was mutated in the UNWOUND suffix (at the switch slot):
	// the unwind cannot roll the caches back -> must fall back (nil).
	markVoteStakeDirty(8)
	rs, reason := tryInLoopUnwind(sw, newTail(), mithrilState, sched, epoch, nil)
	assert.Nil(t, rs, "dirty cache in the unwound suffix must force the rooted re-replay fallback")
	assert.Equal(t, unwindFallbackVoteStakeDirty, reason)
	resetVoteStakeDirty()

	// A cache write in the RETAINED suffix (below the switch slot but above the
	// rooted watermark) is just as unsafe: the resume path reloads the vote
	// cache from durable, which cannot see the retained suffix's writes — the
	// reload would REGRESS the cache below live account state.
	markVoteStakeDirty(7) // retained slot; rooted watermark is 0
	rs, reason = tryInLoopUnwind(sw, newTail(), mithrilState, sched, epoch, nil)
	assert.Nil(t, rs, "dirty cache in the retained suffix must also force the fallback")
	assert.Equal(t, unwindFallbackVoteStakeDirty, reason)
	resetVoteStakeDirty()

	// Once the rooted watermark passes the dirty slot, the write is folded into
	// durable and reload-from-durable is exact again -> fast path allowed.
	markVoteStakeDirty(7)
	rootedPast := &state.MithrilState{LastRootedSlot: 7}
	rs, _ = tryInLoopUnwind(sw, newTail(), rootedPast, sched, epoch, nil)
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
			tail.SetContext(7, validCtx)
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

	// Missing parent context: nothing retained to rebuild execution state from.
	assertUnwindFallbackReason(t, unwindFallbackMissingContext, sw, newTail(false), mithrilState, sched, 0, nil)

	// Control: with every guard clear, the unwind proceeds.
	assertUnwindOK(t, sw, newTail(true), mithrilState, sched, 0, nil)
}

// assertUnwindOK asserts the in-RAM unwind proceeds (no fallback reason).
func assertUnwindOK(t *testing.T, sw *CertifiedSwitch, tail *unrootedTail, ms *state.MithrilState, sched *sealevel.SysvarEpochSchedule, epoch uint64, ri *rewards.PartitionedRewardDistributionInfo) {
	t.Helper()
	rs, reason := tryInLoopUnwind(sw, tail, ms, sched, epoch, ri)
	require.NotNil(t, rs, "expected in-RAM unwind to proceed, got fallback %q", reason)
	assert.Empty(t, reason)
}

// assertUnwindFallback asserts the unwind falls back (any reason).
func assertUnwindFallback(t *testing.T, sw *CertifiedSwitch, tail *unrootedTail, ms *state.MithrilState, sched *sealevel.SysvarEpochSchedule, epoch uint64, ri *rewards.PartitionedRewardDistributionInfo) {
	t.Helper()
	rs, reason := tryInLoopUnwind(sw, tail, ms, sched, epoch, ri)
	assert.Nil(t, rs)
	assert.NotEmpty(t, reason, "fallback must carry a reason for the instrumentation")
}

// assertUnwindFallbackReason asserts the unwind falls back with the exact
// instrumented reason operators will see.
func assertUnwindFallbackReason(t *testing.T, want string, sw *CertifiedSwitch, tail *unrootedTail, ms *state.MithrilState, sched *sealevel.SysvarEpochSchedule, epoch uint64, ri *rewards.PartitionedRewardDistributionInfo) {
	t.Helper()
	rs, reason := tryInLoopUnwind(sw, tail, ms, sched, epoch, ri)
	assert.Nil(t, rs)
	assert.Equal(t, want, reason)
}
