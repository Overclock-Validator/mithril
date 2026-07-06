package replay

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeDurable is a blockAccountSource: known keys return their value, unknown
// keys return a placeholder (mirroring AccountsDb.GetAccountsBatch). batchCalls
// counts GetAccountsBatch invocations so tests can assert misses are batched.
type fakeDurable struct {
	known      map[solana.PublicKey]uint64
	batchCalls int
}

func (d *fakeDurable) GetAccount(slot uint64, pubkey solana.PublicKey) (*accounts.Account, error) {
	if l, ok := d.known[pubkey]; ok {
		return &accounts.Account{Key: pubkey, Lamports: l}, nil
	}
	return &accounts.Account{Key: pubkey}, nil // placeholder (zero lamports)
}

func (d *fakeDurable) GetAccountsBatch(ctx context.Context, slot uint64, pks []solana.PublicKey) ([]*accounts.Account, error) {
	d.batchCalls++
	out := make([]*accounts.Account, len(pks))
	for i, pk := range pks {
		out[i], _ = d.GetAccount(slot, pk)
	}
	return out, nil
}

func testKey(b byte) solana.PublicKey { return solana.PublicKey{b} }
func testAccount(b byte, lamports uint64) *accounts.Account {
	return &accounts.Account{Key: testKey(b), Lamports: lamports}
}
func testHash(b byte) [32]byte    { var h [32]byte; h[0] = b; return h }
func testHashBytes(b byte) []byte { h := make([]byte, 32); h[0] = b; return h }

// fakeCommitter records CommitBatch calls and applies deltas to a durable
// MemAccounts, optionally failing any chunk containing a chosen slot.
// Failure is all-or-nothing per chunk, mirroring the real segment fold.
type fakeCommitter struct {
	durable   accounts.MemAccounts
	committed []uint64 // every folded slot, ascending across batches
	throughs  []uint64 // chunkThrough per CommitBatch call
	slotBH    map[uint64][32]byte
	ctxs      map[uint64][]byte
	failOn    uint64
}

func (f *fakeCommitter) CommitBatch(deltas []accounts.SlotDelta, throughSlot uint64, bankhashes map[uint64][32]byte, resumeCtx []byte) (accountsdb.BatchCommitResult, error) {
	for _, sd := range deltas {
		if f.failOn != 0 && sd.Slot == f.failOn {
			return accountsdb.BatchCommitResult{}, fmt.Errorf("commit boom at slot %d", sd.Slot)
		}
	}
	keys := 0
	for _, sd := range deltas {
		for _, a := range sd.Delta {
			if a == nil {
				continue
			}
			_ = f.durable.SetAccountWithoutLock(a.Key, a)
			keys++
		}
		f.committed = append(f.committed, sd.Slot)
	}
	f.throughs = append(f.throughs, throughSlot)
	if f.slotBH == nil {
		f.slotBH = make(map[uint64][32]byte)
	}
	for slot, bh := range bankhashes {
		f.slotBH[slot] = bh
	}
	if f.ctxs == nil {
		f.ctxs = make(map[uint64][]byte)
	}
	f.ctxs[throughSlot] = append([]byte(nil), resumeCtx...)
	return accountsdb.BatchCommitResult{ThroughSlot: throughSlot, Keys: keys}, nil
}

// Happy path: rooted prefix is committed durably in slot order and dropped from
// the overlay; the unrooted tail above `through` stays held; bankhashes pruned.
func TestPromoteRootedHappyPath(t *testing.T) {
	durable := accounts.NewMemAccounts()
	overlay := accounts.NewWorkingSet()
	overlay.Add(5, []*accounts.Account{testAccount(1, 51), testAccount(2, 52)})
	overlay.Add(7, []*accounts.Account{testAccount(2, 72)})
	overlay.Add(9, []*accounts.Account{testAccount(3, 93)}) // unrooted tip, stays held

	bankhashes := map[uint64][32]byte{5: testHash(5), 7: testHash(7), 9: testHash(9)}
	contexts := map[uint64]*state.ResumeContext{5: {Slot: 5}, 7: {Slot: 7}, 9: {Slot: 9}}
	fc := &fakeCommitter{durable: durable}

	promoted, err := promoteRootedBatched(overlay, 7, bankhashes, contexts, fc, 2, "", false)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), promoted)
	assert.Equal(t, []uint64{5, 7}, fc.committed, "committed ascending")
	assert.Equal(t, 1, overlay.HeldSlots(), "only slot 9 remains")
	_, has5 := bankhashes[5]
	_, has9 := bankhashes[9]
	assert.False(t, has5, "promoted bankhash pruned")
	assert.True(t, has9, "unrooted bankhash retained")
}

// Partial failure: a mid-batch commit error must promote ONLY through the last
// durably-committed slot. The failed slot and all later held slots remain in the
// overlay so no fall-through read can see a gap.
func TestPromoteRootedPartialFailureStopsAtLastDurable(t *testing.T) {
	durable := accounts.NewMemAccounts()
	overlay := accounts.NewWorkingSet()
	overlay.Add(5, []*accounts.Account{testAccount(1, 51)})
	overlay.Add(7, []*accounts.Account{testAccount(2, 72)})
	overlay.Add(9, []*accounts.Account{testAccount(3, 93)})

	bankhashes := map[uint64][32]byte{5: testHash(5), 7: testHash(7), 9: testHash(9)}
	contexts := map[uint64]*state.ResumeContext{5: {Slot: 5}, 7: {Slot: 7}, 9: {Slot: 9}}
	fc := &fakeCommitter{durable: durable, failOn: 7}

	promoted, err := promoteRootedBatched(overlay, 9, bankhashes, contexts, fc, 1, "", false)
	require.Error(t, err, "commit failure surfaces")
	assert.Equal(t, uint64(5), promoted, "advance only to last durable slot")
	assert.Equal(t, []uint64{5}, fc.committed)
	assert.Equal(t, 2, overlay.HeldSlots(), "slots 7 and 9 stay held (not durable)")
	_, has5 := bankhashes[5]
	_, has7 := bankhashes[7]
	assert.False(t, has5, "promoted slot 5 pruned")
	assert.True(t, has7, "un-promoted slot 7 retained")
}

// An empty-delta (skip/empty) slot must still be committed so its bankhash is
// recorded in the durable store.
func TestPromoteRootedEmptyDeltaSlot(t *testing.T) {
	durable := accounts.NewMemAccounts()
	overlay := accounts.NewWorkingSet()
	overlay.Add(5, nil) // empty block

	bankhashes := map[uint64][32]byte{5: testHash(5)}
	contexts := map[uint64]*state.ResumeContext{5: {Slot: 5}}
	fc := &fakeCommitter{durable: durable}

	promoted, err := promoteRootedBatched(overlay, 5, bankhashes, contexts, fc, 1, "", false)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), promoted)
	assert.Equal(t, []uint64{5}, fc.committed, "empty slot still committed for its bankhash")
	assert.Equal(t, 0, overlay.HeldSlots())
}

// The dual-watermark target: finality clamped by verification (when required)
// and the divergence floor. The key property (Codex review) is that verified
// progress advances the target even when finality is flat.
func TestSafePromoteTarget(t *testing.T) {
	// Verifier not required: target follows finality regardless of verified.
	assert.Equal(t, uint64(100), safePromoteTarget(100, false, 0, 0))
	assert.Equal(t, uint64(100), safePromoteTarget(100, false, 40, 0))

	// Required + verifier lagged: clamped to the verified watermark...
	assert.Equal(t, uint64(50), safePromoteTarget(100, true, 50, 0))
	// ...and as verification advances with finality FLAT at 100, the target
	// advances too — the verification-driven retry this fix enables.
	assert.Equal(t, uint64(75), safePromoteTarget(100, true, 75, 0))
	assert.Equal(t, uint64(100), safePromoteTarget(100, true, 100, 0))
	// Verified past finality never exceeds finality.
	assert.Equal(t, uint64(100), safePromoteTarget(100, true, 150, 0))

	// Persisted divergence floor holds the target below the disputed slot,
	// independent of the verifier.
	assert.Equal(t, uint64(79), safePromoteTarget(100, true, 100, 80))
	assert.Equal(t, uint64(79), safePromoteTarget(100, false, 0, 80))
	// Floor above the target does not raise it.
	assert.Equal(t, uint64(100), safePromoteTarget(100, true, 100, 200))
}

// A chunk whose top slot has no recorded resume context must NOT fold: the
// manifest would carry no context and recovery fatals if the store later
// outruns the state file. The fold fails and the watermark stays back.
func TestPromoteRootedMissingContextFailsClosed(t *testing.T) {
	durable := accounts.NewMemAccounts()
	overlay := accounts.NewWorkingSet()
	overlay.Add(5, []*accounts.Account{testAccount(1, 51)})
	overlay.Add(7, []*accounts.Account{testAccount(2, 72)})

	bankhashes := map[uint64][32]byte{5: testHash(5), 7: testHash(7)}
	// Context for slot 5 only; the chunk through slot 7 has none.
	contexts := map[uint64]*state.ResumeContext{5: {Slot: 5}}
	fc := &fakeCommitter{durable: durable}

	promoted, err := promoteRootedBatched(overlay, 7, bankhashes, contexts, fc, 1, "", false)
	require.ErrorContains(t, err, "no resume context")
	assert.Equal(t, uint64(5), promoted, "advance only through the last chunk that had a context")
	assert.Equal(t, []uint64{5}, fc.committed, "context-less chunk never committed")
}

// Tail reads: overlay value wins; misses fall through to durable.
func TestUnrootedTailGetAccount(t *testing.T) {
	durable := &fakeDurable{known: map[solana.PublicKey]uint64{testKey(1): 100, testKey(2): 200}}
	tail := newUnrootedTail(durable, &fakeCommitter{}, 512, 1, "")
	tail.Add(5, []*accounts.Account{testAccount(1, 51)}, testHashBytes(5)) // key 1 written unrooted

	a, err := tail.GetAccount(5, testKey(1))
	require.NoError(t, err)
	assert.Equal(t, uint64(51), a.Lamports, "overlay value wins over durable 100")

	b, err := tail.GetAccount(5, testKey(2))
	require.NoError(t, err)
	assert.Equal(t, uint64(200), b.Lamports, "miss falls through to durable")
}

// Tail batch read: order preserved, overlay hits win, misses come from ONE
// durable batch, placeholder for unknown keys.
func TestUnrootedTailGetAccountsBatch(t *testing.T) {
	durable := &fakeDurable{known: map[solana.PublicKey]uint64{testKey(2): 200}}
	tail := newUnrootedTail(durable, &fakeCommitter{}, 512, 1, "")
	tail.Add(5, []*accounts.Account{testAccount(1, 51), testAccount(4, 54)}, testHashBytes(5))

	keys := []solana.PublicKey{testKey(1), testKey(2), testKey(3), testKey(4)}
	out, err := tail.GetAccountsBatch(context.Background(), 5, keys)
	require.NoError(t, err)
	require.Len(t, out, 4, "one entry per requested key, in order")
	assert.Equal(t, uint64(51), out[0].Lamports, "overlay hit")
	assert.Equal(t, uint64(200), out[1].Lamports, "durable hit")
	assert.Equal(t, uint64(0), out[2].Lamports, "unknown -> placeholder")
	assert.Equal(t, uint64(54), out[3].Lamports, "overlay hit")
	assert.Equal(t, 1, durable.batchCalls, "misses fetched in a single durable batch")
}

// OverCap trips only when held slots exceed the cap (backpressure on stalled rooting).
func TestUnrootedTailOverCap(t *testing.T) {
	tail := newUnrootedTail(&fakeDurable{}, &fakeCommitter{}, 2, 1, "")
	tail.Add(1, nil, testHashBytes(1))
	tail.Add(2, nil, testHashBytes(2))
	assert.False(t, tail.OverCap(), "2 held == cap, not over")
	tail.Add(3, nil, testHashBytes(3))
	assert.True(t, tail.OverCap(), "3 held > cap 2")
}

// promote returns the resume context as of the highest promoted slot and prunes
// the context map for promoted slots, retaining still-held ones.
func TestUnrootedTailContextCaptureAndPromote(t *testing.T) {
	tail := newUnrootedTail(&fakeDurable{}, &fakeCommitter{durable: accounts.NewMemAccounts()}, 512, 1, "")
	tail.Add(5, []*accounts.Account{testAccount(1, 51)}, testHashBytes(5))
	tail.SetContext(5, &state.ResumeContext{Slot: 5, Bankhash: "bh5"})
	tail.Add(7, []*accounts.Account{testAccount(2, 72)}, testHashBytes(7))
	tail.SetContext(7, &state.ResumeContext{Slot: 7, Bankhash: "bh7"})
	tail.Add(9, []*accounts.Account{testAccount(3, 93)}, testHashBytes(9))
	tail.SetContext(9, &state.ResumeContext{Slot: 9, Bankhash: "bh9"})

	promotedThrough, ctx, err := tail.promote(7)
	require.NoError(t, err)
	assert.Equal(t, uint64(7), promotedThrough)
	require.NotNil(t, ctx, "promote returns the as-of-R context")
	assert.Equal(t, uint64(7), ctx.Slot, "context is for the highest promoted slot (R)")

	_, has5 := tail.contexts[5]
	_, has7 := tail.contexts[7]
	_, has9 := tail.contexts[9]
	assert.False(t, has5, "promoted context pruned")
	assert.False(t, has7, "promoted context pruned")
	assert.True(t, has9, "still-held context retained")
}

// Nothing to promote (through below all held slots) is a no-op, not an error.
func TestPromoteRootedNoPrefix(t *testing.T) {
	durable := accounts.NewMemAccounts()
	overlay := accounts.NewWorkingSet()
	overlay.Add(10, []*accounts.Account{testAccount(1, 10)})

	fc := &fakeCommitter{durable: durable}
	promoted, err := promoteRootedBatched(overlay, 5, map[uint64][32]byte{}, map[uint64]*state.ResumeContext{}, fc, 1, "", false)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), promoted)
	assert.Empty(t, fc.committed)
	assert.Equal(t, 1, overlay.HeldSlots())
}

// A trailing partial chunk (fewer than batchSlots rooted slots) must NOT fold
// on promote() — restart re-execution stays bounded by the chunk size — but
// flush() (graceful shutdown) forces it.
func TestPromoteBatchedDefersPartialChunkUntilFlush(t *testing.T) {
	fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
	tail := newUnrootedTail(&fakeDurable{}, fc, 512, 4, "")
	for slot := uint64(1); slot <= 3; slot++ {
		tail.Add(slot, []*accounts.Account{testAccount(byte(slot), slot)}, testHashBytes(byte(slot)))
		tail.SetContext(slot, &state.ResumeContext{Slot: slot})
	}

	promoted, ctx, err := tail.promote(3)
	require.NoError(t, err)
	assert.Zero(t, promoted, "partial chunk must stay in RAM on promote")
	assert.Nil(t, ctx)
	assert.Empty(t, fc.committed)

	flushed, fctx, err := tail.flush(3)
	require.NoError(t, err)
	assert.Equal(t, uint64(3), flushed, "flush force-folds the partial chunk")
	require.NotNil(t, fctx)
	assert.Equal(t, uint64(3), fctx.Slot)
	assert.Equal(t, []uint64{1, 2, 3}, fc.committed)
	assert.Equal(t, []uint64{3}, fc.throughs, "one batch, through the tip")
}

// Full chunks fold on promote; the chunk-top resume context rides along as
// serialized JSON; per-chunk bankhash maps carry exactly the chunk's slots.
func TestPromoteBatchedChunkBoundariesAndContext(t *testing.T) {
	fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
	tail := newUnrootedTail(&fakeDurable{}, fc, 512, 2, "")
	for slot := uint64(1); slot <= 5; slot++ {
		tail.Add(slot, []*accounts.Account{testAccount(byte(slot), slot)}, testHashBytes(byte(slot)))
		tail.SetContext(slot, &state.ResumeContext{Slot: slot, Bankhash: "bh"})
	}

	promoted, ctx, err := tail.promote(5)
	require.NoError(t, err)
	assert.Equal(t, uint64(4), promoted, "two full chunks fold; slot 5 is a deferred partial")
	require.NotNil(t, ctx)
	assert.Equal(t, uint64(4), ctx.Slot)
	assert.Equal(t, []uint64{2, 4}, fc.throughs)
	assert.NotEmpty(t, fc.ctxs[2], "chunk-top context serialized into the fold")
	assert.NotEmpty(t, fc.ctxs[4])

	var decoded state.ResumeContext
	require.NoError(t, json.Unmarshal(fc.ctxs[4], &decoded))
	assert.Equal(t, uint64(4), decoded.Slot)

	flushed, _, err := tail.flush(5)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), flushed)
	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, fc.committed)
}
