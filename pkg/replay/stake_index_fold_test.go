package replay

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// orderAssertingCommitter checks, AT CommitBatch time, that the stake-index
// file already contains the chunk's entries: the flush must happen BEFORE the
// fold commit so a crash between the two leaves a harmless superset (slots
// re-execute and re-enqueue), never a subset — the index feeds epoch-stakes
// enumeration and must not miss folded slots' stake accounts.
type orderAssertingCommitter struct {
	fakeCommitter
	t        *testing.T
	idxPath  string
	expectAt map[uint64][]solana.PublicKey // chunkThrough -> pubkeys that must already be durable
}

func (o *orderAssertingCommitter) CommitBatch(deltas []accounts.SlotDelta, throughSlot uint64, bankhashes map[uint64][32]byte, resumeCtx []byte) (accountsdb.BatchCommitResult, error) {
	if want := o.expectAt[throughSlot]; len(want) > 0 {
		onDisk := readStakeIndexPubkeys(o.t, o.idxPath)
		for _, pk := range want {
			if _, ok := onDisk[pk]; !ok {
				o.t.Fatalf("CommitBatch(through=%d): stake pubkey %s not yet flushed to the index — flush must precede the fold commit", throughSlot, pk)
			}
		}
	}
	return o.fakeCommitter.CommitBatch(deltas, throughSlot, bankhashes, resumeCtx)
}

// readStakeIndexPubkeys parses the on-disk index directly (8-byte "STKI"
// header + 48-byte records) so assertions are independent of the global's
// load cache.
func readStakeIndexPubkeys(t *testing.T, path string) map[solana.PublicKey]struct{} {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		return map[solana.PublicKey]struct{}{} // no file yet = nothing flushed
	}
	out := make(map[solana.PublicKey]struct{})
	if len(data) < 8 {
		return out
	}
	for off := 8; off+accountsdb.StakeIndexRecordSize <= len(data); off += accountsdb.StakeIndexRecordSize {
		var pk solana.PublicKey
		copy(pk[:], data[off:off+32])
		out[pk] = struct{}{}
	}
	return out
}

// Stake-index entries are slot-scoped: they reach the durable index only when
// their slot FOLDS (flushed before the commit), entries above the fold stay
// RAM-pending and visible to scans, and a fork unwind drops the evicted
// suffix's entries instead of leaking them into the file.
func TestStakeIndexFoldScopedFlush(t *testing.T) {
	global.ClearPendingStakePubkeys()
	defer global.ClearPendingStakePubkeys()

	dir := t.TempDir()
	idxPath := filepath.Join(dir, global.StakePubkeyIndexFileName)

	inChunk := testKey(0xA1)   // created at slot 5 — folds
	inChunk2 := testKey(0xA2)  // created at slot 7 — folds
	aboveFold := testKey(0xB1) // created at slot 9 — stays RAM-pending
	wrongFork := testKey(0xC1) // created at slot 9 — dropped by unwind

	global.EnqueuePendingStakePubkey(5, inChunk)
	global.EnqueuePendingStakePubkey(7, inChunk2)
	global.EnqueuePendingStakePubkey(9, aboveFold)
	global.EnqueuePendingStakePubkey(9, wrongFork)

	tail := newUnrootedTail(&fakeDurable{}, nil, 512, 2, dir)
	oc := &orderAssertingCommitter{
		fakeCommitter: fakeCommitter{durable: accounts.NewMemAccounts()},
		t:             t,
		idxPath:       idxPath,
		expectAt:      map[uint64][]solana.PublicKey{7: {inChunk, inChunk2}},
	}
	tail.committer = oc
	tail.Add(5, []*accounts.Account{testAccount(1, 51)}, testHashBytes(5))
	tail.Add(7, []*accounts.Account{testAccount(2, 72)}, testHashBytes(7))
	tail.Add(9, []*accounts.Account{testAccount(3, 93)}, testHashBytes(9))
	tail.SetContext(5, &state.ResumeContext{Slot: 5})
	tail.SetContext(7, &state.ResumeContext{Slot: 7})
	tail.SetContext(9, &state.ResumeContext{Slot: 9})

	// Fold slots 5..7 (one chunk of 2). The committer asserts the flush order.
	promoted, _, err := tail.promote(7)
	require.NoError(t, err)
	require.Equal(t, uint64(7), promoted)

	// Folded slots' entries are durable; slot 9's are NOT in the file...
	onDisk := readStakeIndexPubkeys(t, idxPath)
	assert.Contains(t, onDisk, inChunk)
	assert.Contains(t, onDisk, inChunk2)
	assert.NotContains(t, onDisk, aboveFold, "unfolded slot's entries must not be durable")

	// ...but ARE visible to scans via the pending snapshot (completeness).
	pending := global.PendingStakeEntriesSnapshot()
	require.Len(t, pending, 2)

	// Fork switch unwinds slot 9: its entries drop with the state.
	tail.unwind(9)
	assert.Empty(t, global.PendingStakeEntriesSnapshot(), "unwound slots' stake entries must be dropped, not flushed")

	// The file is untouched by the unwind — wrong-fork pubkeys never leaked.
	onDisk = readStakeIndexPubkeys(t, idxPath)
	assert.NotContains(t, onDisk, wrongFork)
	assert.Len(t, onDisk, 2)
}

func TestStakeIndexFoldDerivesMissingHintsFromAcceptedWrites(t *testing.T) {
	for _, async := range []bool{false, true} {
		name := "forced"
		if async {
			name = "async"
		}
		t.Run(name, func(t *testing.T) {
			global.ClearPendingStakePubkeys()
			t.Cleanup(global.ClearPendingStakePubkeys)
			dir := t.TempDir()
			key, future, voter := testKey(0xc1), testKey(0xc2), testKey(0xc3)
			stake := &sealevel.StakeStateV2{Status: sealevel.StakeStateV2StatusStake}
			stake.Stake.Stake.Delegation.VoterPubkey = voter
			stake.Stake.Stake.Delegation.StakeLamports = 1_000_000_000
			stake.Stake.Stake.Delegation.ActivationEpoch = 1082
			data, err := sealevel.MarshalStakeStake(stake)
			require.NoError(t, err)
			acct := &accounts.Account{Key: key, Owner: addresses.StakeProgramAddr, Lamports: 1_002_282_880, Data: data}
			committer := &orderAssertingCommitter{
				fakeCommitter: fakeCommitter{durable: accounts.NewMemAccounts()},
				t:             t, idxPath: filepath.Join(dir, global.StakePubkeyIndexFileName),
				expectAt: map[uint64][]solana.PublicKey{5: {key}},
			}
			tail := newUnrootedTail(&fakeDurable{}, committer, 512, 1, dir)
			tail.stakeTotals = newStakeTotalsTracker(nil, newStakeIndexMembers(nil))

			// An execution-discard path can remove slot-scoped hints even though
			// the accepted slot's account writes remain queued for durability.
			global.EnqueuePendingStakePubkey(5, key)
			global.DropPendingStakePubkeys(5)
			global.EnqueuePendingStakePubkey(9, future)
			tail.recordStakeChange(&sealevel.SlotCtx{Slot: 5, ParentAccts: accounts.NewMemAccounts()}, []*accounts.Account{acct})
			tail.Add(5, []*accounts.Account{acct}, testHashBytes(5))
			tail.SetContext(5, &state.ResumeContext{Slot: 5})
			if async {
				job, err := tail.buildFoldJob(5, false)
				require.NoError(t, err)
				require.NotNil(t, job)
				require.NoError(t, runFoldJob(committer, job))
				tail.applyFoldJob(job)
			} else {
				through, _, err := tail.flush(5)
				require.NoError(t, err)
				require.Equal(t, uint64(5), through)
			}
			onDisk := readStakeIndexPubkeys(t, committer.idxPath)
			require.Contains(t, onDisk, key)
			require.NotContains(t, onDisk, future)
			require.Equal(t, []solana.PublicKey{future}, global.PendingStakePubkeysForSlot(9))
			require.False(t, tail.stakeTotals.broken)
			require.Equal(t, voteDelegation{stake: 1_000_000_000, accounts: 1}, tail.stakeTotals.durable[voter])
			require.Equal(t, digestStakeIndex([]accountsdb.StakeIndexEntry{{Pubkey: key}}), tail.stakeTotals.members.digest)
		})
	}
}
