package replay

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/global"
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
