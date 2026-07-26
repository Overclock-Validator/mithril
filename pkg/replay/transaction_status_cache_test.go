package replay

import (
	"errors"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestClassicTransactionStatusCheckpointRestoresPostSnapshotTip(t *testing.T) {
	cache := NewTransactionStatusCache()
	first := statusCacheTestBlock(1, statusCacheTestTransaction(1, 2, 3))
	second := statusCacheTestBlock(2, statusCacheTestTransaction(2, 3, 4))
	require.NoError(t, cache.CommitBlock(first))
	require.NoError(t, cache.CommitBlock(second))

	payload, err := cache.SnapshotThrough(second.Slot)
	require.NoError(t, err)

	root := t.TempDir()
	_, err = loadTransactionStatusCacheForReplay(
		root, second.Slot, 0, nil, solana.Hash{}, false,
	)
	require.ErrorContains(t, err, "checkpoint reference is missing")

	ref, err := PrepareTransactionStatusCheckpoint(root, second.Slot, payload)
	require.NoError(t, err)
	restored, err := loadTransactionStatusCacheForReplay(
		root, second.Slot, 0, ref, solana.Hash{}, false,
	)
	require.NoError(t, err)
	require.True(t, restored.CoverageComplete())
	require.Equal(t, second.Slot, restored.RootedThrough())
	tip, ok := restored.TipSlot()
	require.True(t, ok)
	require.Equal(t, second.Slot, tip)
}

func statusCacheTestTransaction(blockhashByte, messageByte, signatureByte byte) *solana.Transaction {
	return &solana.Transaction{
		Signatures: []solana.Signature{{signatureByte}},
		Message: solana.Message{
			Header:          solana.MessageHeader{NumRequiredSignatures: 1},
			AccountKeys:     []solana.PublicKey{{messageByte}, {0x44}},
			RecentBlockhash: solana.Hash{blockhashByte},
			Instructions: []solana.CompiledInstruction{{
				ProgramIDIndex: 1,
				Accounts:       []uint16{0},
				Data:           []byte{messageByte},
			}},
		},
	}
}

func statusCacheTestBlock(slot uint64, txs ...*solana.Transaction) *b.Block {
	return &b.Block{Slot: slot, ParentSlot: slot - 1, Transactions: txs}
}

func TestTransactionStatusCacheRejectsAncestorMessageWithDifferentSignature(t *testing.T) {
	cache := NewTransactionStatusCache()
	parentTx := statusCacheTestTransaction(1, 2, 3)
	retry := statusCacheTestTransaction(1, 2, 9)
	require.NoError(t, cache.ValidateBlock(statusCacheTestBlock(10, parentTx)))
	require.NoError(t, cache.CommitBlock(statusCacheTestBlock(10, parentTx)))

	err := cache.ValidateBlock(statusCacheTestBlock(11, retry))
	var ancestorErr *AncestorAlreadyProcessedTransactionMessagesError
	require.Error(t, err)
	require.True(t, errors.As(err, &ancestorErr))
	require.Equal(t, uint64(11), ancestorErr.Slot)
	require.Equal(t, uint64(1), ancestorErr.AlreadyProcessedCount)
	require.Equal(t, []AncestorAlreadyProcessedOccurrence{{Index: 0, ProcessedSlot: 10}}, ancestorErr.Occurrences)
	require.ErrorContains(t, err, "0->slot 10")
}

func TestTransactionStatusCacheAllowsSamePayloadWithDifferentRecentBlockhash(t *testing.T) {
	cache := NewTransactionStatusCache()
	parentTx := statusCacheTestTransaction(1, 2, 3)
	differentBlockhash := statusCacheTestTransaction(9, 2, 4)
	require.NoError(t, cache.CommitBlock(statusCacheTestBlock(10, parentTx)))
	require.NoError(t, cache.ValidateBlock(statusCacheTestBlock(11, differentBlockhash)))
}

func TestTransactionStatusCacheUnwindAllowsSiblingAndPinnedViewStaysCoherent(t *testing.T) {
	cache := NewTransactionStatusCache()
	commonTx := statusCacheTestTransaction(1, 1, 1)
	branchTx := statusCacheTestTransaction(2, 2, 2)
	require.NoError(t, cache.CommitBlock(statusCacheTestBlock(10, commonTx)))
	require.NoError(t, cache.CommitBlock(statusCacheTestBlock(11, branchTx)))

	pinnedBranch := cache.View()
	contains, err := pinnedBranch.ContainsTransaction(statusCacheTestTransaction(2, 2, 8))
	require.NoError(t, err)
	require.True(t, contains)

	require.NoError(t, cache.Unwind(11))
	sibling := statusCacheTestBlock(12, statusCacheTestTransaction(2, 2, 9))
	sibling.ParentSlot = 10
	require.NoError(t, cache.ValidateBlock(sibling))
	contains, err = pinnedBranch.ContainsTransaction(statusCacheTestTransaction(2, 2, 10))
	require.NoError(t, err)
	require.True(t, contains, "immutable producer view changed under replay unwind")

	newSiblingView := cache.View()
	contains, err = newSiblingView.ContainsTransaction(statusCacheTestTransaction(2, 2, 11))
	require.NoError(t, err)
	require.False(t, contains)
}

func TestTransactionStatusCacheRejectedBankDoesNotPoison(t *testing.T) {
	cache := NewTransactionStatusCache()
	tx := statusCacheTestTransaction(1, 2, 3)
	// Validation alone is not publication. This models any execution/footer/
	// commit failure after the pre-consensus check.
	require.NoError(t, cache.ValidateBlock(statusCacheTestBlock(10, tx)))
	require.NoError(t, cache.ValidateBlock(statusCacheTestBlock(11, statusCacheTestTransaction(1, 2, 9))))
}

func TestTransactionStatusCacheRootPrunesAfterMaxRecentBlockhashes(t *testing.T) {
	cache := NewTransactionStatusCache()
	var oldest, newest *solana.Transaction
	for i := uint64(1); i <= maxTransactionStatusRoots+1; i++ {
		tx := statusCacheTestTransaction(byte(i), byte(i>>8), byte(i>>16))
		// Ensure uniqueness after the byte fields wrap.
		tx.Message.Instructions[0].Data = []byte{
			byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24),
		}
		if i == 1 {
			oldest = tx
		}
		if i == maxTransactionStatusRoots+1 {
			newest = tx
		}
		require.NoError(t, cache.CommitBlock(statusCacheTestBlock(i, tx)))
	}
	require.False(t, cache.Root(maxTransactionStatusRoots+1))
	require.True(t, cache.CoverageComplete())

	oldestRetry := statusCacheTestBlock(1000, oldest)
	oldestRetry.ParentSlot = maxTransactionStatusRoots + 1
	require.NoError(t, cache.ValidateBlock(oldestRetry), "oldest root should have expired")
	newestRetry := statusCacheTestBlock(1000, newest)
	newestRetry.ParentSlot = maxTransactionStatusRoots + 1
	err := cache.ValidateBlock(newestRetry)
	var ancestorErr *AncestorAlreadyProcessedTransactionMessagesError
	require.Error(t, err)
	require.True(t, errors.As(err, &ancestorErr))
}

func TestTransactionStatusCacheSnapshotRoundTripPreservesCoverageAndKeyIndex(t *testing.T) {
	cache := NewTransactionStatusCache()
	for i := uint64(1); i <= 3; i++ {
		require.NoError(t, cache.CommitBlock(statusCacheTestBlock(i, statusCacheTestTransaction(byte(i), byte(i+1), byte(i+2)))))
	}
	blob, err := cache.SnapshotThrough(2)
	require.NoError(t, err)
	restored, err := NewTransactionStatusCacheFromSnapshot(blob)
	require.NoError(t, err)
	require.True(t, restored.CoverageComplete())

	parentRetry := statusCacheTestBlock(4, statusCacheTestTransaction(1, 2, 99))
	parentRetry.ParentSlot = 2
	err = restored.ValidateBlock(parentRetry)
	var ancestorErr *AncestorAlreadyProcessedTransactionMessagesError
	require.Error(t, err)
	require.True(t, errors.As(err, &ancestorErr))
	require.Equal(t, uint64(1), ancestorErr.Occurrences[0].ProcessedSlot)
	notSnapshotted := statusCacheTestBlock(4, statusCacheTestTransaction(3, 4, 99))
	notSnapshotted.ParentSlot = 2
	require.NoError(t, restored.ValidateBlock(notSnapshotted), "snapshot through slot 2 must not contain slot 3")
}

func TestTransactionStatusCacheSnapshotRejectsCorruption(t *testing.T) {
	_, err := NewTransactionStatusCacheFromSnapshot([]byte("not-a-status-cache"))
	require.Error(t, err)
}

func TestTransactionStatusCacheSnapshotRejectsForgedCompleteCoverage(t *testing.T) {
	cache := NewTransactionStatusCache()
	require.NoError(t, cache.CommitBlock(statusCacheTestBlock(1, statusCacheTestTransaction(1, 2, 3))))
	blob, err := cache.SnapshotThrough(1)
	require.NoError(t, err)
	require.Equal(t, byte(3), blob[4], "complete + genesis-origin flags")

	withoutGenesisProof := append([]byte(nil), blob...)
	withoutGenesisProof[4] &^= 2
	_, err = NewTransactionStatusCacheFromSnapshot(withoutGenesisProof)
	require.ErrorContains(t, err, "no genesis-origin proof")

	wrongRootCount := append([]byte(nil), blob...)
	wrongRootCount[5] = 0 // rootedSinceSeed is little-endian u16 at bytes 5..6
	_, err = NewTransactionStatusCacheFromSnapshot(wrongRootCount)
	require.ErrorContains(t, err, "rooted count 0 for 1 retained banks")
}

func TestTransactionStatusCacheRejectsWrongParentLineage(t *testing.T) {
	cache := NewTransactionStatusCache()
	parent := statusCacheTestBlock(10, statusCacheTestTransaction(1, 2, 3))
	parent.HasAlpenglowBlockID = true
	parent.AlpenglowBlockID = solana.Hash{0xaa}
	require.NoError(t, cache.CommitBlock(parent))

	wrongSlot := statusCacheTestBlock(12)
	wrongSlot.ParentSlot = 9
	var lineageErr *TransactionStatusLineageError
	require.Error(t, cache.ValidateBlock(wrongSlot))
	require.ErrorAs(t, cache.ValidateBlock(wrongSlot), &lineageErr)
	require.False(t, lineageErr.BlockIDMismatch)

	wrongID := statusCacheTestBlock(12)
	wrongID.ParentSlot = 10
	wrongID.HasAlpenglowParentBlockID = true
	wrongID.AlpenglowParentBlockID = solana.Hash{0xbb}
	lineageErr = nil
	require.ErrorAs(t, cache.ValidateBlock(wrongID), &lineageErr)
	require.True(t, lineageErr.BlockIDMismatch)
}

func TestPreConsensusTransactionStatusValidationUsesIngressParent(t *testing.T) {
	cache := NewTransactionStatusCache()
	parent := statusCacheTestBlock(10, statusCacheTestTransaction(1, 2, 3))
	parent.HasAlpenglowBlockID = true
	parent.AlpenglowBlockID = solana.Hash{0xaa}
	require.NoError(t, cache.CommitBlock(parent))

	// Fresh turbine/Lightbringer candidates have immutable ingress metadata,
	// but replay has not configured ParentSlot yet.
	ingress := &b.Block{
		Slot:                      11,
		SourceParentSlot:          10,
		HasAlpenglowParentBlockID: true,
		AlpenglowParentBlockID:    solana.Hash{0xaa},
	}
	require.NoError(t, validatePreConsensusTransactionStatuses(cache, ingress, 10))
	require.Zero(t, ingress.ParentSlot, "pre-consensus validation must not mutate the emitted candidate")

	t.Run("wrong source parent is not hidden by replay parent", func(t *testing.T) {
		wrongParent := *ingress
		wrongParent.SourceParentSlot = 9
		wrongParent.ParentSlot = 10
		var lineageErr *TransactionStatusLineageError
		require.ErrorAs(t, validatePreConsensusTransactionStatuses(cache, &wrongParent, 10), &lineageErr)
		require.Equal(t, uint64(9), lineageErr.ParentSlot)
	})

	t.Run("configured replay parent is independently revalidated", func(t *testing.T) {
		configuredWrong := *ingress
		configuredWrong.ParentSlot = 9
		var lineageErr *TransactionStatusLineageError
		require.ErrorAs(t, cache.ValidateBlock(&configuredWrong), &lineageErr)
		require.Equal(t, uint64(9), lineageErr.ParentSlot)
	})

	t.Run("wrong source parent block id is rejected", func(t *testing.T) {
		wrongID := *ingress
		wrongID.AlpenglowParentBlockID = solana.Hash{0xbb}
		var lineageErr *TransactionStatusLineageError
		require.ErrorAs(t, validatePreConsensusTransactionStatuses(cache, &wrongID, 10), &lineageErr)
		require.True(t, lineageErr.BlockIDMismatch)
	})

	t.Run("local block uses configured parent", func(t *testing.T) {
		local := *ingress
		local.SourceParentSlot = 0
		local.ParentSlot = 10
		require.NoError(t, validatePreConsensusTransactionStatuses(cache, &local, 9))
	})

	t.Run("source without parent metadata uses selected anchor", func(t *testing.T) {
		anchorCache := NewTransactionStatusCache()
		require.NoError(t, anchorCache.CommitBlock(statusCacheTestBlock(10)))
		candidate := &b.Block{Slot: 11}
		require.NoError(t, validatePreConsensusTransactionStatuses(anchorCache, candidate, 10))
		require.Zero(t, candidate.ParentSlot)
	})

	t.Run("skipped slot gap may still link to selected parent", func(t *testing.T) {
		afterGap := *ingress
		afterGap.Slot = 14
		require.NoError(t, validatePreConsensusTransactionStatuses(cache, &afterGap, 10))
	})
}

func snapshotStatusCacheStatusForTx(t *testing.T, tx *solana.Transaction, keyIndex uint64) txstatus.SnapshotStatus {
	t.Helper()
	messageHash, err := TransactionMessageHash(tx)
	require.NoError(t, err)
	key := sliceTransactionStatusKey(messageHash, uint8(keyIndex))
	var imported [txstatus.CachedKeySize]byte
	copy(imported[:], key[:])
	return txstatus.SnapshotStatus{
		RecentBlockhash: [32]byte(tx.Message.RecentBlockhash),
		KeyIndex:        keyIndex,
		Keys:            [][txstatus.CachedKeySize]byte{imported},
	}
}

func TestTransactionStatusCacheAgaveSeedSortsRootsAndFiltersNonRoots(t *testing.T) {
	root10Tx := statusCacheTestTransaction(1, 2, 3)
	root12Tx := statusCacheTestTransaction(4, 5, 6)
	nonRootTx := statusCacheTestTransaction(7, 8, 9)
	deltas := []txstatus.SnapshotSlotDelta{
		{Slot: 0, IsRoot: true},
		{Slot: 12, IsRoot: true, Statuses: []txstatus.SnapshotStatus{snapshotStatusCacheStatusForTx(t, root12Tx, 0)}},
		{Slot: 11, IsRoot: false, Statuses: []txstatus.SnapshotStatus{snapshotStatusCacheStatusForTx(t, nonRootTx, 0)}},
		{Slot: 10, IsRoot: true, Statuses: []txstatus.SnapshotStatus{snapshotStatusCacheStatusForTx(t, root10Tx, 0)}},
	}
	cache, err := NewTransactionStatusCacheFromAgaveSnapshot(deltas, 12)
	require.NoError(t, err)
	require.True(t, cache.CoverageComplete())

	rootRetry := statusCacheTestBlock(13, statusCacheTestTransaction(1, 2, 99))
	rootRetry.ParentSlot = 12
	var ancestorErr *AncestorAlreadyProcessedTransactionMessagesError
	require.ErrorAs(t, cache.ValidateBlock(rootRetry), &ancestorErr)

	nonRootRetry := statusCacheTestBlock(13, statusCacheTestTransaction(7, 8, 99))
	nonRootRetry.ParentSlot = 12
	require.NoError(t, cache.ValidateBlock(nonRootRetry))
}

func TestTransactionStatusCacheAgaveSeedRejectsStaleRoot(t *testing.T) {
	_, err := NewTransactionStatusCacheFromAgaveSnapshot([]txstatus.SnapshotSlotDelta{{Slot: 10, IsRoot: true}}, 11)
	require.ErrorContains(t, err, "latest root 10 does not match replay parent 11")
}

func TestTransactionStatusCacheAgaveSeedRejectsInconsistentKeyIndex(t *testing.T) {
	tx := statusCacheTestTransaction(1, 2, 3)
	status0 := snapshotStatusCacheStatusForTx(t, tx, 0)
	status1 := snapshotStatusCacheStatusForTx(t, tx, 1)
	_, err := NewTransactionStatusCacheFromAgaveSnapshot([]txstatus.SnapshotSlotDelta{
		{Slot: 0, IsRoot: true},
		{Slot: 10, IsRoot: true, Statuses: []txstatus.SnapshotStatus{status0}},
		{Slot: 11, IsRoot: true, Statuses: []txstatus.SnapshotStatus{status1}},
	}, 11)
	require.ErrorContains(t, err, "inconsistent key indexes")
}
