package replay

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

const (
	zeroTransactionStatusDigest = "0000000000000000000000000000000000000000000000000000000000000000"
	oneTransactionStatusDigest  = "0000000000000000000000000000000000000000000000000000000000000001"
)

// A root can own several content-addressed siblings after a crash between
// prepare and commit. Recovery scans them in directory order, so an unusable
// sibling encountered first must not stop the scan before the good file.
func TestTransactionStatusCheckpointRecoverySkipsUnusableSiblingsAtSameRoot(t *testing.T) {
	rootDir := t.TempDir()
	const root = uint64(7700)
	good, err := PrepareTransactionStatusCheckpoint(rootDir, root, bytes.Repeat([]byte{0xa1, 0x5c}, 256))
	require.NoError(t, err)

	dir := filepath.Join(rootDir, TransactionStatusCheckpointDirectory)
	// Digests of all-zero and near-zero sort ahead of any real payload digest,
	// so both bad siblings are reached before the good one.
	tampered := transactionStatusCheckpointBasename(root, zeroTransactionStatusDigest)
	empty := transactionStatusCheckpointBasename(root, oneTransactionStatusDigest)
	require.Less(t, tampered, good.File)
	require.Less(t, empty, good.File)
	require.NoError(t, os.WriteFile(filepath.Join(dir, tampered), bytes.Repeat([]byte{0xff}, 512), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, empty), nil, 0o644))

	recovered, err := RecoverTransactionStatusCheckpoint(rootDir, root)
	require.NoError(t, err)
	require.NotNil(t, recovered, "a verified sibling must be found past unusable ones")
	require.Equal(t, good, recovered)
}

// The in-memory blob round-trip cannot tell an empty AlreadyProcessed window
// from a populated one. Drive the whole file path and assert on behaviour: a
// pre-checkpoint message must still be rejected after restore.
func TestClassicStatusCheckpointFileRoundTripPreservesAlreadyProcessedWindow(t *testing.T) {
	rootDir := t.TempDir()
	live := NewTransactionStatusCache()
	committed := statusCacheTestTransaction(0x11, 0x22, 0x33)
	require.NoError(t, live.CommitBlock(statusCacheTestBlock(1, committed)))
	require.NoError(t, live.CommitBlock(statusCacheTestBlock(2, statusCacheTestTransaction(0x44, 0x55, 0x66))))

	payload, err := live.SnapshotThrough(2)
	require.NoError(t, err)
	ref, err := PrepareTransactionStatusCheckpoint(rootDir, 2, payload)
	require.NoError(t, err)

	data, err := ReadTransactionStatusCheckpoint(rootDir, ref)
	require.NoError(t, err)
	restored, err := NewTransactionStatusCacheFromSnapshot(data)
	require.NoError(t, err)
	require.True(t, restored.CoverageComplete())
	tip, ok := restored.TipSlot()
	require.True(t, ok)
	require.Equal(t, uint64(2), tip)
	require.Equal(t, uint64(2), restored.RootedThrough())

	// Same message, new signature: Agave's AlreadyProcessed key is
	// (recent_blockhash, message_hash), so the retry must be rejected.
	err = restored.ValidateBlock(statusCacheTestBlock(3, statusCacheTestTransaction(0x11, 0x22, 0x99)))
	var ancestor *AncestorAlreadyProcessedTransactionMessagesError
	require.ErrorAs(t, err, &ancestor)
	require.Equal(t, uint64(1), ancestor.AlreadyProcessedCount)
	require.Equal(t, uint64(1), ancestor.Occurrences[0].ProcessedSlot)

	// The second checkpointed bank is present too, not just the tip's delta.
	err = restored.ValidateBlock(statusCacheTestBlock(3, statusCacheTestTransaction(0x44, 0x55, 0x67)))
	require.ErrorAs(t, err, &ancestor)
	require.Equal(t, uint64(2), ancestor.Occurrences[0].ProcessedSlot)

	// A transaction outside the restored window is still admissible.
	require.NoError(t, restored.ValidateBlock(statusCacheTestBlock(3, statusCacheTestTransaction(0x77, 0x88, 0x01))))
}

// A payload whose tip is behind the root it is named for must never load. The
// mint-time tip>=root guard is what stops such a file existing; this pins the
// backstop, so a checkpoint that slips past minting still fails closed instead
// of restoring a short AlreadyProcessed window.
func TestClassicStatusCheckpointWithPayloadTipBehindRootFailsClosed(t *testing.T) {
	rootDir := t.TempDir()
	live := NewTransactionStatusCache()
	for slot := uint64(1); slot <= 9; slot++ {
		require.NoError(t, live.CommitBlock(statusCacheTestBlock(slot,
			statusCacheTestTransaction(byte(slot), byte(slot), byte(slot)))))
	}
	tip, ok := live.TipSlot()
	require.True(t, ok)
	require.Equal(t, uint64(9), tip, "the status cache never committed slot 10")

	// persistedHashes published slot 10 before the status cache committed it.
	payload, err := live.SnapshotThrough(10)
	require.NoError(t, err)
	ref, err := PrepareTransactionStatusCheckpoint(rootDir, 10, payload)
	require.NoError(t, err)

	_, err = loadTransactionStatusCacheForReplay(rootDir, 10, 0, ref, solana.Hash{}, false)
	require.Error(t, err, "a payload short of its named root must not load")
	require.ErrorContains(t, err, "durable replay parent 10")
}

// This package's enum is a second, independent transcription of the Agave
// TransactionError list, and the original bug was a transcription error. Pin
// the values here; the decoder itself is pinned by its own arity test in
// pkg/txstatus, since this package cannot observe its case list.
func TestTransactionErrorTagValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		got  TransactionErrorType
		want int
	}{
		{"WouldExceedAccountDataTotalLimit", TransactionErrorWouldExceedAccountDataTotalLimit, 29},
		{"DuplicateInstruction", TransactionErrorDuplicateInstruction, 30},
		{"InsufficientFundsForRent", TransactionErrorInsufficientFundsForRent, 31},
		{"ResanitizationNeeded", TransactionErrorResanitizationNeeded, 34},
		{"ProgramExecutionTemporarilyRestricted", TransactionErrorProgramExecutionTemporarilyRestricted, 35},
	} {
		if int(tc.got) != tc.want {
			t.Errorf("%s = %d, want %d: the snapshot decoder assumes this tag, so a shift here desyncs status-cache decoding",
				tc.name, int(tc.got), tc.want)
		}
	}
}

// The mint-time guard is the only thing stopping a checkpoint that names a root
// its payload never reaches. A short tip must be refused; a tip past root is
// fine because SnapshotThrough truncates.
func TestCheckStatusCacheCoversRoot(t *testing.T) {
	cache := NewTransactionStatusCache()
	for slot := uint64(1); slot <= 5; slot++ {
		require.NoError(t, cache.CommitBlock(statusCacheTestBlock(slot,
			statusCacheTestTransaction(byte(slot), byte(slot), byte(slot)))))
	}

	require.NoError(t, checkStatusCacheCoversRoot(cache, 5), "tip == root must be accepted")
	require.NoError(t, checkStatusCacheCoversRoot(cache, 4), "tip past root is truncated by SnapshotThrough")

	err := checkStatusCacheCoversRoot(cache, 6)
	require.Error(t, err, "a tip short of root must be refused at mint time")
	require.ErrorContains(t, err, "does not cover persisted slot 6")

	// A cache that committed nothing has no tip and cannot cover any root.
	require.Error(t, checkStatusCacheCoversRoot(NewTransactionStatusCache(), 1))
}
