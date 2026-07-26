package replay

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/stretchr/testify/require"
)

func TestTransactionStatusCheckpointPrepareReadAndIdempotence(t *testing.T) {
	rootDir := t.TempDir()
	payload := bytes.Repeat([]byte{0x42, 0x91, 0x07}, 4096)

	ref, err := PrepareTransactionStatusCheckpoint(rootDir, 4431122, payload)
	require.NoError(t, err)
	require.Equal(t, uint64(4431122), ref.Root)
	require.Equal(t, uint64(len(payload)), ref.Size)
	require.Len(t, ref.SHA256, 64)
	require.Equal(t, filepath.Base(ref.File), ref.File)
	require.NoError(t, ValidateTransactionStatusCheckpointRef(ref, 4431122))

	got, err := ReadTransactionStatusCheckpoint(rootDir, ref)
	require.NoError(t, err)
	require.Equal(t, payload, got)
	require.NoError(t, VerifyTransactionStatusCheckpoint(rootDir, ref, 4431122))

	// Preparing identical content converges on the immutable existing inode.
	retryRef, err := PrepareTransactionStatusCheckpoint(rootDir, 4431122, payload)
	require.NoError(t, err)
	require.Equal(t, ref, retryRef)
	entries, err := os.ReadDir(filepath.Join(rootDir, TransactionStatusCheckpointDirectory))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, ref.File, entries[0].Name())
}

// An older binary rewrites the state file without the checkpoint reference,
// leaving a usable sidecar that nothing names. Recovery must rebuild the exact
// reference from the filename so resume does not demand a re-bootstrap.
func TestTransactionStatusCheckpointRecoversReferenceFromDisk(t *testing.T) {
	rootDir := t.TempDir()
	payload := bytes.Repeat([]byte{0x17, 0x3c}, 8192)

	original, err := PrepareTransactionStatusCheckpoint(rootDir, 991122, payload)
	require.NoError(t, err)

	recovered, err := RecoverTransactionStatusCheckpoint(rootDir, 991122)
	require.NoError(t, err)
	require.NotNil(t, recovered, "a verified sidecar for the expected root must be recoverable")
	require.Equal(t, original, recovered)

	got, err := ReadTransactionStatusCheckpoint(rootDir, recovered)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestTransactionStatusCheckpointRecoveryIsFailClosed(t *testing.T) {
	rootDir := t.TempDir()
	ref, err := PrepareTransactionStatusCheckpoint(rootDir, 550000, bytes.Repeat([]byte{0x5a}, 2048))
	require.NoError(t, err)

	// A sidecar for a different root is never substituted.
	other, err := RecoverTransactionStatusCheckpoint(rootDir, 550001)
	require.NoError(t, err)
	require.Nil(t, other)

	// An empty directory yields no reference rather than an error.
	empty, err := RecoverTransactionStatusCheckpoint(t.TempDir(), 550000)
	require.NoError(t, err)
	require.Nil(t, empty)

	// Content that no longer matches the digest in its own name is rejected:
	// the filename alone must never be enough to resurrect a corrupt payload.
	path := filepath.Join(rootDir, TransactionStatusCheckpointDirectory, ref.File)
	require.NoError(t, os.Chmod(path, 0o600))
	require.NoError(t, os.WriteFile(path, bytes.Repeat([]byte{0x00}, 2048), 0o600))
	corrupt, err := RecoverTransactionStatusCheckpoint(rootDir, 550000)
	require.NoError(t, err)
	require.Nil(t, corrupt, "a tampered sidecar must not be recovered")
}

func TestTransactionStatusCheckpointRejectsTraversalTamperAndSymlink(t *testing.T) {
	rootDir := t.TempDir()
	ref, err := PrepareTransactionStatusCheckpoint(rootDir, 91, []byte("authoritative-status-window"))
	require.NoError(t, err)

	traversal := *ref
	traversal.File = filepath.Join("..", ref.File)
	require.ErrorContains(t, ValidateTransactionStatusCheckpointRef(&traversal, 91), "filename")
	_, err = ReadTransactionStatusCheckpoint(rootDir, &traversal)
	require.Error(t, err)

	path := filepath.Join(rootDir, TransactionStatusCheckpointDirectory, ref.File)
	tampered := []byte("authoritative-status-windox") // same length, different digest
	require.Equal(t, int(ref.Size), len(tampered))
	require.NoError(t, os.WriteFile(path, tampered, 0o644))
	_, err = ReadTransactionStatusCheckpoint(rootDir, ref)
	require.ErrorContains(t, err, "SHA-256 mismatch")

	// Even a symlink with the exact content-addressed basename is rejected.
	require.NoError(t, os.Remove(path))
	target := filepath.Join(rootDir, "outside-checkpoint")
	require.NoError(t, os.WriteFile(target, []byte("authoritative-status-window"), 0o644))
	require.NoError(t, os.Symlink(target, path))
	_, err = ReadTransactionStatusCheckpoint(rootDir, ref)
	require.ErrorContains(t, err, "non-symlink")
}

func TestTransactionStatusCheckpointCleanupKeepsOnlySelectedRefs(t *testing.T) {
	rootDir := t.TempDir()
	ref1, err := PrepareTransactionStatusCheckpoint(rootDir, 100, []byte("checkpoint-one"))
	require.NoError(t, err)
	ref2, err := PrepareTransactionStatusCheckpoint(rootDir, 200, []byte("checkpoint-two"))
	require.NoError(t, err)
	dir := filepath.Join(rootDir, TransactionStatusCheckpointDirectory)
	partial := filepath.Join(dir, ".checkpoint-interrupted.partial")
	unknown := filepath.Join(dir, "operator-notes")
	require.NoError(t, os.WriteFile(partial, []byte("partial"), 0o644))
	require.NoError(t, os.WriteFile(unknown, []byte("keep unknown files"), 0o644))

	removed, err := CleanupTransactionStatusCheckpoints(rootDir, []*state.TransactionStatusCheckpointRef{ref2})
	require.NoError(t, err)
	require.Len(t, removed, 2, "unselected final plus interrupted partial")
	_, err = os.Stat(filepath.Join(dir, ref1.File))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(partial)
	require.ErrorIs(t, err, os.ErrNotExist)
	require.FileExists(t, filepath.Join(dir, ref2.File))
	require.FileExists(t, unknown)
	_, err = ReadTransactionStatusCheckpoint(rootDir, ref2)
	require.NoError(t, err)
}

func TestTransactionStatusCheckpointCleanupValidatesKeepSetBeforeDeleting(t *testing.T) {
	rootDir := t.TempDir()
	ref, err := PrepareTransactionStatusCheckpoint(rootDir, 100, []byte("selected"))
	require.NoError(t, err)
	bad := *ref
	bad.Root++

	_, err = CleanupTransactionStatusCheckpoints(rootDir, []*state.TransactionStatusCheckpointRef{&bad})
	require.Error(t, err)
	require.FileExists(t, filepath.Join(rootDir, TransactionStatusCheckpointDirectory, ref.File), "invalid keep set must fail before deletion")
}

func TestTransactionStatusCheckpointRejectsOversizeReferenceBeforeIO(t *testing.T) {
	const zeroDigest = "0000000000000000000000000000000000000000000000000000000000000000"
	ref := &state.TransactionStatusCheckpointRef{
		Version: TransactionStatusCheckpointVersion,
		Root:    55,
		File:    transactionStatusCheckpointBasename(55, zeroDigest),
		Size:    transactionStatusCheckpointMaxSize + 1,
		SHA256:  zeroDigest,
	}
	require.ErrorContains(t, ValidateTransactionStatusCheckpointRef(ref, 55), "outside allowed range")
	_, err := ReadTransactionStatusCheckpoint(t.TempDir(), ref)
	require.ErrorContains(t, err, "outside allowed range")
}
