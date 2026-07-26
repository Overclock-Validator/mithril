package replay

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/state"
)

const (
	// TransactionStatusCheckpointDirectory is rooted directly in the AccountsDB
	// directory. Checkpoints are kept out of fold manifests so the latter remain
	// small and cheap to scan during recovery.
	TransactionStatusCheckpointDirectory = "transaction-status-checkpoints"
	TransactionStatusCheckpointVersion   = uint32(1)

	transactionStatusCheckpointPrefix = "checkpoint-"
	transactionStatusCheckpointSuffix = ".bin"
	// A 300-root window at the cluster's largest observed block sizes remains
	// comfortably below this. The bound prevents a corrupt local reference from
	// driving an attacker-sized allocation before the codec can reject it.
	transactionStatusCheckpointMaxSize = uint64(1 << 30)
)

// PrepareTransactionStatusCheckpoint durably installs an immutable checkpoint
// before the rooted fold manifest or classic state file that will reference it.
// The returned ref is only prepared; the durable record is the commit selector.
//
// Installation is temp-write + file fsync + hard-link-without-replacement +
// directory fsync. A retry for identical bytes is idempotent. A crash before
// the durable record commits can leave an unreferenced final file; cleanup may
// remove it after collecting refs from committed records.
func PrepareTransactionStatusCheckpoint(accountsDBRoot string, root uint64, payload []byte) (*state.TransactionStatusCheckpointRef, error) {
	if len(payload) == 0 {
		return nil, errors.New("prepare transaction status checkpoint: empty payload")
	}
	if uint64(len(payload)) > transactionStatusCheckpointMaxSize {
		return nil, fmt.Errorf("prepare transaction status checkpoint: payload is %d bytes, limit %d", len(payload), transactionStatusCheckpointMaxSize)
	}
	digest := sha256.Sum256(payload)
	digestHex := hex.EncodeToString(digest[:])
	ref := &state.TransactionStatusCheckpointRef{
		Version: TransactionStatusCheckpointVersion,
		Root:    root,
		File:    transactionStatusCheckpointBasename(root, digestHex),
		Size:    uint64(len(payload)),
		SHA256:  digestHex,
	}
	if err := ValidateTransactionStatusCheckpointRef(ref, root); err != nil {
		return nil, err
	}

	dir, err := transactionStatusCheckpointDir(accountsDBRoot)
	if err != nil {
		return nil, err
	}
	dirWasMissing := false
	if _, err := os.Stat(dir); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("stat transaction status checkpoint directory: %w", err)
		}
		dirWasMissing = true
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create transaction status checkpoint directory: %w", err)
	}
	if dirWasMissing {
		// Make creation of the sidecar directory itself durable. Syncing only the
		// new directory does not persist its entry in the AccountsDB parent.
		if err := syncTransactionStatusDirectory(filepath.Dir(dir)); err != nil {
			return nil, fmt.Errorf("sync AccountsDB directory after checkpoint directory creation: %w", err)
		}
	}

	finalPath := filepath.Join(dir, ref.File)
	if _, err := os.Lstat(finalPath); err == nil {
		if _, err := ReadTransactionStatusCheckpoint(accountsDBRoot, ref); err != nil {
			return nil, fmt.Errorf("existing immutable transaction status checkpoint is invalid: %w", err)
		}
		return ref, nil
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("stat transaction status checkpoint: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".checkpoint-*.partial")
	if err != nil {
		return nil, fmt.Errorf("create transaction status checkpoint temporary: %w", err)
	}
	tmpPath := tmp.Name()
	tmpOpen := true
	defer func() {
		if tmpOpen {
			_ = tmp.Close()
		}
		_ = os.Remove(tmpPath)
	}()

	if err := tmp.Chmod(0o644); err != nil {
		return nil, fmt.Errorf("chmod transaction status checkpoint temporary: %w", err)
	}
	if _, err := io.Copy(tmp, bytes.NewReader(payload)); err != nil {
		return nil, fmt.Errorf("write transaction status checkpoint temporary: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return nil, fmt.Errorf("sync transaction status checkpoint temporary: %w", err)
	}
	if err := tmp.Close(); err != nil {
		tmpOpen = false
		return nil, fmt.Errorf("close transaction status checkpoint temporary: %w", err)
	}
	tmpOpen = false

	// Link, rather than rename, gives content-addressed files no-overwrite
	// semantics. Two idempotent preparers may converge on the same final name;
	// neither can replace an already selected immutable checkpoint.
	if err := os.Link(tmpPath, finalPath); err != nil {
		if !os.IsExist(err) {
			return nil, fmt.Errorf("install immutable transaction status checkpoint: %w", err)
		}
		if _, verifyErr := ReadTransactionStatusCheckpoint(accountsDBRoot, ref); verifyErr != nil {
			return nil, fmt.Errorf("concurrent immutable transaction status checkpoint is invalid: %w", verifyErr)
		}
	}
	if err := os.Remove(tmpPath); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("remove transaction status checkpoint temporary: %w", err)
	}
	if err := syncTransactionStatusDirectory(dir); err != nil {
		return nil, fmt.Errorf("sync transaction status checkpoint directory: %w", err)
	}
	return ref, nil
}

// ValidateTransactionStatusCheckpointRef validates the complete external
// reference without touching the filesystem. The filename is derived from the
// root and digest, so basename checks are also strict path-confinement checks.
func ValidateTransactionStatusCheckpointRef(ref *state.TransactionStatusCheckpointRef, expectedRoot uint64) error {
	if ref == nil {
		return errors.New("transaction status checkpoint reference is missing")
	}
	if ref.Version != TransactionStatusCheckpointVersion {
		return fmt.Errorf("transaction status checkpoint version %d is unsupported (want %d)", ref.Version, TransactionStatusCheckpointVersion)
	}
	if ref.Root != expectedRoot {
		return fmt.Errorf("transaction status checkpoint root %d does not match expected durable root %d", ref.Root, expectedRoot)
	}
	if ref.Size == 0 || ref.Size > transactionStatusCheckpointMaxSize {
		return fmt.Errorf("transaction status checkpoint size %d is outside allowed range 1..%d", ref.Size, transactionStatusCheckpointMaxSize)
	}
	decodedDigest, err := hex.DecodeString(ref.SHA256)
	if err != nil || len(decodedDigest) != sha256.Size || hex.EncodeToString(decodedDigest) != ref.SHA256 {
		return fmt.Errorf("transaction status checkpoint SHA-256 %q is not 64 lowercase hexadecimal characters", ref.SHA256)
	}
	wantFile := transactionStatusCheckpointBasename(ref.Root, ref.SHA256)
	if ref.File != wantFile || filepath.Base(ref.File) != ref.File || filepath.Clean(ref.File) != ref.File {
		return fmt.Errorf("transaction status checkpoint filename %q is invalid (want %q)", ref.File, wantFile)
	}
	return nil
}

// ReadTransactionStatusCheckpoint verifies confinement, regular-file type,
// exact size, and SHA-256 before returning any bytes to the status-cache codec.
func ReadTransactionStatusCheckpoint(accountsDBRoot string, ref *state.TransactionStatusCheckpointRef) ([]byte, error) {
	if ref == nil {
		return nil, errors.New("read transaction status checkpoint: missing reference")
	}
	if err := ValidateTransactionStatusCheckpointRef(ref, ref.Root); err != nil {
		return nil, err
	}
	dir, err := transactionStatusCheckpointDir(accountsDBRoot)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dir, ref.File)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("stat transaction status checkpoint %q: %w", ref.File, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("transaction status checkpoint %q is not a regular non-symlink file", ref.File)
	}
	if uint64(info.Size()) != ref.Size {
		return nil, fmt.Errorf("transaction status checkpoint %q size %d does not match reference %d", ref.File, info.Size(), ref.Size)
	}
	if ref.Size > uint64(^uint(0)>>1) {
		return nil, fmt.Errorf("transaction status checkpoint %q size %d exceeds platform int", ref.File, ref.Size)
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open transaction status checkpoint %q: %w", ref.File, err)
	}
	defer f.Close()
	payload := make([]byte, int(ref.Size))
	if _, err := io.ReadFull(f, payload); err != nil {
		return nil, fmt.Errorf("read transaction status checkpoint %q: %w", ref.File, err)
	}
	var extra [1]byte
	if n, err := f.Read(extra[:]); err != io.EOF || n != 0 {
		if err == nil {
			return nil, fmt.Errorf("transaction status checkpoint %q grew beyond referenced size", ref.File)
		}
		return nil, fmt.Errorf("finish transaction status checkpoint %q: %w", ref.File, err)
	}
	got := sha256.Sum256(payload)
	want, _ := hex.DecodeString(ref.SHA256) // validated above
	if subtle.ConstantTimeCompare(got[:], want) != 1 {
		return nil, fmt.Errorf("transaction status checkpoint %q SHA-256 mismatch", ref.File)
	}
	return payload, nil
}

// VerifyTransactionStatusCheckpoint performs the same checks as Read without
// retaining the checkpoint bytes after return.
func VerifyTransactionStatusCheckpoint(accountsDBRoot string, ref *state.TransactionStatusCheckpointRef, expectedRoot uint64) error {
	if err := ValidateTransactionStatusCheckpointRef(ref, expectedRoot); err != nil {
		return err
	}
	_, err := ReadTransactionStatusCheckpoint(accountsDBRoot, ref)
	return err
}

// CleanupTransactionStatusCheckpoints removes partial writes and immutable
// checkpoints not present in keep. Callers must invoke it only while folds are
// quiescent, after collecting refs from every actionable rewind manifest. It
// deliberately ignores unknown files in the directory.
func CleanupTransactionStatusCheckpoints(accountsDBRoot string, keep []*state.TransactionStatusCheckpointRef) ([]string, error) {
	dir, err := transactionStatusCheckpointDir(accountsDBRoot)
	if err != nil {
		return nil, err
	}
	keptFiles := make(map[string]struct{}, len(keep))
	for _, ref := range keep {
		if ref == nil {
			return nil, errors.New("cleanup transaction status checkpoints: nil keep reference")
		}
		if err := ValidateTransactionStatusCheckpointRef(ref, ref.Root); err != nil {
			return nil, fmt.Errorf("cleanup transaction status checkpoints: invalid keep reference: %w", err)
		}
		keptFiles[ref.File] = struct{}{}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read transaction status checkpoint directory: %w", err)
	}
	var removed []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		isPartial := strings.HasPrefix(name, ".checkpoint-") && strings.HasSuffix(name, ".partial")
		_, isCheckpoint := parseTransactionStatusCheckpointBasename(name)
		if !isPartial && !isCheckpoint {
			continue
		}
		if _, keep := keptFiles[name]; keep {
			continue
		}
		path := filepath.Join(dir, name)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return removed, fmt.Errorf("remove unreferenced transaction status checkpoint %q: %w", name, err)
		}
		removed = append(removed, path)
	}
	if len(removed) > 0 {
		if err := syncTransactionStatusDirectory(dir); err != nil {
			return removed, fmt.Errorf("sync transaction status checkpoint directory after cleanup: %w", err)
		}
	}
	return removed, nil
}

func transactionStatusCheckpointDir(accountsDBRoot string) (string, error) {
	if strings.TrimSpace(accountsDBRoot) == "" {
		return "", errors.New("transaction status checkpoint: AccountsDB root is empty")
	}
	absRoot, err := filepath.Abs(accountsDBRoot)
	if err != nil {
		return "", fmt.Errorf("resolve AccountsDB root for transaction status checkpoint: %w", err)
	}
	return filepath.Join(filepath.Clean(absRoot), TransactionStatusCheckpointDirectory), nil
}

func transactionStatusCheckpointBasename(root uint64, digest string) string {
	return transactionStatusCheckpointPrefix + strconv.FormatUint(root, 10) + "-" + digest + transactionStatusCheckpointSuffix
}

func parseTransactionStatusCheckpointBasename(name string) (*state.TransactionStatusCheckpointRef, bool) {
	if filepath.Base(name) != name || filepath.Clean(name) != name ||
		!strings.HasPrefix(name, transactionStatusCheckpointPrefix) || !strings.HasSuffix(name, transactionStatusCheckpointSuffix) {
		return nil, false
	}
	body := strings.TrimSuffix(strings.TrimPrefix(name, transactionStatusCheckpointPrefix), transactionStatusCheckpointSuffix)
	separator := strings.LastIndexByte(body, '-')
	if separator <= 0 {
		return nil, false
	}
	root, err := strconv.ParseUint(body[:separator], 10, 64)
	if err != nil || strconv.FormatUint(root, 10) != body[:separator] {
		return nil, false
	}
	digest := body[separator+1:]
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != digest {
		return nil, false
	}
	return &state.TransactionStatusCheckpointRef{Root: root, File: name, SHA256: digest}, true
}

// ClassicStatusCheckpointForRoot resolves which sidecar describes root: rooted
// context, then the state file's reference, then a sidecar recovered from disk.
// Collection and resume must agree, or collection deletes what resume wants.
func ClassicStatusCheckpointForRoot(st *state.MithrilState, root uint64, accountsDBRoot string) *state.TransactionStatusCheckpointRef {
	if st == nil {
		return nil
	}
	if rooted := st.LastRootedContext; rooted != nil && rooted.Slot == root && rooted.TransactionStatusCheckpoint != nil {
		return rooted.TransactionStatusCheckpoint
	}
	// Validate before returning: collection re-validates every keep reference and
	// aborts wholesale on a bad one, so handing back a malformed reference here
	// would leave the directory uncollectable on every future start.
	if ref := st.LastTransactionStatusCheckpoint; ref != nil && ref.Root == root &&
		ValidateTransactionStatusCheckpointRef(ref, root) == nil {
		return ref
	}
	// Below the snapshot root the Agave seed is authoritative and no sidecar is
	// expected, so a stray file there is collectible rather than recoverable.
	if root <= st.SnapshotSlot {
		return nil
	}
	recovered, err := RecoverTransactionStatusCheckpoint(accountsDBRoot, root)
	if err != nil {
		mlog.Log.Warnf("transaction status checkpoint recovery scan failed for slot %d: %v", root, err)
		return nil
	}
	return recovered
}

// scanTransactionStatusCheckpoints yields every sidecar whose content-addressed
// name claims root, contents unread. A stat failure leaves Size 0, which every
// caller's validation then rejects.
func scanTransactionStatusCheckpoints(accountsDBRoot string, root uint64) ([]*state.TransactionStatusCheckpointRef, error) {
	dir, err := transactionStatusCheckpointDir(accountsDBRoot)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read transaction status checkpoint directory: %w", err)
	}
	var out []*state.TransactionStatusCheckpointRef
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ref, ok := parseTransactionStatusCheckpointBasename(entry.Name())
		if !ok || ref.Root != root {
			continue
		}
		ref.Version = TransactionStatusCheckpointVersion
		if info, statErr := entry.Info(); statErr == nil {
			ref.Size = uint64(info.Size())
		}
		out = append(out, ref)
	}
	return out, nil
}

// TransactionStatusCheckpointCandidates lists sidecars that could describe root.
// Retention uses this rather than the verified lookup: verification can fail for
// reasons that say nothing about the file, and collection is irreversible.
func TransactionStatusCheckpointCandidates(accountsDBRoot string, root uint64) ([]*state.TransactionStatusCheckpointRef, error) {
	refs, err := scanTransactionStatusCheckpoints(accountsDBRoot, root)
	if err != nil {
		return nil, err
	}
	var out []*state.TransactionStatusCheckpointRef
	for _, ref := range refs {
		if ValidateTransactionStatusCheckpointRef(ref, root) != nil {
			continue
		}
		out = append(out, ref)
	}
	return out, nil
}

// RecoverTransactionStatusCheckpoint rebuilds a lost reference from disk. The
// filename is content-addressed, so root and digest survive a state file that no
// longer names them — what an older binary leaves behind. Verified before trust.
func RecoverTransactionStatusCheckpoint(accountsDBRoot string, expectedRoot uint64) (*state.TransactionStatusCheckpointRef, error) {
	refs, err := scanTransactionStatusCheckpoints(accountsDBRoot, expectedRoot)
	if err != nil {
		return nil, err
	}
	for _, ref := range refs {
		if err := VerifyTransactionStatusCheckpoint(accountsDBRoot, ref, expectedRoot); err != nil {
			mlog.Log.Warnf("transaction status checkpoint %s matches root %d but failed verification: %v", ref.File, expectedRoot, err)
			continue
		}
		return ref, nil
	}
	return nil, nil
}

func syncTransactionStatusDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	err = dir.Sync()
	closeErr := dir.Close()
	if err != nil {
		return err
	}
	return closeErr
}
