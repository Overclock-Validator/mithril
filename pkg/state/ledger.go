package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/mr-tron/base58"
	"golang.org/x/sys/unix"
)

const (
	// LedgerChainBindingFileName records which genesis owns the ledger-scoped
	// catchup and voting artifacts. It deliberately lives outside AccountsDB so
	// a snapshot rebuild cannot erase the evidence needed to detect re-genesis.
	LedgerChainBindingFileName = "mithril_ledger_chain.json"
	LedgerQuarantineDirName    = "mithril-chain-quarantine"
	ledgerChainBindingVersion  = 1

	ledgerChainLockFileName          = ".mithril_ledger_chain.lock"
	ledgerTransitionIntentFileName   = ".mithril_ledger_chain_transition.json"
	ledgerTransitionIntentVersion    = 1
	ledgerTransitionPhaseMoving      = "moving"
	ledgerTransitionPhaseQuarantined = "quarantined"
)

// ErrUnboundLedgerArtifacts means safety-sensitive ledger artifacts predate
// the chain binding file. The caller must obtain an explicit operator assertion
// of their genesis; absence of metadata is never permission to reset votes.
var ErrUnboundLedgerArtifacts = errors.New("ledger artifacts have no genesis binding")

// LedgerChainBinding binds the disposable shred spool and the signed voting
// safety record in one ledger directory to a particular chain lineage.
type LedgerChainBinding struct {
	Version     uint32    `json:"version"`
	Cluster     string    `json:"cluster"`
	GenesisHash string    `json:"genesis_hash"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// LedgerGenesisTransition describes a completed reconciliation. MovedPaths are
// relative to LedgerDir and point at the original artifact names; QuarantineDir
// is empty when the prior and current genesis matched.
type LedgerGenesisTransition struct {
	PreviousGenesisHash string
	CurrentGenesisHash  string
	QuarantineDir       string
	MovedPaths          []string
}

type ledgerQuarantineProvenance struct {
	Version             uint32    `json:"version"`
	DetectedAt          time.Time `json:"detected_at"`
	PreviousCluster     string    `json:"previous_cluster,omitempty"`
	PreviousGenesisHash string    `json:"previous_genesis_hash"`
	CurrentCluster      string    `json:"current_cluster"`
	CurrentGenesisHash  string    `json:"current_genesis_hash"`
	MovedPaths          []string  `json:"moved_paths"`
}

// ledgerTransitionIntent is written and fsynced before the first artifact is
// moved. It remains in the ledger root until every exact target is present in
// quarantine and provenance is durable. The final commit atomically renames
// this file over the chain binding; its binding-compatible fields make the
// result immediately readable even if the process dies before another startup
// rewrites the compact binding form.
type ledgerTransitionIntent struct {
	Version     uint32    `json:"version"`
	Cluster     string    `json:"cluster"`
	GenesisHash string    `json:"genesis_hash"`
	UpdatedAt   time.Time `json:"updated_at"`

	IntentVersion       uint32    `json:"transition_version"`
	Phase               string    `json:"transition_phase"`
	DetectedAt          time.Time `json:"detected_at"`
	PreviousCluster     string    `json:"previous_cluster,omitempty"`
	PreviousGenesisHash string    `json:"previous_genesis_hash"`
	QuarantineName      string    `json:"quarantine_name"`
	MovedPaths          []string  `json:"moved_paths"`
}

type ledgerTransitionOps struct {
	rename func(string, string) error
}

var defaultLedgerTransitionOps = ledgerTransitionOps{rename: os.Rename}

type ledgerChainLock struct {
	file *os.File
}

// LoadLedgerChainBinding reads the durable ledger lineage marker. A malformed
// marker fails closed because guessing which chain owns signed votes is unsafe.
func LoadLedgerChainBinding(ledgerDir string) (*LedgerChainBinding, error) {
	if strings.TrimSpace(ledgerDir) == "" {
		return nil, fmt.Errorf("load ledger chain binding: empty ledger directory")
	}
	path := filepath.Join(ledgerDir, LedgerChainBindingFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read ledger chain binding %s: %w", path, err)
	}
	var binding LedgerChainBinding
	if err := json.Unmarshal(data, &binding); err != nil {
		return nil, fmt.Errorf("parse ledger chain binding %s: %w", path, err)
	}
	if binding.Version != ledgerChainBindingVersion {
		return nil, fmt.Errorf("ledger chain binding %s has unsupported version %d", path, binding.Version)
	}
	if err := validateLedgerGenesisHash("stored ledger", binding.GenesisHash); err != nil {
		return nil, fmt.Errorf("ledger chain binding %s: %w", path, err)
	}
	return &binding, nil
}

// LedgerScopedArtifacts lists only artifacts whose contents are tied to chain
// lineage. It intentionally ignores every other file in the ledger directory.
func LedgerScopedArtifacts(ledgerDir string) ([]string, error) {
	entries, err := os.ReadDir(ledgerDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("list ledger directory %s: %w", ledgerDir, err)
	}
	paths := make([]string, 0)
	for _, entry := range entries {
		name := entry.Name()
		switch {
		case name == "catchup-spool":
			paths = append(paths, name)
		case strings.HasPrefix(name, "vote_history-") && strings.HasSuffix(name, ".mithril.json"):
			paths = append(paths, name)
		case strings.HasPrefix(name, ".vote_history-") && strings.Contains(name, ".mithril.json.tmp-"):
			// A crash can leave a temporary vote-history file. It belongs to the
			// same old lineage even though it was never made authoritative.
			paths = append(paths, name)
		}
	}
	sort.Strings(paths)
	return paths, nil
}

// EnsureLedgerChainBinding reconciles ledger-scoped artifacts with the current
// RPC genesis. legacyGenesisHash is an explicit assertion used only when an old
// ledger has artifacts but no binding. An AccountsDB binding is intentionally
// not accepted as an implicit fallback: older startup code could stamp a freshly
// observed genesis onto state while leaving pre-re-genesis ledger artifacts.
//
// On mismatch, exact known artifacts are atomically renamed into a quarantine
// directory on the same filesystem, provenance is persisted, and only then is
// the ledger rebound. Any error leaves startup failed closed.
func EnsureLedgerChainBinding(ledgerDir, cluster, currentGenesisHash, legacyGenesisHash string) (*LedgerGenesisTransition, error) {
	return ensureLedgerChainBinding(ledgerDir, cluster, currentGenesisHash, legacyGenesisHash, defaultLedgerTransitionOps)
}

func ensureLedgerChainBinding(ledgerDir, cluster, currentGenesisHash, legacyGenesisHash string, ops ledgerTransitionOps) (*LedgerGenesisTransition, error) {
	if strings.TrimSpace(ledgerDir) == "" {
		return nil, fmt.Errorf("ensure ledger chain binding: empty ledger directory")
	}
	if strings.TrimSpace(currentGenesisHash) == "" {
		return nil, fmt.Errorf("ensure ledger chain binding: empty current genesis hash")
	}
	if err := validateLedgerGenesisHash("current RPC", currentGenesisHash); err != nil {
		return nil, fmt.Errorf("ensure ledger chain binding: %w", err)
	}
	if legacyGenesisHash != "" {
		if err := validateLedgerGenesisHash("legacy assertion", legacyGenesisHash); err != nil {
			return nil, fmt.Errorf("ensure ledger chain binding: %w", err)
		}
	}
	if ops.rename == nil {
		return nil, fmt.Errorf("ensure ledger chain binding: missing rename operation")
	}
	if err := os.MkdirAll(ledgerDir, 0o700); err != nil {
		return nil, fmt.Errorf("create ledger directory %s: %w", ledgerDir, err)
	}
	lock, err := acquireLedgerChainLock(ledgerDir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = lock.release() }()

	// A durable intent is authoritative while present. Resume it before reading
	// the ordinary binding: the old binding may already be in quarantine.
	intent, err := loadLedgerTransitionIntent(ledgerDir)
	if err != nil {
		return nil, err
	}
	if intent != nil {
		if intent.GenesisHash != currentGenesisHash {
			return nil, fmt.Errorf("pending ledger transition targets genesis %s, not requested genesis %s", intent.GenesisHash, currentGenesisHash)
		}
		if legacyGenesisHash != "" && legacyGenesisHash != intent.PreviousGenesisHash {
			return nil, fmt.Errorf("legacy genesis assertion %s conflicts with pending ledger transition from %s", legacyGenesisHash, intent.PreviousGenesisHash)
		}
		return resumeLedgerTransition(ledgerDir, intent, ops)
	}

	binding, err := LoadLedgerChainBinding(ledgerDir)
	if err != nil {
		return nil, err
	}
	artifacts, err := LedgerScopedArtifacts(ledgerDir)
	if err != nil {
		return nil, err
	}

	previousHash := ""
	previousCluster := ""
	if binding != nil {
		previousHash = binding.GenesisHash
		previousCluster = binding.Cluster
		if legacyGenesisHash != "" && legacyGenesisHash != previousHash {
			return nil, fmt.Errorf("legacy genesis assertion %s conflicts with stored ledger binding %s", legacyGenesisHash, previousHash)
		}
	} else if legacyGenesisHash != "" {
		previousHash = legacyGenesisHash
		previousCluster = cluster
	} else if len(artifacts) > 0 {
		return nil, fmt.Errorf("%w in %s (%s); supply their known genesis explicitly before startup",
			ErrUnboundLedgerArtifacts, ledgerDir, strings.Join(artifacts, ", "))
	}

	transition := &LedgerGenesisTransition{
		PreviousGenesisHash: previousHash,
		CurrentGenesisHash:  currentGenesisHash,
	}
	if previousHash != "" && previousHash != currentGenesisHash {
		intent, err := createLedgerTransitionIntent(
			ledgerDir, artifacts, binding != nil, previousCluster, previousHash, cluster, currentGenesisHash, time.Now().UTC(),
		)
		if err != nil {
			return nil, err
		}
		return resumeLedgerTransition(ledgerDir, intent, ops)
	}

	if err := saveLedgerChainBinding(ledgerDir, &LedgerChainBinding{
		Version:     ledgerChainBindingVersion,
		Cluster:     cluster,
		GenesisHash: currentGenesisHash,
		UpdatedAt:   time.Now().UTC(),
	}); err != nil {
		return nil, err
	}
	return transition, nil
}

func createLedgerTransitionIntent(ledgerDir string, artifacts []string, includeBinding bool, previousCluster, previousHash, currentCluster, currentHash string, now time.Time) (*ledgerTransitionIntent, error) {
	quarantineRoot := filepath.Join(ledgerDir, LedgerQuarantineDirName)
	if err := os.MkdirAll(quarantineRoot, 0o700); err != nil {
		return nil, fmt.Errorf("create ledger quarantine root %s: %w", quarantineRoot, err)
	}
	if err := syncDirectory(ledgerDir); err != nil {
		return nil, fmt.Errorf("persist ledger quarantine root: %w", err)
	}
	name := fmt.Sprintf("%s-%s-to-%s", now.Format("20060102T150405.000000000Z"), shortGenesis(previousHash), shortGenesis(currentHash))
	quarantineDir := filepath.Join(quarantineRoot, name)
	if err := os.Mkdir(quarantineDir, 0o700); err != nil {
		return nil, fmt.Errorf("create ledger quarantine directory %s: %w", quarantineDir, err)
	}
	if err := syncDirectory(quarantineRoot); err != nil {
		return nil, fmt.Errorf("persist ledger quarantine directory %s: %w", quarantineDir, err)
	}

	toMove := append([]string(nil), artifacts...)
	if includeBinding {
		toMove = append(toMove, LedgerChainBindingFileName)
	}
	sort.Strings(toMove)
	intent := &ledgerTransitionIntent{
		Version:             ledgerChainBindingVersion,
		Cluster:             currentCluster,
		GenesisHash:         currentHash,
		UpdatedAt:           now,
		IntentVersion:       ledgerTransitionIntentVersion,
		Phase:               ledgerTransitionPhaseMoving,
		DetectedAt:          now,
		PreviousCluster:     previousCluster,
		PreviousGenesisHash: previousHash,
		QuarantineName:      name,
		MovedPaths:          toMove,
	}
	if err := validateLedgerTransitionIntent(intent); err != nil {
		return nil, fmt.Errorf("create ledger transition intent: %w", err)
	}
	if err := writeJSONAtomically(ledgerDir, ledgerTransitionIntentFileName, intent, 0o600); err != nil {
		return nil, fmt.Errorf("persist ledger transition intent: %w", err)
	}
	return intent, nil
}

func resumeLedgerTransition(ledgerDir string, intent *ledgerTransitionIntent, ops ledgerTransitionOps) (*LedgerGenesisTransition, error) {
	if err := validateLedgerTransitionIntent(intent); err != nil {
		return nil, fmt.Errorf("resume ledger transition: %w", err)
	}
	quarantineRoot := filepath.Join(ledgerDir, LedgerQuarantineDirName)
	quarantineDir := filepath.Join(quarantineRoot, intent.QuarantineName)
	if info, err := os.Lstat(quarantineDir); err != nil {
		return nil, fmt.Errorf("resume ledger transition: stat quarantine directory %s: %w", quarantineDir, err)
	} else if !info.IsDir() {
		return nil, fmt.Errorf("resume ledger transition: quarantine path %s is not a directory", quarantineDir)
	}
	if err := rejectUnjournaledLedgerArtifacts(ledgerDir, intent.MovedPaths); err != nil {
		return nil, err
	}

	if intent.Phase == ledgerTransitionPhaseMoving {
		for _, relative := range intent.MovedPaths {
			if err := resumeLedgerArtifactMove(ledgerDir, quarantineDir, relative, intent.PreviousGenesisHash, ops); err != nil {
				return nil, err
			}
		}
		if err := ensureLedgerQuarantineProvenance(quarantineDir, intent, true); err != nil {
			return nil, err
		}
		intent.Phase = ledgerTransitionPhaseQuarantined
		if err := writeJSONAtomically(ledgerDir, ledgerTransitionIntentFileName, intent, 0o600); err != nil {
			return nil, fmt.Errorf("persist quarantined ledger transition phase: %w", err)
		}
	} else if err := verifyQuarantinedLedgerTransition(ledgerDir, quarantineDir, intent); err != nil {
		return nil, err
	}

	// Re-check immediately before the commit so an artifact that was not named in
	// the durable intent can never be silently adopted by the new binding.
	if err := rejectUnjournaledLedgerArtifacts(ledgerDir, intent.MovedPaths); err != nil {
		return nil, err
	}
	if exists, err := pathExists(filepath.Join(ledgerDir, LedgerChainBindingFileName)); err != nil {
		return nil, fmt.Errorf("stat ledger binding before transition commit: %w", err)
	} else if exists {
		return nil, fmt.Errorf("refusing to commit ledger transition: live binding unexpectedly exists")
	}
	intentPath := filepath.Join(ledgerDir, ledgerTransitionIntentFileName)
	bindingPath := filepath.Join(ledgerDir, LedgerChainBindingFileName)
	if err := ops.rename(intentPath, bindingPath); err != nil {
		return nil, fmt.Errorf("commit ledger transition binding: %w", err)
	}
	if err := syncDirectory(ledgerDir); err != nil {
		return nil, fmt.Errorf("persist ledger transition binding: %w", err)
	}
	return &LedgerGenesisTransition{
		PreviousGenesisHash: intent.PreviousGenesisHash,
		CurrentGenesisHash:  intent.GenesisHash,
		QuarantineDir:       quarantineDir,
		MovedPaths:          append([]string(nil), intent.MovedPaths...),
	}, nil
}

func acquireLedgerChainLock(ledgerDir string) (*ledgerChainLock, error) {
	path := filepath.Join(ledgerDir, ledgerChainLockFileName)
	fd, err := unix.Open(path, unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open ledger chain lock %s: %w", path, err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("open ledger chain lock %s: invalid file descriptor", path)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("stat ledger chain lock %s: %w", path, err)
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, fmt.Errorf("ledger chain lock %s is not a regular file", path)
	}
	for {
		err = unix.Flock(fd, unix.LOCK_EX)
		if !errors.Is(err, unix.EINTR) {
			break
		}
	}
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("lock ledger chain state %s: %w", path, err)
	}
	return &ledgerChainLock{file: file}, nil
}

func (lock *ledgerChainLock) release() error {
	if lock == nil || lock.file == nil {
		return nil
	}
	fd := int(lock.file.Fd())
	unlockErr := unix.Flock(fd, unix.LOCK_UN)
	closeErr := lock.file.Close()
	lock.file = nil
	if unlockErr != nil {
		return unlockErr
	}
	return closeErr
}

func loadLedgerTransitionIntent(ledgerDir string) (*ledgerTransitionIntent, error) {
	path := filepath.Join(ledgerDir, ledgerTransitionIntentFileName)
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("stat ledger transition intent %s: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("ledger transition intent %s is not a regular non-symlink file", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read ledger transition intent %s: %w", path, err)
	}
	var intent ledgerTransitionIntent
	if err := json.Unmarshal(data, &intent); err != nil {
		return nil, fmt.Errorf("parse ledger transition intent %s: %w", path, err)
	}
	if err := validateLedgerTransitionIntent(&intent); err != nil {
		return nil, fmt.Errorf("ledger transition intent %s: %w", path, err)
	}
	return &intent, nil
}

func validateLedgerTransitionIntent(intent *ledgerTransitionIntent) error {
	if intent == nil {
		return errors.New("missing ledger transition intent")
	}
	if intent.Version != ledgerChainBindingVersion {
		return fmt.Errorf("binding version %d is unsupported", intent.Version)
	}
	if intent.IntentVersion != ledgerTransitionIntentVersion {
		return fmt.Errorf("transition version %d is unsupported", intent.IntentVersion)
	}
	if intent.Phase != ledgerTransitionPhaseMoving && intent.Phase != ledgerTransitionPhaseQuarantined {
		return fmt.Errorf("transition phase %q is invalid", intent.Phase)
	}
	if err := validateLedgerGenesisHash("pending transition previous", intent.PreviousGenesisHash); err != nil {
		return err
	}
	if err := validateLedgerGenesisHash("pending transition current", intent.GenesisHash); err != nil {
		return err
	}
	if intent.PreviousGenesisHash == intent.GenesisHash {
		return errors.New("pending transition has identical previous and current genesis hashes")
	}
	if intent.DetectedAt.IsZero() || intent.UpdatedAt.IsZero() {
		return errors.New("pending transition timestamp is missing")
	}
	if intent.QuarantineName == "" || intent.QuarantineName == "." || filepath.IsAbs(intent.QuarantineName) ||
		filepath.Base(intent.QuarantineName) != intent.QuarantineName || filepath.Clean(intent.QuarantineName) != intent.QuarantineName {
		return fmt.Errorf("pending transition quarantine name %q is invalid", intent.QuarantineName)
	}
	for i, relative := range intent.MovedPaths {
		if !isLedgerTransitionTarget(relative) {
			return fmt.Errorf("pending transition target %q is not ledger-scoped", relative)
		}
		if i > 0 && intent.MovedPaths[i-1] >= relative {
			return errors.New("pending transition targets are not strictly sorted and unique")
		}
	}
	return nil
}

func isLedgerTransitionTarget(name string) bool {
	if name == "" || name == "." || filepath.IsAbs(name) || filepath.Base(name) != name || filepath.Clean(name) != name {
		return false
	}
	switch {
	case name == LedgerChainBindingFileName:
		return true
	case name == "catchup-spool":
		return true
	case strings.HasPrefix(name, "vote_history-") && strings.HasSuffix(name, ".mithril.json"):
		return true
	case strings.HasPrefix(name, ".vote_history-") && strings.Contains(name, ".mithril.json.tmp-"):
		return true
	default:
		return false
	}
}

func rejectUnjournaledLedgerArtifacts(ledgerDir string, planned []string) error {
	plannedSet := make(map[string]struct{}, len(planned))
	for _, relative := range planned {
		plannedSet[relative] = struct{}{}
	}
	artifacts, err := LedgerScopedArtifacts(ledgerDir)
	if err != nil {
		return err
	}
	for _, relative := range artifacts {
		if _, ok := plannedSet[relative]; !ok {
			return fmt.Errorf("pending ledger transition did not journal live artifact %s; refusing to bind a new genesis", relative)
		}
	}
	if _, plannedBinding := plannedSet[LedgerChainBindingFileName]; !plannedBinding {
		if exists, err := pathExists(filepath.Join(ledgerDir, LedgerChainBindingFileName)); err != nil {
			return fmt.Errorf("stat unjournaled ledger binding: %w", err)
		} else if exists {
			return errors.New("pending ledger transition did not journal the live binding; refusing to replace it")
		}
	}
	return nil
}

func resumeLedgerArtifactMove(ledgerDir, quarantineDir, relative, previousGenesis string, ops ledgerTransitionOps) error {
	source := filepath.Join(ledgerDir, relative)
	destination := filepath.Join(quarantineDir, relative)
	sourceExists, err := pathExists(source)
	if err != nil {
		return fmt.Errorf("stat ledger transition source %s: %w", source, err)
	}
	destinationExists, err := pathExists(destination)
	if err != nil {
		return fmt.Errorf("stat ledger transition destination %s: %w", destination, err)
	}
	if sourceExists && destinationExists {
		return fmt.Errorf("ledger transition target %s exists both live and in quarantine", relative)
	}
	if !sourceExists && !destinationExists {
		return fmt.Errorf("ledger transition target %s is missing both live and from quarantine", relative)
	}
	if relative == LedgerChainBindingFileName {
		bindingPath := destination
		if sourceExists {
			bindingPath = source
		}
		if err := validateLedgerBindingAtPath(bindingPath, previousGenesis); err != nil {
			return err
		}
	}
	if destinationExists {
		if err := syncDirectory(quarantineDir); err != nil {
			return fmt.Errorf("persist previously quarantined ledger artifact %s: %w", relative, err)
		}
		if err := syncDirectory(ledgerDir); err != nil {
			return fmt.Errorf("persist previous removal of live ledger artifact %s: %w", relative, err)
		}
		return nil
	}
	if err := ops.rename(source, destination); err != nil {
		return fmt.Errorf("quarantine ledger artifact %s: %w", source, err)
	}
	// Rename crosses two directories. Persist both before advancing to another
	// target; after a crash the intent can therefore trust source XOR destination.
	if err := syncDirectory(quarantineDir); err != nil {
		return fmt.Errorf("persist quarantined ledger artifact %s: %w", relative, err)
	}
	if err := syncDirectory(ledgerDir); err != nil {
		return fmt.Errorf("persist removal of live ledger artifact %s: %w", relative, err)
	}
	return nil
}

func verifyQuarantinedLedgerTransition(ledgerDir, quarantineDir string, intent *ledgerTransitionIntent) error {
	for _, relative := range intent.MovedPaths {
		source := filepath.Join(ledgerDir, relative)
		destination := filepath.Join(quarantineDir, relative)
		if exists, err := pathExists(source); err != nil {
			return fmt.Errorf("stat quarantined ledger source %s: %w", source, err)
		} else if exists {
			return fmt.Errorf("ledger transition phase is quarantined but %s is still live", relative)
		}
		if exists, err := pathExists(destination); err != nil {
			return fmt.Errorf("stat quarantined ledger destination %s: %w", destination, err)
		} else if !exists {
			return fmt.Errorf("ledger transition phase is quarantined but %s is missing", relative)
		}
		if relative == LedgerChainBindingFileName {
			if err := validateLedgerBindingAtPath(destination, intent.PreviousGenesisHash); err != nil {
				return err
			}
		}
	}
	return ensureLedgerQuarantineProvenance(quarantineDir, intent, false)
}

func ensureLedgerQuarantineProvenance(quarantineDir string, intent *ledgerTransitionIntent, allowCreate bool) error {
	expected := ledgerQuarantineProvenance{
		Version:             1,
		DetectedAt:          intent.DetectedAt,
		PreviousCluster:     intent.PreviousCluster,
		PreviousGenesisHash: intent.PreviousGenesisHash,
		CurrentCluster:      intent.Cluster,
		CurrentGenesisHash:  intent.GenesisHash,
		MovedPaths:          append([]string(nil), intent.MovedPaths...),
	}
	path := filepath.Join(quarantineDir, "provenance.json")
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		if !allowCreate {
			return errors.New("ledger transition phase is quarantined but provenance is missing")
		}
		if err := writeJSONAtomically(quarantineDir, "provenance.json", expected, 0o600); err != nil {
			return fmt.Errorf("persist ledger quarantine provenance: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("stat ledger quarantine provenance: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return errors.New("ledger quarantine provenance is not a regular non-symlink file")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read ledger quarantine provenance: %w", err)
	}
	var actual ledgerQuarantineProvenance
	if err := json.Unmarshal(data, &actual); err != nil {
		return fmt.Errorf("parse ledger quarantine provenance: %w", err)
	}
	if !sameLedgerQuarantineProvenance(actual, expected) {
		return errors.New("ledger quarantine provenance conflicts with pending transition intent")
	}
	return nil
}

func sameLedgerQuarantineProvenance(a, b ledgerQuarantineProvenance) bool {
	if a.Version != b.Version || !a.DetectedAt.Equal(b.DetectedAt) ||
		a.PreviousCluster != b.PreviousCluster || a.PreviousGenesisHash != b.PreviousGenesisHash ||
		a.CurrentCluster != b.CurrentCluster || a.CurrentGenesisHash != b.CurrentGenesisHash ||
		len(a.MovedPaths) != len(b.MovedPaths) {
		return false
	}
	for i := range a.MovedPaths {
		if a.MovedPaths[i] != b.MovedPaths[i] {
			return false
		}
	}
	return true
}

func validateLedgerBindingAtPath(path, expectedGenesis string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("stat quarantined ledger binding %s: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return fmt.Errorf("quarantined ledger binding %s is not a regular non-symlink file", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read quarantined ledger binding %s: %w", path, err)
	}
	var binding LedgerChainBinding
	if err := json.Unmarshal(data, &binding); err != nil {
		return fmt.Errorf("parse quarantined ledger binding %s: %w", path, err)
	}
	if binding.Version != ledgerChainBindingVersion {
		return fmt.Errorf("quarantined ledger binding %s has unsupported version %d", path, binding.Version)
	}
	if err := validateLedgerGenesisHash("quarantined ledger", binding.GenesisHash); err != nil {
		return fmt.Errorf("quarantined ledger binding %s: %w", path, err)
	}
	if binding.GenesisHash != expectedGenesis {
		return fmt.Errorf("quarantined ledger binding %s belongs to genesis %s, expected %s", path, binding.GenesisHash, expectedGenesis)
	}
	return nil
}

func pathExists(path string) (bool, error) {
	_, err := os.Lstat(path)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

func saveLedgerChainBinding(ledgerDir string, binding *LedgerChainBinding) error {
	if binding == nil {
		return fmt.Errorf("save ledger chain binding: nil binding")
	}
	if err := validateLedgerGenesisHash("ledger binding", binding.GenesisHash); err != nil {
		return fmt.Errorf("save ledger chain binding: %w", err)
	}
	if err := writeJSONAtomically(ledgerDir, LedgerChainBindingFileName, binding, 0o600); err != nil {
		return fmt.Errorf("save ledger chain binding: %w", err)
	}
	return nil
}

func writeJSONAtomically(dir, name string, value any, mode os.FileMode) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	temporary, err := os.CreateTemp(dir, "."+name+".tmp-")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	closed := false
	defer func() {
		if !closed {
			_ = temporary.Close()
		}
		_ = os.Remove(temporaryName)
	}()
	if err := temporary.Chmod(mode); err != nil {
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		closed = true
		return err
	}
	closed = true
	if err := os.Rename(temporaryName, filepath.Join(dir, name)); err != nil {
		return err
	}
	return syncDirectory(dir)
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	syncErr := dir.Sync()
	closeErr := dir.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func shortGenesis(hash string) string {
	const length = 12
	if len(hash) <= length {
		return hash
	}
	return hash[:length]
}

func validateLedgerGenesisHash(label, hash string) error {
	decoded, err := base58.Decode(hash)
	if err != nil || len(decoded) != 32 {
		return fmt.Errorf("%s genesis hash must be base58 encoding of exactly 32 bytes", label)
	}
	return nil
}
