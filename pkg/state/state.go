package state

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/mr-tron/base58"
)

const StateFileName = "mithril_state.json"
const HistoryFileName = "mithril_state.history.jsonl"

// CurrentStateSchemaVersion is the current version of the state file format.
// Increment this when making breaking changes to the state file structure.
const CurrentStateSchemaVersion uint32 = 2

// MithrilState tracks the current state of the mithril node.
// The state file serves as an atomic marker of validity - AccountsDB is valid
// if and only if this file exists with Stage == "ready".
type MithrilState struct {
	// =========================================================================
	// Schema & Run Lineage
	// =========================================================================
	StateSchemaVersion uint32 `json:"state_schema_version"`
	// Run lineage - tracks the chain of sessions that have used this AccountsDB
	RootRunID    string `json:"root_run_id,omitempty"`    // Run that built AccountsDB from snapshot (never changes)
	ParentRunID  string `json:"parent_run_id,omitempty"`  // Run we resumed from (empty if fresh start)
	CurrentRunID string `json:"current_run_id,omitempty"` // Run that last wrote this file

	// =========================================================================
	// Writer Metadata (who last wrote this file and why they stopped)
	// =========================================================================
	LastWriterVersion  string    `json:"last_writer_version,omitempty"`  // Semver tag (e.g., "v0.1.0" or "dev")
	LastWriterCommit   string    `json:"last_writer_commit,omitempty"`   // Git commit hash of writer binary
	LastWriterBranch   string    `json:"last_writer_branch,omitempty"`   // Git branch name (may be empty)
	LastShutdownReason string    `json:"last_shutdown_reason,omitempty"` // human-readable reason
	LastShutdownAt     time.Time `json:"last_shutdown_at,omitempty"`     // when shutdown occurred

	// =========================================================================
	// AccountsDB Origin (snapshot info - set once, never changes)
	// =========================================================================
	Stage          string        `json:"stage"`                        // "ready", "downloading", "building", "corrupted"
	SnapshotSlot   uint64        `json:"snapshot_slot"`                // Slot of the snapshot used to build AccountsDB
	SnapshotEpoch  uint64        `json:"snapshot_epoch,omitempty"`     // Epoch of the snapshot
	FullSnapshot   *SnapshotInfo `json:"full_snapshot,omitempty"`      // Full snapshot file info
	IncrSnapshot   *SnapshotInfo `json:"incr_snapshot,omitempty"`      // Incremental snapshot file info
	BuildCompleted time.Time     `json:"build_completed_at,omitempty"` // When AccountsDB build finished
	BuildStartedAt time.Time     `json:"build_started_at,omitempty"`   // When bootstrap started
	BuildMode      string        `json:"build_mode,omitempty"`         // "auto", "snapshot", "new-snapshot", "accountsdb"
	Cluster        string        `json:"cluster,omitempty"`            // "mainnet-beta", "testnet", "devnet"
	GenesisHash    string        `json:"genesis_hash,omitempty"`       // Base58 genesis hash from RPC

	// Corruption tracking - set when integrity check fails
	CorruptionReason     string    `json:"corruption_reason,omitempty"`
	CorruptionDetectedAt time.Time `json:"corruption_detected_at,omitempty"`

	// =========================================================================
	// Manifest Seed Data (copied from manifest at snapshot build time)
	// Used ONLY for fresh-start replay. Resume uses Last* fields instead.
	// =========================================================================

	// Block configuration seed
	ManifestParentSlot     uint64 `json:"manifest_parent_slot,omitempty"`
	ManifestParentBankhash string `json:"manifest_parent_bankhash,omitempty"` // base58
	ManifestBlockHeight    uint64 `json:"manifest_block_height,omitempty"`
	ManifestAcctsLtHash    string `json:"manifest_accts_lt_hash,omitempty"` // base64

	// Fee rate governor seed (static fields only)
	ManifestFeeRateGovernor *ManifestFeeRateGovernorSeed `json:"manifest_fee_rate_governor,omitempty"`

	// Signature/fee state at snapshot
	ManifestSignatureCount       uint64 `json:"manifest_signature_count,omitempty"`
	ManifestLamportsPerSignature uint64 `json:"manifest_lamports_per_sig,omitempty"`

	// Blockhash context (150 recent + 1 evicted)
	ManifestRecentBlockhashes []BlockhashEntry `json:"manifest_recent_blockhashes,omitempty"`
	ManifestEvictedBlockhash  string           `json:"manifest_evicted_blockhash,omitempty"` // base58

	// ReplayCtx seed (inflation/capitalization at snapshot)
	ManifestCapitalization          uint64                     `json:"manifest_capitalization,omitempty"`
	ManifestSlotsPerYear            float64                    `json:"manifest_slots_per_year,omitempty"`
	ManifestInflationInitial        float64                    `json:"manifest_inflation_initial,omitempty"`
	ManifestInflationTerminal       float64                    `json:"manifest_inflation_terminal,omitempty"`
	ManifestInflationTaper          float64                    `json:"manifest_inflation_taper,omitempty"`
	ManifestInflationFoundation     float64                    `json:"manifest_inflation_foundation,omitempty"`
	ManifestInflationFoundationTerm float64                    `json:"manifest_inflation_foundation_term,omitempty"`
	ManifestEpochSchedule           *ManifestEpochScheduleSeed `json:"manifest_epoch_schedule,omitempty"`

	// Epoch account hash (base64 for consistency with LtHash)
	ManifestEpochAcctsHash string `json:"manifest_epoch_accts_hash,omitempty"` // base64

	// Transaction count at snapshot slot
	ManifestTransactionCount uint64 `json:"manifest_transaction_count,omitempty"`

	// Epoch stakes seed - AGGREGATED vote-account stakes only (NOT full VersionedEpochStakes)
	// Same format as ComputedEpochStakes (PersistedEpochStakes JSON)
	// Cleared after first replayed slot to save space.
	ManifestEpochStakes map[uint64]string `json:"manifest_epoch_stakes,omitempty"`

	// =========================================================================
	// Current Position (where we left off)
	// =========================================================================
	LastSlot        uint64 `json:"last_slot,omitempty"`         // Last successfully replayed slot (the replayed tip)
	LastEpoch       uint64 `json:"last_epoch,omitempty"`        // Epoch of last replayed slot
	LastBankhash    string `json:"last_bankhash,omitempty"`     // Bankhash of last replayed slot (base58)
	LastBlockHeight uint64 `json:"last_block_height,omitempty"` // Block height of last replayed slot

	// Rooted-durable watermark: the canonical store holds only rooted (finalized) slots
	// through here while replay runs ahead in RAM. Resume starts at the next slot. Zero = legacy.
	LastRootedSlot     uint64 `json:"last_rooted_slot,omitempty"`     // Highest durably-committed (rooted) slot
	LastRootedBankhash string `json:"last_rooted_bankhash,omitempty"` // Bankhash of the last rooted slot (base58)

	// LastRootedContext is the replay context as of the last rooted slot; rooted-durable
	// resume restarts from the next slot using it. nil in legacy mode.
	LastRootedContext *ResumeContext `json:"last_rooted_context,omitempty"`

	// AlpenglowEvidence persists finality mismatches/conflicts that halted promotion,
	// so a restart cannot fold the disputed slot under delegated trust. An entry
	// clears when its slot promotes with a matching identity; conflict entries (zero
	// hashes) block promotion until removed by an operator.
	AlpenglowEvidence []AlpenglowFinalityEvidence `json:"alpenglow_finality_evidence,omitempty"`

	// ReplayDivergenceEvidence records trailing-verifier execution mismatches
	// (replayed results vs RPC metadata). Deterministic divergence is not
	// self-healing: while evidence is present the node refuses to fold at or
	// past the disputed slot; the operator clears it after triage.
	ReplayDivergenceEvidence []ReplayDivergenceRecord `json:"replay_divergence_evidence,omitempty"`

	// =========================================================================
	// Resume Context (everything needed to continue replay from LastSlot)
	// These fields capture state at the end of the last successfully replayed slot
	// =========================================================================

	// LtHash and fee state
	LastAcctsLtHash          string `json:"last_accts_lt_hash,omitempty"`         // base64 encoded cumulative LtHash
	LastLamportsPerSignature uint64 `json:"last_lamports_per_sig,omitempty"`      // FeeRateGovernor.LamportsPerSignature
	LastPrevLamportsPerSig   uint64 `json:"last_prev_lamports_per_sig,omitempty"` // FeeRateGovernor.PrevLamportsPerSignature
	LastNumSignatures        uint64 `json:"last_num_signatures,omitempty"`        // SlotCtx.NumSignatures

	// Blockhash context - required because appendvec writes are not fsynced
	LastRecentBlockhashes []BlockhashEntry `json:"last_recent_blockhashes,omitempty"` // 150 entries, newest first
	LastEvictedBlockhash  string           `json:"last_evicted_blockhash,omitempty"`  // 151st blockhash (base58)
	LastBlockhash         string           `json:"last_blockhash,omitempty"`          // Blockhash of last slot (base58)

	// SlotHashes context - vote program needs accurate slot→hash mappings
	LastSlotHashes []SlotHashEntry `json:"last_slot_hashes,omitempty"` // up to 512 entries, newest first

	// ReplayCtx fields - so resume uses fresh values instead of stale manifest
	LastCapitalization          uint64  `json:"last_capitalization,omitempty"`    // Total lamports in circulation
	LastSlotsPerYear            float64 `json:"last_slots_per_year,omitempty"`    // Slots per year for inflation calc
	LastInflationInitial        float64 `json:"last_inflation_initial,omitempty"` // Inflation parameters
	LastInflationTerminal       float64 `json:"last_inflation_terminal,omitempty"`
	LastInflationTaper          float64 `json:"last_inflation_taper,omitempty"`
	LastInflationFoundation     float64 `json:"last_inflation_foundation,omitempty"`
	LastInflationFoundationTerm float64 `json:"last_inflation_foundation_term,omitempty"`

	// EpochStakes - computed at epoch boundaries, required for leader schedule on resume
	// Key: epoch number (leader schedule epoch), Value: JSON-serialized epoch stakes
	// These are NOT loaded from manifest - they are computed during replay and persisted.
	ComputedEpochStakes map[uint64]string `json:"computed_epoch_stakes,omitempty"`

	// =========================================================================
	// Legacy fields - kept for backwards compatibility
	// =========================================================================
	// TODO: Remove after v1.0 release
	LastCommit string    `json:"last_commit,omitempty"` // Deprecated: use last_writer_commit
	LastRunID  string    `json:"last_run_id,omitempty"` // Deprecated: use current_run_id
	LastRunAt  time.Time `json:"last_run_at,omitempty"` // Deprecated: tracked via last_shutdown_at
}

// Shutdown reason constants - these are stored in the state file and should be
// human-readable without needing to look up what they mean.
const (
	ShutdownReasonNormal         = "graceful shutdown (Ctrl+C)"
	ShutdownReasonStall          = "block fetch stalled - no RPC progress for 5+ minutes"
	ShutdownReasonLeaderSchedule = "leader schedule fetch failed from all RPC endpoints"
	ShutdownReasonError          = "replay error" // Will be suffixed with actual error
	ShutdownReasonCompleted      = "replay completed - reached end slot"
)

// BlockhashEntry represents a single entry in the RecentBlockhashes sysvar
type BlockhashEntry struct {
	Blockhash            string `json:"blockhash"`
	LamportsPerSignature uint64 `json:"lamports_per_sig"`
}

// ManifestFeeRateGovernorSeed contains the static fields from FeeRateGovernor
// that do not change during replay. Dynamic fields (LamportsPerSignature,
// PrevLamportsPerSignature) are stored separately and updated on resume.
type ManifestFeeRateGovernorSeed struct {
	TargetLamportsPerSignature uint64 `json:"target_lamports_per_sig"`
	TargetSignaturesPerSlot    uint64 `json:"target_sigs_per_slot"`
	MinLamportsPerSignature    uint64 `json:"min_lamports_per_sig"`
	MaxLamportsPerSignature    uint64 `json:"max_lamports_per_sig"`
	BurnPercent                byte   `json:"burn_percent"`
}

// ManifestEpochScheduleSeed contains the bank epoch schedule serialized in the
// snapshot manifest. Some clusters can expose a divergent EpochSchedule sysvar
// account, so replay uses this bank schedule for epoch/leader/rewards logic.
type ManifestEpochScheduleSeed struct {
	SlotsPerEpoch            uint64 `json:"slots_per_epoch"`
	LeaderScheduleSlotOffset uint64 `json:"leader_schedule_slot_offset"`
	Warmup                   bool   `json:"warmup"`
	FirstNormalEpoch         uint64 `json:"first_normal_epoch"`
	FirstNormalSlot          uint64 `json:"first_normal_slot"`
}

// SlotHashEntry represents a single entry in the SlotHashes sysvar
type SlotHashEntry struct {
	Slot uint64 `json:"slot"`
	Hash string `json:"hash"` // base58 encoded
}

// ResumeContext bundles the end-of-slot replay context to continue replay from a slot.
// In rooted-durable mode it is captured as of the last rooted slot (stored in
// MithrilState.LastRootedContext). Epoch stakes live in ComputedEpochStakes, not here.
// AlpenglowFinalityEvidence is one persisted promotion-gate violation: an executed
// block-id contradicting the certificate-finalized one, or a certificate conflict
// (Conflict=true, empty hashes).
type AlpenglowFinalityEvidence struct {
	Slot      uint64 `json:"slot"`
	Executed  string `json:"executed,omitempty"`  // hex block-id
	Finalized string `json:"finalized,omitempty"` // hex block-id
	Conflict  bool   `json:"conflict,omitempty"`
}

// ReplayDivergenceRecord is one trailing-verifier execution mismatch.
type ReplayDivergenceRecord struct {
	Slot        uint64 `json:"slot"`
	TxIndex     int    `json:"tx_index"`
	TxSignature string `json:"tx_signature,omitempty"`
	Kind        string `json:"kind"`
	Detail      string `json:"detail"`
	RecordedAt  string `json:"recorded_at"`
}

type ResumeContext struct {
	Slot                    uint64           `json:"slot"`
	Bankhash                string           `json:"bankhash"` // base58
	BlockHeight             uint64           `json:"block_height"`
	Epoch                   uint64           `json:"epoch"`
	AcctsLtHash             string           `json:"accts_lt_hash"` // base64
	LamportsPerSignature    uint64           `json:"lamports_per_sig"`
	PrevLamportsPerSig      uint64           `json:"prev_lamports_per_sig"`
	NumSignatures           uint64           `json:"num_signatures"`
	RecentBlockhashes       []BlockhashEntry `json:"recent_blockhashes"`
	EvictedBlockhash        string           `json:"evicted_blockhash"` // base58
	Blockhash               string           `json:"blockhash"`         // base58
	SlotHashes              []SlotHashEntry  `json:"slot_hashes"`
	Clock                   string           `json:"clock"` // base64 of the Clock sysvar account data as of the last rooted slot
	Capitalization          uint64           `json:"capitalization"`
	SlotsPerYear            float64          `json:"slots_per_year"`
	InflationInitial        float64          `json:"inflation_initial"`
	InflationTerminal       float64          `json:"inflation_terminal"`
	InflationTaper          float64          `json:"inflation_taper"`
	InflationFoundation     float64          `json:"inflation_foundation"`
	InflationFoundationTerm float64          `json:"inflation_foundation_term"`
	// TransactionCount is the running chain transaction count as of this slot, so
	// resume and the in-loop fork-switch unwind restore it exactly (getEpochInfo
	// and future bank metadata must not carry a discarded fork's transactions).
	// A pointer so presence is explicit: nil = context predates the field
	// (callers fall back to the snapshot-manifest count and flag the count as
	// approximate); non-nil is exact even when the value is zero (dev genesis).
	TransactionCount *uint64 `json:"transaction_count,omitempty"`
}

// SnapshotInfo contains metadata about a downloaded snapshot file.
type SnapshotInfo struct {
	Path     string `json:"path"`
	Slot     uint64 `json:"slot"`
	BaseSlot uint64 `json:"base_slot,omitempty"` // only for incrementals
}

// LoadState loads the state file from the accountsdb directory.
// Returns nil and no error if the file doesn't exist.
// Handles migration from older schema versions automatically.
func LoadState(accountsDbDir string) (*MithrilState, error) {
	stateFile := filepath.Join(accountsDbDir, StateFileName)

	data, err := os.ReadFile(stateFile)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to read state file: %w", err)
	}

	var state MithrilState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to parse state file: %w", err)
	}

	// Require schema version 2 - no migration from older versions
	if state.StateSchemaVersion != CurrentStateSchemaVersion {
		return nil, fmt.Errorf("state file schema version %d is not supported (requires v%d). Delete AccountsDB and rebuild from snapshot", state.StateSchemaVersion, CurrentStateSchemaVersion)
	}

	return &state, nil
}

// Save writes the state to the accountsdb directory.
func (s *MithrilState) Save(accountsDbDir string) error {
	stateFile := filepath.Join(accountsDbDir, StateFileName)

	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	// Full-durability write: tmp + fsync + rename + dir fsync. Atomicity alone
	// (tmp+rename) survives process crashes but a power cut can lose the
	// un-synced rename and revert to the old file. Most state fields self-heal
	// from the fsynced fold manifests at startup, but the epoch stakes saved at
	// an epoch boundary have no other durable home — losing that save would
	// force a snapshot re-bootstrap. Save is called only at rare moments
	// (bootstrap, epoch boundary, shutdown, halt evidence), so the fsyncs are
	// free.
	tmpFile := stateFile + ".tmp"
	f, err := os.OpenFile(tmpFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create state tmp file: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmpFile)
		return fmt.Errorf("failed to write state file: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpFile)
		return fmt.Errorf("failed to fsync state file: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("failed to close state tmp file: %w", err)
	}

	if err := os.Rename(tmpFile, stateFile); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("failed to rename state file: %w", err)
	}
	// Make the rename itself durable.
	if dir, err := os.Open(accountsDbDir); err == nil {
		syncErr := dir.Sync()
		dir.Close()
		if syncErr != nil {
			return fmt.Errorf("failed to fsync state directory: %w", syncErr)
		}
	}

	return nil
}

// IsReady returns true if the state indicates AccountsDB is valid and ready.
func (s *MithrilState) IsReady() bool {
	return s != nil && s.Stage == "ready"
}

// IsCorrupted returns true if the state indicates AccountsDB is corrupted.
func (s *MithrilState) IsCorrupted() bool {
	return s != nil && s.Stage == "corrupted"
}

// MarkCorrupted updates the state file to indicate AccountsDB is corrupted.
// This persists the corruption status so the next startup knows to rebuild.
func (s *MithrilState) MarkCorrupted(accountsDbDir string, reason string) error {
	s.Stage = "corrupted"
	s.CorruptionReason = reason
	s.CorruptionDetectedAt = time.Now()
	return s.Save(accountsDbDir)
}

// ShutdownContext contains the data to persist on graceful shutdown.
// This is passed to UpdateOnShutdown to save the current session state.
type ShutdownContext struct {
	// Current session's run ID (becomes CurrentRunID in state file)
	RunID string

	// Writer info
	WriterVersion string // Semver tag (e.g., "v0.1.0" or "dev")
	WriterCommit  string // Git commit hash
	WriterBranch  string // Git branch name (may be empty if not available)

	// Shutdown reason
	ShutdownReason string // Why the session ended (see ShutdownReason* constants)

	// Epoch of the last replayed slot
	Epoch uint64
	// Block height of the last replayed slot
	BlockHeight uint64

	// LtHash and fee state
	AcctsLtHash          string // base64 encoded
	LamportsPerSignature uint64
	PrevLamportsPerSig   uint64
	NumSignatures        uint64

	// Blockhash context
	RecentBlockhashes []BlockhashEntry // 150 entries, newest first
	EvictedBlockhash  string           // base58 encoded, 151st blockhash
	LastBlockhash     string           // base58 encoded, blockhash of last slot (parent for next)

	// SlotHashes context - vote program uses this to verify slot→hash mappings
	SlotHashes []SlotHashEntry // up to 512 entries, newest first

	// ReplayCtx fields - so resume uses fresh values instead of stale manifest
	Capitalization          uint64  // Total lamports in circulation
	SlotsPerYear            float64 // Slots per year for inflation calc
	InflationInitial        float64
	InflationTerminal       float64
	InflationTaper          float64
	InflationFoundation     float64
	InflationFoundationTerm float64

	// EpochStakes - computed at epoch boundaries, required for leader schedule on resume
	// Key: epoch number, Value: JSON-serialized epoch stakes (as []byte)
	ComputedEpochStakes map[uint64][]byte
}

// UpdateLastSlot updates the last slot and bankhash in the state file.
// This should be called after successfully committing a slot during replay.
func (s *MithrilState) UpdateLastSlot(accountsDbDir string, slot uint64, bankhash []byte) error {
	s.LastSlot = slot
	s.LastBankhash = base58.Encode(bankhash)
	return s.Save(accountsDbDir)
}

// UpdateOnShutdown updates the state file with full shutdown context.
// This handles the run lineage: the current session's RunID becomes CurrentRunID,
// and if this is a resume, the previous CurrentRunID becomes ParentRunID.
func (s *MithrilState) UpdateOnShutdown(accountsDbDir string, slot uint64, bankhash []byte, ctx *ShutdownContext) error {
	s.LastSlot = slot
	s.LastBankhash = base58.Encode(bankhash)
	if ctx != nil {
		s.LastBlockHeight = ctx.BlockHeight
	}

	if ctx != nil {
		// Update run lineage:
		// - If this is first run (RootRunID empty), set RootRunID = CurrentRunID = ctx.RunID
		// - If resuming, ParentRunID = old CurrentRunID, CurrentRunID = ctx.RunID
		if s.RootRunID == "" {
			// First run - this session built AccountsDB from snapshot
			s.RootRunID = ctx.RunID
		}
		if s.CurrentRunID != "" && s.CurrentRunID != ctx.RunID {
			// We're a new session resuming from a previous one
			s.ParentRunID = s.CurrentRunID
		}
		s.CurrentRunID = ctx.RunID

		// Writer info
		s.LastWriterVersion = ctx.WriterVersion
		s.LastWriterCommit = ctx.WriterCommit
		s.LastWriterBranch = ctx.WriterBranch
		s.LastCommit = ctx.WriterCommit // Legacy field

		// Shutdown tracking
		if ctx.ShutdownReason != "" {
			s.LastShutdownReason = ctx.ShutdownReason
			s.LastShutdownAt = time.Now()
		}

		// Position and epoch
		s.LastEpoch = ctx.Epoch
		s.LastBlockHeight = ctx.BlockHeight

		// Resume context - LtHash and fee state
		s.LastAcctsLtHash = ctx.AcctsLtHash
		s.LastLamportsPerSignature = ctx.LamportsPerSignature
		s.LastPrevLamportsPerSig = ctx.PrevLamportsPerSig
		s.LastNumSignatures = ctx.NumSignatures

		// Blockhash context
		s.LastRecentBlockhashes = ctx.RecentBlockhashes
		s.LastEvictedBlockhash = ctx.EvictedBlockhash
		s.LastBlockhash = ctx.LastBlockhash

		// SlotHashes context
		s.LastSlotHashes = ctx.SlotHashes

		// ReplayCtx fields
		s.LastCapitalization = ctx.Capitalization
		s.LastSlotsPerYear = ctx.SlotsPerYear
		s.LastInflationInitial = ctx.InflationInitial
		s.LastInflationTerminal = ctx.InflationTerminal
		s.LastInflationTaper = ctx.InflationTaper
		s.LastInflationFoundation = ctx.InflationFoundation
		s.LastInflationFoundationTerm = ctx.InflationFoundationTerm

		// EpochStakes - convert []byte values to string for JSON storage
		if len(ctx.ComputedEpochStakes) > 0 {
			s.ComputedEpochStakes = make(map[uint64]string, len(ctx.ComputedEpochStakes))
			for epoch, data := range ctx.ComputedEpochStakes {
				s.ComputedEpochStakes[epoch] = string(data)
			}
		}

		// Also update legacy fields for backwards compatibility
		s.LastRunID = ctx.RunID
		s.LastRunAt = time.Now()
	}

	// Ensure schema version is set
	s.StateSchemaVersion = CurrentStateSchemaVersion

	return s.Save(accountsDbDir)
}

// HasResumeData returns true if the state has resume context stored.
// This indicates the state was saved during a graceful shutdown with full context.
func (s *MithrilState) HasResumeData() bool {
	return s != nil && s.LastSlot > 0 && s.LastAcctsLtHash != ""
}

// ClearManifestEpochStakes removes the manifest epoch stakes after they're no longer needed.
// This should be called after the first slot is replayed past the snapshot slot.
func (s *MithrilState) ClearManifestEpochStakes() {
	s.ManifestEpochStakes = nil
}

// getWriterCommit returns the writer commit, preferring the new field but falling back to legacy.
func (s *MithrilState) getWriterCommit() string {
	if s.LastWriterCommit != "" {
		return s.LastWriterCommit
	}
	return s.LastCommit // Legacy field fallback
}

// GetResumeSlot returns the slot to resume replay from: the slot after the last
// rooted slot in rooted-durable mode (the in-RAM slots above the last rooted slot
// are lost on restart), else LastSlot+1, else SnapshotSlot+1.
func (s *MithrilState) GetResumeSlot() uint64 {
	if s.LastRootedSlot > 0 {
		return s.LastRootedSlot + 1
	}
	if s.LastSlot > 0 {
		return s.LastSlot + 1
	}
	return s.SnapshotSlot + 1
}

// DurableHighWater returns the highest slot durably committed: the rooted watermark
// in rooted-durable mode, else the replayed/snapshot slot (legacy).
func (s *MithrilState) DurableHighWater() uint64 {
	if s.LastRootedSlot > 0 {
		return s.LastRootedSlot
	}
	return s.GetCurrentSlot()
}

// GetCurrentSlot returns the most recent slot (LastSlot if replayed, else SnapshotSlot).
func (s *MithrilState) GetCurrentSlot() uint64 {
	if s.LastSlot > 0 {
		return s.LastSlot
	}
	return s.SnapshotSlot
}

// IsStale returns true if the state is significantly behind the given slot.
func (s *MithrilState) IsStale(latestSlot uint64, threshold uint64) bool {
	currentSlot := s.GetCurrentSlot()
	return latestSlot > currentSlot && (latestSlot-currentSlot) > threshold
}

// BankhashGetter is an interface for getting bankhashes by slot.
// This is used for integrity validation without creating import cycles.
type BankhashGetter interface {
	GetBankHashForSlot(slot uint64) ([]byte, error)
}

// ValidateAgainstBankhashDB checks if AccountsDB has been modified beyond state file.
// This detects cases where the process was killed (Ctrl+Z, kill -9) without
// updating the state file, leaving AccountsDB in an inconsistent state.
func (s *MithrilState) ValidateAgainstBankhashDB(bankhashDb BankhashGetter) error {
	// Rooted-durable: the fold meta + manifests are the commit authority (see
	// accountsdb.RecoverFoldState, which reconciles R before this runs).
	// Bankhash rows BEYOND R are legal: fold bankhashes are written NoSync and
	// batches can carry rows for slots above a partially-advanced state file.
	// The only hard check left is that R itself matches.
	if s.LastRootedSlot > 0 {
		rootedBankhash, err := bankhashDb.GetBankHashForSlot(s.LastRootedSlot)
		if err != nil || len(rootedBankhash) == 0 {
			return fmt.Errorf("state file shows last_rooted_slot=%d, but no bankhash found in bankhash_db", s.LastRootedSlot)
		}
		if s.LastRootedBankhash != "" && base58.Encode(rootedBankhash) != s.LastRootedBankhash {
			return fmt.Errorf("rooted bankhash mismatch for slot %d: state file has %s, bankhash_db has %s",
				s.LastRootedSlot, s.LastRootedBankhash, base58.Encode(rootedBankhash))
		}
		return nil
	}

	if s.LastSlot == 0 {
		// No replay happened according to state file
		// Check if bankhash exists for snapshot_slot + 1
		checkSlot := s.SnapshotSlot + 1
		bankhash, err := bankhashDb.GetBankHashForSlot(checkSlot)
		if err == nil && len(bankhash) > 0 {
			return fmt.Errorf("state file shows no replay (last_slot=0), but bankhash_db has entry for slot %d - AccountsDB may be corrupted", checkSlot)
		}
	} else {
		// Replay happened, check for slots beyond last_slot
		checkSlot := s.LastSlot + 1
		bankhash, err := bankhashDb.GetBankHashForSlot(checkSlot)
		if err == nil && len(bankhash) > 0 {
			return fmt.Errorf("state file shows last_slot=%d, but bankhash_db has entry for slot %d - AccountsDB may be corrupted", s.LastSlot, checkSlot)
		}

		// Also verify last_slot's bankhash matches
		expectedBankhash, err := bankhashDb.GetBankHashForSlot(s.LastSlot)
		if err != nil || len(expectedBankhash) == 0 {
			return fmt.Errorf("state file shows last_slot=%d, but no bankhash found in bankhash_db", s.LastSlot)
		}
		if s.LastBankhash != "" && base58.Encode(expectedBankhash) != s.LastBankhash {
			return fmt.Errorf("bankhash mismatch for slot %d: state file has %s, bankhash_db has %s",
				s.LastSlot, s.LastBankhash, base58.Encode(expectedBankhash))
		}
	}
	return nil
}

// NewReadyStateOpts contains options for creating a new ready state.
type NewReadyStateOpts struct {
	SnapshotSlot     uint64
	SnapshotEpoch    uint64
	FullSnapshotPath string
	IncrSnapshotPath string
	IncrBaseSlot     uint64
	IncrSlot         uint64
	BuildMode        string // "auto", "snapshot", "new-snapshot", "accountsdb"
	BuildStartedAt   time.Time
	Cluster          string // "mainnet-beta", "testnet", "devnet"
	GenesisHash      string // Base58 genesis hash
	WriterVersion    string // Semver tag
	WriterCommit     string // Git commit hash
}

// NewReadyState creates a new state marking the AccountsDB as ready.
func NewReadyState(snapshotSlot uint64, snapshotEpoch uint64, fullSnapshotPath string, incrSnapshotPath string, incrBaseSlot uint64, incrSlot uint64) *MithrilState {
	return NewReadyStateWithOpts(NewReadyStateOpts{
		SnapshotSlot:     snapshotSlot,
		SnapshotEpoch:    snapshotEpoch,
		FullSnapshotPath: fullSnapshotPath,
		IncrSnapshotPath: incrSnapshotPath,
		IncrBaseSlot:     incrBaseSlot,
		IncrSlot:         incrSlot,
	})
}

// NewReadyStateWithOpts creates a new state with full options including cluster and version info.
func NewReadyStateWithOpts(opts NewReadyStateOpts) *MithrilState {
	state := &MithrilState{
		StateSchemaVersion: CurrentStateSchemaVersion,
		Stage:              "ready",
		SnapshotSlot:       opts.SnapshotSlot,
		SnapshotEpoch:      opts.SnapshotEpoch,
		BuildCompleted:     time.Now(),
		BuildStartedAt:     opts.BuildStartedAt,
		BuildMode:          opts.BuildMode,
		Cluster:            opts.Cluster,
		GenesisHash:        opts.GenesisHash,
		LastWriterVersion:  opts.WriterVersion,
		LastWriterCommit:   opts.WriterCommit,
		LastCommit:         opts.WriterCommit, // Also set legacy field
	}

	if opts.FullSnapshotPath != "" {
		state.FullSnapshot = &SnapshotInfo{
			Path: opts.FullSnapshotPath,
			Slot: opts.SnapshotSlot,
		}
	}

	if opts.IncrSnapshotPath != "" {
		state.IncrSnapshot = &SnapshotInfo{
			Path:     opts.IncrSnapshotPath,
			BaseSlot: opts.IncrBaseSlot,
			Slot:     opts.IncrSlot,
		}
	}

	return state
}

// ValidateGenesisHash checks if the stored genesis hash matches the expected value.
// Returns an error if there's a mismatch (prevents mainnet/testnet mixups).
// Returns nil if state has no genesis hash stored (first run after upgrade).
func (s *MithrilState) ValidateGenesisHash(expectedGenesisHash string) error {
	if s.GenesisHash == "" {
		// No genesis hash stored (older state file) - allow but log warning
		return nil
	}
	if s.GenesisHash != expectedGenesisHash {
		return fmt.Errorf("genesis hash mismatch: state has %s, RPC returned %s - refusing to use AccountsDB built for a different cluster",
			s.GenesisHash, expectedGenesisHash)
	}
	return nil
}

// SetClusterInfo updates the cluster and genesis hash in the state.
// Should be called on first run after upgrade if genesis hash is missing.
func (s *MithrilState) SetClusterInfo(cluster, genesisHash string) {
	s.Cluster = cluster
	s.GenesisHash = genesisHash
}

// DeleteState removes the state file from the accountsdb directory.
func DeleteState(accountsDbDir string) error {
	stateFile := filepath.Join(accountsDbDir, StateFileName)
	err := os.Remove(stateFile)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete state file: %w", err)
	}
	return nil
}

// ValidateAccountsDbArtifacts checks if expected AccountsDB artifacts exist.
// This provides an extra layer of validation beyond just checking the state file.
func ValidateAccountsDbArtifacts(accountsDbDir string) error {
	requiredFiles := []string{
		"mithril_db",
		"bankhash_db",
		"accounts",
		"largest_file_id",
		"bank_hash",
		"manifest",
	}

	for _, file := range requiredFiles {
		path := filepath.Join(accountsDbDir, file)
		if _, err := os.Stat(path); err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("missing required artifact: %s", file)
			}
			return fmt.Errorf("error checking artifact %s: %w", file, err)
		}
	}

	return nil
}

// CheckAndLoadValidState loads state and validates that AccountsDB is ready.
// Returns (state, nil) if valid, or (nil, nil) if state is invalid/missing.
// Returns (nil, error) only for unexpected errors.
func CheckAndLoadValidState(accountsDbDir string) (*MithrilState, error) {
	state, err := LoadState(accountsDbDir)
	if err != nil {
		mlog.Log.Infof("error loading state file: %v", err)
		return nil, nil
	}

	if state == nil {
		mlog.Log.Infof("no state file found in %s", accountsDbDir)
		return nil, nil
	}

	// Handle corrupted state with specific logging
	if state.IsCorrupted() {
		mlog.Log.Infof("previous corruption detected: %s", state.CorruptionReason)
		return nil, nil
	}

	if !state.IsReady() {
		mlog.Log.Infof("state file exists but stage is %q (not ready)", state.Stage)
		return nil, nil
	}

	// Extra validation: check that artifacts actually exist
	if err := ValidateAccountsDbArtifacts(accountsDbDir); err != nil {
		mlog.Log.Infof("state file says ready but artifacts invalid: %v", err)
		return nil, nil
	}

	return state, nil
}
