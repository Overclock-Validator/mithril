package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	corestate "github.com/Overclock-Validator/mithril/pkg/state"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// maxStateFileBytes caps state-file reads at 64 MB. Typical files are ~11 MB;
// the extra capacity allows growth without trusting a corrupted file size.
const maxStateFileBytes = 64 * 1024 * 1024

const (
	maxStateVisibleStringBytes = 1024
	maxStateExtraFieldNames    = 128
	maxStateExtraNameBytes     = 256
)

// Shutdown-reason categories used by current and legacy state files.
const (
	shutdownNormal         = "Normal"
	shutdownStall          = "Stall"
	shutdownLeaderSchedule = "LeaderSchedule"
	shutdownCompleted      = "Completed"
	shutdownError          = "Error"
)

// ShutdownState contains operational fields from current and legacy state files.
// Extra preserves unknown fields for forward compatibility.
type ShutdownState struct {
	StateSchemaVersion       *uint32 `json:"state_schema_version,omitempty"`
	LastSlot                 *uint64 `json:"last_slot,omitempty"`
	LastEpoch                *uint64 `json:"last_epoch,omitempty"`
	LastBankhash             *string `json:"last_bankhash,omitempty"`
	LastBlockHeight          *uint64 `json:"last_block_height,omitempty"`
	LastRootedSlot           *uint64 `json:"last_rooted_slot,omitempty"`
	LastRootedBankhash       *string `json:"last_rooted_bankhash,omitempty"`
	SnapshotSlot             *uint64 `json:"snapshot_slot,omitempty"`
	SnapshotEpoch            *uint64 `json:"snapshot_epoch,omitempty"`
	Stage                    *string `json:"stage,omitempty"`
	BuildMode                *string `json:"build_mode,omitempty"`
	BuildStartedAt           *string `json:"build_started_at,omitempty"`
	BuildCompletedAt         *string `json:"build_completed_at,omitempty"`
	Cluster                  *string `json:"cluster,omitempty"`
	GenesisHash              *string `json:"genesis_hash,omitempty"`
	CorruptionReason         *string `json:"corruption_reason,omitempty"`
	CorruptionDetectedAt     *string `json:"corruption_detected_at,omitempty"`
	LastShutdownReason       *string `json:"last_shutdown_reason,omitempty"`
	LastShutdownAt           *string `json:"last_shutdown_at,omitempty"`
	CurrentRunID             *string `json:"current_run_id,omitempty"`
	LastWriterVersion        *string `json:"last_writer_version,omitempty"`
	LastWriterCommit         *string `json:"last_writer_commit,omitempty"`
	LastWriterBranch         *string `json:"last_writer_branch,omitempty"`
	ManifestTransactionCount *uint64 `json:"manifest_transaction_count,omitempty"`

	AlpenglowEvidence        []corestate.AlpenglowFinalityEvidence `json:"alpenglow_finality_evidence,omitempty"`
	ReplayDivergenceEvidence []corestate.ReplayDivergenceRecord    `json:"replay_divergence_evidence,omitempty"`

	// Extra is retained only for omitted-field metadata and forward-compatible
	// round trips. MCP responses never include these raw values.
	Extra map[string]json.RawMessage `json:"-"`
	// SourceSizeBytes is descriptor metadata, not part of the state file.
	SourceSizeBytes int64 `json:"-"`

	// SourceModTime is when the state file was last written. The node writes it
	// at lifecycle checkpoints and shutdown, not continuously, so positions
	// below are dated rather than live. Descriptor metadata, not part of the
	// state file.
	SourceModTime time.Time `json:"-"`
}

// knownStateKeys are the JSON keys mapped to named fields above; everything else
// lands in Extra.
var knownStateKeys = map[string]bool{
	"state_schema_version": true, "last_slot": true, "last_epoch": true,
	"last_bankhash": true, "last_block_height": true, "last_rooted_slot": true,
	"last_rooted_bankhash": true, "snapshot_slot": true, "snapshot_epoch": true,
	"stage": true, "build_mode": true,
	"build_started_at": true, "build_completed_at": true, "cluster": true, "genesis_hash": true,
	"corruption_reason": true, "corruption_detected_at": true, "last_shutdown_reason": true,
	"last_shutdown_at": true, "current_run_id": true, "last_writer_version": true,
	"last_writer_commit": true, "last_writer_branch": true, "manifest_transaction_count": true,
	"alpenglow_finality_evidence": true, "replay_divergence_evidence": true,
}

// stateAlias avoids infinite recursion in (Un)MarshalJSON.
type stateAlias ShutdownState

func (s *ShutdownState) UnmarshalJSON(data []byte) error {
	var a stateAlias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	*s = ShutdownState(a)
	extra, err := splitExtra(data, knownStateKeys)
	if err != nil {
		return err
	}
	s.Extra = extra
	return nil
}

func (s ShutdownState) MarshalJSON() ([]byte, error) {
	named, err := json.Marshal(stateAlias(s))
	if err != nil {
		return nil, err
	}
	return mergeExtra(named, s.Extra)
}

// parsedShutdownReason interprets last_shutdown_reason as a category, matching
// both real Mithril's Go-style strings and the simulator's enum names.
func (s *ShutdownState) parsedShutdownReason() (string, bool) {
	if s.LastShutdownReason == nil {
		return "", false
	}
	switch r := *s.LastShutdownReason; r {
	case "graceful shutdown (Ctrl+C)", "Normal":
		return shutdownNormal, true
	case "block fetch stalled - no RPC progress for 5+ minutes", "Stall":
		return shutdownStall, true
	case "leader schedule fetch failed from all RPC endpoints", "LeaderSchedule":
		return shutdownLeaderSchedule, true
	case "replay completed - reached end slot", "Completed":
		return shutdownCompleted, true
	case "Error":
		return shutdownError, true
	default:
		if strings.HasPrefix(r, "replay error") {
			return shutdownError, true
		}
		return "", false
	}
}

func (s *ShutdownState) isErrorShutdown() bool {
	r, ok := s.parsedShutdownReason()
	return ok && r == shutdownError
}

// readShutdownState opens and parses Mithril's state file through one file
// descriptor. The regular-file check and capped read both apply to that same
// opened inode, avoiding a stat-then-open race and preventing growth during the
// read from bypassing maxStateFileBytes.
// Returns (nil, nil) when the file does not exist.
func readShutdownStateContext(ctx context.Context, statePath string) (*ShutdownState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cleanPath := filepath.Clean(statePath)
	f, info, err := openStateRegularFile(cleanPath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open state file: %w", err)
	}
	defer f.Close()

	if info.Size() > maxStateFileBytes {
		return nil, fmt.Errorf("state file too large: %d bytes exceeds %d byte limit", info.Size(), maxStateFileBytes)
	}
	data, err := io.ReadAll(io.LimitReader(&contextReader{ctx: ctx, reader: f}, maxStateFileBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read state file: %w", err)
	}
	if len(data) > maxStateFileBytes {
		return nil, fmt.Errorf("state file too large: exceeds %d byte limit", maxStateFileBytes)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	after, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat state file after reading: %w", err)
	}
	if fileMetadataChanged(info, after) {
		return nil, fmt.Errorf("state file changed while reading")
	}
	var s ShutdownState
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("failed to parse state file")
	}
	s.SourceSizeBytes = info.Size()
	s.SourceModTime = info.ModTime()
	return &s, nil
}

// openStateRegularFile opens only the configured file. The MCP identity needs
// directory traversal and read access to this file, not permission to list the
// AccountsDB directory that contains it.
func openStateRegularFile(path string) (*os.File, os.FileInfo, error) {
	linfo, err := os.Lstat(path)
	if err != nil {
		return nil, nil, err
	}
	if linfo.Mode()&os.ModeSymlink != 0 {
		return nil, nil, errors.New("symbolic-link files are not permitted")
	}
	if !linfo.Mode().IsRegular() {
		return nil, nil, errors.New("state path is not a regular file")
	}
	f, err := os.OpenFile(path, os.O_RDONLY|nonBlockingOpenFlag|noFollowOpenFlag, 0)
	if err != nil {
		return nil, nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	if !info.Mode().IsRegular() || !os.SameFile(linfo, info) {
		f.Close()
		return nil, nil, errors.New("state file changed while opening")
	}
	return f, info, nil
}

type shutdownStateInput struct{}

// AlpenglowFinalityEvidenceSummary reports only bounded operational metadata.
type AlpenglowFinalityEvidenceSummary struct {
	Count         int     `json:"count"`
	ConflictCount int     `json:"conflict_count"`
	EarliestSlot  *uint64 `json:"earliest_slot,omitempty"`
	LatestSlot    *uint64 `json:"latest_slot,omitempty"`
}

// ReplayDivergenceEvidenceSummary reports only bounded operational metadata.
type ReplayDivergenceEvidenceSummary struct {
	Count        int     `json:"count"`
	EarliestSlot *uint64 `json:"earliest_slot,omitempty"`
	LatestSlot   *uint64 `json:"latest_slot,omitempty"`
}

// maxSourceClockSkew is how far the state file's timestamp may sit ahead of now
// before it is treated as unusable rather than as a fresh write. Filesystem
// timestamp granularity and a write that lands microseconds before this read can
// both produce a slightly future stamp; anything beyond that means the writing
// clock disagrees with ours and the file's real age cannot be derived.
const maxSourceClockSkew = 5 * time.Second

// ShutdownStateSummary omits large manifest and resume values while reporting
// their names and aggregate size.
//
// Every position below is as-of SourceAsOf, the moment the state file was last
// written. The node writes it at lifecycle checkpoints and shutdown, not
// continuously, so a running node's positions can still be a session old.
// SourceAgeSeconds states how stale they are. Treat them as a dated record,
// never as a live reading.
//
// SourceAgeSeconds is 0 both for a file written just now and for one whose
// timestamp is unusable, so SourceClockAnomaly distinguishes them. Without it a
// state file stamped in the future would read as maximally fresh, which is the
// one direction an automation gate must never be wrong in.
type ShutdownStateSummary struct {
	StateSchemaVersion       *uint32  `json:"state_schema_version,omitempty"`
	SchemaSupported          bool     `json:"schema_supported"`
	LastSlot                 *uint64  `json:"last_slot,omitempty"`
	LastEpoch                *uint64  `json:"last_epoch,omitempty"`
	LastBankhash             *string  `json:"last_bankhash,omitempty"`
	LastBlockHeight          *uint64  `json:"last_block_height,omitempty"`
	LastRootedSlot           *uint64  `json:"last_rooted_slot,omitempty"`
	LastRootedBankhash       *string  `json:"last_rooted_bankhash,omitempty"`
	SnapshotSlot             *uint64  `json:"snapshot_slot,omitempty"`
	SnapshotEpoch            *uint64  `json:"snapshot_epoch,omitempty"`
	Stage                    *string  `json:"stage,omitempty"`
	BuildMode                *string  `json:"build_mode,omitempty"`
	BuildStartedAt           *string  `json:"build_started_at,omitempty"`
	BuildCompletedAt         *string  `json:"build_completed_at,omitempty"`
	Cluster                  *string  `json:"cluster,omitempty"`
	GenesisHash              *string  `json:"genesis_hash,omitempty"`
	CorruptionReason         *string  `json:"corruption_reason,omitempty"`
	CorruptionDetectedAt     *string  `json:"corruption_detected_at,omitempty"`
	LastShutdownReason       *string  `json:"last_shutdown_reason,omitempty"`
	LastShutdownAt           *string  `json:"last_shutdown_at,omitempty"`
	CurrentRunID             *string  `json:"current_run_id,omitempty"`
	LastWriterVersion        *string  `json:"last_writer_version,omitempty"`
	LastWriterCommit         *string  `json:"last_writer_commit,omitempty"`
	LastWriterBranch         *string  `json:"last_writer_branch,omitempty"`
	ManifestTransactionCount *uint64  `json:"manifest_transaction_count,omitempty"`
	SourceSizeBytes          int64    `json:"source_size_bytes"`
	SourceAsOf               *string  `json:"source_as_of,omitempty"`
	SourceAgeSeconds         int64    `json:"source_age_seconds"`
	SourceClockAnomaly       bool     `json:"source_clock_anomaly,omitempty"`
	OmittedExtraFieldCount   int      `json:"omitted_extra_field_count"`
	OmittedExtraBytes        int64    `json:"omitted_extra_bytes"`
	OmittedExtraFields       []string `json:"omitted_extra_fields"`
	ExtraFieldsTruncated     bool     `json:"extra_fields_truncated"`

	AlpenglowEvidence        *AlpenglowFinalityEvidenceSummary `json:"alpenglow_finality_evidence_summary,omitempty"`
	ReplayDivergenceEvidence *ReplayDivergenceEvidenceSummary  `json:"replay_divergence_evidence_summary,omitempty"`
}

func boundedStateString(value *string) *string {
	if value == nil {
		return nil
	}
	bounded, _ := truncateUTF8Bytes(redactUntrustedText(*value), maxStateVisibleStringBytes)
	return &bounded
}

func boundedStateTimestamp(value *string) *string {
	if value == nil {
		return nil
	}
	if parsed, err := time.Parse(time.RFC3339Nano, *value); err == nil && parsed.Equal(time.Time{}) {
		return nil
	}
	return boundedStateString(value)
}

func summarizeAlpenglowEvidence(evidence []corestate.AlpenglowFinalityEvidence) *AlpenglowFinalityEvidenceSummary {
	summary := &AlpenglowFinalityEvidenceSummary{Count: len(evidence)}
	if len(evidence) == 0 {
		return summary
	}
	earliest, latest := evidence[0].Slot, evidence[0].Slot
	for _, item := range evidence {
		if item.Slot < earliest {
			earliest = item.Slot
		}
		if item.Slot > latest {
			latest = item.Slot
		}
		if item.Conflict {
			summary.ConflictCount++
		}
	}
	summary.EarliestSlot = &earliest
	summary.LatestSlot = &latest
	return summary
}

func summarizeReplayDivergenceEvidence(evidence []corestate.ReplayDivergenceRecord) *ReplayDivergenceEvidenceSummary {
	summary := &ReplayDivergenceEvidenceSummary{Count: len(evidence)}
	if len(evidence) == 0 {
		return summary
	}
	earliest, latest := evidence[0].Slot, evidence[0].Slot
	for _, item := range evidence[1:] {
		if item.Slot < earliest {
			earliest = item.Slot
		}
		if item.Slot > latest {
			latest = item.Slot
		}
	}
	summary.EarliestSlot = &earliest
	summary.LatestSlot = &latest
	return summary
}

func summarizeShutdownState(state *ShutdownState) *ShutdownStateSummary {
	return summarizeShutdownStateAt(state, time.Now())
}

func summarizeShutdownStateAt(state *ShutdownState, now time.Time) *ShutdownStateSummary {
	if state == nil {
		return nil
	}
	keys, omittedBytes, truncated := boundedExtraMetadata(state.Extra, maxStateExtraFieldNames, maxStateExtraNameBytes)
	schemaSupported := state.StateSchemaVersion != nil && *state.StateSchemaVersion == corestate.CurrentStateSchemaVersion
	var alpenglowEvidence *AlpenglowFinalityEvidenceSummary
	var replayDivergenceEvidence *ReplayDivergenceEvidenceSummary
	if schemaSupported {
		alpenglowEvidence = summarizeAlpenglowEvidence(state.AlpenglowEvidence)
		replayDivergenceEvidence = summarizeReplayDivergenceEvidence(state.ReplayDivergenceEvidence)
	}
	var sourceAsOf *string
	var sourceAgeSeconds int64
	var sourceClockAnomaly bool
	if !state.SourceModTime.IsZero() {
		stamp := state.SourceModTime.UTC().Format(time.RFC3339)
		sourceAsOf = &stamp
		age := now.Sub(state.SourceModTime)
		switch {
		case age >= 0:
			sourceAgeSeconds = int64(age.Seconds())
		case age >= -maxSourceClockSkew:
			// Within tolerance: filesystem timestamp granularity, or the node
			// writing microseconds before this read. Treat as just written.
		default:
			sourceClockAnomaly = true
		}
	}

	return &ShutdownStateSummary{
		StateSchemaVersion:       state.StateSchemaVersion,
		SchemaSupported:          schemaSupported,
		SourceAsOf:               sourceAsOf,
		SourceAgeSeconds:         sourceAgeSeconds,
		SourceClockAnomaly:       sourceClockAnomaly,
		LastSlot:                 state.LastSlot,
		LastEpoch:                state.LastEpoch,
		LastBankhash:             boundedStateString(state.LastBankhash),
		LastBlockHeight:          state.LastBlockHeight,
		LastRootedSlot:           state.LastRootedSlot,
		LastRootedBankhash:       boundedStateString(state.LastRootedBankhash),
		SnapshotSlot:             state.SnapshotSlot,
		SnapshotEpoch:            state.SnapshotEpoch,
		Stage:                    boundedStateString(state.Stage),
		BuildMode:                boundedStateString(state.BuildMode),
		BuildStartedAt:           boundedStateTimestamp(state.BuildStartedAt),
		BuildCompletedAt:         boundedStateTimestamp(state.BuildCompletedAt),
		Cluster:                  boundedStateString(state.Cluster),
		GenesisHash:              boundedStateString(state.GenesisHash),
		CorruptionReason:         boundedStateString(state.CorruptionReason),
		CorruptionDetectedAt:     boundedStateTimestamp(state.CorruptionDetectedAt),
		LastShutdownReason:       boundedStateString(state.LastShutdownReason),
		LastShutdownAt:           boundedStateTimestamp(state.LastShutdownAt),
		CurrentRunID:             boundedStateString(state.CurrentRunID),
		LastWriterVersion:        boundedStateString(state.LastWriterVersion),
		LastWriterCommit:         boundedStateString(state.LastWriterCommit),
		LastWriterBranch:         boundedStateString(state.LastWriterBranch),
		ManifestTransactionCount: state.ManifestTransactionCount,
		SourceSizeBytes:          state.SourceSizeBytes,
		OmittedExtraFieldCount:   len(state.Extra),
		OmittedExtraBytes:        omittedBytes,
		OmittedExtraFields:       keys,
		ExtraFieldsTruncated:     truncated,

		AlpenglowEvidence:        alpenglowEvidence,
		ReplayDivergenceEvidence: replayDivergenceEvidence,
	}
}

type shutdownStateOutput struct {
	Found                bool                  `json:"found"`
	State                *ShutdownStateSummary `json:"state,omitempty"`
	ParsedShutdownReason string                `json:"parsed_shutdown_reason,omitempty"`
	IsErrorShutdown      bool                  `json:"is_error_shutdown"`
}

func registerStateTools(server *mcpsdk.Server, cfg Config) {
	addTool(server, cfg, &mcpsdk.Tool{
		Name:         "mithril_read_shutdown_state",
		Annotations:  annReadOnlyLocal,
		OutputSchema: nil,
		Description:  "Read a bounded operational state summary: schema, replay and rooted positions, snapshot, persisted safety evidence, writer, shutdown, and omitted-field metadata.",
	}, func(ctx context.Context, _ *mcpsdk.CallToolRequest, _ shutdownStateInput) (*mcpsdk.CallToolResult, shutdownStateOutput, error) {
		statePath, err := requireConfiguredPath(cfg.StatePath, "MITHRIL_STATE_PATH is not configured")
		if err != nil {
			return nil, shutdownStateOutput{}, err
		}
		state, err := readShutdownStateContext(ctx, statePath)
		if err != nil {
			return nil, shutdownStateOutput{}, err
		}
		if state == nil {
			return nil, shutdownStateOutput{Found: false}, nil
		}
		reason, _ := state.parsedShutdownReason()
		return nil, shutdownStateOutput{
			Found:                true,
			State:                summarizeShutdownState(state),
			ParsedShutdownReason: reason,
			IsErrorShutdown:      state.isErrorShutdown(),
		}, nil
	})
}
