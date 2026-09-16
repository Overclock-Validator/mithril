package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

const (
	maxRewardArtifactBytes = 2 * 1024 * 1024 // compact calculated summary
	maxRewardIssueBytes    = 512
	maxRewardListEntries   = 10_000
	defaultRewardListLimit = 100
	maxRewardListLimit     = 500
	rewardPrefix           = "epoch_boundary_"
	rewardMarker           = "_rewards_slot_"
)

// rewardArtifact is one epoch-boundary reward file's bounded metadata.
type rewardArtifact struct {
	File      string `json:"file"`
	Slot      uint64 `json:"slot"`
	Kind      string `json:"kind"`   // calculated | voting
	Format    string `json:"format"` // json | csv
	SizeBytes int64  `json:"size_bytes"`
}

// parseRewardName extracts kind, slot, and format from
// epoch_boundary_<kind>_rewards_slot_<N>.<ext>.
func parseRewardName(name string) (kind string, slot uint64, format string, ok bool) {
	if !strings.HasPrefix(name, rewardPrefix) {
		return "", 0, "", false
	}
	rest := strings.TrimPrefix(name, rewardPrefix)
	i := strings.Index(rest, rewardMarker)
	if i <= 0 {
		return "", 0, "", false
	}
	kind = rest[:i]
	if kind != "calculated" && kind != "voting" {
		return "", 0, "", false
	}
	tail := rest[i+len(rewardMarker):]
	ext := filepath.Ext(tail)
	if ext != ".json" && ext != ".csv" {
		return "", 0, "", false
	}
	if kind == "voting" && ext != ".json" {
		return "", 0, "", false
	}
	slot, err := strconv.ParseUint(strings.TrimSuffix(tail, ext), 10, 64)
	if err != nil {
		return "", 0, "", false
	}
	return kind, slot, strings.TrimPrefix(ext, "."), true
}

// resolveRewardsDir prefers the active per-run layout. A present but unsafe or
// unreadable latest path is an error and must not silently fall back to stale
// legacy data.
func resolveRewardsDir(logDir string) (dir, layout string, err error) {
	dir, layout, _, err = resolveActiveOrFlatDir(logDir, "rewards")
	return dir, layout, err
}

func listRewardArtifacts(ctx context.Context, logDir, dir string, limit int) (artifacts []rewardArtifact, total int, truncated bool, err error) {
	if limit <= 0 {
		limit = defaultRewardListLimit
	}
	if limit > maxRewardListLimit {
		limit = maxRewardListLimit
	}

	root, err := openConfinedRoot(dir, logDir)
	if err != nil {
		return nil, 0, false, err
	}
	defer root.Close()
	d, err := root.Open(".")
	if err != nil {
		return nil, 0, false, err
	}
	defer d.Close()

	all := make([]rewardArtifact, 0, min(limit, 64))
	for scanned := 0; ; {
		if err := ctx.Err(); err != nil {
			return nil, 0, false, err
		}
		entries, readErr := d.ReadDir(256)
		for _, entry := range entries {
			scanned++
			if scanned > maxRewardListEntries {
				return nil, 0, false, fmt.Errorf("too many reward directory entries: exceeds %d limit", maxRewardListEntries)
			}
			kind, slot, format, ok := parseRewardName(entry.Name())
			if !ok || entry.Type()&os.ModeSymlink != 0 {
				continue
			}
			info, infoErr := entry.Info()
			if infoErr != nil {
				return nil, 0, false, fmt.Errorf("inspect reward artifact %s: %w", entry.Name(), infoErr)
			}
			if !info.Mode().IsRegular() {
				continue
			}
			all = append(all, rewardArtifact{File: entry.Name(), Slot: slot, Kind: kind, Format: format, SizeBytes: info.Size()})
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, 0, false, readErr
		}
	}

	sort.Slice(all, func(i, j int) bool {
		if all[i].Slot != all[j].Slot {
			return all[i].Slot < all[j].Slot
		}
		if all[i].Kind != all[j].Kind {
			return all[i].Kind < all[j].Kind
		}
		return all[i].Format < all[j].Format
	})
	total = len(all)
	if len(all) > limit {
		all = all[len(all)-limit:]
		truncated = true
	}
	return all, total, truncated, nil
}

type rewardTypeSummary struct {
	TrackedAccounts int    `json:"tracked_accounts"`
	RewardCount     uint64 `json:"reward_count"`
	TotalLamports   uint64 `json:"total_lamports"`
}

type stakingRewardSummary struct {
	RecordCount         uint64 `json:"record_count"`
	RewardCount         uint64 `json:"reward_count"`
	CreditsOnlyCount    uint64 `json:"credits_only_count"`
	TotalLamports       uint64 `json:"total_lamports"`
	ExpectedRecordCount uint64 `json:"expected_record_count"`
}

type totalRewardSummary struct {
	RecordCount      uint64 `json:"record_count"`
	RewardCount      uint64 `json:"reward_count"`
	CreditsOnlyCount uint64 `json:"credits_only_count"`
	TotalLamports    uint64 `json:"total_lamports"`
}

type calculatedRewardArtifact struct {
	Slot          uint64               `json:"slot"`
	Epoch         uint64               `json:"epoch"`
	GeneratedAt   string               `json:"generated_at"`
	RewardsCSV    string               `json:"rewards_csv"`
	NumPartitions uint64               `json:"num_partitions"`
	Voting        rewardTypeSummary    `json:"voting"`
	Staking       stakingRewardSummary `json:"staking"`
	Totals        totalRewardSummary   `json:"totals"`
}

type comparisonSummary struct {
	LeftTotalLamports  *int64 `json:"left_total_lamports"`
	RightTotalLamports *int64 `json:"right_total_lamports"`
	TotalLamportsDelta *int64 `json:"total_lamports_delta"`
	MismatchedCount    *int   `json:"mismatched_count"`
	MissingInLeft      *int   `json:"missing_in_left"`
	MissingInRight     *int   `json:"missing_in_right"`
}

func (s *comparisonSummary) matched() bool {
	return validateComparisonSummary(s) && *s.TotalLamportsDelta == 0 &&
		*s.MismatchedCount == 0 && *s.MissingInLeft == 0 && *s.MissingInRight == 0
}

type votingRewardSnapshot struct {
	TrackedVoteAccounts *int   `json:"tracked_vote_accounts"`
	RewardCount         *int   `json:"reward_count"`
	TotalLamports       *int64 `json:"total_lamports"`
	validated           *validatedRewardSnapshot
}

type votingRewardArtifact struct {
	Slot        uint64 `json:"slot"`
	Epoch       uint64 `json:"epoch"`
	RewardType  string `json:"reward_type"`
	GeneratedAt string `json:"generated_at"`
	RPCEndpoint string `json:"rpc_endpoint"`

	Local       *votingRewardSnapshot `json:"local"`
	SourceBlock *votingRewardSnapshot `json:"source_block"`

	RPCConfirmed      *votingRewardSnapshot `json:"rpc_confirmed"`
	RPCConfirmedError string                `json:"rpc_confirmed_error"`
	RPCFinalized      *votingRewardSnapshot `json:"rpc_finalized"`
	RPCFinalizedError string                `json:"rpc_finalized_error"`

	LocalVsSource        *comparisonSummary `json:"local_vs_source"`
	LocalVsRPCConfirmed  *comparisonSummary `json:"local_vs_rpc_confirmed"`
	LocalVsRPCFinalized  *comparisonSummary `json:"local_vs_rpc_finalized"`
	SourceVsRPCConfirmed *comparisonSummary `json:"source_vs_rpc_confirmed"`
	SourceVsRPCFinalized *comparisonSummary `json:"source_vs_rpc_finalized"`
}

type calculatedRewardOutput struct {
	Epoch           uint64 `json:"epoch"`
	GeneratedAt     string `json:"generated_at"`
	NumPartitions   uint64 `json:"num_partitions"`
	RewardCount     uint64 `json:"reward_count"`
	TotalLamports   string `json:"total_lamports"`
	VotingLamports  string `json:"voting_lamports"`
	StakingLamports string `json:"staking_lamports"`
}

type votingRewardOutput struct {
	Epoch             uint64 `json:"epoch"`
	GeneratedAt       string `json:"generated_at"`
	LocalSourceStatus string `json:"local_source_status"`
	ConfirmedStatus   string `json:"confirmed_status"`
	FinalizedStatus   string `json:"finalized_status"`
}

type rewardsListOutput struct {
	LogDir      string           `json:"log_dir"`
	Layout      string           `json:"layout,omitempty"`
	State       string           `json:"state"`
	Total       int              `json:"total"`
	Returned    int              `json:"returned"`
	Truncated   bool             `json:"truncated"`
	Artifacts   []rewardArtifact `json:"artifacts"`
	SummaryOnly bool             `json:"summary_only"`
}

type rewardsSlotOutput struct {
	LogDir          string                  `json:"log_dir"`
	Layout          string                  `json:"layout,omitempty"`
	Slot            uint64                  `json:"slot"`
	Found           bool                    `json:"found"`
	ArtifactState   string                  `json:"artifact_state"` // unavailable | absent | partial | invalid | complete
	AvailableParts  []string                `json:"available_parts"`
	MissingParts    []string                `json:"missing_parts"`
	Issues          []string                `json:"issues,omitempty"`
	Verification    string                  `json:"verification_state"`
	ComparisonBasis string                  `json:"comparison_basis"`
	Calculated      *calculatedRewardOutput `json:"calculated,omitempty"`
	Voting          *votingRewardOutput     `json:"voting,omitempty"`
	SummaryOnly     bool                    `json:"summary_only"`
}

type rewardsInput struct {
	Slot  *uint64 `json:"slot,omitempty" jsonschema:"if set, return a compact completeness and verification summary for that slot"`
	Limit int     `json:"limit,omitempty" jsonschema:"when listing, return the newest N artifacts (default 100, max 500)"`
}

func boundedRewardIssue(part string, err error) string {
	message := redactUntrustedText(err.Error())
	message, _ = truncateUTF8Bytes(message, maxRewardIssueBytes)
	return part + ": " + message
}

func parseCalculatedReward(data []byte, slot uint64) (*calculatedRewardArtifact, error) {
	var artifact calculatedRewardArtifact
	if err := json.Unmarshal(data, &artifact); err != nil {
		return nil, errors.New("invalid calculated JSON")
	}
	if err := requireJSONFields(data, "calculated",
		"slot", "epoch", "generated_at", "rewards_csv", "num_partitions",
		"voting.tracked_accounts", "voting.reward_count", "voting.total_lamports",
		"staking.record_count", "staking.reward_count", "staking.credits_only_count", "staking.total_lamports", "staking.expected_record_count",
		"totals.record_count", "totals.reward_count", "totals.credits_only_count", "totals.total_lamports",
	); err != nil {
		return nil, err
	}
	if artifact.Slot != slot {
		return nil, fmt.Errorf("calculated artifact slot %d does not match requested slot %d", artifact.Slot, slot)
	}
	if _, err := time.Parse(time.RFC3339Nano, artifact.GeneratedAt); err != nil {
		return nil, errors.New("calculated artifact has invalid generated_at")
	}
	if artifact.Voting.TotalLamports > ^uint64(0)-artifact.Staking.TotalLamports ||
		artifact.Totals.TotalLamports != artifact.Voting.TotalLamports+artifact.Staking.TotalLamports {
		return nil, errors.New("calculated artifact total lamports are inconsistent")
	}
	if artifact.Voting.RewardCount > ^uint64(0)-artifact.Staking.RewardCount ||
		artifact.Totals.RewardCount != artifact.Voting.RewardCount+artifact.Staking.RewardCount {
		return nil, errors.New("calculated artifact reward counts are inconsistent")
	}
	if artifact.Staking.RewardCount > ^uint64(0)-artifact.Staking.CreditsOnlyCount ||
		artifact.Staking.RecordCount != artifact.Staking.RewardCount+artifact.Staking.CreditsOnlyCount ||
		artifact.Staking.RecordCount != artifact.Staking.ExpectedRecordCount {
		return nil, errors.New("calculated artifact staking record counts are inconsistent")
	}
	if artifact.Voting.RewardCount > ^uint64(0)-artifact.Staking.RecordCount ||
		artifact.Totals.RecordCount != artifact.Voting.RewardCount+artifact.Staking.RecordCount ||
		artifact.Totals.CreditsOnlyCount != artifact.Staking.CreditsOnlyCount {
		return nil, errors.New("calculated artifact total record counts are inconsistent")
	}
	if artifact.Voting.TrackedAccounts < 0 || artifact.Voting.RewardCount > uint64(artifact.Voting.TrackedAccounts) {
		return nil, errors.New("calculated artifact voting counts are inconsistent")
	}
	return &artifact, nil
}

func requireJSONFields(data []byte, artifactKind string, paths ...string) error {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(data, &root); err != nil || root == nil {
		return fmt.Errorf("%s artifact must be a JSON object", artifactKind)
	}
	for _, path := range paths {
		object := root
		parts := strings.Split(path, ".")
		for i, part := range parts {
			raw, ok := object[part]
			if !ok || strings.TrimSpace(string(raw)) == "null" {
				return fmt.Errorf("%s artifact is missing required field %s", artifactKind, path)
			}
			if i == len(parts)-1 {
				break
			}
			var nested map[string]json.RawMessage
			if err := json.Unmarshal(raw, &nested); err != nil || nested == nil {
				return fmt.Errorf("%s artifact has invalid required object %s", artifactKind, strings.Join(parts[:i+1], "."))
			}
			object = nested
		}
	}
	return nil
}

func comparisonStatus(snapshot *votingRewardSnapshot, rawErr string, left, right *comparisonSummary, endpointConfigured bool) string {
	if snapshot == nil {
		if rawErr != "" {
			return "unavailable"
		}
		if !endpointConfigured {
			return "not_configured"
		}
		return "unavailable"
	}
	if left == nil || right == nil {
		return "observed_unverified"
	}
	if left.matched() && right.matched() {
		return "matched"
	}
	return "mismatched"
}

func inspectRewardSlot(ctx context.Context, logDir, dir, layout string, slot uint64) (rewardsSlotOutput, error) {
	out := rewardsSlotOutput{
		LogDir:          logDir,
		Layout:          layout,
		Slot:            slot,
		ArtifactState:   "absent",
		AvailableParts:  []string{},
		MissingParts:    []string{},
		Issues:          []string{},
		Verification:    "unavailable",
		ComparisonBasis: "lamports_only",
		SummaryOnly:     true,
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if dir == "" {
		out.ArtifactState = "unavailable"
		out.MissingParts = []string{"calculated_json", "calculated_csv", "voting_json"}
		return out, nil
	}

	root, err := openConfinedRoot(dir, logDir)
	if err != nil {
		out.ArtifactState = "invalid"
		out.Issues = append(out.Issues, "could not open rewards directory")
		return out, nil
	}
	defer root.Close()

	calculatedName := fmt.Sprintf("%scalculated_rewards_slot_%d.json", rewardPrefix, slot)
	votingName := fmt.Sprintf("%svoting_rewards_slot_%d.json", rewardPrefix, slot)
	csvName := fmt.Sprintf("%scalculated_rewards_slot_%d.csv", rewardPrefix, slot)

	var calculated *calculatedRewardArtifact
	var voting *votingRewardArtifact
	for _, item := range []struct {
		name string
		part string
		json bool
	}{
		{calculatedName, "calculated_json", true},
		{csvName, "calculated_csv", false},
		{votingName, "voting_json", true},
	} {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		if _, err := root.Lstat(item.name); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				out.MissingParts = append(out.MissingParts, item.part)
				continue
			}
			out.Issues = append(out.Issues, item.part+": unreadable")
			continue
		}
		if !item.json {
			// Mainnet CSVs contain one row per reward and can legitimately be much
			// larger than the JSON summary cap. MCP never returns their rows, so
			// validate only the confined, non-empty regular file on the opened root.
			if _, err := statRootRegularFile(root, item.name); err != nil {
				out.Issues = append(out.Issues, boundedRewardIssue(item.part, err))
				continue
			}
			out.AvailableParts = append(out.AvailableParts, item.part)
			continue
		}
		if item.part == "calculated_json" {
			data, readErr := readRootFile(ctx, root, item.name, maxRewardArtifactBytes)
			if readErr != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return out, ctxErr
				}
				out.Issues = append(out.Issues, boundedRewardIssue(item.part, readErr))
				continue
			}
			calculated, err = parseCalculatedReward(data, slot)
		} else {
			voting, err = parseVotingRewardFile(ctx, root, item.name, slot)
		}
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return out, ctxErr
			}
			out.Issues = append(out.Issues, boundedRewardIssue(item.part, err))
			continue
		}
		out.AvailableParts = append(out.AvailableParts, item.part)
	}

	out.Found = len(out.AvailableParts) > 0 || len(out.Issues) > 0
	switch {
	case len(out.Issues) > 0:
		out.ArtifactState = "invalid"
	case len(out.AvailableParts) == 0:
		out.ArtifactState = "absent"
	case len(out.MissingParts) > 0:
		out.ArtifactState = "partial"
	default:
		out.ArtifactState = "complete"
	}

	if calculated != nil {
		expectedCSV := filepath.ToSlash(filepath.Join("rewards", csvName))
		if filepath.ToSlash(calculated.RewardsCSV) != expectedCSV {
			out.ArtifactState = "invalid"
			out.Issues = append(out.Issues, "calculated_json: rewards_csv does not match the expected confined artifact")
		}
		out.Calculated = &calculatedRewardOutput{
			Epoch:           calculated.Epoch,
			GeneratedAt:     calculated.GeneratedAt,
			NumPartitions:   calculated.NumPartitions,
			RewardCount:     calculated.Totals.RewardCount,
			TotalLamports:   strconv.FormatUint(calculated.Totals.TotalLamports, 10),
			VotingLamports:  strconv.FormatUint(calculated.Voting.TotalLamports, 10),
			StakingLamports: strconv.FormatUint(calculated.Staking.TotalLamports, 10),
		}
	}
	if voting != nil {
		confirmed := comparisonStatus(voting.RPCConfirmed, voting.RPCConfirmedError, voting.LocalVsRPCConfirmed, voting.SourceVsRPCConfirmed, voting.RPCEndpoint != "")
		finalized := comparisonStatus(voting.RPCFinalized, voting.RPCFinalizedError, voting.LocalVsRPCFinalized, voting.SourceVsRPCFinalized, voting.RPCEndpoint != "")
		localSource := "mismatched"
		if voting.LocalVsSource.matched() {
			localSource = "matched"
		}
		out.Voting = &votingRewardOutput{
			Epoch:             voting.Epoch,
			GeneratedAt:       voting.GeneratedAt,
			LocalSourceStatus: localSource,
			ConfirmedStatus:   confirmed,
			FinalizedStatus:   finalized,
		}
		switch {
		case localSource == "mismatched" || confirmed == "mismatched" || finalized == "mismatched":
			out.Verification = "mismatched"
		case finalized == "matched":
			out.Verification = "reference_matched"
		case confirmed == "matched":
			out.Verification = "confirmed_reference_matched"
		default:
			out.Verification = "unavailable"
		}
	}
	if calculated != nil && voting != nil {
		switch {
		case calculated.Epoch != voting.Epoch:
			out.ArtifactState = "invalid"
			out.Verification = "unavailable"
			out.Issues = append(out.Issues, "calculated and voting epochs do not match")
		case voting.Local == nil || voting.Local.TrackedVoteAccounts == nil || voting.Local.RewardCount == nil || voting.Local.TotalLamports == nil ||
			*voting.Local.TrackedVoteAccounts < 0 || *voting.Local.RewardCount < 0 || *voting.Local.TotalLamports < 0 ||
			calculated.Voting.TrackedAccounts != *voting.Local.TrackedVoteAccounts ||
			calculated.Voting.RewardCount != uint64(*voting.Local.RewardCount) ||
			calculated.Voting.TotalLamports != uint64(*voting.Local.TotalLamports):
			out.ArtifactState = "invalid"
			out.Verification = "unavailable"
			out.Issues = append(out.Issues, "calculated and voting summaries do not match")
		}
	}
	if err := ctx.Err(); err != nil {
		return out, err
	}
	return out, nil
}

func registerRewardsTool(server *mcpsdk.Server, cfg Config) {
	addTool(server, cfg, &mcpsdk.Tool{
		Name:         "mithril_read_rewards",
		Annotations:  annReadOnlyLocal,
		OutputSchema: dynamicObjectOutputSchema,
		Description:  "Inspect epoch-boundary reward summaries from debug.dump_epoch_voting_reward_diff. List recent artifacts or query one slot for completeness and lamports verification.",
	}, func(ctx context.Context, _ *mcpsdk.CallToolRequest, in rewardsInput) (*mcpsdk.CallToolResult, any, error) {
		logDir, err := requireConfiguredPath(cfg.LogDir, "MITHRIL_LOG_DIR is not configured")
		if err != nil {
			return nil, nil, err
		}
		dir, layout, err := resolveRewardsDir(logDir)
		if err != nil {
			return nil, nil, err
		}
		if in.Slot != nil {
			out, err := inspectRewardSlot(ctx, logDir, dir, layout, *in.Slot)
			if err != nil {
				return nil, nil, err
			}
			return nil, out, nil
		}
		if dir == "" {
			return nil, rewardsListOutput{
				LogDir: logDir, Layout: layout, State: "unavailable", Artifacts: []rewardArtifact{}, SummaryOnly: true,
			}, nil
		}
		artifacts, total, truncated, err := listRewardArtifacts(ctx, logDir, dir, in.Limit)
		if err != nil {
			return nil, nil, err
		}
		return nil, rewardsListOutput{
			LogDir: logDir, Layout: layout, State: "available", Total: total, Returned: len(artifacts),
			Truncated: truncated, Artifacts: artifacts, SummaryOnly: true,
		}, nil
	})
}
