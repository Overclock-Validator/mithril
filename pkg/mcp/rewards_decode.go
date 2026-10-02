package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const (
	maxVotingRewardArtifactBytes = 16 * 1024 * 1024
	maxRewardSnapshotEntries     = 100_000
	maxRewardMetadataStringBytes = 4096
	maxRewardPubkeyBytes         = 128
	maxRewardFieldNameBytes      = 128
	maxRewardObjectFields        = 64
	maxRewardJSONDepth           = 64
)

func checkedAddInt64(left, right int64) (int64, bool) {
	total := left + right
	return total, !((right > 0 && total < left) || (right < 0 && total > left))
}

func checkedSubInt64(left, right int64) (int64, bool) {
	delta := left - right
	return delta, !((right > 0 && delta > left) || (right < 0 && delta < left))
}

func validateComparisonSummary(summary *comparisonSummary) bool {
	if summary == nil || summary.LeftTotalLamports == nil || summary.RightTotalLamports == nil || summary.TotalLamportsDelta == nil ||
		summary.MismatchedCount == nil || summary.MissingInLeft == nil || summary.MissingInRight == nil ||
		*summary.MismatchedCount < 0 || *summary.MissingInLeft < 0 || *summary.MissingInRight < 0 {
		return false
	}
	delta, ok := checkedSubInt64(*summary.LeftTotalLamports, *summary.RightTotalLamports)
	return ok && *summary.TotalLamportsDelta == delta
}

type validatedRewardSnapshot struct {
	total   int64
	rewards map[string]int64
}

func validateRewardSnapshot(name string, snapshot *votingRewardSnapshot, requireTracked bool) (validatedRewardSnapshot, error) {
	if snapshot == nil || snapshot.RewardCount == nil || snapshot.TotalLamports == nil || snapshot.validated == nil {
		return validatedRewardSnapshot{}, fmt.Errorf("voting artifact %s snapshot is missing or invalid", name)
	}
	if *snapshot.RewardCount < 0 || len(snapshot.validated.rewards) != *snapshot.RewardCount || snapshot.validated.total != *snapshot.TotalLamports {
		return validatedRewardSnapshot{}, fmt.Errorf("voting artifact %s snapshot counts are inconsistent", name)
	}
	if requireTracked && (snapshot.TrackedVoteAccounts == nil || *snapshot.TrackedVoteAccounts < *snapshot.RewardCount) {
		return validatedRewardSnapshot{}, fmt.Errorf("voting artifact %s snapshot counts are inconsistent", name)
	}
	return *snapshot.validated, nil
}

func comparisonCoherent(summary *comparisonSummary, left, right validatedRewardSnapshot) bool {
	if !validateComparisonSummary(summary) || *summary.LeftTotalLamports != left.total || *summary.RightTotalLamports != right.total {
		return false
	}
	mismatched, missingLeft, missingRight := 0, 0, 0
	for pubkey, leftLamports := range left.rewards {
		rightLamports, ok := right.rewards[pubkey]
		if !ok {
			missingRight++
		}
		if !ok || leftLamports != rightLamports {
			mismatched++
		}
	}
	for pubkey := range right.rewards {
		if _, ok := left.rewards[pubkey]; !ok {
			missingLeft++
			mismatched++
		}
	}
	return *summary.MismatchedCount == mismatched && *summary.MissingInLeft == missingLeft && *summary.MissingInRight == missingRight
}

func validateRPCRewardObservation(
	name string,
	endpointConfigured bool,
	snapshot *votingRewardSnapshot,
	rawErr string,
	localComparison, sourceComparison *comparisonSummary,
	local, source validatedRewardSnapshot,
) error {
	if !endpointConfigured {
		if snapshot != nil || rawErr != "" || localComparison != nil || sourceComparison != nil {
			return fmt.Errorf("voting artifact %s RPC observation exists without an endpoint", name)
		}
		return nil
	}
	if snapshot == nil {
		if rawErr == "" || localComparison != nil || sourceComparison != nil {
			return fmt.Errorf("voting artifact %s RPC observation is inconsistent", name)
		}
		return nil
	}
	if rawErr != "" {
		return fmt.Errorf("voting artifact %s RPC snapshot and error are mutually exclusive", name)
	}
	validated, err := validateRewardSnapshot("rpc_"+name, snapshot, false)
	if err != nil {
		return err
	}
	if !comparisonCoherent(localComparison, local, validated) || !comparisonCoherent(sourceComparison, source, validated) {
		return fmt.Errorf("voting artifact %s RPC comparisons are inconsistent", name)
	}
	return nil
}

func skipRewardJSONValue(decoder *json.Decoder, currentDepth int) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok || (delim != '{' && delim != '[') {
		return nil
	}
	depth := currentDepth + 1
	if depth > maxRewardJSONDepth {
		return fmt.Errorf("voting artifact JSON exceeds %d-level nesting limit", maxRewardJSONDepth)
	}
	for depth > currentDepth {
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			continue
		}
		switch delim {
		case '{', '[':
			depth++
			if depth > maxRewardJSONDepth {
				return fmt.Errorf("voting artifact JSON exceeds %d-level nesting limit", maxRewardJSONDepth)
			}
		case '}', ']':
			depth--
		}
	}
	return nil
}

func decodeRewardObjectStart(decoder *json.Decoder, kind string) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("voting artifact %s must be an object", kind)
	}
	return nil
}

func decodeRewardString(decoder *json.Decoder, field string, max int) (string, error) {
	token, err := decoder.Token()
	if err != nil {
		return "", errors.New("voting artifact contains an invalid string")
	}
	value, ok := token.(string)
	if !ok {
		return "", fmt.Errorf("voting artifact %s must be a string", field)
	}
	if len(value) > max {
		return "", fmt.Errorf("voting artifact %s exceeds its length limit", field)
	}
	return value, nil
}

func decodeRewardInt64(decoder *json.Decoder, field string) (int64, error) {
	token, err := decoder.Token()
	if err != nil {
		return 0, fmt.Errorf("voting artifact %s must be an integer", field)
	}
	number, ok := token.(json.Number)
	if !ok {
		return 0, fmt.Errorf("voting artifact %s must be an integer", field)
	}
	value, err := strconv.ParseInt(number.String(), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("voting artifact %s must be an integer", field)
	}
	return value, nil
}

func decodeRewardUint64(decoder *json.Decoder, field string) (uint64, error) {
	token, err := decoder.Token()
	if err != nil {
		return 0, fmt.Errorf("voting artifact %s must be a non-negative integer", field)
	}
	number, ok := token.(json.Number)
	if !ok {
		return 0, fmt.Errorf("voting artifact %s must be a non-negative integer", field)
	}
	value, err := strconv.ParseUint(number.String(), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("voting artifact %s must be a non-negative integer", field)
	}
	return value, nil
}

func decodeRewardInt(decoder *json.Decoder, field string) (int, error) {
	value, err := decodeRewardInt64(decoder, field)
	if err != nil {
		return 0, err
	}
	converted := int(value)
	if int64(converted) != value {
		return 0, fmt.Errorf("voting artifact %s must be an integer", field)
	}
	return converted, nil
}

func decodeComparisonSummary(decoder *json.Decoder, name string) (*comparisonSummary, error) {
	if err := decodeRewardObjectStart(decoder, name+" comparison"); err != nil {
		return nil, err
	}
	summary := &comparisonSummary{}
	seen := make(map[string]bool, 6)
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, errors.New("voting artifact comparison has invalid JSON")
		}
		field, ok := token.(string)
		if !ok || len(field) > maxRewardFieldNameBytes || len(seen) == maxRewardObjectFields {
			return nil, errors.New("voting artifact comparison exceeds its field limits")
		}
		if seen[field] {
			return nil, errors.New("voting artifact comparison contains a duplicate field")
		}
		seen[field] = true
		switch field {
		case "left_total_lamports":
			value, err := decodeRewardInt64(decoder, name+"."+field)
			if err != nil {
				return nil, err
			}
			summary.LeftTotalLamports = &value
		case "right_total_lamports":
			value, err := decodeRewardInt64(decoder, name+"."+field)
			if err != nil {
				return nil, err
			}
			summary.RightTotalLamports = &value
		case "total_lamports_delta":
			value, err := decodeRewardInt64(decoder, name+"."+field)
			if err != nil {
				return nil, err
			}
			summary.TotalLamportsDelta = &value
		case "mismatched_count":
			value, err := decodeRewardInt(decoder, name+"."+field)
			if err != nil {
				return nil, err
			}
			summary.MismatchedCount = &value
		case "missing_in_left":
			value, err := decodeRewardInt(decoder, name+"."+field)
			if err != nil {
				return nil, err
			}
			summary.MissingInLeft = &value
		case "missing_in_right":
			value, err := decodeRewardInt(decoder, name+"."+field)
			if err != nil {
				return nil, err
			}
			summary.MissingInRight = &value
		default:
			if err := skipRewardJSONValue(decoder, 2); err != nil {
				return nil, err
			}
		}
	}
	if _, err := decoder.Token(); err != nil {
		return nil, errors.New("voting artifact comparison has invalid JSON")
	}
	return summary, nil
}

func decodeVotingRewardEntry(decoder *json.Decoder) (string, int64, error) {
	if err := decodeRewardObjectStart(decoder, "reward entry"); err != nil {
		return "", 0, err
	}
	seen := make(map[string]bool, 4)
	var pubkey *string
	var lamports *int64
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return "", 0, err
		}
		name, ok := token.(string)
		if !ok {
			return "", 0, errors.New("voting artifact reward entry has an invalid field name")
		}
		if len(name) > maxRewardFieldNameBytes || len(seen) == maxRewardObjectFields {
			return "", 0, errors.New("voting artifact reward entry exceeds its field limits")
		}
		if seen[name] {
			return "", 0, errors.New("voting artifact reward entry contains a duplicate field")
		}
		seen[name] = true
		switch name {
		case "pubkey":
			value, err := decodeRewardString(decoder, "reward.pubkey", maxRewardPubkeyBytes)
			if err != nil {
				return "", 0, err
			}
			pubkey = &value
		case "lamports":
			value, err := decodeRewardInt64(decoder, "reward.lamports")
			if err != nil {
				return "", 0, err
			}
			lamports = &value
		default:
			if err := skipRewardJSONValue(decoder, 4); err != nil {
				return "", 0, err
			}
		}
	}
	if _, err := decoder.Token(); err != nil {
		return "", 0, err
	}
	if pubkey == nil || *pubkey == "" || len(*pubkey) > maxRewardPubkeyBytes || lamports == nil {
		return "", 0, errors.New("voting artifact snapshot reward is missing or invalid")
	}
	return *pubkey, *lamports, nil
}

func decodeVotingRewardSnapshot(decoder *json.Decoder, name string, requireTracked bool) (*votingRewardSnapshot, error) {
	if err := decodeRewardObjectStart(decoder, name+" snapshot"); err != nil {
		return nil, err
	}
	snapshot := &votingRewardSnapshot{}
	validated := &validatedRewardSnapshot{rewards: make(map[string]int64)}
	seen := make(map[string]bool, 4)
	rewardsSeen := false
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		field, ok := token.(string)
		if !ok {
			return nil, fmt.Errorf("voting artifact %s snapshot has an invalid field name", name)
		}
		if len(field) > maxRewardFieldNameBytes || len(seen) == maxRewardObjectFields {
			return nil, fmt.Errorf("voting artifact %s snapshot exceeds its field limits", name)
		}
		if seen[field] {
			return nil, fmt.Errorf("voting artifact %s snapshot contains a duplicate field", name)
		}
		seen[field] = true
		switch field {
		case "tracked_vote_accounts":
			value, err := decodeRewardInt(decoder, name+".tracked_vote_accounts")
			if err != nil {
				return nil, err
			}
			snapshot.TrackedVoteAccounts = &value
		case "reward_count":
			value, err := decodeRewardInt(decoder, name+".reward_count")
			if err != nil {
				return nil, err
			}
			snapshot.RewardCount = &value
		case "total_lamports":
			value, err := decodeRewardInt64(decoder, name+".total_lamports")
			if err != nil {
				return nil, err
			}
			snapshot.TotalLamports = &value
		case "rewards":
			token, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			if delim, ok := token.(json.Delim); !ok || delim != '[' {
				return nil, fmt.Errorf("voting artifact %s rewards must be an array", name)
			}
			for decoder.More() {
				if len(validated.rewards) == maxRewardSnapshotEntries {
					return nil, fmt.Errorf("voting artifact %s snapshot exceeds %d-reward limit", name, maxRewardSnapshotEntries)
				}
				pubkey, lamports, err := decodeVotingRewardEntry(decoder)
				if err != nil {
					return nil, err
				}
				if _, duplicate := validated.rewards[pubkey]; duplicate {
					return nil, fmt.Errorf("voting artifact %s snapshot contains duplicate rewards", name)
				}
				total, ok := checkedAddInt64(validated.total, lamports)
				if !ok {
					return nil, fmt.Errorf("voting artifact %s snapshot total overflows", name)
				}
				validated.total = total
				validated.rewards[pubkey] = lamports
			}
			if _, err := decoder.Token(); err != nil {
				return nil, err
			}
			rewardsSeen = true
		default:
			if err := skipRewardJSONValue(decoder, 2); err != nil {
				return nil, err
			}
		}
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	if snapshot.RewardCount == nil || snapshot.TotalLamports == nil || !rewardsSeen || *snapshot.RewardCount < 0 || len(validated.rewards) != *snapshot.RewardCount || validated.total != *snapshot.TotalLamports {
		return nil, fmt.Errorf("voting artifact %s snapshot counts are inconsistent", name)
	}
	if requireTracked && (snapshot.TrackedVoteAccounts == nil || *snapshot.TrackedVoteAccounts < *snapshot.RewardCount) {
		return nil, fmt.Errorf("voting artifact %s snapshot counts are inconsistent", name)
	}
	snapshot.validated = validated
	return snapshot, nil
}

func parseVotingRewardReader(ctx context.Context, reader io.Reader, slot uint64) (*votingRewardArtifact, error) {
	decoder := json.NewDecoder(&contextReader{ctx: ctx, reader: reader})
	decoder.UseNumber()
	if err := decodeRewardObjectStart(decoder, "root"); err != nil {
		return nil, fmt.Errorf("invalid voting JSON: %w", err)
	}
	artifact := &votingRewardArtifact{}
	seen := make(map[string]bool, 20)
	var slotValue, epochValue *uint64
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("invalid voting JSON: %w", err)
		}
		field, ok := token.(string)
		if !ok {
			return nil, errors.New("invalid voting JSON: root has an invalid field name")
		}
		if len(field) > maxRewardFieldNameBytes || len(seen) == maxRewardObjectFields {
			return nil, errors.New("invalid voting JSON: root exceeds its field limits")
		}
		if seen[field] {
			return nil, errors.New("invalid voting JSON: root contains a duplicate field")
		}
		seen[field] = true
		switch field {
		case "slot":
			value, err := decodeRewardUint64(decoder, "slot")
			if err != nil {
				return nil, err
			}
			slotValue = &value
		case "epoch":
			value, err := decodeRewardUint64(decoder, "epoch")
			if err != nil {
				return nil, err
			}
			epochValue = &value
		case "reward_type":
			artifact.RewardType, err = decodeRewardString(decoder, field, 32)
			if err != nil {
				return nil, err
			}
		case "generated_at":
			artifact.GeneratedAt, err = decodeRewardString(decoder, field, 128)
			if err != nil {
				return nil, err
			}
		case "rpc_endpoint":
			artifact.RPCEndpoint, err = decodeRewardString(decoder, field, maxRewardMetadataStringBytes)
			if err != nil {
				return nil, err
			}
		case "rpc_confirmed_error":
			artifact.RPCConfirmedError, err = decodeRewardString(decoder, field, maxRewardMetadataStringBytes)
			if err != nil {
				return nil, err
			}
		case "rpc_finalized_error":
			artifact.RPCFinalizedError, err = decodeRewardString(decoder, field, maxRewardMetadataStringBytes)
			if err != nil {
				return nil, err
			}
		case "local":
			artifact.Local, err = decodeVotingRewardSnapshot(decoder, "local", true)
			if err != nil {
				return nil, err
			}
		case "source_block":
			artifact.SourceBlock, err = decodeVotingRewardSnapshot(decoder, "source_block", false)
			if err != nil {
				return nil, err
			}
		case "rpc_confirmed":
			artifact.RPCConfirmed, err = decodeVotingRewardSnapshot(decoder, "rpc_confirmed", false)
			if err != nil {
				return nil, err
			}
		case "rpc_finalized":
			artifact.RPCFinalized, err = decodeVotingRewardSnapshot(decoder, "rpc_finalized", false)
			if err != nil {
				return nil, err
			}
		case "local_vs_source":
			artifact.LocalVsSource, err = decodeComparisonSummary(decoder, field)
		case "local_vs_rpc_confirmed":
			artifact.LocalVsRPCConfirmed, err = decodeComparisonSummary(decoder, field)
		case "local_vs_rpc_finalized":
			artifact.LocalVsRPCFinalized, err = decodeComparisonSummary(decoder, field)
		case "source_vs_rpc_confirmed":
			artifact.SourceVsRPCConfirmed, err = decodeComparisonSummary(decoder, field)
		case "source_vs_rpc_finalized":
			artifact.SourceVsRPCFinalized, err = decodeComparisonSummary(decoder, field)
		default:
			err = skipRewardJSONValue(decoder, 1)
		}
		if err != nil {
			return nil, fmt.Errorf("invalid voting JSON: %w", err)
		}
	}
	if _, err := decoder.Token(); err != nil {
		return nil, fmt.Errorf("invalid voting JSON: %w", err)
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("invalid voting JSON: trailing value")
		}
		return nil, fmt.Errorf("invalid voting JSON: %w", err)
	}
	if slotValue == nil || epochValue == nil || !seen["reward_type"] || !seen["generated_at"] {
		return nil, errors.New("voting artifact is missing a required field")
	}
	artifact.Slot, artifact.Epoch = *slotValue, *epochValue
	if artifact.Slot != slot {
		return nil, fmt.Errorf("voting artifact slot %d does not match requested slot %d", artifact.Slot, slot)
	}
	if artifact.RewardType != "Voting" {
		return nil, errors.New("voting artifact has unexpected reward_type")
	}
	if _, err := time.Parse(time.RFC3339Nano, artifact.GeneratedAt); err != nil {
		return nil, errors.New("voting artifact has invalid generated_at")
	}
	local, err := validateRewardSnapshot("local", artifact.Local, true)
	if err != nil {
		return nil, err
	}
	source, err := validateRewardSnapshot("source_block", artifact.SourceBlock, false)
	if err != nil {
		return nil, err
	}
	if !comparisonCoherent(artifact.LocalVsSource, local, source) {
		return nil, errors.New("voting artifact local_vs_source comparison is missing or inconsistent")
	}
	endpointConfigured := artifact.RPCEndpoint != ""
	if err := validateRPCRewardObservation("confirmed", endpointConfigured, artifact.RPCConfirmed, artifact.RPCConfirmedError, artifact.LocalVsRPCConfirmed, artifact.SourceVsRPCConfirmed, local, source); err != nil {
		return nil, err
	}
	if err := validateRPCRewardObservation("finalized", endpointConfigured, artifact.RPCFinalized, artifact.RPCFinalizedError, artifact.LocalVsRPCFinalized, artifact.SourceVsRPCFinalized, local, source); err != nil {
		return nil, err
	}
	return artifact, nil
}

func parseVotingRewardFile(ctx context.Context, root *os.Root, name string, slot uint64) (*votingRewardArtifact, error) {
	if filepath.Base(name) != name {
		return nil, errors.New("artifact name must be a basename")
	}
	file, info, err := openRootRegularFile(root, name)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if info.Size() > maxVotingRewardArtifactBytes {
		return nil, fmt.Errorf("artifact exceeds %d-byte limit", maxVotingRewardArtifactBytes)
	}
	artifact, err := parseVotingRewardReader(ctx, io.LimitReader(file, maxVotingRewardArtifactBytes+1), slot)
	if err != nil {
		return nil, err
	}
	after, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if fileMetadataChanged(info, after) || after.Size() > maxVotingRewardArtifactBytes {
		return nil, errors.New("artifact changed while reading")
	}
	return artifact, nil
}
