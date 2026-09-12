package replay

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
)

const (
	rewardArtifactRecordTypeReward            = "reward"
	rewardArtifactRecordTypeCreditsUpdateOnly = "credits_update_only"
)

type epochBoundaryCalculatedRewardSummary struct {
	Slot        uint64 `json:"slot"`
	Epoch       uint64 `json:"epoch"`
	GeneratedAt string `json:"generated_at"`

	RewardsCSV    string `json:"rewards_csv"`
	NumPartitions uint64 `json:"num_partitions"`

	Voting  epochBoundaryCalculatedRewardTypeSummary `json:"voting"`
	Staking epochBoundaryCalculatedStakingSummary    `json:"staking"`
	Totals  epochBoundaryCalculatedTotalsSummary     `json:"totals"`
}

type epochBoundaryCalculatedRewardTypeSummary struct {
	TrackedAccounts int    `json:"tracked_accounts"`
	RewardCount     uint64 `json:"reward_count"`
	TotalLamports   uint64 `json:"total_lamports"`
}

type epochBoundaryCalculatedStakingSummary struct {
	RecordCount         uint64 `json:"record_count"`
	RewardCount         uint64 `json:"reward_count"`
	CreditsOnlyCount    uint64 `json:"credits_only_count"`
	TotalLamports       uint64 `json:"total_lamports"`
	ExpectedRecordCount uint64 `json:"expected_record_count"`
}

type epochBoundaryCalculatedTotalsSummary struct {
	RecordCount      uint64 `json:"record_count"`
	RewardCount      uint64 `json:"reward_count"`
	CreditsOnlyCount uint64 `json:"credits_only_count"`
	TotalLamports    uint64 `json:"total_lamports"`
}

func maybeDumpEpochCalculatedRewards(dbgOpts *DebugOptions, epoch uint64, slot uint64, streamResult *rewards.StreamingRewardsResult) {
	if dbgOpts == nil || !dbgOpts.DumpEpochRewardArtifacts() {
		return
	}

	logDir := mlog.GetLogDir()
	if logDir == "" {
		return
	}

	artifact, csvPath, err := writeEpochBoundaryCalculatedRewards(logDir, epoch, slot, streamResult)
	if err != nil {
		mlog.Log.Warnf("artifact: failed to write epoch-boundary calculated rewards for slot %d: %v", slot, err)
		return
	}

	mlog.Log.FileOnlyf("artifact written: %s", csvPath)
	writeReplayArtifact("rewards", fmt.Sprintf("epoch_boundary_calculated_rewards_slot_%d.json", slot), artifact)
}

func writeEpochBoundaryCalculatedRewards(logDir string, epoch uint64, slot uint64, streamResult *rewards.StreamingRewardsResult) (*epochBoundaryCalculatedRewardSummary, string, error) {
	dir := filepath.Join(logDir, "rewards")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, "", fmt.Errorf("creating rewards artifact directory: %w", err)
	}

	filename := fmt.Sprintf("epoch_boundary_calculated_rewards_slot_%d.csv", slot)
	relativePath := filepath.Join("rewards", filename)
	artifactPath := filepath.Join(dir, filename)

	file, err := openPrivateArtifact(artifactPath)
	if err != nil {
		return nil, "", fmt.Errorf("creating reward artifact csv: %w", err)
	}
	defer discardPrivateArtifact(file)

	bufw := bufio.NewWriterSize(file, 1<<20)
	csvw := csv.NewWriter(bufw)

	closeWithErr := func() error {
		csvw.Flush()
		if err := csvw.Error(); err != nil {
			file.Close()
			return err
		}
		if err := bufw.Flush(); err != nil {
			file.Close()
			return err
		}
		return publishPrivateArtifact(file, artifactPath)
	}

	if err := csvw.Write([]string{
		"record_type",
		"reward_type",
		"recipient_pubkey",
		"vote_pubkey",
		"lamports",
		"stake_lamports",
		"credits_observed",
		"partition_index",
	}); err != nil {
		file.Close()
		return nil, "", fmt.Errorf("writing reward artifact header: %w", err)
	}

	localVotingSnapshot, _ := collectLocalVotingRewards(streamResult.ValidatorRewards)
	if localVotingSnapshot.TotalLamports < 0 {
		file.Close()
		return nil, "", fmt.Errorf("unexpected negative local voting lamports: %d", localVotingSnapshot.TotalLamports)
	}

	var votingLamports uint64
	for _, reward := range localVotingSnapshot.Rewards {
		if err := csvw.Write([]string{
			rewardArtifactRecordTypeReward,
			rewards.RewardTypeVoting,
			reward.Pubkey,
			"",
			strconv.FormatInt(reward.Lamports, 10),
			"",
			"",
			"",
		}); err != nil {
			file.Close()
			return nil, "", fmt.Errorf("writing voting reward artifact row: %w", err)
		}
		votingLamports += uint64(reward.Lamports)
	}

	var stakingRecordCount uint64
	var stakingRewardCount uint64
	var stakingCreditsOnlyCount uint64
	var stakingLamports uint64

	for partition := uint64(0); partition < streamResult.NumPartitions; partition++ {
		reader, err := rewards.NewPartitionReader(streamResult.SpoolDir, streamResult.SpoolSlot, uint32(partition))
		if err != nil {
			file.Close()
			return nil, "", fmt.Errorf("opening partition %d reward spool: %w", partition, err)
		}
		if reader == nil {
			continue
		}

		for {
			rec, err := reader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				reader.Close()
				file.Close()
				return nil, "", fmt.Errorf("reading partition %d reward spool: %w", partition, err)
			}

			recordType := rewardArtifactRecordTypeReward
			if rec.RewardLamports == 0 {
				recordType = rewardArtifactRecordTypeCreditsUpdateOnly
				stakingCreditsOnlyCount++
			} else {
				stakingRewardCount++
				stakingLamports += rec.RewardLamports
			}
			stakingRecordCount++

			if err := csvw.Write([]string{
				recordType,
				rewards.RewardTypeStaking,
				rec.StakePubkey.String(),
				rec.VotePubkey.String(),
				strconv.FormatUint(rec.RewardLamports, 10),
				strconv.FormatUint(rec.StakeLamports, 10),
				strconv.FormatUint(rec.CreditsObserved, 10),
				strconv.FormatUint(partition, 10),
			}); err != nil {
				reader.Close()
				file.Close()
				return nil, "", fmt.Errorf("writing staking reward artifact row: %w", err)
			}
		}

		if err := reader.Close(); err != nil {
			file.Close()
			return nil, "", fmt.Errorf("closing partition %d reward spool: %w", partition, err)
		}
	}

	if stakingRecordCount != streamResult.NumStakeRewards {
		file.Close()
		return nil, "", fmt.Errorf("staking reward artifact row count mismatch: expected %d records, found %d", streamResult.NumStakeRewards, stakingRecordCount)
	}

	if err := closeWithErr(); err != nil {
		return nil, "", fmt.Errorf("closing reward artifact csv: %w", err)
	}

	artifact := &epochBoundaryCalculatedRewardSummary{
		Slot:          slot,
		Epoch:         epoch,
		GeneratedAt:   time.Now().UTC().Format(time.RFC3339Nano),
		RewardsCSV:    relativePath,
		NumPartitions: streamResult.NumPartitions,
		Voting: epochBoundaryCalculatedRewardTypeSummary{
			TrackedAccounts: localVotingSnapshot.TrackedVoteAccounts,
			RewardCount:     uint64(localVotingSnapshot.RewardCount),
			TotalLamports:   votingLamports,
		},
		Staking: epochBoundaryCalculatedStakingSummary{
			RecordCount:         stakingRecordCount,
			RewardCount:         stakingRewardCount,
			CreditsOnlyCount:    stakingCreditsOnlyCount,
			TotalLamports:       stakingLamports,
			ExpectedRecordCount: streamResult.NumStakeRewards,
		},
		Totals: epochBoundaryCalculatedTotalsSummary{
			RecordCount:      uint64(localVotingSnapshot.RewardCount) + stakingRecordCount,
			RewardCount:      uint64(localVotingSnapshot.RewardCount) + stakingRewardCount,
			CreditsOnlyCount: stakingCreditsOnlyCount,
			TotalLamports:    votingLamports + stakingLamports,
		},
	}

	return artifact, artifactPath, nil
}
