package replay

import (
	"encoding/csv"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/rpcclient"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestWriteEpochBoundaryCalculatedRewards(t *testing.T) {
	logDir := t.TempDir()
	spoolDir := t.TempDir()

	slot := uint64(321)
	epoch := uint64(9)

	writers := rewards.NewPartitionedSpoolWriters(spoolDir, slot, 2)
	require.NoError(t, writers.WriteRecord(&rewards.SpoolRecord{
		StakePubkey:     solana.MustPublicKeyFromBase58("CktRuQ4VpYxVgLJYPGn9tD6xLdjvxRKqGZo4PMBVXsfS"),
		VotePubkey:      solana.MustPublicKeyFromBase58("6QWeT6FpJrm8AF1btu6WH2k2Xhq6t5vbheKVfQavmeoZ"),
		StakeLamports:   100,
		CreditsObserved: 42,
		RewardLamports:  7,
		PartitionIndex:  0,
	}))
	require.NoError(t, writers.WriteRecord(&rewards.SpoolRecord{
		StakePubkey:     solana.MustPublicKeyFromBase58("cGfHiC6Kgg3FpFZvgwGcswsCRtp4aBP2fzuXRQPizuN"),
		VotePubkey:      solana.MustPublicKeyFromBase58("6QWeT6FpJrm8AF1btu6WH2k2Xhq6t5vbheKVfQavmeoZ"),
		StakeLamports:   200,
		CreditsObserved: 77,
		RewardLamports:  0,
		PartitionIndex:  1,
	}))
	require.NoError(t, writers.Close())

	nonZeroVotingReward := &atomic.Uint64{}
	nonZeroVotingReward.Store(3)
	zeroVotingReward := &atomic.Uint64{}

	streamResult := &rewards.StreamingRewardsResult{
		SpoolDir:  spoolDir,
		SpoolSlot: slot,
		ValidatorRewards: map[solana.PublicKey]*atomic.Uint64{
			solana.MustPublicKeyFromBase58("6QWeT6FpJrm8AF1btu6WH2k2Xhq6t5vbheKVfQavmeoZ"): nonZeroVotingReward,
			solana.MustPublicKeyFromBase58("8opHzTAnfzRpPEx21XtnrVTX28YQuCpAjcn1PczScKh"):  zeroVotingReward,
		},
		NumStakeRewards: 2,
		NumPartitions:   2,
	}

	artifact, csvPath, err := writeEpochBoundaryCalculatedRewards(logDir, epoch, slot, streamResult)
	require.NoError(t, err)
	require.Equal(t, filepath.Join(logDir, "rewards", "epoch_boundary_calculated_rewards_slot_321.csv"), csvPath)
	require.Equal(t, "rewards/epoch_boundary_calculated_rewards_slot_321.csv", artifact.RewardsCSV)
	require.Equal(t, uint64(2), artifact.NumPartitions)
	require.Equal(t, 2, artifact.Voting.TrackedAccounts)
	require.Equal(t, uint64(1), artifact.Voting.RewardCount)
	require.Equal(t, uint64(3), artifact.Voting.TotalLamports)
	require.Equal(t, uint64(2), artifact.Staking.RecordCount)
	require.Equal(t, uint64(1), artifact.Staking.RewardCount)
	require.Equal(t, uint64(1), artifact.Staking.CreditsOnlyCount)
	require.Equal(t, uint64(7), artifact.Staking.TotalLamports)
	require.Equal(t, uint64(3), artifact.Totals.RecordCount)
	require.Equal(t, uint64(2), artifact.Totals.RewardCount)
	require.Equal(t, uint64(1), artifact.Totals.CreditsOnlyCount)
	require.Equal(t, uint64(10), artifact.Totals.TotalLamports)

	info, err := os.Stat(csvPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())

	file, err := os.Open(csvPath)
	require.NoError(t, err)
	defer file.Close()

	rows, err := csv.NewReader(file).ReadAll()
	require.NoError(t, err)
	require.Equal(t, [][]string{
		{"record_type", "reward_type", "recipient_pubkey", "vote_pubkey", "lamports", "stake_lamports", "credits_observed", "partition_index"},
		{"reward", "Voting", "6QWeT6FpJrm8AF1btu6WH2k2Xhq6t5vbheKVfQavmeoZ", "", "3", "", "", ""},
		{"reward", "Staking", "CktRuQ4VpYxVgLJYPGn9tD6xLdjvxRKqGZo4PMBVXsfS", "6QWeT6FpJrm8AF1btu6WH2k2Xhq6t5vbheKVfQavmeoZ", "7", "100", "42", "0"},
		{"credits_update_only", "Staking", "cGfHiC6Kgg3FpFZvgwGcswsCRtp4aBP2fzuXRQPizuN", "6QWeT6FpJrm8AF1btu6WH2k2Xhq6t5vbheKVfQavmeoZ", "0", "200", "77", "1"},
	}, rows)
}

func TestPrivateArtifactPublicationIsAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "artifact.json")
	require.NoError(t, os.WriteFile(path, []byte("old"), 0o644))

	file, err := openPrivateArtifact(path)
	require.NoError(t, err)
	t.Cleanup(func() { discardPrivateArtifact(file) })
	_, err = file.Write([]byte("new"))
	require.NoError(t, err)

	visible, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "old", string(visible), "partial temp content became visible before publication")

	require.NoError(t, publishPrivateArtifact(file, path))
	visible, err = os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "new", string(visible))
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestEpochVotingRewardArtifactRedactsRPCSecretsAndTightensMode(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, "not-json")
	}))
	t.Cleanup(server.Close)

	endpoint, err := url.Parse(server.URL)
	require.NoError(t, err)
	endpoint.User = url.UserPassword("rpc-user", "RPC_PASSWORD")
	endpoint.Path = "/private/RPC_PATH_SECRET"
	endpoint.RawQuery = "api-key=RPC_QUERY_SECRET"

	logBase := t.TempDir()
	require.NoError(t, mlog.Initialize(mlog.LogConfig{Dir: logBase, ToStdout: false}, "reward-redaction-test"))
	t.Cleanup(mlog.Shutdown)

	const slot = uint64(777)
	rewardsDir := filepath.Join(mlog.GetLogDir(), "rewards")
	require.NoError(t, os.MkdirAll(rewardsDir, 0755))
	artifactPath := filepath.Join(rewardsDir, "epoch_boundary_voting_rewards_slot_777.json")
	require.NoError(t, os.WriteFile(artifactPath, []byte("old content"), 0644))
	require.NoError(t, os.Chmod(artifactPath, 0644))

	dbgOpts, err := NewDebugOptions(nil, nil, true)
	require.NoError(t, err)
	maybeDumpEpochVotingRewardDiff(
		dbgOpts,
		rpcclient.NewRpcClient(endpoint.String()),
		&b.Block{},
		9,
		slot,
		map[solana.PublicKey]*atomic.Uint64{},
	)

	contents, err := os.ReadFile(artifactPath)
	require.NoError(t, err)
	text := string(contents)
	for _, secret := range []string{"rpc-user", "RPC_PASSWORD", "private", "RPC_PATH_SECRET", "RPC_QUERY_SECRET"} {
		require.NotContains(t, text, secret)
	}
	require.Contains(t, text, endpoint.Scheme+"://"+endpoint.Host)
	require.Contains(t, text, `"rpc_confirmed_error"`)

	info, err := os.Stat(artifactPath)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
}
