package rewards

import (
	"os"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func testReusableRewardPlan(t *testing.T) *PartitionedRewardPlan {
	t.Helper()
	spool := NewPartitionedSpoolWriters(t.TempDir(), 100, 2)
	require.NoError(t, spool.WriteRecord(&SpoolRecord{StakePubkey: solana.PublicKey{1}, RewardLamports: 7}))
	require.NoError(t, spool.Close())
	p, err := NewPartitionedRewardPlan(spool.SpoolDir(), spool.Slot(), sealevel.SysvarEpochRewards{Active: true, NumPartitions: 2, TotalRewards: 7}, nil)
	require.NoError(t, err)
	return p
}

func TestRewardPlanDistinguishesMissingAndEmptyPartitions(t *testing.T) {
	p := testReusableRewardPlan(t)
	loader := func(solana.PublicKey) (*accounts.Account, error) { return nil, accountsdb.ErrNoAccount }
	_, _, paid, burned, _, err := p.Distribute(nil, 1, 101, loader, StakeRewardDistributionConfig{})
	require.NoError(t, err, "a partition recorded empty is legitimate")
	require.Zero(t, paid)
	require.Zero(t, burned)
	require.NoError(t, os.Remove(partitionFilePath(p.spoolDir, p.spoolSlot, 0)))
	_, _, _, _, _, err = p.Distribute(nil, 0, 101, loader, StakeRewardDistributionConfig{})
	require.Error(t, err, "a lost nonempty partition must not silently distribute zero rewards")
}

func TestRewardPlanRetirementWaitsForSpeculativeReaders(t *testing.T) {
	p := testReusableRewardPlan(t)
	reading, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		_, _, _, _, _, err := p.Distribute(nil, 0, 101, func(solana.PublicKey) (*accounts.Account, error) {
			close(reading)
			<-release
			return nil, accountsdb.ErrNoAccount
		}, StakeRewardDistributionConfig{})
		done <- err
	}()
	<-reading
	retired := make(chan struct{})
	go func() { p.Retire(); close(retired) }()
	select {
	case <-retired:
		t.Fatal("retired a plan with an active producer reader")
	case <-time.After(10 * time.Millisecond):
	}
	_, err := os.Stat(partitionFilePath(p.spoolDir, p.spoolSlot, 0))
	require.NoError(t, err)
	close(release)
	require.NoError(t, <-done)
	<-retired
	_, err = os.Stat(p.spoolDir)
	require.True(t, os.IsNotExist(err))
	p.Discard() // cleanup is idempotent across durable/abandoned ownership paths
}
