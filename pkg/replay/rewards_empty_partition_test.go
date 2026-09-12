package replay

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/stretchr/testify/require"
)

func encodeEpochRewardsForTest(t *testing.T, epochRewards sealevel.SysvarEpochRewards) []byte {
	t.Helper()
	writer := new(bytes.Buffer)
	epochRewards.MustMarshalWithEncoder(bin.NewBinEncoder(writer))
	return writer.Bytes()
}

func decodeEpochRewardsForTest(t *testing.T, data []byte) sealevel.SysvarEpochRewards {
	t.Helper()
	var epochRewards sealevel.SysvarEpochRewards
	require.NoError(t, epochRewards.UnmarshalWithDecoder(bin.NewBinDecoder(data)))
	return epochRewards
}

func TestSameBankRewardPartitionWaitsForStartingBlockHeight(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "accounts"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0o644))

	db, err := accountsdb.OpenDb(dir)
	require.NoError(t, err)
	db.InitCaches()
	t.Cleanup(db.CloseDb)

	previousCacheAcct := sealevel.SysvarCache.EpochRewards.Acct
	previousCacheSysvar := sealevel.SysvarCache.EpochRewards.Sysvar
	t.Cleanup(func() {
		sealevel.SysvarCache.EpochRewards.Acct = previousCacheAcct
		sealevel.SysvarCache.EpochRewards.Sysvar = previousCacheSysvar
	})

	active := sealevel.SysvarEpochRewards{
		DistributionStartingBlockHeight: 101,
		NumPartitions:                   1,
		TotalRewards:                    77,
		DistributedRewards:              77,
		Active:                          true,
	}
	activeAccount := &accounts.Account{
		Key:      sealevel.SysvarEpochRewardsAddr,
		Lamports: 1,
		Data:     encodeEpochRewardsForTest(t, active),
		Owner:    a.SysvarOwnerAddr,
	}
	// A skipped boundary slot can make epoch initialization and partition zero
	// execute in one bank. AccountsDB still contains the previous epoch sysvar;
	// distribution must consume the fresh same-bank write instead.
	stale := active
	stale.DistributionStartingBlockHeight = 1
	stale.TotalRewards = 999
	stale.DistributedRewards = 999
	stale.Active = false
	staleAccount := activeAccount.Clone()
	staleAccount.Data = encodeEpochRewardsForTest(t, stale)
	stored := make(chan struct{})
	require.NoError(t, db.StoreAccounts([]*accounts.Account{staleAccount}, 10, func() { close(stored) }))
	<-stored

	distribution := &rewards.PartitionedRewardDistributionInfo{
		SpoolDir:                     dir,
		SpoolSlot:                    10,
		NumRewardPartitionsRemaining: 1,
	}
	replayCtx := &ReplayCtx{Capitalization: 12345}
	updated, parents := distributePartitionedEpochRewardsForSlot(db, nil, []*accounts.Account{activeAccount}, replayCtx, distribution, 11, 100)

	require.Equal(t, uint64(1), distribution.NumRewardPartitionsRemaining)
	require.Equal(t, uint64(12345), replayCtx.Capitalization)
	require.Empty(t, updated)
	require.Empty(t, parents)

	updated, parents = distributePartitionedEpochRewardsForSlot(db, nil, []*accounts.Account{activeAccount}, replayCtx, distribution, 11, 101)

	require.Zero(t, distribution.NumRewardPartitionsRemaining)
	require.Equal(t, uint64(12345), replayCtx.Capitalization)
	require.Len(t, updated, 1)
	require.Len(t, parents, 1)
	require.True(t, decodeEpochRewardsForTest(t, parents[0].Data).Active)
	require.Equal(t, active.TotalRewards, decodeEpochRewardsForTest(t, parents[0].Data).TotalRewards)

	completed := decodeEpochRewardsForTest(t, updated[0].Data)
	require.False(t, completed.Active)
	require.Equal(t, uint64(1), completed.NumPartitions)
	require.Equal(t, active.DistributedRewards, completed.DistributedRewards)
	require.NotNil(t, sealevel.SysvarCache.EpochRewards.Sysvar)
	require.False(t, sealevel.SysvarCache.EpochRewards.Sysvar.Active)

	db.WaitForStoreWorker()
	persisted, err := db.GetAccount(11, sealevel.SysvarEpochRewardsAddr)
	require.NoError(t, err)
	require.False(t, decodeEpochRewardsForTest(t, persisted.Data).Active)
}
