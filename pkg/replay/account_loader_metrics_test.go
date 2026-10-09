package replay

import (
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/stretchr/testify/require"
)

func TestAccountLoaderRetainsGroupsAcrossReset(t *testing.T) {
	previous := metrics.GlobalBlockReplay.AccountLoader
	t.Cleanup(func() { metrics.GlobalBlockReplay.AccountLoader = previous })
	exec := &blockExecution{}
	metrics.GlobalBlockReplay.AccountLoader = metrics.AccountLoader{RequestedKeys: 99}
	func() {
		defer exec.captureAccountLoader()()
		recordAccountLoaderBatchStats(&metrics.GlobalBlockReplay.AccountLoader, accountsdb.BatchReadStats{RequestedKeys: 3, IndexHits: 2, AppendVecAccounts: 2, AppendVecReadNanoseconds: 1000})
		recordAccountLoaderBatchStats(&metrics.GlobalBlockReplay.AccountLoader, accountsdb.BatchReadStats{RequestedKeys: 4, CacheHits: 4})
	}()
	require.EqualValues(t, 99, metrics.GlobalBlockReplay.AccountLoader.RequestedKeys, "another slot's record is unchanged")
	metrics.GlobalBlockReplay.AccountLoader = metrics.AccountLoader{}
	func() {
		defer exec.captureAccountLoader()()
		recordAccountLoaderBatchStats(&metrics.GlobalBlockReplay.AccountLoader, accountsdb.BatchReadStats{RequestedKeys: 5, CacheHits: 5})
	}()
	require.Zero(t, metrics.GlobalBlockReplay.AccountLoader.RequestedKeys, "speculative work is not published before acceptance")
	require.EqualValues(t, 12, exec.accountLoader.RequestedKeys)
	require.EqualValues(t, 9, exec.accountLoader.CacheHits)
	require.EqualValues(t, 2, exec.accountLoader.AppendVecAccounts)
	require.EqualValues(t, time.Microsecond, exec.accountLoader.AppendVecRead.SumNanoseconds)
	require.EqualValues(t, 3, exec.accountLoader.AppendVecRead.Count)
}
