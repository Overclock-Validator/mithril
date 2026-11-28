package transactionhistory

import (
	"fmt"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/stretchr/testify/require"
)

func TestStoreWALIndependentOfAccountsIndex(t *testing.T) {
	// The process-wide AccountsDB option must be restored only after the
	// synchronous subtest and its real history writer/snapshot have joined.
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("account-index-wal-disabled=%t", disabled), func(t *testing.T) {
			previous := accountsdb.DisableIndexWAL
			accountsdb.DisableIndexWAL = disabled
			defer func() { accountsdb.DisableIndexWAL = previous }()
			require.Equal(t, disabled, accountsdb.NewAccountsIndexPebbleOptions(nil).DisableWAL)

			// Reuse the actual blocked WAL Sync/SyncData path, not an options
			// assertion alone: Store publication must still wait for its ack.
			TestStoreSnapshotWaitsForSyncAcknowledgement(t)
		})
	}
}
