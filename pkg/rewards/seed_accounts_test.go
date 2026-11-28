package rewards

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/stretchr/testify/require"
)

// seedDurableAccounts writes fixture accounts to disk the way production does:
// as one fold of a single slot's delta.
func seedDurableAccounts(t testing.TB, db *accountsdb.AccountsDb, accts []*accounts.Account, slot uint64) {
	t.Helper()
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: accts}}, slot, nil, nil)
	require.NoError(t, err)
}
