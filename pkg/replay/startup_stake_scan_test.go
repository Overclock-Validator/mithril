package replay

import (
	"github.com/gagliardetto/solana-go"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartupStakeScanHandoff(t *testing.T) {
	// Neither directory has a stake index, so every scan reports an error;
	// the test checks which scan's result is used, not the totals.
	dbA := &accountsdb.AccountsDb{AcctsDir: filepath.Join(t.TempDir(), "accounts")}
	dbB := &accountsdb.AccountsDb{AcctsDir: filepath.Join(t.TempDir(), "accounts")}

	startStartupStakeScan(dbA, nil)
	_, err := takeStartupStakeScan(dbA)
	require.Error(t, err, "the background result, including its error, is handed over")
	assert.Nil(t, pendingStartupStakeScan.Load(), "a result is taken once")

	startStartupStakeScan(dbA, nil)
	_, err = takeStartupStakeScan(dbB)
	require.Error(t, err, "a scan of another AccountsDB is never used; this one scans dbB itself")
	assert.Nil(t, pendingStartupStakeScan.Load())

	startStartupStakeScan(dbA, nil)
	finishStartupStakeScan()
	assert.Nil(t, pendingStartupStakeScan.Load(), "an unconsumed scan is waited out and cleared")
	finishStartupStakeScan() // no-op when none is pending
}

// A later reconfiguration (no startup scan pending, e.g. after a fork switch
// unwound in RAM) takes the tracked durable totals instead of scanning.
func TestTakeStartupStakeScanUsesTrackedTotals(t *testing.T) {
	saved := trackedStakeTotals
	defer func() { trackedStakeTotals = saved }()
	pendingStartupStakeScan.Store(nil)

	vote := solana.PublicKey{7}
	tail := &unrootedTail{stakeTotals: newStakeTotalsTracker(map[solana.PublicKey]voteDelegation{vote: {stake: 40, accounts: 2}}, &stakeIndexMembers{})}
	trackedStakeTotals = tail.durableStakeTotals

	got, err := takeStartupStakeScan(nil) // a nil database would panic if it scanned
	if err != nil || got[vote] != (voteDelegation{stake: 40, accounts: 2}) || len(got) != 1 {
		t.Fatalf("tracked totals: %v %v", got, err)
	}
	got[vote] = voteDelegation{stake: 1, accounts: 1}
	if tail.stakeTotals.durable[vote].stake != 40 {
		t.Fatal("the caller must get a copy of the tracked totals")
	}

	tail.stakeTotals.broken = true
	if _, ok := tail.durableStakeTotals(); ok {
		t.Fatal("a stopped tracker has no totals to offer")
	}
	var none *unrootedTail
	if _, ok := none.durableStakeTotals(); ok {
		t.Fatal("no tail, no totals")
	}
}
