package accountsdb

import (
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/stretchr/testify/require"
)

type accountObserverFunc func(uint64, []*accounts.Account, bool)

func (f *accountObserverFunc) AccountsChanged(version uint64, changed []*accounts.Account, reset bool) {
	(*f)(version, changed, reset)
}

func TestAccountObserverCommitFailureRecoveryAndUnregister(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	notifications := 0
	resetCount := 0
	var lastVersion uint64
	observer := accountObserverFunc(func(version uint64, changed []*accounts.Account, reset bool) {
		_, stable := db.CommittedAccountVersion()
		require.False(t, stable, "derived state must be updated before the version is readable")
		require.Greater(t, version, lastVersion)
		lastVersion = version
		notifications++
		if reset {
			resetCount++
		} else {
			require.Len(t, changed, 1)
		}
	})
	unregister := db.ObserveAccountState(&observer)
	db.foldHooks.afterPublicationStart = func() { _, stable := db.CommittedAccountVersion(); require.False(t, stable) }
	_, err := db.CommitBatch(foldDeltas(accounts.SlotDelta{Slot: 10, Delta: []*accounts.Account{foldAcct(1, 100, nil)}}), 10, nil, nil)
	require.NoError(t, err)
	version, stable := db.CommittedAccountVersion()
	require.True(t, stable)
	require.Equal(t, lastVersion, version)
	require.Equal(t, 1, notifications)
	require.Zero(t, resetCount)
	cached, err := db.GetAccount(10, foldAcct(1, 0, nil).Key)
	require.NoError(t, err)
	db.CommonAcctsCache.Set(cached.Key, cached)
	// Rejecting invalid arguments does not poison a previously valid view.
	_, err = db.CommitBatch(foldDeltas(accounts.SlotDelta{Slot: 12}, accounts.SlotDelta{Slot: 11}), 12, nil, nil)
	require.Error(t, err)
	got, stable := db.CommittedAccountVersion()
	require.True(t, stable)
	require.Equal(t, version, got)

	db.foldHooks.afterPublicationStart = func() { panic("crash after publication began") }
	require.Panics(t, func() {
		_, _ = db.CommitBatch(foldDeltas(accounts.SlotDelta{Slot: 11, Delta: []*accounts.Account{foldAcct(1, 200, nil)}}), 11, nil, nil)
	})
	_, stable = db.CommittedAccountVersion()
	require.False(t, stable, "partial writes fail closed until repaired")
	require.Equal(t, 1, resetCount)
	db.foldHooks.afterPublicationStart = nil
	_, err = db.RecoverFoldState()
	require.NoError(t, err)
	_, stable = db.CommittedAccountVersion()
	require.True(t, stable)
	require.Equal(t, 2, resetCount)
	account, err := db.GetAccount(11, foldAcct(1, 0, nil).Key)
	require.NoError(t, err)
	require.Equal(t, uint64(200), account.Lamports)
	unregister()
	before := notifications
	_, err = db.CommitBatch(foldDeltas(accounts.SlotDelta{Slot: 12}), 12, nil, nil)
	require.NoError(t, err)
	require.Equal(t, before, notifications)
}
