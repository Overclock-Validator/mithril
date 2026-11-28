package accountsdb

import "github.com/Overclock-Validator/mithril/pkg/accounts"

// AccountStateObserver maintains disposable derived state, not durable state.
// Calls are serialized after a successful write, before its version becomes
// readable. Reset is true on rewind, recovery, or any failed/partial write.
// Accounts are borrowed for this call only. Observers must not retain mutable
// account data, call back into AccountsDb, or perform I/O.
type AccountStateObserver interface {
	AccountsChanged(version uint64, changed []*accounts.Account, reset bool)
}

// ObserveAccountState installs an observer for subsequent write completions. It is intended for
// node-lifetime derived indexes; the returned function unregisters it.
func (db *AccountsDb) ObserveAccountState(observer AccountStateObserver) func() {
	db.accountObserversMu.Lock()
	if db.accountObservers == nil {
		db.accountObservers = make(map[AccountStateObserver]struct{})
	}
	db.accountObservers[observer] = struct{}{}
	db.accountObserversMu.Unlock()
	return func() {
		db.accountObserversMu.Lock()
		delete(db.accountObservers, observer)
		db.accountObserversMu.Unlock()
	}
}

// CommittedAccountVersion is a process-local coherence fence, not a slot or an
// on-disk format version. Readers must check it before AND after reading derived
// state and accounts, and independently match the RPC's published bank. An odd
// version cannot serve a committed view.
func (db *AccountsDb) CommittedAccountVersion() (uint64, bool) {
	version := db.accountStateVersion.Load()
	if version&1 != 0 || db.accountStateUncertain.Load() {
		return version, false
	}
	return version, db.accountStateVersion.Load() == version
}

type accountFoldRange struct{ version, firstSlot uint64 }

// CanServePinnedAccountVersion checks immutable data already read at version.
// Only the next forward fold may overlap that published view; rewinds, failed
// writes and any later account changes require a fresh read.
func (db *AccountsDb) CanServePinnedAccountVersion(version, rootedSlot uint64) bool {
	current := db.accountStateVersion.Load()
	if version&1 != 0 || db.accountStateUncertain.Load() {
		return false
	}
	if current != version {
		fold := db.accountForwardFold.Load()
		if fold == nil || fold.version != version+1 || fold.firstSlot <= rootedSlot || current-version > 2 {
			return false
		}
	}
	return db.accountStateVersion.Load() == current
}

// CommittedAccountSlot identifies the last completed write. Zero is the
// snapshot baseline, whose slot is supplied by the RPC's initial bank state.
// Use only inside a stable CommittedAccountVersion fence.
func (db *AccountsDb) CommittedAccountSlot() uint64 { return db.accountCommittedSlot.Load() }

// Existing fold locks are acquired first. No RPC read holds this mutex while
// scanning or loading accounts.
func (db *AccountsDb) beginAccountChange() func([]*accounts.Account, uint64, bool, bool) {
	db.accountStateMu.Lock()
	db.accountStateVersion.Add(1)
	return func(changed []*accounts.Account, slot uint64, success, reset bool) {
		version := db.accountStateVersion.Load() + 1
		if reset || !success {
			db.voteIndexInvalidations.Add(1)
		}
		db.accountObserversMu.Lock()
		for observer := range db.accountObservers {
			observer.AccountsChanged(version, changed, reset || !success)
		}
		db.accountObserversMu.Unlock()
		if success {
			db.accountCommittedSlot.Store(slot)
		}
		db.accountStateUncertain.Store(!success)
		db.accountStateVersion.Store(version)
		db.accountStateMu.Unlock()
	}
}
