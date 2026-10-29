package replay

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The stake index file is appended before the fold that makes its slots
// durable and is never cut back. A key it gained after the saved totals'
// boundary (a fold that never committed, or one a durable rewind discarded)
// is read by a scan but was never counted by the totals, so startup must
// scan instead of using them. With the index unchanged it uses them.
// Reproduces Codex's review case: saved 10/1 against a scan of 65/2.
func TestSavedStakeTotalsBoundToDurableStakeIndex(t *testing.T) {
	for _, tc := range []struct {
		name               string
		suffix, commitThen bool
	}{
		{name: "clean restart"},
		{name: "index flushed, fold not committed", suffix: true},
		{name: "fold committed, then durable rewind", suffix: true, commitThen: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resetStakeIndexState(t)
			dir := t.TempDir()
			require.NoError(t, os.MkdirAll(filepath.Join(dir, "accounts"), 0755))
			require.NoError(t, os.WriteFile(filepath.Join(dir, "largest_file_id"), make([]byte, 8), 0644))
			db, err := accountsdb.OpenDb(dir)
			require.NoError(t, err)
			defer func() { db.CloseDb() }()
			db.InitCaches()
			db.RootedDurable = true

			stakeOwner := [32]byte(addresses.StakeProgramAddr)
			voter, aKey, bKey := solana.PublicKey{7}, solana.PublicKey{1}, solana.PublicKey{2}
			a := stakeAcct(t, aKey, 100, stakeOwner, sealevel.StakeStateV2StatusStake, voter, 10)
			// b already holds Stake data but is not in the index yet.
			b := stakeAcct(t, bKey, 100, [32]byte{9}, sealevel.StakeStateV2StatusStake, voter, 55)
			global.EnqueuePendingStakePubkey(100, aKey)
			_, err = global.FlushPendingStakePubkeysThrough(dir, 100)
			require.NoError(t, err)
			saved := map[solana.PublicKey]voteDelegation{voter: {stake: 10, accounts: 1}}
			if !tc.suffix {
				// Distinguishable from a scan, to show which one startup took.
				saved[solana.PublicKey{8}] = voteDelegation{stake: 1, accounts: 1}
			}
			members := digestStakeIndex([]accountsdb.StakeIndexEntry{{Pubkey: aKey}})
			raw, err := json.Marshal(&state.ResumeContext{Slot: 100, DelegatedStake: encodeDelegatedStake(saved, members)})
			require.NoError(t, err)
			_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: 100, Delta: []*accounts.Account{a, b}}}, 100, map[uint64][32]byte{100: {1}}, raw)
			require.NoError(t, err)

			if tc.suffix {
				global.EnqueuePendingStakePubkey(101, bKey)
				_, err = global.FlushPendingStakePubkeysThrough(dir, 101)
				require.NoError(t, err)
			}
			if tc.commitThen {
				changed := stakeAcct(t, bKey, 100, stakeOwner, sealevel.StakeStateV2StatusStake, voter, 60)
				_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: 101, Delta: []*accounts.Account{changed}}}, 101, map[uint64][32]byte{101: {2}}, []byte(`{"slot":101}`))
				require.NoError(t, err)
				_, err = db.RewindToBatchBoundary(100)
				require.NoError(t, err)
			}

			db.CloseDb()
			db, err = accountsdb.OpenDb(dir)
			require.NoError(t, err)
			db.InitCaches()
			db.RootedDurable = true
			recovered, err := db.RecoverFoldState()
			require.NoError(t, err)
			var ctx state.ResumeContext
			require.NoError(t, json.Unmarshal(recovered.ResumeCtx, &ctx))
			restored, index, err := decodeDelegatedStake(ctx.DelegatedStake)
			require.NoError(t, err)
			require.Equal(t, saved, restored)

			startStartupStakeScan(db, restored, index)
			got, err := takeStartupStakeScan(db)
			require.NoError(t, err)
			if !tc.suffix {
				assert.Equal(t, saved, got, "an unchanged index keeps the saved totals")
				return
			}
			scanned, err := scanDelegatedStake(db)
			require.NoError(t, err)
			assert.Equal(t, voteDelegation{stake: 65, accounts: 2}, scanned[voter])
			assert.Equal(t, scanned, got, "a grown index must not take the saved totals")
		})
	}
}

// A stored record that cannot be read stops the tracker; only a missing
// account counts as absent.
func TestStakeTotalsTrackerStopsOnStoredReadError(t *testing.T) {
	key, vote := solana.PublicKey{1}, solana.PublicKey{7}
	members := newStakeIndexMembers([]accountsdb.StakeIndexEntry{{Pubkey: key}})
	tracker := newStakeTotalsTracker(map[solana.PublicKey]voteDelegation{vote: {stake: 10, accounts: 1}}, members)
	// A zero-lamport parent image may be the loader's placeholder, so the
	// stored record is read.
	parents := accounts.NewMemAccountsWithLen(1)
	require.NoError(t, parents.SetAccountWithoutLock(key, &accounts.Account{Key: key}))
	slotCtx := &sealevel.SlotCtx{Slot: 5, ParentAccts: parents}
	write := stakeAcct(t, key, 100, [32]byte(addresses.StakeProgramAddr), sealevel.StakeStateV2StatusStake, vote, 20)
	tracker.recordSlot(5, slotCtx, []*accounts.Account{write}, nil, func(solana.PublicKey) (*accounts.Account, error) {
		return nil, errors.New("read failed")
	})
	_, ok := tracker.totalsThrough(5)
	assert.False(t, ok, "an unreadable stored record must not count as absent")
}

// Each fold's binding covers the keys added by the slots it commits, and
// only those.
func TestStakeIndexDigestFollowsFolds(t *testing.T) {
	a, b, c := solana.PublicKey{1}, solana.PublicKey{2}, solana.PublicKey{3}
	members := newStakeIndexMembers([]accountsdb.StakeIndexEntry{{Pubkey: a}})
	members.add(5, b)
	members.add(6, c)
	members.add(6, a) // already a member
	at := func(keys ...solana.PublicKey) stakeIndexDigest {
		entries := make([]accountsdb.StakeIndexEntry, len(keys))
		for i, k := range keys {
			entries[i] = accountsdb.StakeIndexEntry{Pubkey: k}
		}
		return digestStakeIndex(entries)
	}
	assert.Equal(t, at(a), members.digestThrough(4))
	assert.Equal(t, at(b, a), members.digestThrough(5), "order does not matter")
	assert.Equal(t, at(a, b, c), members.digestThrough(6))
	members.commitThrough(5)
	members.dropFrom(6)
	assert.Equal(t, at(a, b), members.digestThrough(9))
}

// On the normal path the binding a fold writes is the key set of the index
// file that fold flushed, so the next start keeps the saved totals.
func TestFoldStakeBindingMatchesFlushedIndex(t *testing.T) {
	resetStakeIndexState(t)
	dir := t.TempDir()
	stakeOwner := [32]byte(addresses.StakeProgramAddr)
	vote, a, b := solana.PublicKey{7}, solana.PublicKey{1}, solana.PublicKey{2}
	global.EnqueuePendingStakePubkey(1, a)
	_, err := global.FlushPendingStakePubkeysThrough(dir, 1)
	require.NoError(t, err)
	entries, err := global.LoadStakePubkeyIndex(dir)
	require.NoError(t, err)
	tracker := newStakeTotalsTracker(map[solana.PublicKey]voteDelegation{}, newStakeIndexMembers(entries))

	overlay := accounts.NewWorkingSet()
	contexts := map[uint64]*state.ResumeContext{}
	for _, sk := range []struct {
		slot uint64
		key  solana.PublicKey
	}{{5, b}, {6, a}, {7, solana.PublicKey{3}}} {
		slot, key := sk.slot, sk.key
		global.EnqueuePendingStakePubkey(slot, key)
		parents := accounts.NewMemAccountsWithLen(1)
		require.NoError(t, parents.SetAccountWithoutLock(key, &accounts.Account{Key: key}))
		write := stakeAcct(t, key, 100, stakeOwner, sealevel.StakeStateV2StatusStake, vote, slot)
		tracker.recordSlot(slot, &sealevel.SlotCtx{Slot: slot, ParentAccts: parents}, []*accounts.Account{write}, global.PendingStakePubkeysForSlot(slot), func(solana.PublicKey) (*accounts.Account, error) { return nil, nil })
		overlay.Add(slot, []*accounts.Account{write})
		contexts[slot] = &state.ResumeContext{Slot: slot}
	}
	fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
	promoted, err := promoteRootedBatchedWithStakeTotals(overlay, 6, map[uint64][32]byte{}, contexts, fc, 1, dir, false, tracker)
	require.NoError(t, err)
	require.Equal(t, uint64(6), promoted)

	var ctx state.ResumeContext
	require.NoError(t, json.Unmarshal(fc.ctxs[6], &ctx))
	_, saved, err := decodeDelegatedStake(ctx.DelegatedStake)
	require.NoError(t, err)
	flushed, err := global.LoadStakePubkeyIndex(dir)
	require.NoError(t, err)
	assert.Equal(t, digestStakeIndex(flushed), saved)
	assert.Equal(t, uint64(2), saved.keys, "slot 7 is not folded yet")
}

// resetStakeIndexState clears the process-wide pending stake keys and loaded
// index around a test that uses a real index file.
func resetStakeIndexState(t *testing.T) {
	reset := func() {
		global.ClearPendingStakePubkeys()
		global.ResetStakePubkeyIndexCache()
	}
	reset()
	t.Cleanup(reset)
}
