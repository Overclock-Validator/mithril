package replay

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stakeWorld is a simulated store plus stake pubkey index. referenceScan is
// mainline's startup aggregation over it, the result the tracker must equal
// at every fold boundary.
type stakeWorld struct {
	stored map[solana.PublicKey]*accounts.Account
	index  map[solana.PublicKey]bool
}

func (w *stakeWorld) clone() *stakeWorld {
	c := &stakeWorld{stored: map[solana.PublicKey]*accounts.Account{}, index: map[solana.PublicKey]bool{}}
	for k, v := range w.stored {
		c.stored[k] = v.Clone()
	}
	for k := range w.index {
		c.index[k] = true
	}
	return c
}

func (w *stakeWorld) referenceScan() map[solana.PublicKey]voteDelegation {
	totals := map[solana.PublicKey]voteDelegation{}
	for key := range w.index {
		if vote, stake, ok := scannedDelegation(w.stored[key]); ok {
			d := totals[vote]
			d.stake += stake
			d.accounts++
			totals[vote] = d
		}
	}
	return totals
}

func (w *stakeWorld) indexEntries() []accountsdb.StakeIndexEntry {
	var entries []accountsdb.StakeIndexEntry
	for key := range w.index {
		entries = append(entries, accountsdb.StakeIndexEntry{Pubkey: key})
	}
	return entries
}

// slot is one replayed slot: the accounts it writes (as stored), the keys it
// enqueues into the index, and keys whose parent image the batch loader
// would have replaced with a placeholder (zero-lamport stored records).
type stakeSlot struct {
	slot        uint64
	writes      []*accounts.Account
	enqueue     []solana.PublicKey
	placeholder map[solana.PublicKey]bool
}

// apply records the slot in the tracker the way replay does (parent images in
// the SlotCtx, old stored records readable as of the parent), then applies
// the writes and index additions to the world.
func (w *stakeWorld) apply(t *testing.T, tracker *stakeTotalsTracker, s stakeSlot) {
	t.Helper()
	parents := accounts.NewMemAccountsWithLen(uint64(len(s.writes)))
	for _, acct := range s.writes {
		parent := &accounts.Account{Key: acct.Key} // ensureParentAccountsForModified's absent image
		if stored, ok := w.stored[acct.Key]; ok && !s.placeholder[acct.Key] {
			parent = stored.Clone()
		}
		require.NoError(t, parents.SetAccountWithoutLock(acct.Key, parent))
	}
	slotCtx := &sealevel.SlotCtx{Slot: s.slot, ParentAccts: parents}
	readStored := func(key solana.PublicKey) (*accounts.Account, error) {
		if stored, ok := w.stored[key]; ok {
			return stored.Clone(), nil
		}
		return nil, nil
	}
	tracker.recordSlot(s.slot, slotCtx, s.writes, s.enqueue, readStored)
	for _, acct := range s.writes {
		w.stored[acct.Key] = acct.Clone()
	}
	for _, key := range s.enqueue {
		w.index[key] = true
	}
}

func stakeData(t *testing.T, status uint32, vote solana.PublicKey, stake uint64) []byte {
	t.Helper()
	st := sealevel.StakeStateV2{Status: status}
	if status == sealevel.StakeStateV2StatusStake {
		st.Stake.Stake.Delegation = sealevel.Delegation{VoterPubkey: vote, StakeLamports: stake, ActivationEpoch: 1, DeactivationEpoch: ^uint64(0)}
	}
	var buf bytes.Buffer
	require.NoError(t, st.MarshalWithEncoder(bin.NewBinEncoder(&buf)))
	data := make([]byte, 200)
	copy(data, buf.Bytes())
	return data
}

func stakeAcct(t *testing.T, key solana.PublicKey, lamports uint64, owner [32]byte, status uint32, vote solana.PublicKey, stake uint64) *accounts.Account {
	return &accounts.Account{Key: key, Lamports: lamports, Owner: owner, Data: stakeData(t, status, vote, stake)}
}

func TestStakeTotalsTrackerMatchesMainlineScanAcrossTombstones(t *testing.T) {
	stakeOwner := [32]byte(addresses.StakeProgramAddr)
	otherOwner := [32]byte{9, 9, 9}
	v1, v2, v3 := solana.PublicKey{0xa1}, solana.PublicKey{0xa2}, solana.PublicKey{0xa3}
	key := func(b byte) solana.PublicKey { return solana.PublicKey{0x10, b} }
	const stake, uninit = sealevel.StakeStateV2StatusStake, sealevel.StakeStateV2StatusUninitialized

	w := &stakeWorld{stored: map[solana.PublicKey]*accounts.Account{}, index: map[solana.PublicKey]bool{}}
	put := func(a *accounts.Account, indexed bool) {
		w.stored[a.Key] = a
		if indexed {
			w.index[a.Key] = true
		}
	}
	put(stakeAcct(t, key('A'), 1000, stakeOwner, stake, v1, 100), true)
	put(stakeAcct(t, key('B'), 1000, stakeOwner, stake, v1, 50), true)
	put(stakeAcct(t, key('C'), 1000, stakeOwner, stake, v2, 70), true)
	// D: a stale tombstone, zero lamports but still holding Stake data. The
	// mainline scan reads the stored record and counts it.
	put(stakeAcct(t, key('D'), 0, stakeOwner, stake, v3, 30), true)
	// E: Stake-shaped data outside the index; the scan never reads it.
	put(stakeAcct(t, key('E'), 1000, otherOwner, stake, v1, 999), false)
	// G: in the index but not stake-owned; the scan counts its data anyway.
	put(stakeAcct(t, key('G'), 1000, otherOwner, stake, v2, 5), true)
	// H: an index key with no stored account.
	w.index[key('H')] = true

	tracker := newStakeTotalsTracker(w.referenceScan(), newStakeIndexMembers(w.indexEntries()))
	foldAndCheck := func(through uint64, want map[solana.PublicKey]voteDelegation) {
		t.Helper()
		got, ok := tracker.totalsThrough(through)
		require.True(t, ok)
		require.Equal(t, want, got, "tracked totals through slot %d must equal the mainline scan", through)
		tracker.commitThrough(through, got)
	}

	w.apply(t, tracker, stakeSlot{slot: 1, writes: []*accounts.Account{
		stakeAcct(t, key('A'), 1100, stakeOwner, stake, v1, 110), // rewards
		stakeAcct(t, key('B'), 1000, stakeOwner, stake, v2, 50),  // redelegation
	}})
	w.apply(t, tracker, stakeSlot{slot: 2, writes: []*accounts.Account{
		stakeAcct(t, key('C'), 0, stakeOwner, uninit, solana.PublicKey{}, 0), // full withdrawal
		stakeAcct(t, key('E'), 1000, otherOwner, stake, v2, 1),               // still not indexed
		stakeAcct(t, key('F'), 1000, stakeOwner, stake, v3, 0),               // new, zero-stake delegation
	}, enqueue: []solana.PublicKey{key('F')}})
	afterSlot2 := w.clone()
	w.apply(t, tracker, stakeSlot{slot: 3, writes: []*accounts.Account{
		// D refunded and redelegated; the loader saw a placeholder parent.
		stakeAcct(t, key('D'), 5, stakeOwner, stake, v1, 20),
		stakeAcct(t, key('G'), 1000, otherOwner, stake, v2, 6),
	}, placeholder: map[solana.PublicKey]bool{key('D'): true}})

	// A fold through slot 2 with slot 3 still pending sees only slots 1-2.
	foldAndCheck(2, afterSlot2.referenceScan())
	foldAndCheck(3, w.referenceScan())
	assert.Equal(t, voteDelegation{stake: 0, accounts: 1}, w.referenceScan()[v3], "a zero-stake delegation keeps its vote account")

	// An unwound fork: slot 4 creates I and moves A, slot 5 touches I; both are
	// discarded, and the replacement slot 4 withdraws A instead.
	beforeFork := w.clone()
	w.apply(t, tracker, stakeSlot{slot: 4, writes: []*accounts.Account{
		stakeAcct(t, key('I'), 1000, stakeOwner, stake, v2, 40),
		stakeAcct(t, key('A'), 1100, stakeOwner, stake, v3, 110),
	}, enqueue: []solana.PublicKey{key('I')}})
	w.apply(t, tracker, stakeSlot{slot: 5, writes: []*accounts.Account{
		stakeAcct(t, key('I'), 1000, stakeOwner, stake, v2, 41),
	}})
	tracker.dropFrom(4)
	w = beforeFork
	w.apply(t, tracker, stakeSlot{slot: 4, writes: []*accounts.Account{
		stakeAcct(t, key('A'), 0, stakeOwner, uninit, solana.PublicKey{}, 0),
	}})
	// I's index membership came from the discarded slot 4: a later write with
	// Stake data and no enqueue must not count.
	w.apply(t, tracker, stakeSlot{slot: 5, writes: []*accounts.Account{
		stakeAcct(t, key('I'), 0, otherOwner, stake, v1, 77),
	}})
	foldAndCheck(5, w.referenceScan())
	require.False(t, w.index[key('I')])
}

func TestStakeTotalsTrackerStopsWithoutParentImage(t *testing.T) {
	members := newStakeIndexMembers([]accountsdb.StakeIndexEntry{{Pubkey: solana.PublicKey{1}}})
	tracker := newStakeTotalsTracker(map[solana.PublicKey]voteDelegation{}, members)
	slotCtx := &sealevel.SlotCtx{Slot: 1, ParentAccts: accounts.NewMemAccountsWithLen(0)}
	tracker.recordSlot(1, slotCtx, []*accounts.Account{{Key: solana.PublicKey{1}, Lamports: 1}}, nil, func(solana.PublicKey) (*accounts.Account, error) { return nil, nil })
	_, ok := tracker.totalsThrough(1)
	assert.False(t, ok, "a slot whose change cannot be computed stops the tracker instead of guessing")
}

func TestDelegatedStakeEncodingRoundTrip(t *testing.T) {
	totals := map[solana.PublicKey]voteDelegation{
		{1}: {stake: 5, accounts: 2},
		{2}: {stake: 0, accounts: 1},
	}
	index := digestStakeIndex([]accountsdb.StakeIndexEntry{{Pubkey: solana.PublicKey{3}}, {Pubkey: solana.PublicKey{4}}})
	encoded := encodeDelegatedStake(totals, index)
	raw, err := json.Marshal(&state.ResumeContext{Slot: 9, DelegatedStake: encoded})
	require.NoError(t, err)
	var ctx state.ResumeContext
	require.NoError(t, json.Unmarshal(raw, &ctx))
	decoded, decodedIndex, err := decodeDelegatedStake(ctx.DelegatedStake)
	require.NoError(t, err)
	assert.Equal(t, totals, decoded)
	assert.Equal(t, index, decodedIndex)

	unbound := *encoded
	unbound.IndexKeys, unbound.IndexDigest = 0, ""
	_, _, err = decodeDelegatedStake(&unbound)
	assert.Error(t, err, "totals without a stake index binding are not used")

	var old state.ResumeContext
	require.NoError(t, json.Unmarshal([]byte(`{"slot":9}`), &old))
	assert.Nil(t, old.DelegatedStake, "a context from an older binary has no totals, not empty ones")

	bad := *encoded
	bad.Votes++
	_, err = bad.Decode()
	assert.Error(t, err)
	assert.Empty(t, diffDelegatedStake(totals, decoded))
	assert.NotEmpty(t, diffDelegatedStake(totals, map[solana.PublicKey]voteDelegation{{1}: {stake: 5, accounts: 2}}))
}

// Both fold paths carry the tracked totals into the committed resume context
// and advance the durable totals only with the commit.
func TestFoldsWriteDelegatedStakeIntoResumeContext(t *testing.T) {
	stakeOwner := [32]byte(addresses.StakeProgramAddr)
	vote := solana.PublicKey{0xb1}
	acctKey := solana.PublicKey{0xc1}
	members := newStakeIndexMembers([]accountsdb.StakeIndexEntry{{Pubkey: acctKey}})
	initial := map[solana.PublicKey]voteDelegation{vote: {stake: 10, accounts: 1}}
	parentOf := func(stakeAmt uint64) *sealevel.SlotCtx {
		parents := accounts.NewMemAccountsWithLen(1)
		require.NoError(t, parents.SetAccountWithoutLock(acctKey, stakeAcct(t, acctKey, 100, stakeOwner, sealevel.StakeStateV2StatusStake, vote, stakeAmt)))
		return &sealevel.SlotCtx{ParentAccts: parents}
	}
	write := func(stakeAmt uint64) *accounts.Account {
		return stakeAcct(t, acctKey, 100, stakeOwner, sealevel.StakeStateV2StatusStake, vote, stakeAmt)
	}
	decodeCtx := func(raw []byte) map[solana.PublicKey]voteDelegation {
		var ctx state.ResumeContext
		require.NoError(t, json.Unmarshal(raw, &ctx))
		require.NotNil(t, ctx.DelegatedStake)
		totals, _, err := decodeDelegatedStake(ctx.DelegatedStake)
		require.NoError(t, err)
		return totals
	}

	t.Run("synchronous", func(t *testing.T) {
		tracker := newStakeTotalsTracker(initial, members)
		overlay := accounts.NewWorkingSet()
		contexts := map[uint64]*state.ResumeContext{}
		for _, input := range [][2]uint64{{5, 20}, {6, 30}} {
			slot, amt := input[0], input[1]
			ctx := parentOf(amt - 10)
			ctx.Slot = slot
			tracker.recordSlot(slot, ctx, []*accounts.Account{write(amt)}, nil, func(solana.PublicKey) (*accounts.Account, error) { return nil, nil })
			overlay.Add(slot, []*accounts.Account{write(amt)})
			contexts[slot] = &state.ResumeContext{Slot: slot}
		}
		fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
		promoted, err := promoteRootedBatchedWithStakeTotals(overlay, 6, map[uint64][32]byte{}, contexts, fc, 1, "", false, tracker)
		require.NoError(t, err)
		assert.Equal(t, uint64(6), promoted)
		assert.Equal(t, voteDelegation{stake: 20, accounts: 1}, decodeCtx(fc.ctxs[5])[vote])
		assert.Equal(t, voteDelegation{stake: 30, accounts: 1}, decodeCtx(fc.ctxs[6])[vote])
		assert.Equal(t, voteDelegation{stake: 30, accounts: 1}, tracker.durable[vote])
		assert.Empty(t, tracker.pending)
	})

	t.Run("async", func(t *testing.T) {
		fc := &fakeCommitter{durable: accounts.NewMemAccounts()}
		tail := newUnrootedTail(&fakeDurable{}, fc, unrootedTailHaltCap, 4, "")
		tail.stakeTotals = newStakeTotalsTracker(initial, members)
		ctx := parentOf(10)
		ctx.Slot = 7
		tail.stakeTotals.recordSlot(7, ctx, []*accounts.Account{write(25)}, nil, func(solana.PublicKey) (*accounts.Account, error) { return nil, nil })
		tail.Add(7, []*accounts.Account{write(25)}, testHashBytes(7))
		tail.SetContext(7, &state.ResumeContext{Slot: 7})

		job, err := tail.buildFoldJob(7, true)
		require.NoError(t, err)
		require.NotNil(t, job)
		assert.Equal(t, voteDelegation{stake: 10, accounts: 1}, tail.stakeTotals.durable[vote], "building a fold does not advance the durable totals")
		require.NoError(t, runFoldJob(fc, job))
		assert.Equal(t, voteDelegation{stake: 25, accounts: 1}, decodeCtx(fc.ctxs[7])[vote])
		tail.applyFoldJob(job)
		assert.Equal(t, voteDelegation{stake: 25, accounts: 1}, tail.stakeTotals.durable[vote])
	})
}

// mayDelegate must hold whenever mainline's rule counts a record; it only
// lets the tracker skip records that cannot count.
func TestMayDelegateIsNecessaryForScannedDelegation(t *testing.T) {
	vote := solana.PublicKey{0xd1}
	cases := [][]byte{nil, {}, {2, 0, 0}, {2, 0, 0, 0}}
	for _, status := range []uint32{sealevel.StakeStateV2StatusUninitialized, sealevel.StakeStateV2StatusInitialized, sealevel.StakeStateV2StatusStake, sealevel.StakeStateV2StatusRewardsPool, 7} {
		data := stakeData(t, sealevel.StakeStateV2StatusStake, vote, 5)
		data[0], data[1], data[2], data[3] = byte(status), 0, 0, 0
		cases = append(cases, data, data[:len(data)/2])
	}
	for i := 0; i < 2000; i++ {
		data := make([]byte, i%260)
		for j := range data {
			data[j] = byte(i*31 + j*7)
		}
		if len(data) >= 4 && i%3 == 0 {
			data[0], data[1], data[2], data[3] = 2, 0, 0, 0
		}
		cases = append(cases, data)
	}
	for _, data := range cases {
		if _, _, ok := scannedDelegation(&accounts.Account{Data: data}); ok {
			require.True(t, mayDelegate(data), "a counted record must pass the filter: % x", data[:min(len(data), 8)])
		}
	}
}

// A key listed twice in one slot's modified accounts counts once, with its
// last value, like the working set and the lattice hash.
func TestStakeTotalsTrackerCountsDuplicateWritesOnce(t *testing.T) {
	stakeOwner := [32]byte(addresses.StakeProgramAddr)
	vote, key := solana.PublicKey{0xb2}, solana.PublicKey{0xc2}
	members := newStakeIndexMembers([]accountsdb.StakeIndexEntry{{Pubkey: key}})
	tracker := newStakeTotalsTracker(map[solana.PublicKey]voteDelegation{vote: {stake: 10, accounts: 1}}, members)
	parents := accounts.NewMemAccountsWithLen(1)
	require.NoError(t, parents.SetAccountWithoutLock(key, stakeAcct(t, key, 100, stakeOwner, sealevel.StakeStateV2StatusStake, vote, 10)))
	ctx := &sealevel.SlotCtx{Slot: 5, ParentAccts: parents}
	first := stakeAcct(t, key, 100, stakeOwner, sealevel.StakeStateV2StatusStake, vote, 15)
	last := stakeAcct(t, key, 100, stakeOwner, sealevel.StakeStateV2StatusStake, vote, 40)
	tracker.recordSlot(5, ctx, []*accounts.Account{first, last}, nil, func(solana.PublicKey) (*accounts.Account, error) { return nil, nil })
	totals, ok := tracker.totalsThrough(5)
	require.True(t, ok)
	assert.Equal(t, voteDelegation{stake: 40, accounts: 1}, totals[vote])
}

// A final non-delegating value must suppress an intermediate Stake record,
// even when the final value and parent both take the cheap non-stake filter.
func TestStakeTotalsTrackerNewestNonStakeSuppressesDuplicate(t *testing.T) {
	stakeOwner := [32]byte(addresses.StakeProgramAddr)
	vote, key := solana.PublicKey{0xb3}, solana.PublicKey{0xc3}
	members := newStakeIndexMembers([]accountsdb.StakeIndexEntry{{Pubkey: key}})
	tracker := newStakeTotalsTracker(map[solana.PublicKey]voteDelegation{}, members)
	parent := stakeAcct(t, key, 100, stakeOwner, sealevel.StakeStateV2StatusUninitialized, vote, 0)
	parents := accounts.NewMemAccountsWithLen(1)
	require.NoError(t, parents.SetAccountWithoutLock(key, parent))
	ctx := &sealevel.SlotCtx{Slot: 5, ParentAccts: parents}
	intermediate := stakeAcct(t, key, 100, stakeOwner, sealevel.StakeStateV2StatusStake, vote, 40)
	final := parent.Clone()
	tracker.recordSlot(5, ctx, []*accounts.Account{intermediate, final}, nil, func(solana.PublicKey) (*accounts.Account, error) {
		return parent, nil
	})
	totals, ok := tracker.totalsThrough(5)
	require.True(t, ok)
	require.Empty(t, totals, "the superseded Stake record must not contribute delegated stake")
}
