package replay

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/gagliardetto/solana-go"
)

// Delegated stake totals: what they are and how they stay exact.
//
// At startup, the vote and stake caches take, per vote account, the stake
// delegated to it and (implicitly) whether anything delegates to it at all.
// Mainline computes that by scanning: for every key in the stake pubkey index
// (built at snapshot processing, extended by folded slots, never shrunk), read
// the stored record and, if its data decodes as a Stake-state delegation, add
// it to its vote account (scanDelegatedStake; scannedDelegation is the
// per-account rule). Lamports and owner are not checked, so a closed record
// that still holds Stake data counts, and so does a zero-stake delegation.
//
// The tracker reproduces that result at every fold boundary without scanning.
// For each replayed slot and each account the slot modified:
//
//	change = [key in index after the slot]  * delegation(new record)
//	       - [key in index before the slot] * delegation(old record)
//
// The new record is the account as the slot writes it, which is exactly what
// the fold stores. The old record is the account's parent image (the stored
// record whenever it has lamports; a zero-lamport parent may be the batch
// loader's placeholder, so it is re-read through the unrooted tail). Index
// membership is the index loaded at startup plus the keys each slot enqueues
// (the only way keys are added). Summed over a fold's slots these changes
// turn the scan result at the previous boundary into the scan result at this
// one, so each fold's resume context carries exactly what a scan of that
// boundary would compute.

// voteDelegation is a vote account's delegated stake and the number of index
// accounts delegating it. The count keeps a vote account whose delegations
// sum to zero stake, as the scan does.
type voteDelegation struct {
	stake, accounts uint64
}

type voteDelegationChange struct {
	stake, accounts int64
}

// stakeIndexMembers is the stake pubkey index's key set: the keys loaded at
// startup (sorted) plus keys added by replayed slots.
type stakeIndexMembers struct {
	base  []solana.PublicKey
	added map[solana.PublicKey]uint64 // key -> first slot that enqueued it
}

func newStakeIndexMembers(entries []accountsdb.StakeIndexEntry) *stakeIndexMembers {
	base := make([]solana.PublicKey, len(entries))
	for i, entry := range entries {
		base[i] = entry.Pubkey
	}
	sort.Slice(base, func(i, j int) bool { return bytes.Compare(base[i][:], base[j][:]) < 0 })
	return &stakeIndexMembers{base: base, added: make(map[solana.PublicKey]uint64)}
}

func (m *stakeIndexMembers) contains(key solana.PublicKey) bool {
	if _, ok := m.added[key]; ok {
		return true
	}
	i := sort.Search(len(m.base), func(i int) bool { return bytes.Compare(m.base[i][:], key[:]) >= 0 })
	return i < len(m.base) && m.base[i] == key
}

func (m *stakeIndexMembers) add(slot uint64, key solana.PublicKey) {
	if !m.contains(key) {
		m.added[key] = slot
	}
}

// dropFrom forgets keys first added by unwound slots >= from.
func (m *stakeIndexMembers) dropFrom(from uint64) {
	for key, slot := range m.added {
		if slot >= from {
			delete(m.added, key)
		}
	}
}

// stakeTotalsTracker keeps the scan result of durable state current across
// folds. durable is the result at the last fold boundary (or startup);
// pending holds each replayed, unfolded slot's change. A fold applies the
// changes of the slots it commits; an unwind drops those of discarded slots.
// If a change cannot be computed, or a total would become impossible, the
// tracker stops for the rest of the run and later boundaries carry no totals:
// it never writes totals it cannot vouch for.
type stakeTotalsTracker struct {
	members *stakeIndexMembers
	durable map[solana.PublicKey]voteDelegation
	pending map[uint64]map[solana.PublicKey]voteDelegationChange
	broken  bool
}

func newStakeTotalsTracker(initial map[solana.PublicKey]voteDelegation, members *stakeIndexMembers) *stakeTotalsTracker {
	durable := make(map[solana.PublicKey]voteDelegation, len(initial))
	for vote, d := range initial {
		durable[vote] = d
	}
	return &stakeTotalsTracker{
		members: members,
		durable: durable,
		pending: make(map[uint64]map[solana.PublicKey]voteDelegationChange),
	}
}

// recordSlot computes and records a replayed slot's change. modified are the
// accounts the slot writes (as stored), enqueued the keys the slot added to
// the stake index, and readStored returns an account's stored record as of
// the slot's parent (nil when absent).
func (t *stakeTotalsTracker) recordSlot(slot uint64, slotCtx *sealevel.SlotCtx, modified []*accounts.Account, enqueued []solana.PublicKey, readStored func(solana.PublicKey) *accounts.Account) {
	if t == nil || t.broken {
		return
	}
	addedNow := make(map[solana.PublicKey]struct{}, len(enqueued))
	for _, key := range enqueued {
		addedNow[key] = struct{}{}
	}
	var change map[solana.PublicKey]voteDelegationChange
	add := func(vote solana.PublicKey, stake, accounts int64) {
		if change == nil {
			change = make(map[solana.PublicKey]voteDelegationChange)
		}
		c := change[vote]
		c.stake += stake
		c.accounts += accounts
		if c.stake == 0 && c.accounts == 0 {
			delete(change, vote)
		} else {
			change[vote] = c
		}
	}
	// Newest first, so a key listed twice counts once, with its final value
	// (the working set and the lattice hash keep the last write too).
	var counted map[solana.PublicKey]struct{}
	for i := len(modified) - 1; i >= 0; i-- {
		acct := modified[i]
		if acct == nil {
			continue
		}
		key := acct.Key
		// Fast exact filter: a record can count only if its data starts with
		// the Stake status. When neither the new record nor the stored old one
		// does, the account cannot change the totals and its index membership
		// does not matter. A parent image without lamports may be the loader's
		// placeholder, so the stored old record is unknown and is checked below.
		parent, parentErr := slotCtx.GetParentAccount(key)
		parentIsStored := parentErr == nil && parent != nil && parent.Lamports > 0
		if !mayDelegate(acct.Data) && parentIsStored && !mayDelegate(parent.Data) {
			continue
		}
		if _, dup := counted[key]; dup {
			continue
		}
		if counted == nil {
			counted = make(map[solana.PublicKey]struct{})
		}
		counted[key] = struct{}{}
		memberBefore := t.members.contains(key)
		_, enqueuedNow := addedNow[key]
		memberAfter := memberBefore || enqueuedNow
		if !memberAfter {
			continue // never in the index: the scan never reads it
		}
		if memberBefore {
			if parentErr != nil {
				t.disable("slot %d: no parent image for stake index account %s", slot, key)
				return
			}
			old := parent
			if !parentIsStored {
				old = readStored(key)
			}
			if vote, stake, ok := scannedDelegation(old); ok {
				add(vote, -int64(stake), -1)
			}
		}
		if vote, stake, ok := scannedDelegation(acct); ok {
			add(vote, int64(stake), 1)
		}
	}
	for key := range addedNow {
		t.members.add(slot, key)
	}
	if len(change) > 0 {
		t.pending[slot] = change
	} else {
		delete(t.pending, slot)
	}
}

// totalsThrough returns the durable totals with the pending changes of every
// slot <= through applied, without changing the tracker.
func (t *stakeTotalsTracker) totalsThrough(through uint64) (map[solana.PublicKey]voteDelegation, bool) {
	if t == nil || t.broken {
		return nil, false
	}
	signed := make(map[solana.PublicKey]voteDelegationChange, len(t.durable))
	for vote, d := range t.durable {
		signed[vote] = voteDelegationChange{stake: int64(d.stake), accounts: int64(d.accounts)}
	}
	for slot, change := range t.pending {
		if slot > through {
			continue
		}
		for vote, c := range change {
			cur := signed[vote]
			cur.stake += c.stake
			cur.accounts += c.accounts
			signed[vote] = cur
		}
	}
	totals := make(map[solana.PublicKey]voteDelegation, len(signed))
	for vote, c := range signed {
		if c.stake < 0 || c.accounts < 0 || (c.accounts == 0 && c.stake != 0) {
			t.disable("vote account %s would hold %d stake from %d accounts through slot %d", vote, c.stake, c.accounts, through)
			return nil, false
		}
		if c.accounts > 0 {
			totals[vote] = voteDelegation{stake: uint64(c.stake), accounts: uint64(c.accounts)}
		}
	}
	return totals, true
}

// commitThrough makes totals (from totalsThrough(through)) the durable
// totals once the fold through that slot has committed.
func (t *stakeTotalsTracker) commitThrough(through uint64, totals map[solana.PublicKey]voteDelegation) {
	if t == nil || t.broken || totals == nil {
		return
	}
	t.durable = totals
	for slot := range t.pending {
		if slot <= through {
			delete(t.pending, slot)
		}
	}
}

// dropFrom discards unwound slots >= from: their changes and the index keys
// they added.
func (t *stakeTotalsTracker) dropFrom(from uint64) {
	if t == nil || t.broken {
		return
	}
	for slot := range t.pending {
		if slot >= from {
			delete(t.pending, slot)
		}
	}
	t.members.dropFrom(from)
}

func (t *stakeTotalsTracker) disable(format string, args ...any) {
	if t.broken {
		return
	}
	t.broken = true
	t.durable, t.pending, t.members = nil, nil, nil
	mlog.Log.Warnf("delegated stake totals: no longer tracked this run; fold boundaries from here on carry none: "+format, args...)
}

// encodeDelegatedStake converts totals to their resume-context encoding.
func encodeDelegatedStake(totals map[solana.PublicKey]voteDelegation) *state.DelegatedStakeTotals {
	raw := make(map[[32]byte]state.VoteDelegation, len(totals))
	for vote, d := range totals {
		raw[[32]byte(vote)] = state.VoteDelegation{Stake: d.stake, Accounts: d.accounts}
	}
	return state.EncodeDelegatedStake(raw)
}

// decodeDelegatedStake converts a resume context's totals back.
func decodeDelegatedStake(encoded *state.DelegatedStakeTotals) (map[solana.PublicKey]voteDelegation, error) {
	raw, err := encoded.Decode()
	if err != nil {
		return nil, err
	}
	totals := make(map[solana.PublicKey]voteDelegation, len(raw))
	for vote, d := range raw {
		totals[solana.PublicKey(vote)] = voteDelegation{stake: d.Stake, accounts: d.Accounts}
	}
	return totals, nil
}

// delegationStakes is the per-vote-account stake the vote cache rebuild takes:
// every vote account with at least one delegation, zero stake included.
func delegationStakes(totals map[solana.PublicKey]voteDelegation) map[solana.PublicKey]uint64 {
	stakes := make(map[solana.PublicKey]uint64, len(totals))
	for vote, d := range totals {
		stakes[vote] = d.stake
	}
	return stakes
}

// diffDelegatedStake describes how two totals differ ("" when identical), for
// verification.
func diffDelegatedStake(tracked, scanned map[solana.PublicKey]voteDelegation) string {
	var diffs []string
	for vote, s := range scanned {
		if t, ok := tracked[vote]; !ok || t != s {
			diffs = append(diffs, fmt.Sprintf("%s scan=%d/%d tracked=%d/%d", vote, s.stake, s.accounts, t.stake, t.accounts))
		}
	}
	for vote, t := range tracked {
		if _, ok := scanned[vote]; !ok {
			diffs = append(diffs, fmt.Sprintf("%s scan=absent tracked=%d/%d", vote, t.stake, t.accounts))
		}
	}
	if len(diffs) == 0 {
		return ""
	}
	sort.Strings(diffs)
	if len(diffs) > 10 {
		diffs = append(diffs[:10], fmt.Sprintf("... %d more", len(diffs)-10))
	}
	return fmt.Sprintf("%d vote accounts differ (stake/accounts): %v", len(diffs), diffs)
}

// recordSlotStakeChange records a slot's delegated stake change in the
// unrooted tail's tracker, just before the slot's writes are added.
func recordSlotStakeChange(tail unrootedState, slotCtx *sealevel.SlotCtx, modified []*accounts.Account) {
	if t, ok := tail.(*unrootedTail); ok && t != nil {
		t.recordStakeChange(slotCtx, modified)
	}
}

// mayDelegate reports whether data could decode as a Stake-state delegation:
// StakeStateV2 decoding reads the status first, as a little-endian uint32, so
// a delegation's data always starts with StakeStateV2StatusStake. It is a
// necessary condition for scannedDelegation, used only to skip work.
func mayDelegate(data []byte) bool {
	return len(data) >= 4 && binary.LittleEndian.Uint32(data) == sealevel.StakeStateV2StatusStake
}
