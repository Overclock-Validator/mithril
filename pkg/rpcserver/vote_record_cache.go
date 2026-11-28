package rpcserver

import (
	"context"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// rootedVoteRecordCache retains only immutable, account-derived RPC fields, not
// borrowed account buffers, stake totals, epoch membership or delinquency. Those
// bank/query-dependent values are still calculated for each response.
//
// Writes invalidate touched records before the account version becomes readable.
// Decoding is lazy on the RPC caller, never on the account writer. Recovery,
// rewind and failed writes discard all records, including same-slot fork reuse.
// The enclosing RPC must still validate its account version and bank identity.
type rootedVoteRecordCache struct {
	mu      sync.Mutex
	records map[solana.PublicKey]*VoteAccountInfo
}

func (c *rootedVoteRecordCache) AccountsChanged(_ uint64, changed []*accounts.Account, reset bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if reset {
		c.records = nil
		return
	}
	for _, account := range changed {
		if account != nil {
			delete(c.records, account.Key)
		}
	}
}

func decodeVoteRecord(key solana.PublicKey, account *accounts.Account) *VoteAccountInfo {
	if account == nil || account.Lamports == 0 || account.Owner != addresses.VoteProgramAddr {
		return nil
	}
	versioned, err := sealevel.UnmarshalVersionedVoteState(account.Data)
	if err != nil || !versioned.IsInitialized() {
		return nil
	}
	var voteState *sealevel.VoteState
	var v4View sealevel.VoteState
	if versioned.Type == sealevel.VoteStateVersionV4 {
		// Only these read-only fields are needed by RPC. Avoid allocating the
		// full legacy write-back conversion for every refreshed V4 account.
		state := &versioned.V4
		v4View.NodePubkey, v4View.RootSlot = state.NodePubkey, state.RootSlot
		v4View.Votes, v4View.EpochCredits = state.Votes, state.EpochCredits
		voteState = &v4View
	} else {
		voteState = versioned.ConvertToCurrent()
	}
	lastVote, _ := voteState.LastVotedSlot()
	var rootSlot uint64
	if voteState.RootSlot != nil {
		rootSlot = *voteState.RootSlot
	}
	creditsStart := max(0, len(voteState.EpochCredits)-maxRPCEpochCreditsHistory)
	creditsHistory := voteState.EpochCredits[creditsStart:]
	// Keep the bounded credit history in the same allocation as the record.
	// The returned pointer retains this storage; responses still copy the slice.
	record := new(struct {
		info    VoteAccountInfo
		credits [maxRPCEpochCreditsHistory][3]uint64
	})
	epochCredits := record.credits[:len(creditsHistory):len(creditsHistory)]
	for i, credits := range creditsHistory {
		epochCredits[i] = [3]uint64{credits.Epoch, credits.Credits, credits.PrevCredits}
	}
	commission, commissionBPS := voteCommission(versioned, voteState)
	record.info = VoteAccountInfo{
		VotePubkey: key.String(), NodePubkey: voteState.NodePubkey.String(),
		Commission: commission, InflationRewardsCommissionBPS: commissionBPS,
		EpochCredits: epochCredits, LastVote: lastVote, RootSlot: rootSlot,
	}
	return &record.info
}

func (s *RpcServer) rootedVoteRecords(ctx context.Context, slot, version uint64, keys []solana.PublicKey) ([]*VoteAccountInfo, error) {
	s.voteRecordsOnce.Do(func() { s.acctsDb.ObserveAccountState(&s.voteRecords) })
	c := &s.voteRecords
	records := make([]*VoteAccountInfo, len(keys))
	var missing []solana.PublicKey
	var positions []int
	c.mu.Lock()
	for i, key := range keys {
		records[i] = c.records[key]
		if records[i] == nil {
			missing = append(missing, key)
			positions = append(positions, i)
		}
	}
	c.mu.Unlock()
	if len(missing) == 0 {
		return records, nil // The enclosing RPC checks the final version/bank fence.
	}
	rooted, accountSet, err := s.readRootedAccounts(ctx, missing)
	if err != nil {
		return nil, err
	}
	if rooted.Slot != slot {
		return nil, errStakeViewChanged
	}
	decoded := make([]*VoteAccountInfo, len(missing))
	for i, account := range accountSet {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		decoded[i] = decodeVoteRecord(missing[i], account)
		records[positions[i]] = decoded[i]
	}
	if !s.storeVoteRecords(version, missing, decoded) {
		return nil, errStakeViewChanged
	}
	return records, nil
}

func (s *RpcServer) storeVoteRecords(version uint64, keys []solana.PublicKey, records []*VoteAccountInfo) bool {
	c := &s.voteRecords
	c.mu.Lock()
	defer c.mu.Unlock()
	// Check under the invalidation lock: a concurrent commit must either make
	// this load stale or subsequently remove its entries before readers see it.
	latest, stable := s.acctsDb.CommittedAccountVersion()
	if !stable || latest != version {
		return false
	}
	if c.records == nil {
		c.records = make(map[solana.PublicKey]*VoteAccountInfo)
	}
	for i, record := range records {
		// Do not accumulate negative entries for arbitrary filtered RPC keys.
		if record != nil {
			c.records[keys[i]] = record
		}
	}
	return true
}
