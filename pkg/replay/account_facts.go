package replay

import (
	"encoding/binary"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

const accountFactsInterval = time.Second

// AccountFacts contains detached balances, stored vote headers and funding rules
// from one successfully executed local bank, alongside the epoch-cache generations
// observed at that boundary. It does not establish finality, durability, full
// vote-state validity or voting eligibility.
type AccountFacts struct {
	SampledAt                     time.Time
	Slot                          uint64
	Bankhash                      solana.Hash
	Epoch                         uint64
	Identity                      solana.PublicKey
	VoteAccount                   solana.PublicKey
	IdentityLamports              uint64
	VoteLamports                  uint64
	HasIdentityBalance            bool
	HasVoteBalance                bool
	InflationRewardsCommissionBPS uint16
	BlockRevenueCommissionBPS     uint16
	HasInflationRewardsCommission bool
	HasBlockRevenueCommission     bool
	StoredNodeIdentity            solana.PublicKey
	HasStoredNodeIdentity         bool
	PendingDelegatorRewards       uint64
	HasPendingDelegatorRewards    bool
	StoredBLSRegistrationPresent  bool
	HasStoredBLSRegistration      bool
	AdmissionFunding              AdmissionFundingFacts
	AdmissionEpochs               AdmissionEpochFacts
}

// AccountFactsSampler belongs to the replay owner. Sample and Reset must not
// run concurrently or while the bank is executing; publish only returned values.
// Its zero value is ready to use, and a nil sampler disables observation.
type AccountFactsSampler struct {
	latest     AccountFacts
	lastSample time.Time // Keep the monotonic clock for cadence checks.
}

// Reset clears the retained bank and cadence after a rewind or a new attempt.
func (s *AccountFactsSampler) Reset() {
	if s != nil {
		*s = AccountFactsSampler{}
	}
}

// Sample observes at most two configured accounts at a successful replay
// boundary. Between samples it returns the previous bank-bound value unchanged.
// A zero identity disables observation; callers must not pass an unconfigured
// ephemeral identity. A zero vote account omits its balance and stored header.
// epochSchedule is the schedule replay actually uses, not a global sysvar copy.
// Changed epoch-cache facts bypass the one-second account sampling cadence.
func (s *AccountFactsSampler) Sample(bank *sealevel.SlotCtx, identity, vote solana.PublicKey, epochSchedule *sealevel.SysvarEpochSchedule) (AccountFacts, bool) {
	if s == nil || identity.IsZero() {
		s.Reset()
		return AccountFacts{}, false
	}
	return s.sampleAt(time.Now(), bank, identity, vote, epochSchedule)
}

func (s *AccountFactsSampler) sampleAt(now time.Time, bank *sealevel.SlotCtx, identity, vote solana.PublicKey, epochSchedule *sealevel.SysvarEpochSchedule) (AccountFacts, bool) {
	if s == nil || identity.IsZero() || bank == nil || len(bank.FinalBankhash) != len(solana.Hash{}) || now.IsZero() {
		s.Reset()
		return AccountFacts{}, false
	}
	epochs := accountFactAdmissionEpochs(bank, identity, vote, epochSchedule)
	if !s.lastSample.IsZero() && s.latest.Identity == identity && s.latest.VoteAccount == vote && s.latest.Epoch == bank.Epoch &&
		s.latest.AdmissionEpochs == epochs &&
		!now.Before(s.lastSample) && now.Sub(s.lastSample) < accountFactsInterval {
		return s.latest, true
	}

	facts := AccountFacts{SampledAt: now.UTC(), Slot: bank.Slot, Bankhash: solana.HashFromBytes(bank.FinalBankhash),
		Epoch: bank.Epoch, Identity: identity, VoteAccount: vote, AdmissionEpochs: epochs}
	identityAccount := accountFactAccount(bank, identity)
	if identityAccount != nil {
		facts.IdentityLamports, facts.HasIdentityBalance = identityAccount.Lamports, true
	}
	var voteAccount *accounts.Account
	if vote == identity {
		voteAccount = identityAccount
	} else if !vote.IsZero() {
		voteAccount = accountFactAccount(bank, vote)
	}
	if voteAccount != nil {
		facts.VoteLamports, facts.HasVoteBalance = voteAccount.Lamports, true
		facts.InflationRewardsCommissionBPS, facts.BlockRevenueCommissionBPS,
			facts.HasInflationRewardsCommission, facts.HasBlockRevenueCommission = accountFactCommissions(voteAccount)
		accountFactStoredHeader(&facts, voteAccount)
	}
	facts.AdmissionFunding = accountFactAdmissionFunding(bank, epochSchedule, facts.VoteLamports, facts.HasVoteBalance)
	s.latest, s.lastSample = facts, now
	return facts, true
}

// observeAccountFacts stages the owner-bound sample for the next completed
// publication. Readers cannot see it separately from the completed bank.
func (p *Progress) observeAccountFacts(bank *sealevel.SlotCtx, identity, vote solana.PublicKey, epochSchedule *sealevel.SysvarEpochSchedule) {
	if p != nil {
		p.accountFacts, p.hasAccountFacts = p.accountSampler.Sample(bank, identity, vote, epochSchedule)
	}
}

// accountFactStoredHeader observes stored fields, not initialization or BLS
// validity. Pending delegator rewards are not validator income or free balance.
func accountFactStoredHeader(facts *AccountFacts, acct *accounts.Account) {
	if acct == nil || acct.IsDummy || acct.Executable || acct.Owner != addresses.VoteProgramAddr || len(acct.Data) < 36 {
		return
	}
	data := acct.Data
	version := binary.LittleEndian.Uint32(data[:4])
	switch version {
	case sealevel.VoteStateVersionV0_23_5, sealevel.VoteStateVersionV1_14_11, sealevel.VoteStateVersionCurrent, sealevel.VoteStateVersionV4:
		copy(facts.StoredNodeIdentity[:], data[4:36])
		facts.HasStoredNodeIdentity = true
	default:
		return
	}
	if version != sealevel.VoteStateVersionV4 || len(data) < 144 {
		return
	}
	facts.PendingDelegatorRewards, facts.HasPendingDelegatorRewards = binary.LittleEndian.Uint64(data[136:144]), true
	if len(data) < 145 {
		return
	}
	switch data[144] {
	case 0:
		facts.HasStoredBLSRegistration = true
	case 1:
		if len(data) >= 145+48 {
			facts.StoredBLSRegistrationPresent, facts.HasStoredBLSRegistration = true, true
		}
	}
}

// accountFactAccount uses the same in-bank/parent precedence as
// loadAccountLiveOrParentForReplay, using native shared reads to avoid copying
// account data or recording parent images. Shared parent values are immutable;
// an unfamiliar reader retains its ordinary owned-read semantics. Nothing
// escapes the sampler but scalars; never mutate or retain the returned account.
func accountFactAccount(bank *sealevel.SlotCtx, key solana.PublicKey) *accounts.Account {
	var acct *accounts.Account
	var err error
	if bank.Accounts != nil {
		acct, err = bank.GetAccountShared(key)
		if acct != nil || err == nil {
			return accountFactValue(acct, key, err)
		}
	}
	if bank.BankSysvars() == nil || !sealevel.IsBankSysvarAccount(key) {
		if reader, ok := bank.UnrootedRead.(interface {
			GetAccountShared(uint64, solana.PublicKey) (*accounts.Account, error)
		}); ok {
			acct, err = reader.GetAccountShared(bank.Slot, key)
			return accountFactValue(acct, key, err)
		}
	}
	acct, err = bank.GetAccountFromAccountsDb(key)
	return accountFactValue(acct, key, err)
}

func accountFactValue(acct *accounts.Account, key solana.PublicKey, err error) *accounts.Account {
	if err != nil || acct == nil || acct.IsDummy || acct.Key != key {
		return nil
	}
	return acct
}

// accountFactCommissions reads only the fixed header, not vote-state validity or
// initialization. Malformed variable-length tails cannot allocate or delay this
// observation. V4 values are raw basis points, including values above 10,000;
// legacy accounts do not store a separate block-revenue commission.
func accountFactCommissions(acct *accounts.Account) (inflation, block uint16, hasInflation, hasBlock bool) {
	if acct == nil || acct.IsDummy || acct.Executable || acct.Owner != addresses.VoteProgramAddr || len(acct.Data) < 4 {
		return 0, 0, false, false
	}
	data := acct.Data
	var offset int
	switch binary.LittleEndian.Uint32(data[:4]) {
	case sealevel.VoteStateVersionV0_23_5:
		// Version, node, voter, epoch, 32 prior voters, index, withdrawer.
		offset = 4 + 32 + 32 + 8 + 32*(32+8+8+8) + 8 + 32
	case sealevel.VoteStateVersionV1_14_11, sealevel.VoteStateVersionCurrent:
		offset = 4 + 32 + 32
	case sealevel.VoteStateVersionV4:
		// Version and four public keys precede the two uint16 fields.
		const start = 4 + 4*32
		if len(data) < start+4 {
			return 0, 0, false, false
		}
		return binary.LittleEndian.Uint16(data[start : start+2]), binary.LittleEndian.Uint16(data[start+2 : start+4]), true, true
	default:
		return 0, 0, false, false
	}
	if len(data) <= offset {
		return 0, 0, false, false
	}
	return uint16(data[offset]) * 100, 0, true, false
}
