package dashboard

import (
	"math"

	"github.com/gagliardetto/solana-go"
)

// EpochStakeSetsProvenance identifies cached selection material, not bank state.
const EpochStakeSetsProvenance = "local_epoch_stakes_cache"

// StoredVoteHeader contains raw fixed-header fields, not vote-state or BLS
// validity. Pending delegator rewards are reserved funds, not validator income.
type StoredVoteHeader struct {
	NodeIdentity            *string `json:"node_identity"`
	PendingDelegatorRewards *uint64 `json:"pending_delegator_rewards_lamports,string"`
	BLSRegistrationPresent  *bool   `json:"bls_registration_present"`
}

// PublicStoredVoteHeader contains the explicitly exposed stored header fields.
type PublicStoredVoteHeader struct {
	NodeIdentity            *string `json:"node_identity"`
	PendingDelegatorRewards *uint64 `json:"pending_delegator_rewards_lamports,string"`
	BLSRegistrationPresent  *bool   `json:"bls_registration_present"`
}

// FundingRules describes the sampled bank's rules and numerical balance
// comparison. Gates are not charge receipts, and MeetsFloor is not eligibility.
type FundingRules struct {
	ReferenceSlot       uint64  `json:"reference_slot,string"`
	VATFilteringActive  bool    `json:"vat_filtering_active"`
	VATChargeGateActive bool    `json:"vat_charge_gate_active"`
	RentMinimumLamports uint64  `json:"rent_minimum_lamports,string"`
	BurnLamports        uint64  `json:"burn_lamports,string"`
	FloorLamports       uint64  `json:"floor_lamports,string"`
	MeetsFloor          *bool   `json:"meets_floor"`
	ShortfallLamports   *uint64 `json:"shortfall_lamports,string"`
	SurplusLamports     *uint64 `json:"surplus_lamports,string"`
}

// PublicFundingRules contains the explicitly exposed sampled funding rules.
type PublicFundingRules struct {
	ReferenceSlot       uint64  `json:"reference_slot,string"`
	VATFilteringActive  bool    `json:"vat_filtering_active"`
	VATChargeGateActive bool    `json:"vat_charge_gate_active"`
	RentMinimumLamports uint64  `json:"rent_minimum_lamports,string"`
	BurnLamports        uint64  `json:"burn_lamports,string"`
	FloorLamports       uint64  `json:"floor_lamports,string"`
	MeetsFloor          *bool   `json:"meets_floor"`
	ShortfallLamports   *uint64 `json:"shortfall_lamports,string"`
	SurplusLamports     *uint64 `json:"surplus_lamports,string"`
}

// VoteTicketCapacity is a constant-current-ticket, no-income scenario from
// one sampled bank. It does not establish a paid seat or future eligibility.
// Tickets is nil when Status is anything other than "known".
type VoteTicketCapacity struct {
	Tickets *uint64 `json:"tickets,string"`
	Status  string  `json:"status"`
}

// publicVoteTicketCapacity uses only this public bank's validated children.
// Subtract reserves in order so their sum cannot overflow a uint64.
func publicVoteTicketCapacity(f *PublicAccountFacts) *VoteTicketCapacity {
	r := f.FundingRules
	result := &VoteTicketCapacity{Status: "rules_unknown"}
	if r == nil {
		return result
	}
	if !r.VATChargeGateActive {
		result.Status = "charge_inactive"
		return result
	}
	if r.BurnLamports == 0 {
		result.Status = "zero_ticket"
		return result
	}
	if f.VoteBalanceLamports == nil {
		result.Status = "balance_unknown"
		return result
	}
	if f.StoredVoteHeader == nil || f.StoredVoteHeader.PendingDelegatorRewards == nil {
		result.Status = "liability_unknown"
		return result
	}
	var tickets uint64
	if balance := *f.VoteBalanceLamports; balance >= r.RentMinimumLamports {
		available := balance - r.RentMinimumLamports
		if pending := *f.StoredVoteHeader.PendingDelegatorRewards; available >= pending {
			tickets = (available - pending) / r.BurnLamports
		}
	}
	result.Status, result.Tickets = "known", &tickets
	return result
}

// EpochStakeSets contains two individually coherent cache generations, not an
// atomic pair or a ranked BLS signer set. Its observation time is its parent's.
type EpochStakeSets struct {
	Provenance string               `json:"provenance"`
	Current    EpochStakeSet        `json:"current"`
	Next       *EpochStakeSet       `json:"next"`
	Payer      *EpochPayerReference `json:"payer_reference"`
}

// EpochStakeSet distinguishes unavailable material from known nonmembership.
// SelectionBalanceLamports is the cached selection balance, not bank balance.
type EpochStakeSet struct {
	Epoch                    uint64  `json:"epoch,string"`
	Generation               uint64  `json:"generation,string"`
	MaterialKnown            bool    `json:"material_known"`
	Member                   *bool   `json:"member"`
	SelectedStakeLamports    *uint64 `json:"selected_stake_lamports,string"`
	TotalStakeLamports       *uint64 `json:"total_stake_lamports,string"`
	MetadataNodeIdentity     *string `json:"metadata_node_identity"`
	NodeMatchesIdentity      *bool   `json:"node_matches_identity"`
	SelectionBalanceLamports *uint64 `json:"selection_balance_lamports,string"`
}

// EpochPayerReference names the first-slot reference, not an observed debit.
// Member is unknown when its epoch is not a known current/next cache row.
type EpochPayerReference struct {
	ReferenceSlot uint64 `json:"reference_slot,string"`
	Epoch         uint64 `json:"epoch,string"`
	Member        *bool  `json:"member"`
}

// PublicEpochStakeSets contains the explicitly exposed stake-set observations.
type PublicEpochStakeSets struct {
	Provenance string                     `json:"provenance"`
	Current    PublicEpochStakeSet        `json:"current"`
	Next       *PublicEpochStakeSet       `json:"next"`
	Payer      *PublicEpochPayerReference `json:"payer_reference"`
}

// PublicEpochStakeSet contains the explicitly exposed fields for one epoch.
type PublicEpochStakeSet struct {
	Epoch                    uint64  `json:"epoch,string"`
	Generation               uint64  `json:"generation,string"`
	MaterialKnown            bool    `json:"material_known"`
	Member                   *bool   `json:"member"`
	SelectedStakeLamports    *uint64 `json:"selected_stake_lamports,string"`
	TotalStakeLamports       *uint64 `json:"total_stake_lamports,string"`
	MetadataNodeIdentity     *string `json:"metadata_node_identity"`
	NodeMatchesIdentity      *bool   `json:"node_matches_identity"`
	SelectionBalanceLamports *uint64 `json:"selection_balance_lamports,string"`
}

// PublicEpochPayerReference contains the explicitly exposed payer reference.
type PublicEpochPayerReference struct {
	ReferenceSlot uint64 `json:"reference_slot,string"`
	Epoch         uint64 `json:"epoch,string"`
	Member        *bool  `json:"member"`
}

func publicStoredVoteHeader(h *StoredVoteHeader) *PublicStoredVoteHeader {
	if h == nil {
		return nil
	}
	p := &PublicStoredVoteHeader{}
	if h.NodeIdentity != nil {
		if _, err := solana.PublicKeyFromBase58(*h.NodeIdentity); err != nil {
			return nil
		}
		v := *h.NodeIdentity
		p.NodeIdentity = &v
	}
	if h.PendingDelegatorRewards != nil {
		v := *h.PendingDelegatorRewards
		p.PendingDelegatorRewards = &v
	}
	if h.BLSRegistrationPresent != nil {
		v := *h.BLSRegistrationPresent
		p.BLSRegistrationPresent = &v
	}
	return p
}

func publicFundingRules(f *FundingRules, slot uint64, balance *uint64) *PublicFundingRules {
	if f == nil || f.ReferenceSlot != slot {
		return nil
	}
	floor := uint64(math.MaxUint64)
	if f.BurnLamports <= math.MaxUint64-f.RentMinimumLamports {
		floor = f.RentMinimumLamports + f.BurnLamports
	}
	if f.FloorLamports != floor || f.VATChargeGateActive && !f.VATFilteringActive {
		return nil
	}
	p := PublicFundingRules{ReferenceSlot: f.ReferenceSlot, VATFilteringActive: f.VATFilteringActive,
		VATChargeGateActive: f.VATChargeGateActive, RentMinimumLamports: f.RentMinimumLamports,
		BurnLamports: f.BurnLamports, FloorLamports: f.FloorLamports}
	if f.MeetsFloor == nil && f.ShortfallLamports == nil && f.SurplusLamports == nil {
		return &p
	}
	if balance == nil || f.MeetsFloor == nil || f.ShortfallLamports == nil || f.SurplusLamports == nil {
		return nil
	}
	meets := *balance >= floor
	var shortfall, surplus uint64
	if meets {
		surplus = *balance - floor
	} else {
		shortfall = floor - *balance
	}
	if *f.MeetsFloor != meets || *f.ShortfallLamports != shortfall || *f.SurplusLamports != surplus {
		return nil
	}
	p.MeetsFloor, p.ShortfallLamports, p.SurplusLamports = &meets, &shortfall, &surplus
	return &p
}

func publicEpochStakeSets(f *EpochStakeSets, epoch, slot uint64, identity string) *PublicEpochStakeSets {
	if f == nil || f.Provenance != EpochStakeSetsProvenance || f.Current.Epoch != epoch ||
		(epoch == math.MaxUint64) != (f.Next == nil) {
		return nil
	}
	current, ok := publicEpochStakeSet(f.Current, identity)
	if !ok {
		return nil
	}
	p := &PublicEpochStakeSets{Provenance: EpochStakeSetsProvenance, Current: current}
	if f.Next != nil {
		if f.Next.Epoch != epoch+1 {
			return nil
		}
		next, ok := publicEpochStakeSet(*f.Next, identity)
		if !ok {
			return nil
		}
		p.Next = &next
	}
	if r := f.Payer; r != nil {
		if r.ReferenceSlot > slot {
			return nil
		}
		var member *bool
		if r.Epoch == p.Current.Epoch {
			member = p.Current.Member
		} else if p.Next != nil && r.Epoch == p.Next.Epoch {
			member = p.Next.Member
		}
		if (member == nil) != (r.Member == nil) || member != nil && *member != *r.Member {
			return nil
		}
		p.Payer = &PublicEpochPayerReference{ReferenceSlot: r.ReferenceSlot, Epoch: r.Epoch}
		if member != nil {
			v := *member
			p.Payer.Member = &v
		}
	}
	return p
}

func publicEpochStakeSet(f EpochStakeSet, identity string) (PublicEpochStakeSet, bool) {
	if !f.MaterialKnown {
		return PublicEpochStakeSet{Epoch: f.Epoch, Generation: f.Generation}, f.Member == nil && f.SelectedStakeLamports == nil && f.TotalStakeLamports == nil &&
			f.MetadataNodeIdentity == nil && f.NodeMatchesIdentity == nil && f.SelectionBalanceLamports == nil
	}
	if f.Generation == 0 || f.Member == nil || f.SelectedStakeLamports == nil || f.TotalStakeLamports == nil ||
		*f.Member != (*f.SelectedStakeLamports != 0) {
		return PublicEpochStakeSet{}, false
	}
	member, stake, total := *f.Member, *f.SelectedStakeLamports, *f.TotalStakeLamports
	p := PublicEpochStakeSet{Epoch: f.Epoch, Generation: f.Generation, MaterialKnown: true,
		Member: &member, SelectedStakeLamports: &stake, TotalStakeLamports: &total}
	if f.MetadataNodeIdentity == nil && f.NodeMatchesIdentity == nil && f.SelectionBalanceLamports == nil {
		return p, true
	}
	if f.MetadataNodeIdentity == nil || f.NodeMatchesIdentity == nil || f.SelectionBalanceLamports == nil {
		return PublicEpochStakeSet{}, false
	}
	node, matches, balance := *f.MetadataNodeIdentity, *f.NodeMatchesIdentity, *f.SelectionBalanceLamports
	if _, err := solana.PublicKeyFromBase58(node); err != nil || matches != (node == identity) {
		return PublicEpochStakeSet{}, false
	}
	p.MetadataNodeIdentity, p.NodeMatchesIdentity, p.SelectionBalanceLamports = &node, &matches, &balance
	return p, true
}
