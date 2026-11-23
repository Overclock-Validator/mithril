package replay

import (
	"math"

	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// EpochStakeFacts is one immutable epoch-cache generation's configured vote
// account entry, not a cryptographically validated signer set. Positive stake
// means Member even if metadata is missing. SelectionLamports is the cache's
// selection/snapshot balance, not the account balance at the sampled bank.
type EpochStakeFacts struct {
	Epoch, Generation    uint64
	HasMaterial          bool
	Member               bool
	SelectedStake        uint64
	TotalStake           uint64
	HasMetadata          bool
	MetadataNodeIdentity solana.PublicKey
	NodeMatchesIdentity  bool
	SelectionLamports    uint64
}

// AdmissionEpochFacts observes at most two individually coherent generations,
// E and E+1. The pair is not an atomic cross-epoch prediction. The payer reference
// uses the first slot of E; it is not a receipt of a debit or signer eligibility.
// HasMaterial distinguishes an unavailable set from known nonmembership.
type AdmissionEpochFacts struct {
	HasEpochs          bool
	Current, Next      EpochStakeFacts
	HasNext            bool
	HasPayerEpoch      bool
	PayerReferenceSlot uint64
	PayerEpoch         uint64
	HasPayerMembership bool
	PayerMember        bool
}

func accountFactAdmissionEpochs(bank *sealevel.SlotCtx, identity, vote solana.PublicKey, schedule *sealevel.SysvarEpochSchedule) AdmissionEpochFacts {
	if bank == nil || identity.IsZero() || vote.IsZero() || !admissionScheduleValid(schedule) {
		return AdmissionEpochFacts{}
	}
	epoch, index := schedule.GetEpochAndSlotIndex(bank.Slot)
	if epoch != bank.Epoch || index > bank.Slot {
		return AdmissionEpochFacts{}
	}
	// Two point lookups at most; retain no cache maps or metadata pointers.
	current, known := global.EpochStakesSnapshot(epoch)
	facts := AdmissionEpochFacts{HasEpochs: true, Current: accountFactEpochStake(current, known, identity, vote)}
	if epoch < math.MaxUint64 {
		next, nextKnown := global.EpochStakesSnapshot(epoch + 1)
		facts.HasNext, facts.Next = true, accountFactEpochStake(next, nextKnown, identity, vote)
	}
	first := bank.Slot - index
	// LeaderScheduleEpoch saturates arithmetic. Reject an unrepresentable
	// reference instead of turning overflow into a real payer generation.
	if first < schedule.FirstNormalSlot {
		if epoch == math.MaxUint64 {
			return facts
		}
	} else {
		offset := first - schedule.FirstNormalSlot
		if schedule.LeaderScheduleSlotOffset > math.MaxUint64-offset ||
			(offset+schedule.LeaderScheduleSlotOffset)/schedule.SlotsPerEpoch > math.MaxUint64-schedule.FirstNormalEpoch {
			return facts
		}
	}
	facts.HasPayerEpoch, facts.PayerReferenceSlot, facts.PayerEpoch = true, first, schedule.LeaderScheduleEpoch(first)
	var payer EpochStakeFacts
	switch {
	case facts.PayerEpoch == facts.Current.Epoch:
		payer = facts.Current
	case facts.HasNext && facts.PayerEpoch == facts.Next.Epoch:
		payer = facts.Next
	}
	facts.HasPayerMembership, facts.PayerMember = payer.HasMaterial, payer.HasMaterial && payer.Member
	return facts
}

func accountFactEpochStake(snapshot epochstakes.Snapshot, known bool, identity, vote solana.PublicKey) EpochStakeFacts {
	facts := EpochStakeFacts{Epoch: snapshot.Epoch, Generation: snapshot.Generation, HasMaterial: known}
	if !known {
		return facts
	}
	facts.SelectedStake, facts.TotalStake = snapshot.Stakes[vote], snapshot.TotalStake
	facts.Member = facts.SelectedStake != 0
	if metadata := snapshot.VoteAccounts[vote]; metadata != nil {
		facts.HasMetadata, facts.MetadataNodeIdentity = true, metadata.NodePubkey
		facts.NodeMatchesIdentity, facts.SelectionLamports = metadata.NodePubkey == identity, metadata.Lamports
	}
	return facts
}
