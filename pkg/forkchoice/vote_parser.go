package forkchoice

import (
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/gammazero/deque"
)

type voteInfo struct {
	slot       uint64
	bankHash   [32]byte
	votePubkey solana.PublicKey
}

// parseAndValidateVoteTx validates a vote transaction against the given authorized
// voters cache. Accepts the cache as a parameter to avoid racing with epoch updates.
func parseAndValidateVoteTx(tx *solana.Transaction, authorizedVoters *epochstakes.EpochAuthorizedVotersCache) (*voteInfo, bool) {
	if len(tx.Message.Instructions) < 1 {
		return nil, false
	}

	instr := tx.Message.Instructions[0]

	if len(instr.Accounts) < 2 {
		return nil, false
	}
	votePubkey := tx.Message.AccountKeys[instr.Accounts[0]]
	voteAuthority := tx.Message.AccountKeys[instr.Accounts[1]]

	if authorizedVoters == nil {
		return nil, false
	}
	if !(tx.IsSigner(voteAuthority) && authorizedVoters.IsAuthorizedVoter(votePubkey, voteAuthority)) {
		return nil, false
	}

	instrData := instr.Data
	decoder := bin.NewBinDecoder(instrData)
	instructionType, err := decoder.ReadUint32(bin.LE)
	if err != nil {
		return nil, false
	}

	switch instructionType {
	case sealevel.VoteProgramInstrTypeTowerSync:
		{
			var vote sealevel.VoteInstrTowerSync
			err = vote.UnmarshalWithDecoder(decoder)
			if err != nil {
				return nil, false
			}
			lockout, ok := getLastLockout(&vote.Lockouts)
			if !ok {
				return nil, false
			}

			return &voteInfo{slot: lockout.Slot,
				bankHash:   vote.Hash,
				votePubkey: votePubkey}, true
		}

	case sealevel.VoteProgramInstrTypeTowerSyncSwitch:
		{
			var vote sealevel.VoteInstrTowerSyncSwitch
			err = vote.UnmarshalWithDecoder(decoder)
			if err != nil {
				return nil, false
			}
			lockout, ok := getLastLockout(&vote.TowerSync.Lockouts)
			if !ok {
				return nil, false
			}
			return &voteInfo{slot: lockout.Slot,
				bankHash:   vote.Hash,
				votePubkey: votePubkey}, true
		}

	case sealevel.VoteProgramInstrTypeVote:
		{
			var vote sealevel.VoteInstrVote
			err = vote.UnmarshalWithDecoder(decoder)
			if err != nil {
				return nil, false
			}

			slot, ok := getSlot(vote.Slots)
			if !ok {
				return nil, false
			}

			return &voteInfo{slot: slot,
				bankHash:   vote.Hash,
				votePubkey: votePubkey}, true
		}

	case sealevel.VoteProgramInstrTypeVoteSwitch:
		{
			var vote sealevel.VoteInstrVoteSwitch
			err = vote.UnmarshalWithDecoder(decoder)
			if err != nil {
				return nil, false
			}

			slot, ok := getSlot(vote.Vote.Slots)
			if !ok {
				return nil, false
			}

			return &voteInfo{slot: slot,
				bankHash:   vote.Hash,
				votePubkey: votePubkey}, true
		}

	case sealevel.VoteProgramInstrTypeUpdateVoteState:
		{
			var vote sealevel.VoteInstrUpdateVoteState
			err = vote.UnmarshalWithDecoder(decoder)
			if err != nil {
				return nil, false
			}

			lockout, ok := getLastLockout(&vote.Lockouts)
			if !ok {
				return nil, false
			}
			return &voteInfo{slot: lockout.Slot,
				bankHash:   vote.Hash,
				votePubkey: votePubkey}, true
		}

	case sealevel.VoteProgramInstrTypeUpdateVoteStateSwitch:
		{
			var vote sealevel.VoteInstrUpdateVoteStateSwitch
			err = vote.UnmarshalWithDecoder(decoder)
			if err != nil {
				return nil, false
			}

			lockout, ok := getLastLockout(&vote.UpdateVoteState.Lockouts)
			if !ok {
				return nil, false
			}
			return &voteInfo{slot: lockout.Slot,
				bankHash:   vote.Hash,
				votePubkey: votePubkey}, true
		}

	case sealevel.VoteProgramInstrTypeCompactUpdateVoteState:
		{
			var vote sealevel.VoteInstrCompactUpdateVoteState
			err = vote.UnmarshalWithDecoder(decoder)
			if err != nil {
				return nil, false
			}

			lockout, ok := getLastLockout(&vote.UpdateVoteState.Lockouts)
			if !ok {
				return nil, false
			}
			return &voteInfo{slot: lockout.Slot,
				bankHash:   vote.UpdateVoteState.Hash,
				votePubkey: votePubkey}, true
		}

	case sealevel.VoteProgramInstrTypeCompactUpdateVoteStateSwitch:
		{
			var vote sealevel.VoteInstrCompactUpdateVoteStateSwitch
			err = vote.UnmarshalWithDecoder(decoder)
			if err != nil {
				return nil, false
			}

			lockout, ok := getLastLockout(&vote.UpdateVoteState.Lockouts)
			if !ok {
				return nil, false
			}
			return &voteInfo{slot: lockout.Slot,
				bankHash:   vote.Hash,
				votePubkey: votePubkey}, true
		}

	default:
		{
			return nil, false
		}
	}
}

func getLastLockout(lockouts *deque.Deque[sealevel.VoteLockout]) (*sealevel.VoteLockout, bool) {
	lockoutsLen := lockouts.Len()
	if lockoutsLen == 0 {
		return nil, false
	}

	lockout := lockouts.PopBack()
	return &lockout, true
}

func getSlot(slots []uint64) (uint64, bool) {
	if len(slots) == 0 {
		return 0, false
	}
	maxSlot := slots[0]
	for _, slot := range slots {
		if slot > maxSlot {
			maxSlot = slot
		}
	}
	return maxSlot, true
}
