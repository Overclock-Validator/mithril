package replay

import (
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/gagliardetto/solana-go"
)

// AlpenglowFeatureGatePubkey is the on-chain Alpenglow feature gate account.
// We derivee internal PDAs (vote_reward_account, alpenclock, etc.) using this
// pubkey as the program_id argument to find_program_address.
const AlpenglowFeatureGatePubkey = features.AlpenglowFeatureGateAddress

var (
	alpenglowFeatureGatePubkey                    = solana.PublicKey(features.Alpenglow.Address)
	voteRewardAccountPubkey, _, _                 = solana.FindProgramAddress([][]byte{[]byte("vote_reward_account")}, alpenglowFeatureGatePubkey)
	nanosecondClockAccountPubkey, _, _            = solana.FindProgramAddress([][]byte{[]byte("alpenclock")}, alpenglowFeatureGatePubkey)
	rewardEpochDelegatedStakesAccountPubkey, _, _ = solana.FindProgramAddress([][]byte{[]byte("reward_epoch_delegated_stakes")}, alpenglowFeatureGatePubkey)
)

// VoteRewardAccountAddr returns the vote-reward metadata PDA (Agave epoch inflation state).
func VoteRewardAccountAddr() solana.PublicKey {
	return voteRewardAccountPubkey
}

// NanosecondClockAccountAddr returns the Alpenglow nanosecond clock PDA (Agave alpenclock).
func NanosecondClockAccountAddr() solana.PublicKey {
	return nanosecondClockAccountPubkey
}

// RewardEpochDelegatedStakesAccountAddr returns Agave's bounded PDA containing
// the effective-stake denominators used to recalculate Alpenglow epoch rewards
// after restoring a snapshot during partitioned reward distribution.
func RewardEpochDelegatedStakesAccountAddr() solana.PublicKey {
	return rewardEpochDelegatedStakesAccountPubkey
}
