package replay

import (
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/gagliardetto/solana-go"
)

// AlpenglowFeatureGatePubkey is the on-chain Alpenglow feature gate account.
// We derive internal PDAs (vote_reward_account, alpenclock, etc.) using this
// pubkey as the program_id argument to find_program_address.
const AlpenglowFeatureGatePubkey = features.AlpenglowFeatureGateAddress

var (
	genesisV1Alpenglow                            = solana.PublicKey(features.AlpenglowGenesisV1.Address)
	genesisV1VoteReward, _, _                     = solana.FindProgramAddress([][]byte{[]byte("vote_reward_account")}, genesisV1Alpenglow)
	genesisV1NanoClock, _, _                      = solana.FindProgramAddress([][]byte{[]byte("alpenclock")}, genesisV1Alpenglow)
	genesisV1RewardStakes, _, _                   = solana.FindProgramAddress([][]byte{[]byte("reward_epoch_delegated_stakes")}, genesisV1Alpenglow)
	devAlpenglow                                  = solana.PublicKey(features.AlpenglowDevContext.Address)
	devVoteReward, _, _                           = solana.FindProgramAddress([][]byte{[]byte("vote_reward_account")}, devAlpenglow)
	devNanoClock, _, _                            = solana.FindProgramAddress([][]byte{[]byte("alpenclock")}, devAlpenglow)
	devRewardStakes, _, _                         = solana.FindProgramAddress([][]byte{[]byte("reward_epoch_delegated_stakes")}, devAlpenglow)
	alpenglowFeatureGatePubkey                    = solana.PublicKey(features.Alpenglow.Address)
	voteRewardAccountPubkey, _, _                 = solana.FindProgramAddress([][]byte{[]byte("vote_reward_account")}, alpenglowFeatureGatePubkey)
	nanosecondClockAccountPubkey, _, _            = solana.FindProgramAddress([][]byte{[]byte("alpenclock")}, alpenglowFeatureGatePubkey)
	rewardEpochDelegatedStakesAccountPubkey, _, _ = solana.FindProgramAddress([][]byte{[]byte("reward_epoch_delegated_stakes")}, alpenglowFeatureGatePubkey)
)

// VoteRewardAccountAddr returns the vote-reward metadata PDA (Agave epoch inflation state).
func VoteRewardAccountAddr(bankFeatures ...*features.Features) solana.PublicKey {
	if genesisV1AlpenglowMetadata(bankFeatures) {
		return genesisV1VoteReward
	}
	if developmentAlpenglowMetadata(bankFeatures) {
		return devVoteReward
	}
	return voteRewardAccountPubkey
}

// NanosecondClockAccountAddr returns the Alpenglow nanosecond clock PDA (Agave alpenclock).
func NanosecondClockAccountAddr(bankFeatures ...*features.Features) solana.PublicKey {
	if genesisV1AlpenglowMetadata(bankFeatures) {
		return genesisV1NanoClock
	}
	if developmentAlpenglowMetadata(bankFeatures) {
		return devNanoClock
	}
	return nanosecondClockAccountPubkey
}

// RewardEpochDelegatedStakesAccountAddr returns Agave's bounded PDA containing
// the effective-stake denominators used to recalculate Alpenglow epoch rewards
// after restoring a snapshot during partitioned reward distribution.
func RewardEpochDelegatedStakesAccountAddr(bankFeatures ...*features.Features) solana.PublicKey {
	if genesisV1AlpenglowMetadata(bankFeatures) {
		return genesisV1RewardStakes
	}
	if developmentAlpenglowMetadata(bankFeatures) {
		return devRewardStakes
	}
	return rewardEpochDelegatedStakesAccountPubkey
}

// An explicitly supplied dev-context feature-set build uses its own namespace.
// Existing snapshot startup uses the deployed IDs; a deployed feature takes
// precedence if both accounts are present.
func developmentAlpenglowMetadata(fs []*features.Features) bool {
	return len(fs) == 1 && fs[0] != nil && !fs[0].IsActive(features.Alpenglow) && fs[0].IsActive(features.AlpenglowDevContext)
}

func genesisV1AlpenglowMetadata(fs []*features.Features) bool {
	return len(fs) == 1 && fs[0] != nil && !fs[0].IsActive(features.Alpenglow) && fs[0].IsActive(features.AlpenglowGenesisV1)
}
