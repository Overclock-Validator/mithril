package replay

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"os"
	"sync/atomic"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestEpochRewardsAccountingSlot470880000(t *testing.T) {
	var fixture struct {
		Slot                    uint64
		BlockHeight             uint64      `json:"block_height"`
		ParentBankhash          solana.Hash `json:"parent_bankhash"`
		Blockhash               solana.Hash
		PreviousBlockhash       solana.Hash       `json:"previous_blockhash"`
		Signatures              uint64            `json:"num_signatures"`
		BrokenHash              solana.Hash       `json:"broken_bankhash"`
		ExpectedHash            solana.Hash       `json:"expected_bankhash"`
		BrokenLtHash            []byte            `json:"broken_lthash"`
		BrokenEpochRewards      *accounts.Account `json:"broken_epoch_rewards"`
		PaidVotingRewards       uint64            `json:"paid_voting_rewards"`
		CalculatedVotingRewards uint64            `json:"calculated_voting_rewards"`
		UnpaidVotingRewards     uint64            `json:"unpaid_voting_rewards"`
	}
	raw, err := os.ReadFile("testdata/slot_470880000_epoch_rewards.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &fixture))
	broken := decodeEpochRewardsForTest(t, fixture.BrokenEpochRewards.Data)
	require.Equal(t, fixture.PaidVotingRewards, broken.DistributedRewards)
	require.Equal(t, fixture.CalculatedVotingRewards,
		fixture.PaidVotingRewards+fixture.UnpaidVotingRewards)

	// One calculated commission could not be paid because its recipient was
	// absent. Agave still considers that reward processed in EpochRewards;
	// only the actual payment contributes to capitalization. Exercise the
	// production sysvar builder with both allocations, rather than repairing
	// the captured bytes by hand.
	paid, unpaid := new(atomic.Uint64), new(atomic.Uint64)
	paid.Store(fixture.PaidVotingRewards)
	unpaid.Store(fixture.UnpaidVotingRewards)
	initial, err := newEpochRewardsForDistribution(&b.Block{
		Slot: fixture.Slot, BlockHeight: fixture.BlockHeight,
		LastBlockhash: fixture.PreviousBlockhash,
	}, broken.TotalRewards, &rewards.StreamingRewardsResult{
		NumPartitions: broken.NumPartitions, TotalPoints: broken.TotalPoints,
		ValidatorRewards: map[solana.PublicKey]*atomic.Uint64{
			{1}: paid,
			{2}: unpaid,
		},
	})
	require.NoError(t, err)
	require.Equal(t, fixture.CalculatedVotingRewards, initial.DistributedRewards)
	broken.DistributedRewards = fixture.CalculatedVotingRewards
	require.Equal(t, broken, initial, "only the voting-reward accounting field should differ")

	fixed := fixture.BrokenEpochRewards.Clone()
	fixed.Data = encodeEpochRewardsForTest(t, initial)
	lt := new(lthash.LtHash).InitWithHash(fixture.BrokenLtHash)
	bankHash := func() solana.Hash {
		input := append([]byte(nil), fixture.ParentBankhash[:]...)
		input = binary.LittleEndian.AppendUint64(input, fixture.Signatures)
		input = append(input, fixture.Blockhash[:]...)
		inner := sha256.Sum256(input)
		return solana.Hash(sha256.Sum256(append(inner[:], lt.Hash()...)))
	}
	require.Equal(t, fixture.BrokenHash, bankHash())
	lt.Sub(new(lthash.LtHash).InitWithAcct(fixture.BrokenEpochRewards))
	lt.Add(new(lthash.LtHash).InitWithAcct(fixed))
	require.Equal(t, fixture.ExpectedHash, bankHash(),
		"the production accounting correction must recover the exact certified footer")
}
