package replay

import (
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIncrementAlpenglowCreditsNewEpoch(t *testing.T) {
	var credits []sealevel.EpochCredits
	incrementAlpenglowCredits(&credits, 99, 5, 10)
	require.Len(t, credits, 1)
	assert.Equal(t, uint64(5), credits[0].Epoch)
	assert.Equal(t, uint64(10), credits[0].Credits)
}

func TestIncrementAlpenglowCreditsSameEpoch(t *testing.T) {
	credits := []sealevel.EpochCredits{{Epoch: 5, Credits: 10, PrevCredits: 0}}
	incrementAlpenglowCredits(&credits, 99, 5, 3)
	assert.Equal(t, uint64(13), credits[0].Credits)
}

func TestIncrementAlpenglowCreditsAfterMigrationMarker(t *testing.T) {
	credits := []sealevel.EpochCredits{
		{Epoch: 4, Credits: 20, PrevCredits: 10},
		agMigrationEpochCredit,
	}
	incrementAlpenglowCredits(&credits, 4, 5, 7)
	require.Len(t, credits, 3)
	last := credits[2]
	assert.Equal(t, uint64(5), last.Epoch)
	assert.Equal(t, uint64(27), last.Credits)
	assert.Equal(t, uint64(20), last.PrevCredits)
}

func TestCalculateAlpenglowRewardSplit(t *testing.T) {
	inflation := EpochInflationState{
		MaxPossibleValidatorReward: 1_000_000,
		SlotsPerEpoch:              100,
	}
	validator, leader := calculateAlpenglowReward(inflation, 1_000, 500)
	assert.Equal(t, uint64(5000), validator+leader)
	assert.Equal(t, validator, leader)
}

func TestVoteRewardUsesProcessingEpochInflationAcrossEpochBoundary(t *testing.T) {
	// Reward certificates trail the processing bank by eight slots.  At an
	// epoch boundary the rewarded stake snapshot belongs to the previous epoch,
	// but Agave intentionally uses the new bank epoch's inflation budget.
	state := EpochInflationAccountState{
		Current: EpochInflationState{
			Epoch:                      20,
			MaxPossibleValidatorReward: 20_000,
			SlotsPerEpoch:              100,
		},
		Prev: &EpochInflationState{
			Epoch:                      19,
			MaxPossibleValidatorReward: 10_000,
			SlotsPerEpoch:              100,
		},
	}

	inflation, err := voteRewardInflationState(state, 20, 19)
	require.NoError(t, err)
	validator, leader := calculateAlpenglowReward(inflation, 1_000, 500)
	require.Equal(t, uint64(100), validator+leader)

	previousInflation, err := voteRewardInflationState(state, 19, 19)
	require.NoError(t, err)
	previousValidator, previousLeader := calculateAlpenglowReward(previousInflation, 1_000, 500)
	require.Equal(t, uint64(50), previousValidator+previousLeader)
	require.NotEqual(t, validator+leader, previousValidator+previousLeader)
}

func TestVoteRewardInflationRequiresProcessingEpoch(t *testing.T) {
	state := EpochInflationAccountState{
		Current: EpochInflationState{Epoch: 19},
	}

	_, err := voteRewardInflationState(state, 20, 19)
	require.EqualError(t, err, "missing epoch inflation for processing epoch 20 (reward slot epoch 19)")
}

func TestCalcSlotTimestampNanosInclusiveRange(t *testing.T) {
	// producer_ns - slot_range_duration(target+1, bank) with constant 200ms:
	// (bank - target) * alpenglowNsPerSlot
	const producer int64 = 1783653042790683871
	got := calcSlotTimestampNanos(965552, 965560, producer)
	want := producer - int64(965560-965552)*int64(alpenglowNsPerSlot)
	assert.Equal(t, want, got)
	assert.Equal(t, int64(1783653041), got/1_000_000_000)

	assert.Equal(t, producer, calcSlotTimestampNanos(965560, 965560, producer))
	assert.Equal(t, int64(0), calcSlotTimestampNanos(10, 20, 100))
}

func TestMaybeUpdateVotesV4AdvancesLastTimestamp(t *testing.T) {
	vs := &sealevel.VoteState4{
		LastTimestamp: sealevel.BlockTimestamp{Slot: 100, Timestamp: 1_000},
	}
	maybeUpdateVotesV4(vs, 200, 1_500_000_000_000)
	assert.Equal(t, uint64(200), vs.LastTimestamp.Slot)
	assert.Equal(t, int64(1_500), vs.LastTimestamp.Timestamp)
	require.Equal(t, 1, vs.Votes.Len())
	assert.Equal(t, uint64(200), vs.Votes.At(0).Lockout.Slot)

	// Strictly greater timestamp required; equal/older must not rewrite.
	prev := vs.LastTimestamp
	maybeUpdateVotesV4(vs, 300, 1_500_000_000_000)
	assert.Equal(t, prev, vs.LastTimestamp)
}

func TestEnsureMigrationMarker(t *testing.T) {
	var credits []sealevel.EpochCredits
	ensureMigrationMarker(&credits)
	require.Len(t, credits, 1)
	assert.Equal(t, uint64(math.MaxUint64), credits[0].Epoch)
}

func TestWriteVersionedVoteStateInPlacePreservesAccountDataLen(t *testing.T) {
	var votePubkey, withdrawer solana.PublicKey
	votePubkey[0] = 1
	withdrawer[1] = 2

	var authVoters sealevel.AuthorizedVoters
	authVoters.AuthorizedVoters.Set(1, votePubkey)

	versioned := &sealevel.VoteStateVersions{
		Type: sealevel.VoteStateVersionV4,
		V4: sealevel.VoteState4{
			NodePubkey:                    votePubkey,
			AuthorizedWithdrawer:          withdrawer,
			AuthorizedVoters:              authVoters,
			InflationRewardsCollector:     votePubkey,
			BlockRevenueCollector:         votePubkey,
			InflationRewardsCommissionBps: 0,
			BlockRevenueCommissionBps:     10000,
		},
	}

	marshaled, err := sealevel.MarshalVersionedVoteState(versioned)
	require.NoError(t, err)
	require.Less(t, len(marshaled), sealevel.VoteStateV3Size)

	data := make([]byte, sealevel.VoteStateV3Size)
	copy(data, marshaled)
	data[len(data)-1] = 0x42

	require.NoError(t, sealevel.WriteVersionedVoteStateInPlace(data, versioned))
	require.Equal(t, sealevel.VoteStateV3Size, len(data))
	require.Equal(t, marshaled, data[:len(marshaled)])
	for _, b := range data[len(marshaled):] {
		require.Equal(t, byte(0), b)
	}
}
