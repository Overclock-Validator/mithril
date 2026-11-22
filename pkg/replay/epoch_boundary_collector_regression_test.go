package replay

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"os"
	"sync/atomic"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/rewards"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type epochCollectorAccountImage struct {
	Key        solana.PublicKey `json:"key"`
	Lamports   uint64           `json:"lamports"`
	Owner      solana.PublicKey `json:"owner"`
	Executable bool             `json:"executable"`
	Data       []byte           `json:"data_base64"`
}

func (image epochCollectorAccountImage) account() *accounts.Account {
	return &accounts.Account{
		Key: image.Key, Lamports: image.Lamports, Owner: image.Owner,
		Executable: image.Executable, Data: image.Data,
	}
}

func TestEpochBoundaryCreatesRewardCollectorSlot471312000(t *testing.T) {
	// Captured from the epoch 1090 -> 1091 failure. The full artifact's other
	// 678 account images are retained in the broken LtHash. Only this newly
	// funded collector and the resulting inflation-budget correction differ
	// from the canonical bank. Both corrections must recover its exact hash.
	var fixture struct {
		Slot              uint64                     `json:"slot"`
		Epoch             uint64                     `json:"epoch"`
		ParentBankhash    solana.Hash                `json:"parent_bankhash"`
		Blockhash         solana.Hash                `json:"blockhash"`
		Signatures        uint64                     `json:"num_signatures"`
		BrokenBankhash    solana.Hash                `json:"broken_bankhash"`
		ExpectedBankhash  solana.Hash                `json:"expected_bankhash"`
		BrokenLtHash      []byte                     `json:"broken_accts_lthash_base64"`
		ParentCapital     uint64                     `json:"parent_capitalization"`
		StakerRewards     uint64                     `json:"staker_rewards"`
		PaidVotingRewards uint64                     `json:"paid_voting_rewards"`
		CalculatedVoting  uint64                     `json:"calculated_voting_rewards"`
		MissingReward     uint64                     `json:"missing_voting_reward_lamports"`
		SlotsPerYear      float64                    `json:"slots_per_year"`
		MissingCollector  epochCollectorAccountImage `json:"missing_reward_recipient"`
		BrokenInflation   epochCollectorAccountImage `json:"broken_epoch_inflation_account"`
		CorrectInflation  epochCollectorAccountImage `json:"corrected_epoch_inflation_account"`
		EpochSchedule     struct {
			SlotsPerEpoch   uint64 `json:"slots_per_epoch"`
			LeaderOffset    uint64 `json:"leader_schedule_slot_offset"`
			Warmup          bool   `json:"warmup"`
			FirstNormal     uint64 `json:"first_normal_epoch"`
			FirstNormalSlot uint64 `json:"first_normal_slot"`
		} `json:"epoch_schedule"`
		Inflation struct {
			Initial        float64 `json:"initial"`
			Terminal       float64 `json:"terminal"`
			Taper          float64 `json:"taper"`
			Foundation     float64 `json:"foundation"`
			FoundationTerm float64 `json:"foundation_term"`
		} `json:"inflation"`
		FeatureSlots map[string]uint64 `json:"feature_activation_slots"`
	}
	raw, err := os.ReadFile("testdata/slot_471312000_reward_collector.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &fixture))

	f := features.NewFeaturesDefault()
	gates := map[string]features.FeatureGate{
		"FullInflationVote":     features.FullInflationVote,
		"FullInflationEnable":   features.FullInflationEnable,
		"PicoInflation":         features.PicoInflation,
		"ReduceSlotTimeTo350ms": features.ReduceSlotTimeTo350ms,
		"ReduceSlotTimeTo300ms": features.ReduceSlotTimeTo300ms,
		"ReduceSlotTimeTo250ms": features.ReduceSlotTimeTo250ms,
		"ReduceSlotTimeTo200ms": features.ReduceSlotTimeTo200ms,
	}
	for name, slot := range fixture.FeatureSlots {
		gate, exists := gates[name]
		require.True(t, exists, "unknown captured feature %s", name)
		f.EnableFeature(gate, slot)
	}
	f.EnableFeature(features.CustomCommissionCollector, 0)
	schedule := &sealevel.SysvarEpochSchedule{
		SlotsPerEpoch:            fixture.EpochSchedule.SlotsPerEpoch,
		LeaderScheduleSlotOffset: fixture.EpochSchedule.LeaderOffset,
		Warmup:                   fixture.EpochSchedule.Warmup,
		FirstNormalEpoch:         fixture.EpochSchedule.FirstNormal,
		FirstNormalSlot:          fixture.EpochSchedule.FirstNormalSlot,
	}
	inflation := &rewards.Inflation{
		Initial: fixture.Inflation.Initial, Terminal: fixture.Inflation.Terminal,
		Taper: fixture.Inflation.Taper, FoundationVal: fixture.Inflation.Foundation,
		FoundationTerm: fixture.Inflation.FoundationTerm,
	}
	brokenState, err := decodeEpochInflationAccountState(fixture.BrokenInflation.Data)
	require.NoError(t, err)
	brokenCurrent := newEpochInflationState(schedule, inflation, fixture.ParentCapital,
		capitalizingEpochRewards(fixture.PaidVotingRewards, fixture.StakerRewards),
		fixture.Epoch, fixture.SlotsPerYear, f)
	require.Equal(t, brokenState.Current, brokenCurrent, "the unpaid deposit reproduces the captured inflation error")

	reward := new(atomic.Uint64)
	reward.Store(fixture.MissingReward)
	updated, parents, deposited := rewards.DistributeVotingRewards(nil,
		map[solana.PublicKey]*atomic.Uint64{fixture.MissingCollector.Key: reward}, fixture.Slot,
		func(key solana.PublicKey) (*accounts.Account, error) {
			require.Equal(t, fixture.MissingCollector.Key, key)
			return nil, accountsdb.ErrNoAccount
		}, rewards.VotingRewardDistributionConfig{
			Features: f,
			Rent:     sealevel.SysvarRent{LamportsPerUint8Year: 3480, ExemptionThreshold: 2, BurnPercent: 50},
		})
	require.Equal(t, fixture.MissingReward, deposited, "a sufficient commission must create the missing collector")
	require.Len(t, updated, 1)
	require.Len(t, parents, 1)
	require.NotNil(t, updated[0])
	require.NotNil(t, parents[0])
	require.Zero(t, parents[0].Lamports)
	require.Equal(t, fixture.MissingCollector.Key, updated[0].Key)
	require.Equal(t, [32]byte(fixture.MissingCollector.Owner), updated[0].Owner)
	require.Equal(t, fixture.MissingCollector.Lamports, updated[0].Lamports)
	require.Empty(t, updated[0].Data)
	require.False(t, updated[0].Executable)
	require.Equal(t, fixture.CalculatedVoting, fixture.PaidVotingRewards+deposited)

	fixedCurrent := newEpochInflationState(schedule, inflation, fixture.ParentCapital,
		capitalizingEpochRewards(fixture.PaidVotingRewards+deposited, fixture.StakerRewards),
		fixture.Epoch, fixture.SlotsPerYear, f)
	fixedState := brokenState
	fixedState.Current = fixedCurrent
	fixedInflation := fixture.BrokenInflation.account().Clone()
	fixedInflation.Data = encodeEpochInflationAccountState(fixedState)
	require.Equal(t, fixture.CorrectInflation.Data, fixedInflation.Data,
		"production capitalization/inflation calculation must recover the canonical bytes")

	bankHash := func(lt *lthash.LtHash) solana.Hash {
		input := append([]byte(nil), fixture.ParentBankhash[:]...)
		input = binary.LittleEndian.AppendUint64(input, fixture.Signatures)
		input = append(input, fixture.Blockhash[:]...)
		inner := sha256.Sum256(input)
		return solana.Hash(sha256.Sum256(append(inner[:], lt.Hash()...)))
	}
	broken := new(lthash.LtHash).InitWithHash(fixture.BrokenLtHash)
	require.Equal(t, fixture.BrokenBankhash, bankHash(broken))

	collectorOnly := broken.Clone()
	collectorOnly.Add(new(lthash.LtHash).InitWithAcct(updated[0]))
	require.NotEqual(t, fixture.ExpectedBankhash, bankHash(collectorOnly), "the capitalization consequence cannot be omitted")
	inflationOnly := broken.Clone()
	inflationOnly.Sub(new(lthash.LtHash).InitWithAcct(fixture.BrokenInflation.account()))
	inflationOnly.Add(new(lthash.LtHash).InitWithAcct(fixedInflation))
	require.NotEqual(t, fixture.ExpectedBankhash, bankHash(inflationOnly), "the new account itself must be published")

	fixed := collectorOnly.Clone()
	fixed.Sub(new(lthash.LtHash).InitWithAcct(fixture.BrokenInflation.account()))
	fixed.Add(new(lthash.LtHash).InitWithAcct(fixedInflation))
	require.Equal(t, fixture.ExpectedBankhash, bankHash(fixed), "the production fix must recover the exact certified bank hash")
}
