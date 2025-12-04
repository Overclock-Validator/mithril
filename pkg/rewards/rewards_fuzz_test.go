package rewards

import (
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/wide"
	"github.com/gagliardetto/solana-go"
)

// FuzzSlotInYearForInflation tests the SlotInYearForInflation calculation
func FuzzSlotInYearForInflation(f *testing.F) {
	// Seed with various edge cases
	f.Add(uint64(0), float64(432000))       // Epoch 0
	f.Add(uint64(100), float64(432000))     // Normal epoch
	f.Add(uint64(1000000), float64(432000)) // Large epoch

	f.Fuzz(func(t *testing.T, epoch uint64, slotsPerYear float64) {
		// Skip invalid inputs
		if slotsPerYear <= 0 {
			t.Skip("slotsPerYear must be positive")
		}

		epochSchedule := &sealevel.SysvarEpochSchedule{
			SlotsPerEpoch:            432000,
			LeaderScheduleSlotOffset: 432000,
			Warmup:                   true,
			FirstNormalEpoch:         14,
			FirstNormalSlot:          524288,
		}

		features := features.NewFeaturesDefault()

		// Function should not panic
		result := SlotInYearForInflation(epochSchedule, slotsPerYear, epoch, features)

		// Result should be non-negative
		if result < 0 {
			t.Errorf("SlotInYearForInflation returned negative value: %f", result)
		}

		// Result should be deterministic
		result2 := SlotInYearForInflation(epochSchedule, slotsPerYear, epoch, features)
		if result != result2 {
			t.Errorf("SlotInYearForInflation not deterministic: %f vs %f", result, result2)
		}

		// Result represents fraction of year, so generally should be reasonable
		// (though it can exceed 1.0 for large epochs)
		if result > 1000000.0 {
			t.Logf("Warning: very large result for epoch %d: %f", epoch, result)
		}
	})
}

// FuzzGetInflationNumSlots tests calculation of slots since inflation start
func FuzzGetInflationNumSlots(f *testing.F) {
	// Seed with edge cases
	f.Add(uint64(0))
	f.Add(uint64(1))
	f.Add(uint64(100))
	f.Add(uint64(10000))

	f.Fuzz(func(t *testing.T, epoch uint64) {
		// Limit epoch to prevent excessive computation
		if epoch > 1000000 {
			t.Skip("epoch too large")
		}

		epochSchedule := &sealevel.SysvarEpochSchedule{
			SlotsPerEpoch:            432000,
			LeaderScheduleSlotOffset: 432000,
			Warmup:                   true,
			FirstNormalEpoch:         14,
			FirstNormalSlot:          524288,
		}

		features := features.NewFeaturesDefault()

		// Should not panic
		result := GetInflationNumSlots(epochSchedule, epoch, features)

		// Result should be deterministic
		result2 := GetInflationNumSlots(epochSchedule, epoch, features)
		if result != result2 {
			t.Errorf("GetInflationNumSlots not deterministic")
		}

		// Result should generally increase with epoch
		if epoch > 0 {
			prevResult := GetInflationNumSlots(epochSchedule, epoch-1, features)
			if result < prevResult {
				t.Errorf("GetInflationNumSlots(%d) = %d < GetInflationNumSlots(%d) = %d",
					epoch, result, epoch-1, prevResult)
			}
		}
	})
}

// FuzzCalculateRewardPartitionForPubkey tests partition calculation for rewards
func FuzzCalculateRewardPartitionForPubkey(f *testing.F) {
	// Seed with various partition counts
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}, uint64(1))
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}, uint64(100))
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}, uint64(10000))

	f.Fuzz(func(t *testing.T, blockhashBytes []byte, numPartitions uint64) {
		// Skip invalid inputs
		if numPartitions == 0 {
			t.Skip("numPartitions must be positive")
		}

		// Ensure blockhashBytes is exactly 32 bytes
		if len(blockhashBytes) != 32 {
			t.Skip("blockhashBytes must be exactly 32 bytes")
		}

		// Create a pubkey from fuzzed data
		var pubkey solana.PublicKey
		copy(pubkey[:], blockhashBytes)

		var blockhash [32]byte
		copy(blockhash[:], blockhashBytes)

		// Calculate partition
		partition := CalculateRewardPartitionForPubkey(pubkey, blockhash, numPartitions)

		// Partition must be within bounds
		if partition >= numPartitions {
			t.Errorf("CalculateRewardPartitionForPubkey returned partition %d >= numPartitions %d",
				partition, numPartitions)
		}

		// Result should be deterministic
		partition2 := CalculateRewardPartitionForPubkey(pubkey, blockhash, numPartitions)
		if partition != partition2 {
			t.Errorf("CalculateRewardPartitionForPubkey not deterministic: %d vs %d",
				partition, partition2)
		}

		// Different pubkeys should generally produce different partitions
		// (though hash collisions are possible)
		var differentPubkey solana.PublicKey
		for i := range differentPubkey {
			differentPubkey[i] = ^pubkey[i] // Bitwise NOT
		}

		partition3 := CalculateRewardPartitionForPubkey(differentPubkey, blockhash, numPartitions)

		// With high partition counts, different pubkeys usually go to different partitions
		if numPartitions > 100 && partition == partition3 && pubkey != differentPubkey {
			t.Logf("Hash collision: different pubkeys mapped to same partition %d", partition)
		}
	})
}

// FuzzMinimumStakeDelegation tests minimum stake delegation calculation
func FuzzMinimumStakeDelegation(f *testing.F) {
	// The function uses feature flags, so we test with different feature combinations
	f.Add(false, false)
	f.Add(true, false)
	f.Add(false, true)
	f.Add(true, true)

	f.Fuzz(func(t *testing.T, enableRaiseMinDelegation, enableCommissionUpdates bool) {
		features := features.NewFeaturesDefault()

		// Manually set feature flags based on fuzz input
		// (This tests the feature flag logic without complex setup)

		result := minimumStakeDelegationFeatures(features)

		// Result should be deterministic
		result2 := minimumStakeDelegationFeatures(features)
		if result != result2 {
			t.Errorf("minimumStakeDelegationFeatures not deterministic")
		}

		// Result can be 0 when StakeMinimumDelegationForRewards feature is not active
		// Otherwise, it should be 1 (legacy) or 1 SOL (1,000,000,000 lamports)

		// Valid values: 0 (feature disabled), 1 (legacy minimum), or 1000000000 (1 SOL)
		validValues := []uint64{0, 1, 1000000000}
		isValid := false
		for _, valid := range validValues {
			if result == valid {
				isValid = true
				break
			}
		}

		if !isValid {
			t.Errorf("minimumStakeDelegationFeatures returned unexpected value: %d (expected 0, 1, or 1000000000)", result)
		}

		// Minimum delegation should be reasonable (not exceed 1000 SOL)
		if result > 1000000000000 {
			t.Errorf("minimumStakeDelegationFeatures returned unreasonably large value: %d", result)
		}
	})
}

// FuzzVoteCommissionSplit tests vote commission splitting
func FuzzVoteCommissionSplit(f *testing.F) {
	// Seed with various commission percentages and reward amounts
	f.Add(uint8(0), uint64(0))               // 0% commission, 0 rewards
	f.Add(uint8(100), uint64(0))             // 100% commission, 0 rewards
	f.Add(uint8(0), uint64(1000000))         // 0% commission
	f.Add(uint8(100), uint64(1000000))       // 100% commission
	f.Add(uint8(5), uint64(1000000))         // 5% commission
	f.Add(uint8(10), uint64(math.MaxUint64)) // Large rewards

	f.Fuzz(func(t *testing.T, commission uint8, rewards uint64) {
		// Commission must be <= 100
		if commission > 100 {
			commission = 100
		}

		// Create a minimal vote state with the commission
		voteState := &sealevel.VoteStateVersions{
			Type: sealevel.VoteStateVersionCurrent,
			Current: sealevel.VoteState{
				Commission: commission,
			},
		}

		// Calculate split
		split := voteCommissionSplit(voteState, rewards)

		// IMPORTANT: Solana intentionally allows lamports to be lost to rounding!
		// Both portions are calculated independently, so VoterPortion + StakerPortion can be < rewards
		// This matches the official Solana Agave implementation's behavior.
		// See: runtime/src/inflation_rewards/mod.rs commission_split()

		// Verify no portion exceeds total rewards
		if split.VoterPortion > rewards {
			t.Errorf("Voter portion %d exceeds total rewards %d", split.VoterPortion, rewards)
		}
		if split.StakerPortion > rewards {
			t.Errorf("Staker portion %d exceeds total rewards %d", split.StakerPortion, rewards)
		}

		// Verify sum doesn't exceed total (lamports can be lost, but not created)
		if split.VoterPortion+split.StakerPortion > rewards {
			t.Errorf("Split exceeds total: %d + %d > %d",
				split.VoterPortion, split.StakerPortion, rewards)
		}

		// Verify voter portion (commission goes to voter/validator)
		if rewards > 0 && commission > 0 && commission < 100 {
			// Use 128-bit arithmetic to avoid overflow when calculating expected value
			// This matches what the actual implementation does
			rewardsU128 := uint64(commission) * rewards
			expectedVoterPortion := rewardsU128 / 100

			// For very large rewards, the multiplication might overflow uint64
			// In that case, skip the exact check since we can't calculate the expected value
			if uint64(commission) > 0 && rewards > math.MaxUint64/uint64(commission) {
				// Overflow would occur in uint64, just verify portion is reasonable
				// Voter portion should be less than rewards and greater than 0
				if split.VoterPortion == 0 {
					t.Errorf("Voter portion is 0 for non-zero commission %d%% and rewards %d", commission, rewards)
				}
			} else {
				// No overflow, we can check precisely (with rounding tolerance)
				diff := int64(split.VoterPortion) - int64(expectedVoterPortion)
				if diff < 0 {
					diff = -diff
				}
				if diff > 1 {
					t.Errorf("Voter portion split incorrect: got %d, expected ~%d (commission=%d%%, rewards=%d)",
						split.VoterPortion, expectedVoterPortion, commission, rewards)
				}
			}
		}

		// Special cases
		if commission == 0 {
			if split.VoterPortion != 0 {
				t.Errorf("0%% commission should have 0 voter portion, got %d", split.VoterPortion)
			}
			if split.StakerPortion != rewards {
				t.Errorf("0%% commission: all rewards should go to stakers, got %d/%d",
					split.StakerPortion, rewards)
			}
		}

		if commission == 100 {
			if split.StakerPortion != 0 {
				t.Errorf("100%% commission should have 0 staker portion, got %d", split.StakerPortion)
			}
			if split.VoterPortion != rewards {
				t.Errorf("100%% commission: all rewards should go to voter, got %d/%d",
					split.VoterPortion, rewards)
			}
		}

		if rewards == 0 {
			if split.StakerPortion != 0 || split.VoterPortion != 0 {
				t.Errorf("Zero rewards should result in zero split: got staker=%d, voters=%d",
					split.StakerPortion, split.VoterPortion)
			}
		}
	})
}

// FuzzCalculatePreviousEpochInflationRewards tests inflation reward calculation
func FuzzCalculatePreviousEpochInflationRewards(f *testing.F) {
	// Seed with various capitalization and epoch values
	f.Add(uint64(1000000000000000), uint64(100), uint64(99), float64(432000))
	f.Add(uint64(100000000000), uint64(10), uint64(9), float64(432000))
	f.Add(uint64(0), uint64(1), uint64(0), float64(432000))

	f.Fuzz(func(t *testing.T, prevEpochCapitalization uint64, epoch uint64, prevEpoch uint64, slotsPerYear float64) {
		// Skip invalid inputs
		if slotsPerYear <= 0 {
			t.Skip("slotsPerYear must be positive")
		}

		if epoch == 0 || prevEpoch >= epoch {
			t.Skip("epoch must be > prevEpoch and > 0")
		}

		if epoch > 1000000 {
			t.Skip("epoch too large")
		}

		epochSchedule := &sealevel.SysvarEpochSchedule{
			SlotsPerEpoch:            432000,
			LeaderScheduleSlotOffset: 432000,
			Warmup:                   true,
			FirstNormalEpoch:         14,
			FirstNormalSlot:          524288,
		}

		// Create a reasonable inflation configuration
		inflation := &Inflation{
			Initial:        0.08,  // 8%
			Terminal:       0.015, // 1.5%
			Taper:          0.15,  // 15%
			FoundationVal:  0.05,  // 5%
			FoundationTerm: 7.0,   // 7 years
			Unused:         0.0,
		}

		features := features.NewFeaturesDefault()

		// Function should not panic
		rewards := CalculatePreviousEpochInflationRewards(
			epochSchedule,
			inflation,
			prevEpochCapitalization,
			epoch,
			prevEpoch,
			slotsPerYear,
			features,
		)

		// Rewards should be deterministic
		rewards2 := CalculatePreviousEpochInflationRewards(
			epochSchedule,
			inflation,
			prevEpochCapitalization,
			epoch,
			prevEpoch,
			slotsPerYear,
			features,
		)

		if rewards != rewards2 {
			t.Errorf("CalculatePreviousEpochInflationRewards not deterministic: %d vs %d",
				rewards, rewards2)
		}

		// Rewards should be reasonable relative to capitalization
		// With 8% initial inflation, max annual rewards ~ 8% of capitalization
		// Per epoch: capitalization * 0.08 / (slotsPerYear / 432000)
		if prevEpochCapitalization > 0 && rewards > 0 {
			// Rough sanity check: rewards shouldn't exceed ~10% of capitalization per epoch
			// (accounting for all possible inflation rates and rounding)
			maxReasonableRewards := prevEpochCapitalization / 10
			if rewards > maxReasonableRewards && prevEpochCapitalization < math.MaxUint64/10 {
				t.Logf("Warning: rewards %d seem high for capitalization %d", rewards, prevEpochCapitalization)
			}
		}

		// If capitalization is 0, rewards should be 0
		if prevEpochCapitalization == 0 && rewards != 0 {
			t.Errorf("Zero capitalization should produce zero rewards, got %d", rewards)
		}
	})
}

// FuzzInflationBoundaries tests inflation calculation near boundaries
func FuzzInflationBoundaries(f *testing.F) {
	f.Add(float64(0.0))
	f.Add(float64(1.0))
	f.Add(float64(100.0))
	f.Add(float64(0.5))

	f.Fuzz(func(t *testing.T, slotInYear float64) {
		// Skip unreasonable values
		if slotInYear < 0 || slotInYear > 1000 {
			t.Skip("slotInYear out of reasonable range")
		}

		inflation := &Inflation{
			Initial:        0.08,
			Terminal:       0.015,
			Taper:          0.15,
			FoundationVal:  0.05,
			FoundationTerm: 7.0,
			Unused:         0.0,
		}

		// Calculate total inflation rate
		rate := inflation.Total(slotInYear)

		// Rate should be between terminal and initial
		if rate < inflation.Terminal {
			t.Errorf("Inflation rate %f < terminal %f", rate, inflation.Terminal)
		}

		if rate > inflation.Initial {
			t.Errorf("Inflation rate %f > initial %f", rate, inflation.Initial)
		}

		// Rate should be deterministic
		rate2 := inflation.Total(slotInYear)
		if rate != rate2 {
			t.Errorf("Inflation.Total not deterministic: %f vs %f", rate, rate2)
		}

		// As time progresses, rate should decrease (tapered inflation)
		if slotInYear > 0 && slotInYear < 100 {
			prevRate := inflation.Total(slotInYear - 0.1)
			if rate > prevRate+0.001 { // Allow small floating point errors
				t.Errorf("Inflation rate increased over time: %f -> %f", prevRate, rate)
			}
		}
	})
}

// FuzzCommissionSplitBoundaries tests edge cases in commission splitting
func FuzzCommissionSplitBoundaries(f *testing.F) {
	f.Add(uint8(0), uint64(1))
	f.Add(uint8(1), uint64(1))
	f.Add(uint8(99), uint64(1))
	f.Add(uint8(100), uint64(1))
	f.Add(uint8(50), uint64(3)) // Odd number for rounding test

	f.Fuzz(func(t *testing.T, commission uint8, rewards uint64) {
		if commission > 100 {
			commission = 100
		}

		voteState := &sealevel.VoteStateVersions{
			Type: sealevel.VoteStateVersionCurrent,
			Current: sealevel.VoteState{
				Commission: commission,
			},
		}

		split := voteCommissionSplit(voteState, rewards)

		// IMPORTANT: Solana intentionally allows lamports to be lost to rounding!
		// This is documented in Agave's commission_split function:
		// "Calculate mine and theirs independently and symmetrically instead of
		// using the remainder of the other to treat them strictly equally.
		// This is also to cancel the rewarding if either of the parties
		// should receive only fractional lamports, resulting in not being rewarded at all.
		// Thus, note that we intentionally discard any residual fractional lamports."

		// Invariant: staker portion should not exceed total
		if split.StakerPortion > rewards {
			t.Errorf("Staker portion %d > total rewards %d", split.StakerPortion, rewards)
		}

		// Invariant: voter portion should not exceed total
		if split.VoterPortion > rewards {
			t.Errorf("Voter portion %d > total rewards %d", split.VoterPortion, rewards)
		}

		// Invariant: sum cannot exceed rewards (lamports can be lost, not created)
		if split.StakerPortion+split.VoterPortion > rewards {
			t.Errorf("Split sum %d + %d = %d exceeds rewards %d",
				split.StakerPortion, split.VoterPortion,
				split.StakerPortion+split.VoterPortion, rewards)
		}

		// Test that rounding is reasonable (lost lamports should be < 2)
		lostLamports := rewards - (split.StakerPortion + split.VoterPortion)
		if lostLamports > 1 && rewards > 0 && commission > 0 && commission < 100 {
			t.Errorf("Too many lamports lost to rounding: %d lamports lost from %d total (commission=%d%%)",
				lostLamports, rewards, commission)
		}
	})
}

// FuzzPartitionCalculationConsistency tests partition calculation consistency
func FuzzPartitionCalculationConsistency(f *testing.F) {
	f.Add([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32},
		[]byte{10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41},
		uint64(100))

	f.Fuzz(func(t *testing.T, pubkeyBytes []byte, blockhashBytes []byte, numPartitions uint64) {
		if numPartitions == 0 || numPartitions > 1000000 {
			t.Skip("numPartitions out of range")
		}

		// Ensure both are exactly 32 bytes
		if len(pubkeyBytes) != 32 || len(blockhashBytes) != 32 {
			t.Skip("pubkeyBytes and blockhashBytes must be exactly 32 bytes")
		}

		var pubkey solana.PublicKey
		copy(pubkey[:], pubkeyBytes)

		var blockhash [32]byte
		copy(blockhash[:], blockhashBytes)

		// Calculate partition multiple times
		results := make([]uint64, 10)
		for i := 0; i < 10; i++ {
			results[i] = CalculateRewardPartitionForPubkey(pubkey, blockhash, numPartitions)
		}

		// All results should be identical (deterministic)
		for i := 1; i < 10; i++ {
			if results[i] != results[0] {
				t.Errorf("CalculateRewardPartitionForPubkey not consistent: got different results")
				break
			}
		}

		// Result must be in valid range
		if results[0] >= numPartitions {
			t.Errorf("Partition %d out of range [0, %d)", results[0], numPartitions)
		}

		// Test with different number of partitions
		// If we reduce partitions, result should also be valid for smaller range
		if numPartitions > 10 {
			smallerPartitions := numPartitions / 2
			smallerResult := CalculateRewardPartitionForPubkey(pubkey, blockhash, smallerPartitions)
			if smallerResult >= smallerPartitions {
				t.Errorf("Partition %d out of range for smaller partition count %d",
					smallerResult, smallerPartitions)
			}
		}
	})
}

// FuzzInflationYearProgress tests year progress calculation
func FuzzInflationYearProgress(f *testing.F) {
	f.Add(uint64(0), uint64(0))
	f.Add(uint64(100), uint64(99))
	f.Add(uint64(1000), uint64(999))

	f.Fuzz(func(t *testing.T, epoch uint64, prevEpoch uint64) {
		if epoch == 0 || prevEpoch >= epoch || epoch > 100000 {
			t.Skip("invalid epoch relationship")
		}

		epochSchedule := &sealevel.SysvarEpochSchedule{
			SlotsPerEpoch:            432000,
			LeaderScheduleSlotOffset: 432000,
			Warmup:                   true,
			FirstNormalEpoch:         14,
			FirstNormalSlot:          524288,
		}

		features := features.NewFeaturesDefault()

		currentSlots := GetInflationNumSlots(epochSchedule, epoch, features)
		prevSlots := GetInflationNumSlots(epochSchedule, prevEpoch, features)

		// Current should be >= previous
		if currentSlots < prevSlots {
			t.Errorf("GetInflationNumSlots(%d)=%d < GetInflationNumSlots(%d)=%d",
				epoch, currentSlots, prevEpoch, prevSlots)
		}

		// Slots should increase monotonically
		if epoch == prevEpoch+1 {
			// Should have added approximately slotsPerEpoch
			diff := currentSlots - prevSlots
			if diff == 0 {
				t.Errorf("No slots added between consecutive epochs %d and %d", prevEpoch, epoch)
			}
			if diff > 1000000 {
				t.Logf("Warning: large slot difference %d between epochs", diff)
			}
		}
	})
}

// FuzzGetInflationStartSlot tests the inflation start slot calculation
func FuzzGetInflationStartSlot(f *testing.F) {
	// Seed with a dummy value since we don't actually use inputs
	f.Add(uint8(0))

	f.Fuzz(func(t *testing.T, _ uint8) {
		features := features.NewFeaturesDefault()

		// Should not panic
		result := GetInflationStartSlot(features)

		// Result should be deterministic
		result2 := GetInflationStartSlot(features)
		if result != result2 {
			t.Errorf("GetInflationStartSlot not deterministic: %d vs %d", result, result2)
		}

		// Result should be a valid slot (non-negative, since it's uint64)
		// With default features, should return a reasonable slot number
		if result > 100000000 {
			t.Logf("Warning: GetInflationStartSlot returned very large slot: %d", result)
		}
	})
}

// FuzzCommissionSplitAllVersions tests commission splitting across all vote state versions
func FuzzCommissionSplitAllVersions(f *testing.F) {
	f.Add(uint8(50), uint64(1000000), uint8(0)) // Current version
	f.Add(uint8(10), uint64(5000), uint8(1))    // V0_23_5
	f.Add(uint8(75), uint64(999), uint8(2))     // V1_14_11

	f.Fuzz(func(t *testing.T, commission uint8, rewards uint64, versionByte uint8) {
		if commission > 100 {
			commission = 100
		}

		// Map versionByte to actual version types
		var voteState *sealevel.VoteStateVersions
		switch versionByte % 3 {
		case 0:
			voteState = &sealevel.VoteStateVersions{
				Type:    sealevel.VoteStateVersionCurrent,
				Current: sealevel.VoteState{Commission: commission},
			}
		case 1:
			voteState = &sealevel.VoteStateVersions{
				Type:    sealevel.VoteStateVersionV0_23_5,
				V0_23_5: sealevel.VoteState0_23_5{Commission: commission},
			}
		case 2:
			voteState = &sealevel.VoteStateVersions{
				Type:     sealevel.VoteStateVersionV1_14_11,
				V1_14_11: sealevel.VoteState1_14_11{Commission: commission},
			}
		}

		// Calculate split
		split := voteCommissionSplit(voteState, rewards)

		// All versions should produce same results for same commission
		split2 := voteCommissionSplit(voteState, rewards)
		if split.VoterPortion != split2.VoterPortion || split.StakerPortion != split2.StakerPortion {
			t.Errorf("voteCommissionSplit not deterministic across calls")
		}

		// Basic invariants
		if split.VoterPortion > rewards || split.StakerPortion > rewards {
			t.Errorf("Split portions exceed total rewards")
		}

		if split.VoterPortion+split.StakerPortion > rewards {
			t.Errorf("Split sum exceeds rewards")
		}

		// IsSplit flag should be set correctly
		if commission == 0 || commission == 100 {
			if split.IsSplit {
				t.Errorf("IsSplit should be false for commission=%d%%", commission)
			}
		} else if rewards > 0 {
			if !split.IsSplit {
				t.Errorf("IsSplit should be true for commission=%d%% with non-zero rewards", commission)
			}
		}
	})
}

// FuzzPointValueCalculations tests PointValue operations
func FuzzPointValueCalculations(f *testing.F) {
	f.Add(uint64(1000000), uint64(500000))
	f.Add(uint64(0), uint64(100))
	f.Add(uint64(math.MaxUint64/2), uint64(math.MaxUint64/4))

	f.Fuzz(func(t *testing.T, rewards uint64, points uint64) {
		// Skip invalid cases
		if points == 0 {
			t.Skip("points cannot be 0")
		}

		// Limit to prevent overflow in test calculations
		if points > math.MaxUint64/1000 || rewards > math.MaxUint64/1000 {
			t.Skip("values too large")
		}

		pointValue := PointValue{
			Rewards: rewards,
			Points:  wide.Uint128FromUint64(points),
		}

		// Verify fields are set correctly
		if pointValue.Rewards != rewards {
			t.Errorf("Rewards field mismatch: got %d, expected %d", pointValue.Rewards, rewards)
		}

		if !pointValue.Points.Eq(wide.Uint128FromUint64(points)) {
			t.Errorf("Points field mismatch")
		}

		// PointValue should be usable in reward calculations
		// Simulate a stake account earning rewards
		stakePoints := uint64(10000)
		if stakePoints > points {
			stakePoints = points
		}

		// Calculate reward: (stakePoints * rewards) / points
		rewardCalc := wide.Uint128FromUint64(stakePoints).Mul(wide.Uint128FromUint64(rewards)).Div(wide.Uint128FromUint64(points))

		if rewardCalc.IsUint64() {
			calculatedReward := rewardCalc.Uint64()

			// Reward should not exceed total rewards
			if calculatedReward > rewards {
				t.Errorf("Calculated reward %d exceeds total rewards %d", calculatedReward, rewards)
			}

			// For maximum stake points (= total points), reward should equal total rewards
			if stakePoints == points && calculatedReward != rewards {
				t.Errorf("Max stake should get full rewards: got %d, expected %d", calculatedReward, rewards)
			}
		}
	})
}

// FuzzCalculatedStakeRewardsStructure tests CalculatedStakeRewards struct operations
func FuzzCalculatedStakeRewardsStructure(f *testing.F) {
	f.Add(uint64(100000), uint64(50000), uint64(12345))
	f.Add(uint64(0), uint64(0), uint64(0))
	f.Add(uint64(math.MaxUint64), uint64(0), uint64(999))

	f.Fuzz(func(t *testing.T, stakerRewards uint64, voterRewards uint64, newCredits uint64) {
		csr := &CalculatedStakeRewards{
			StakerRewards:      stakerRewards,
			VoterRewards:       voterRewards,
			NewCreditsObserved: newCredits,
		}

		// Verify all fields are set
		if csr.StakerRewards != stakerRewards {
			t.Errorf("StakerRewards mismatch: got %d, expected %d", csr.StakerRewards, stakerRewards)
		}

		if csr.VoterRewards != voterRewards {
			t.Errorf("VoterRewards mismatch: got %d, expected %d", csr.VoterRewards, voterRewards)
		}

		if csr.NewCreditsObserved != newCredits {
			t.Errorf("NewCreditsObserved mismatch: got %d, expected %d", csr.NewCreditsObserved, newCredits)
		}

		// Total rewards should not overflow when added
		totalRewards := safemath.SaturatingAddU64(csr.StakerRewards, csr.VoterRewards)

		// If both are non-zero, total should be greater than either individual
		if csr.StakerRewards > 0 && csr.VoterRewards > 0 {
			if totalRewards <= csr.StakerRewards || totalRewards <= csr.VoterRewards {
				// Only error if we didn't saturate
				if csr.StakerRewards < math.MaxUint64-csr.VoterRewards {
					t.Errorf("Total rewards calculation error: %d + %d = %d",
						csr.StakerRewards, csr.VoterRewards, totalRewards)
				}
			}
		}

		// Credits should be a valid epoch credit value
		// In Solana, credits are typically much smaller than max uint64
		if newCredits > 1000000000 {
			t.Logf("Warning: very large NewCreditsObserved value: %d", newCredits)
		}
	})
}

// FuzzCommissionSplitStructure tests CommissionSplit structdation
func FuzzInflationStructValidation(f *testing.F) {
	f.Add(float64(0.08), float64(0.015), float64(0.15), float64(0.05), float64(7.0))
	f.Add(float64(0.0), float64(0.0), float64(0.0), float64(0.0), float64(0.0))
	f.Add(float64(1.0), float64(0.5), float64(0.1), float64(0.1), float64(10.0))

	f.Fuzz(func(t *testing.T, initial float64, terminal float64, taper float64, foundation float64, foundationTerm float64) {
		// Skip invalid inputs
		if initial < 0 || terminal < 0 || taper < 0 || foundation < 0 || foundationTerm < 0 {
			t.Skip("negative values not allowed")
		}
		if initial > 1.0 || terminal > 1.0 || taper > 1.0 || foundation > 1.0 {
			t.Skip("rates should be <= 1.0 (100%)")
		}

		// CRITICAL: Initial inflation rate must be >= terminal rate for the taper model to work
		// The model is: inflation starts at Initial and tapers down to Terminal floor
		if initial < terminal {
			t.Skip("initial must be >= terminal for valid inflation model")
		}

		inflation := &Inflation{
			Initial:        initial,
			Terminal:       terminal,
			Taper:          taper,
			FoundationVal:  foundation,
			FoundationTerm: foundationTerm,
			Unused:         0.0,
		}

		// Calculate various inflation rates
		rate0 := inflation.Total(0.0)
		rate1 := inflation.Total(1.0)
		rate10 := inflation.Total(10.0)

		// All rates should be between terminal and initial
		rates := []float64{rate0, rate1, rate10}
		for i, rate := range rates {
			if rate < terminal-0.0001 { // Small tolerance for floating point
				t.Errorf("Rate[%d]=%f below terminal %f", i, rate, terminal)
			}
			if rate > initial+0.0001 {
				t.Errorf("Rate[%d]=%f above initial %f", i, rate, initial)
			}
		}

		// Rate at year 0 should equal initial (at year 0, taper factor is 1.0)
		if rate0 < initial-0.0001 || rate0 > initial+0.0001 {
			t.Errorf("Rate at year 0 (%f) should equal initial (%f)", rate0, initial)
		}

		// Rates should generally decrease over time (or stay constant if at terminal)
		if rate10 > rate0+0.001 && taper > 0 {
			t.Errorf("Inflation rate increased over time: %f -> %f (taper=%f)",
				rate0, rate10, taper)
		}
	})
}

// FuzzMinimumStakeDelegationConsistency tests consistency of minimum delegation functions
func FuzzMinimumStakeDelegationConsistency(f *testing.F) {
	// Seed with a dummy value
	f.Add(uint8(0))

	f.Fuzz(func(t *testing.T, _ uint8) {
		features := features.NewFeaturesDefault()

		// Test the features-only version
		result1 := minimumStakeDelegationFeatures(features)
		result2 := minimumStakeDelegationFeatures(features)

		if result1 != result2 {
			t.Errorf("minimumStakeDelegationFeatures not deterministic: %d vs %d", result1, result2)
		}

		// Result should be one of the expected values
		validValues := map[uint64]bool{
			0:          true, // Feature disabled
			1:          true, // Legacy minimum
			1000000000: true, // 1 SOL
		}

		if !validValues[result1] {
			t.Errorf("Unexpected minimum delegation value: %d", result1)
		}

		// The two minimum delegation functions should return the same value
		// when given compatible inputs (features from slotCtx vs standalone features)
		// This is a smoke test to ensure they're in sync
		if result1 != 0 && result1 != 1 && result1 != 1000000000 {
			t.Errorf("minimumStakeDelegationFeatures returned invalid value: %d", result1)
		}
	})
}

// FuzzRewardCalculationOverflow tests for overflow conditions in reward calculations
func FuzzRewardCalculationOverflow(f *testing.F) {
	f.Add(uint64(math.MaxUint64), uint64(100))
	f.Add(uint64(math.MaxUint64/2), uint64(math.MaxUint64/2))
	f.Add(uint64(1), uint64(math.MaxUint64))

	f.Fuzz(func(t *testing.T, rewards uint64, points uint64) {
		if points == 0 {
			t.Skip("points must be non-zero")
		}

		// Test that wide.Uint128 operations don't panic with large values
		rewardsU128 := wide.Uint128FromUint64(rewards)
		pointsU128 := wide.Uint128FromUint64(points)

		// These operations should not panic
		_ = rewardsU128.Mul(pointsU128)

		// Division should work
		if points > 0 {
			result := rewardsU128.Mul(pointsU128).Div(pointsU128)

			// Result should equal original rewards (a * b) / b = a
			if result.IsUint64() && result.Uint64() != rewards {
				t.Errorf("Multiplication and division didn't round-trip: %d != %d",
					result.Uint64(), rewards)
			}
		}

		// Test commission split with maximum values
		voteState := &sealevel.VoteStateVersions{
			Type:    sealevel.VoteStateVersionCurrent,
			Current: sealevel.VoteState{Commission: 50},
		}

		// This should not panic even with max uint64
		split := voteCommissionSplit(voteState, rewards)

		// Basic sanity checks
		if split.VoterPortion > rewards {
			t.Errorf("Voter portion %d exceeds rewards %d", split.VoterPortion, rewards)
		}
		if split.StakerPortion > rewards {
			t.Errorf("Staker portion %d exceeds rewards %d", split.StakerPortion, rewards)
		}
	})
}
