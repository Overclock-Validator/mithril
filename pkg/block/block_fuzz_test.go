package block

import (
	"testing"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

// FuzzBlockRewardRewards tests reward extraction from block rewards
func FuzzBlockRewardRewards(f *testing.F) {
	// Seed with various reward scenarios
	f.Add(uint8(0), int64(0), uint64(0))     // No rewards
	f.Add(uint8(1), int64(100), uint64(100)) // Single reward
	f.Add(uint8(2), int64(-50), uint64(50))  // Negative reward

	f.Fuzz(func(t *testing.T, rewardType uint8, lamports int64, postBalance uint64) {
		// Map rewardType to valid types (0-3)
		rewardType = rewardType % 4

		var rewardTypeEnum rpc.RewardType
		switch rewardType {
		case 0:
			rewardTypeEnum = rpc.RewardTypeFee
		case 1:
			rewardTypeEnum = rpc.RewardTypeRent
		case 2:
			rewardTypeEnum = rpc.RewardTypeVoting
		case 3:
			rewardTypeEnum = rpc.RewardTypeStaking
		}

		// Create test rewards
		rewards := []rpc.BlockReward{
			{
				Pubkey:      solana.PublicKey{1, 2, 3},
				Lamports:    lamports,
				PostBalance: postBalance,
				RewardType:  rewardTypeEnum,
			},
		}

		// Test blockRewardRewards - should not panic
		result := blockRewardRewards(rewards)

		// blockRewardRewards specifically looks for "Fee" type rewards
		// It returns nil if no Fee reward is found
		if rewardType == 0 { // RewardTypeFee
			// Should return the fee reward
			if result == nil {
				t.Errorf("Expected non-nil result for Fee reward")
			} else {
				// Verify it matches input
				if result.Lamports != lamports {
					t.Errorf("Lamports mismatch: got %d, expected %d", result.Lamports, lamports)
				}
				if result.PostBalance != postBalance {
					t.Errorf("PostBalance mismatch: got %d, expected %d", result.PostBalance, postBalance)
				}
			}
		} else {
			// For non-Fee rewards, should return nil
			if result != nil {
				t.Errorf("Expected nil result for non-Fee reward (type=%v), got %+v", rewardTypeEnum, result)
			}
		}
	})
}

// FuzzBlockRewardRewardsMultiple tests multiple rewards handling
func FuzzBlockRewardRewardsMultiple(f *testing.F) {
	f.Add(uint8(2), int64(100), int64(200), uint64(1000), uint64(2000))

	f.Fuzz(func(t *testing.T, count uint8, lamports1 int64, lamports2 int64, balance1 uint64, balance2 uint64) {
		// Limit count to reasonable number
		if count > 10 {
			count = 10
		}
		if count == 0 {
			t.Skip("Need at least one reward")
		}

		// Create multiple rewards
		rewards := make([]rpc.BlockReward, count)
		feeType := rpc.RewardTypeFee

		for i := uint8(0); i < count; i++ {
			lamports := lamports1
			balance := balance1
			if i%2 == 1 {
				lamports = lamports2
				balance = balance2
			}

			rewards[i] = rpc.BlockReward{
				Pubkey:      solana.PublicKey{byte(i)},
				Lamports:    lamports,
				PostBalance: balance,
				RewardType:  feeType,
			}
		}

		// Test - should not panic
		result := blockRewardRewards(rewards)

		// Should return first reward
		if result == nil {
			t.Errorf("Expected non-nil result for %d rewards", count)
		} else {
			if result.Lamports != lamports1 {
				t.Errorf("Expected first reward lamports %d, got %d", lamports1, result.Lamports)
			}
		}
	})
}

// FuzzBlockRewardRewardsEmpty tests empty rewards handling
func FuzzBlockRewardRewardsEmpty(f *testing.F) {
	f.Add(uint8(0))

	f.Fuzz(func(t *testing.T, dummy uint8) {
		// Test with empty rewards slice
		rewards := []rpc.BlockReward{}

		// Should not panic
		result := blockRewardRewards(rewards)

		// Should return nil for empty rewards
		if result != nil {
			t.Errorf("Expected nil result for empty rewards, got %v", result)
		}
	})
}

// FuzzBlockRewardTypeParsing tests reward type handling
func FuzzBlockRewardTypeParsing(f *testing.F) {
	f.Add(uint8(0), int64(100))
	f.Add(uint8(1), int64(200))
	f.Add(uint8(2), int64(300))
	f.Add(uint8(3), int64(400))

	f.Fuzz(func(t *testing.T, rewardTypeVal uint8, lamports int64) {
		// Map to valid reward types (0-3)
		rewardTypeVal = rewardTypeVal % 4

		var rewardType rpc.RewardType
		var expectedType rpc.RewardType

		switch rewardTypeVal {
		case 0:
			rewardType = rpc.RewardTypeFee
			expectedType = rpc.RewardTypeFee
		case 1:
			rewardType = rpc.RewardTypeRent
			expectedType = rpc.RewardTypeRent
		case 2:
			rewardType = rpc.RewardTypeVoting
			expectedType = rpc.RewardTypeVoting
		case 3:
			rewardType = rpc.RewardTypeStaking
			expectedType = rpc.RewardTypeStaking
		}

		rewards := []rpc.BlockReward{
			{
				Pubkey:      solana.PublicKey{1},
				Lamports:    lamports,
				PostBalance: uint64(lamports),
				RewardType:  rewardType,
			},
		}

		// Test
		result := blockRewardRewards(rewards)

		if result != nil {
			if result.RewardType != expectedType {
				t.Errorf("Reward type mismatch: got %v, expected %v", result.RewardType, expectedType)
			}
		}
	})
}

// FuzzBlockRewardNilType tests handling of nil reward type
func FuzzBlockRewardNilType(f *testing.F) {
	f.Add(int64(100), uint64(200))

	f.Fuzz(func(t *testing.T, lamports int64, postBalance uint64) {
		// Create reward with empty RewardType (default zero value)
		rewards := []rpc.BlockReward{
			{
				Pubkey:      solana.PublicKey{1, 2, 3},
				Lamports:    lamports,
				PostBalance: postBalance,
				RewardType:  "", // Empty string (zero value for string type)
			},
		}

		// Should not panic even with empty reward type
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Panic with empty reward type: %v", r)
			}
		}()

		result := blockRewardRewards(rewards)

		// Result might be nil or have empty RewardType, both are acceptable
		_ = result
	})
}

// FuzzBlockRewardCommission tests commission field handling
func FuzzBlockRewardCommission(f *testing.F) {
	f.Add(uint8(0), int64(100))
	f.Add(uint8(50), int64(200))
	f.Add(uint8(100), int64(300))
	f.Add(uint8(255), int64(400))

	f.Fuzz(func(t *testing.T, commission uint8, lamports int64) {
		stakingType := rpc.RewardTypeStaking
		commissionPtr := &commission

		rewards := []rpc.BlockReward{
			{
				Pubkey:      solana.PublicKey{1, 2, 3},
				Lamports:    lamports,
				PostBalance: uint64(lamports),
				RewardType:  stakingType,
				Commission:  commissionPtr,
			},
		}

		// Should not panic
		result := blockRewardRewards(rewards)

		if result != nil {
			if result.Commission == nil {
				t.Errorf("Expected commission to be preserved")
			} else if *result.Commission != commission {
				t.Errorf("Commission mismatch: got %d, expected %d", *result.Commission, commission)
			}
		}
	})
}

// FuzzBlockRewardLamportOverflow tests large lamport values
func FuzzBlockRewardLamportOverflow(f *testing.F) {
	f.Add(int64(9223372036854775807))  // Max int64
	f.Add(int64(-9223372036854775808)) // Min int64
	f.Add(int64(0))
	f.Add(int64(1))
	f.Add(int64(-1))

	f.Fuzz(func(t *testing.T, lamports int64) {
		feeType := rpc.RewardTypeFee

		rewards := []rpc.BlockReward{
			{
				Pubkey:      solana.PublicKey{1},
				Lamports:    lamports,
				PostBalance: 0, // PostBalance is uint64, can't be negative
				RewardType:  feeType,
			},
		}

		// Should handle all int64 values without panic
		result := blockRewardRewards(rewards)

		if result != nil {
			if result.Lamports != lamports {
				t.Errorf("Lamports not preserved: got %d, expected %d", result.Lamports, lamports)
			}
		}
	})
}

// FuzzBlockRewardPostBalanceOverflow tests large post balance values
func FuzzBlockRewardPostBalanceOverflow(f *testing.F) {
	f.Add(uint64(18446744073709551615)) // Max uint64
	f.Add(uint64(0))
	f.Add(uint64(1))
	f.Add(uint64(1000000000000)) // 1 trillion

	f.Fuzz(func(t *testing.T, postBalance uint64) {
		votingType := rpc.RewardTypeVoting

		rewards := []rpc.BlockReward{
			{
				Pubkey:      solana.PublicKey{1, 2, 3},
				Lamports:    100,
				PostBalance: postBalance,
				RewardType:  votingType,
			},
		}

		// Should handle all uint64 values without panic
		result := blockRewardRewards(rewards)

		if result != nil {
			if result.PostBalance != postBalance {
				t.Errorf("PostBalance not preserved: got %d, expected %d", result.PostBalance, postBalance)
			}
		}
	})
}

// FuzzBlockRewardPubkeyVariety tests different pubkey values
func FuzzBlockRewardPubkeyVariety(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	f.Add([]byte{255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255})

	f.Fuzz(func(t *testing.T, pubkeyBytes []byte) {
		// Ensure pubkey is exactly 32 bytes
		if len(pubkeyBytes) != 32 {
			t.Skip("Pubkey must be 32 bytes")
		}

		var pubkey solana.PublicKey
		copy(pubkey[:], pubkeyBytes)

		feeType := rpc.RewardTypeFee
		rewards := []rpc.BlockReward{
			{
				Pubkey:      pubkey,
				Lamports:    100,
				PostBalance: 200,
				RewardType:  feeType,
			},
		}

		// Should not panic with any pubkey
		result := blockRewardRewards(rewards)

		if result != nil {
			if result.Pubkey != pubkey {
				t.Errorf("Pubkey not preserved")
			}
		}
	})
}
