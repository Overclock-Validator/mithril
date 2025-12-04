package rent

import (
	"math"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// FuzzRentStateFromAcct tests rent state classification
func FuzzRentStateFromAcct(f *testing.F) {
	// Seed with various account states
	f.Add(uint64(0), uint64(0))                  // Zero lamports
	f.Add(uint64(1000000), uint64(100))          // Small balance, small data
	f.Add(uint64(10000000), uint64(1000))        // Larger balance
	f.Add(uint64(math.MaxUint64), uint64(10000)) // Max lamports

	f.Fuzz(func(t *testing.T, lamports, dataSize uint64) {
		// Limit data size to reasonable values
		if dataSize > 10*1024*1024 {
			t.Skip("Data size too large")
		}

		// Create test account
		acct := &accounts.Account{
			Lamports: lamports,
			Data:     make([]byte, dataSize),
			Key:      solana.PublicKey{},
		}

		// Create default rent sysvar
		rent := &sealevel.SysvarRent{}
		rent.InitializeDefault()

		// Test rentStateFromAcct
		state := rentStateFromAcct(acct, rent)

		// Verify state consistency
		if lamports == 0 {
			if state.RentState != RentStateUninitialized {
				t.Errorf("Expected Uninitialized state for zero lamports, got %d", state.RentState)
			}
		} else if rent.IsExempt(lamports, dataSize) {
			if state.RentState != RentStateRentExempt {
				t.Errorf("Expected RentExempt state, got %d", state.RentState)
			}
		} else {
			if state.RentState != RentStateRentPaying {
				t.Errorf("Expected RentPaying state, got %d", state.RentState)
			}
		}

		// Verify RentPayingInfo is populated correctly
		if state.RentPayingInfo.Lamports != lamports {
			t.Errorf("RentPayingInfo lamports mismatch")
		}
		if state.RentPayingInfo.DataSize != dataSize {
			t.Errorf("RentPayingInfo data size mismatch")
		}
	})
}

// FuzzCalculateRentResult tests rent calculation logic
func FuzzCalculateRentResult(f *testing.F) {
	// Seed with various rent epoch and balance combinations
	f.Add(uint64(0), uint64(0), uint64(100), uint64(1000000), false)
	f.Add(uint64(1), uint64(0), uint64(100), uint64(1000000), false)
	f.Add(uint64(0), uint64(18446744073709551615), uint64(100), uint64(1000000), false)
	f.Add(uint64(0), uint64(0), uint64(100), uint64(0), true)

	f.Fuzz(func(t *testing.T, epoch, rentEpoch, dataSize, lamports uint64, executable bool) {
		// Limit data size to reasonable values
		if dataSize > 10*1024*1024 {
			t.Skip("Data size too large")
		}

		// Create test slot context (minimal)
		feats := make(features.Features)
		slotCtx := &sealevel.SlotCtx{
			Epoch:    epoch,
			Features: &feats,
		}

		// Create test account
		acct := &accounts.Account{
			Lamports:   lamports,
			Data:       make([]byte, dataSize),
			RentEpoch:  rentEpoch,
			Executable: executable,
			Key:        solana.PublicKey{},
		}

		// Create default rent sysvar
		rent := &sealevel.SysvarRent{}
		rent.InitializeDefault()

		// Test calculateRentResult
		result := calculateRentResult(slotCtx, rent, acct)

		// Verify logic
		if rentEpoch == math.MaxUint64 || rentEpoch > epoch {
			if result != RentNoCollectionNow {
				t.Errorf("Expected RentNoCollectionNow for future rent epoch")
			}
		} else if executable || acct.Key == a.IncineratorAddr {
			if result != RentExempt {
				t.Errorf("Expected RentExempt for executable account")
			}
		} else if lamports >= rent.MinimumBalance(dataSize) {
			if result != RentExempt {
				t.Errorf("Expected RentExempt for sufficient balance")
			}
		} else {
			// TODO: implement rent collection logic
			if result != RentExempt {
				t.Errorf("Expected RentExempt (current implementation)")
			}
		}
	})
}

// FuzzPartitionIdxFromSlotIdx tests partition index calculation
func FuzzPartitionIdxFromSlotIdx(f *testing.F) {
	// Seed with various slot and epoch configurations
	f.Add(uint64(0), uint64(0), uint64(432000), uint64(0), uint64(1))
	f.Add(uint64(100), uint64(5), uint64(432000), uint64(0), uint64(1))
	f.Add(uint64(431999), uint64(10), uint64(432000), uint64(0), uint64(1))
	f.Add(uint64(100), uint64(5), uint64(432000), uint64(0), uint64(3)) // Multi-epoch cycle

	f.Fuzz(func(t *testing.T, slotIdx, epoch, slotsPerEpoch, baseEpoch, epochCountPerCycle uint64) {
		// Prevent division by zero
		if slotsPerEpoch == 0 || epochCountPerCycle == 0 {
			t.Skip("Invalid zero parameters")
		}

		// Limit to reasonable values
		if slotIdx >= slotsPerEpoch || epoch > 10000 || slotsPerEpoch > 1000000 {
			t.Skip("Parameters too large")
		}

		// IMPORTANT: PartitionCount must equal EpochCountPerCycle * SlotsPerEpoch
		// This is an invariant in the actual Solana code
		partitionCount := epochCountPerCycle * slotsPerEpoch

		cycleParams := RentCollectionCycleParams{
			Epoch:              epoch,
			SlotCountPerEpoch:  slotsPerEpoch,
			MultiEpochCycle:    epochCountPerCycle > 1,
			BaseEpoch:          baseEpoch,
			EpochCountPerCycle: epochCountPerCycle,
			PartitionCount:     partitionCount,
		}

		// Test partitionIdxFromSlotIdx
		partitionIdx := partitionIdxFromSlotIdx(slotIdx, cycleParams)

		// Verify result is within bounds
		// BUG DETECTOR: If this fails, partitionIdxFromSlotIdx is not handling multi-epoch cycles correctly
		if partitionIdx >= partitionCount {
			t.Errorf("BUG: Partition index %d exceeds partition count %d (epoch=%d, slotIdx=%d, epochCountPerCycle=%d)",
				partitionIdx, partitionCount, epoch, slotIdx, epochCountPerCycle)
		}

		// Verify calculation matches expected formula
		if epoch >= baseEpoch {
			epochOffset := epoch - baseEpoch
			epochIdxInCycle := epochOffset % epochCountPerCycle
			expected := slotIdx + (epochIdxInCycle * slotsPerEpoch)

			if partitionIdx != expected {
				t.Errorf("Partition index %d doesn't match expected %d", partitionIdx, expected)
			}

			// With correct partitionCount, expected should always be in bounds
			if expected >= partitionCount {
				t.Errorf("INVARIANT VIOLATION: Expected partition %d >= partition count %d", expected, partitionCount)
			}
		}
	})
}

// FuzzPubkeyRangeFromPartition tests public key range calculation
func FuzzPubkeyRangeFromPartition(f *testing.F) {
	// Seed with various partition configurations
	f.Add(uint64(0), uint64(0), uint64(432000))
	f.Add(uint64(0), uint64(431999), uint64(432000))
	f.Add(uint64(100), uint64(200), uint64(432000))
	f.Add(uint64(431998), uint64(431999), uint64(432000))

	f.Fuzz(func(t *testing.T, startIdx, endIdx, partitionCount uint64) {
		// Skip invalid configurations
		if partitionCount == 0 {
			t.Skip("Partition count cannot be zero")
		}

		if startIdx >= partitionCount || endIdx >= partitionCount {
			t.Skip("Indices out of bounds")
		}

		if startIdx > endIdx {
			t.Skip("Start index greater than end index")
		}

		partition := Partition{
			StartIdx:       startIdx,
			EndIdx:         endIdx,
			PartitionCount: partitionCount,
		}

		// Test pubkeyRangeFromPartition - should not panic
		pkRange := pubkeyRangeFromPartition(partition)

		// Verify range properties
		if pkRange.EndPrefix < pkRange.StartPrefix {
			t.Errorf("End prefix %d less than start prefix %d", pkRange.EndPrefix, pkRange.StartPrefix)
		}

		// Verify pubkeys are within expected bounds
		// StartPubkey should be all zeros or have first 8 bytes set
		// EndPubkey should be all 0xff or have first 8 bytes set

		// For edge cases
		if startIdx == 0 && endIdx == 0 {
			if pkRange.StartPrefix != 0 {
				t.Errorf("Expected start prefix 0 for first partition")
			}
		}

		if endIdx+1 == partitionCount {
			if pkRange.EndPrefix != math.MaxUint64 {
				t.Errorf("Expected end prefix MaxUint64 for last partition")
			}
		}
	})
}

// FuzzShouldSetRentExemptRentEpochMax tests rent exemption logic
func FuzzShouldSetRentExemptRentEpochMax(f *testing.F) {
	// Seed with various account configurations
	f.Add(uint64(0), uint64(100), uint64(1000000), false, false)
	f.Add(uint64(1), uint64(100), uint64(1000000), false, false)
	f.Add(uint64(18446744073709551615), uint64(100), uint64(1000000), false, false)
	f.Add(uint64(0), uint64(100), uint64(1000000), true, false)

	f.Fuzz(func(t *testing.T, rentEpoch, dataSize, lamports uint64, executable, isDummy bool) {
		// Limit data size
		if dataSize > 10*1024*1024 {
			t.Skip("Data size too large")
		}

		// Create minimal test context
		feats := make(features.Features)
		slotCtx := &sealevel.SlotCtx{
			Epoch:    10, // Arbitrary epoch
			Features: &feats,
		}

		// Create test account
		acct := &accounts.Account{
			Lamports:   lamports,
			Data:       make([]byte, dataSize),
			RentEpoch:  rentEpoch,
			Executable: executable,
			IsDummy:    isDummy,
			Key:        solana.PublicKey{},
		}

		// Create default rent sysvar
		rent := &sealevel.SysvarRent{}
		rent.InitializeDefault()

		// Test ShouldSetRentExemptRentEpochMax
		result := ShouldSetRentExemptRentEpochMax(slotCtx, rent, &feats, acct)

		// Verify logic - shouldn't panic
		// Detailed verification would require understanding all feature flags
		_ = result
	})
}

// FuzzRentCollectionPartitions tests partition calculation for slots
func FuzzRentCollectionPartitions(f *testing.F) {
	// Seed with various slot ranges
	f.Add(uint64(0), uint64(0))
	f.Add(uint64(0), uint64(1))
	f.Add(uint64(1000), uint64(2000))
	f.Add(uint64(431999), uint64(432000)) // Epoch boundary

	f.Fuzz(func(t *testing.T, startSlot, endSlot uint64) {
		// Skip invalid ranges
		if startSlot > endSlot {
			t.Skip("Invalid slot range")
		}

		// Limit slot range to prevent excessive computation
		if endSlot-startSlot > 1000000 {
			t.Skip("Slot range too large")
		}

		// Create default epoch schedule (mainnet values)
		epochSchedule := &sealevel.SysvarEpochSchedule{
			SlotsPerEpoch:            432000,
			LeaderScheduleSlotOffset: 432000,
			Warmup:                   true,
			FirstNormalEpoch:         14,
			FirstNormalSlot:          524256,
		}

		// Test - may panic for cross-epoch ranges (expected)
		defer func() {
			if r := recover(); r != nil {
				// Expected for cross-epoch ranges
				if r != "cross epoch rent collection" {
					t.Errorf("Unexpected panic: %v", r)
				}
			}
		}()

		partitions := RentCollectionPartitions(startSlot, endSlot, epochSchedule)

		// If we got partitions, verify them
		if len(partitions) > 0 {
			for _, partition := range partitions {
				if partition.PartitionCount == 0 {
					t.Errorf("Partition count is zero")
				}
				if partition.EndIdx < partition.StartIdx {
					t.Errorf("Invalid partition range: start %d > end %d",
						partition.StartIdx, partition.EndIdx)
				}
			}
		}
	})
}

// FuzzCollectRentFromAcct tests rent collection from individual account
func FuzzCollectRentFromAcct(f *testing.F) {
	// Seed with various account states
	f.Add(uint64(0), uint64(0), uint64(100), uint64(1000000), false)
	f.Add(uint64(10), uint64(5), uint64(100), uint64(1000000), false)
	f.Add(uint64(10), uint64(18446744073709551615), uint64(100), uint64(1000000), false)

	f.Fuzz(func(t *testing.T, epoch, rentEpoch, dataSize, lamports uint64, executable bool) {
		// Limit data size
		if dataSize > 10*1024*1024 {
			t.Skip("Data size too large")
		}

		// Create minimal slot context
		feats := make(features.Features)
		slotCtx := &sealevel.SlotCtx{
			Epoch:    epoch,
			Features: &feats,
		}

		// Create test account
		acct := &accounts.Account{
			Lamports:   lamports,
			Data:       make([]byte, dataSize),
			RentEpoch:  rentEpoch,
			Executable: executable,
			Key:        solana.PublicKey{},
		}

		// Create default rent sysvar
		rent := &sealevel.SysvarRent{}
		rent.InitializeDefault()

		// Test collectRentFromAcct
		resultAcct, _ := collectRentFromAcct(slotCtx, rent, acct)

		// Verify rent epoch was updated if exempt and rent collection was due
		if resultAcct != nil {
			// Only check if rent collection was actually attempted (rentEpoch <= current epoch)
			if acct.RentEpoch <= slotCtx.Epoch || acct.RentEpoch == math.MaxUint64 {
				minBalance := rent.MinimumBalance(uint64(len(resultAcct.Data)))
				if resultAcct.Lamports >= minBalance && !acct.Executable && acct.Key != a.IncineratorAddr {
					if resultAcct.RentEpoch != math.MaxUint64 {
						t.Errorf("Expected rent epoch to be MaxUint64 for exempt account after rent collection was due (epoch=%d, rentEpoch=%d->%d, lamports=%d, minBalance=%d)",
							slotCtx.Epoch, acct.RentEpoch, resultAcct.RentEpoch, resultAcct.Lamports, minBalance)
					}
				}
			}
		}
	})
}

// FuzzRentMinimumBalance tests minimum balance calculation
func FuzzRentMinimumBalance(f *testing.F) {
	// Seed with various data sizes
	f.Add(uint64(0))
	f.Add(uint64(1))
	f.Add(uint64(100))
	f.Add(uint64(10000))
	f.Add(uint64(1048576)) // 1MB

	f.Fuzz(func(t *testing.T, dataSize uint64) {
		// Limit to reasonable size
		if dataSize > 10*1024*1024 {
			t.Skip("Data size too large")
		}

		// Create default rent sysvar
		rent := &sealevel.SysvarRent{}
		rent.InitializeDefault()

		// Test MinimumBalance
		minBalance := rent.MinimumBalance(dataSize)

		// Verify minimum balance scales with data size
		// Larger data should require more rent (or same for edge cases)
		if dataSize > 0 {
			smallerBalance := rent.MinimumBalance(dataSize - 1)
			if minBalance < smallerBalance {
				t.Errorf("Minimum balance should increase with data size")
			}
		}
	})
}

// FuzzRentIsExempt tests rent exemption check
func FuzzRentIsExempt(f *testing.F) {
	// Seed with various balance and data size combinations
	f.Add(uint64(0), uint64(0))
	f.Add(uint64(1000000), uint64(100))
	f.Add(uint64(10000000), uint64(1000))
	f.Add(uint64(18446744073709551615), uint64(10000))

	f.Fuzz(func(t *testing.T, balance, dataSize uint64) {
		// Limit data size
		if dataSize > 10*1024*1024 {
			t.Skip("Data size too large")
		}

		// Create default rent sysvar
		rent := &sealevel.SysvarRent{}
		rent.InitializeDefault()

		// Test IsExempt
		isExempt := rent.IsExempt(balance, dataSize)

		// Verify consistency with MinimumBalance
		minBalance := rent.MinimumBalance(dataSize)

		if balance >= minBalance {
			if !isExempt {
				t.Errorf("Account with balance %d >= min %d should be exempt", balance, minBalance)
			}
		} else {
			if isExempt {
				t.Errorf("Account with balance %d < min %d should not be exempt", balance, minBalance)
			}
		}
	})
}
