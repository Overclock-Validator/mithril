package costmodel

import (
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// Block cost limits mirror agave/cost-model/src/block_cost_limits.rs defaults.
const (
	ComputeUnitToUSRatio = 30

	SignatureCost            = ComputeUnitToUSRatio * 24 // 720
	Secp256k1VerifyCost      = ComputeUnitToUSRatio * 223
	Ed25519VerifyCost        = ComputeUnitToUSRatio * 76
	Ed25519VerifyStrictCost  = ComputeUnitToUSRatio * 80
	Secp256r1VerifyCost      = ComputeUnitToUSRatio * 160
	WriteLockUnits           = ComputeUnitToUSRatio * 10  // 300
	InstructionDataBytesCost = 140 / ComputeUnitToUSRatio // ~4 CU per byte

	// Loaded-accounts data size is charged in 32KiB pages at the protocol heap cost (8 CU/page).
	AccountDataCostPageSize = 32 * 1024
	HeapCost                = 8

	MaxBlockUnitsSIMD0256 = 60_000_000
	MaxBlockUnitsSIMD0286 = 100_000_000

	MaxWritableAccountUnits = 24_000_000

	MaxBlockAccountsDataSizeDelta = 100_000_000

	// DefaultMaxDataShredsPerSlot matches agave DEFAULT_MAX_DATA_SHREDS_PER_SLOT.
	DefaultMaxDataShredsPerSlot = 32 * 1024
	// SIMD-0525 max_entry_bytes_per_slot at the 400ms / 32,768-shred baseline.
	DefaultMaxEntryBytesPerSlot = 20 * 1024 * 1024
	// PacketDataSize is the legacy/V0 transaction packet limit.
	PacketDataSize = 1232
	// MaxTransactionSize includes the larger SIMD-0385 V1 transaction limit.
	MaxTransactionSize = solana.MaxTransactionSizeV1
	// EntryHeaderBytes is the Agave/Firedancer 48-byte entry header used for
	// pack byte accounting and the reserved ending-tick.
	EntryHeaderBytes = 48
	// MaxMicroblockBytes is the largest transaction plus its entry header.
	MaxMicroblockBytes = EntryHeaderBytes + MaxTransactionSize

	// TypicalDataShredPayloadBytes is chained-merkle unsigned data capacity
	// for one shred: 1203 - 88 - 32 - 6*20.
	TypicalDataShredPayloadBytes = 963
	// DataShredsPerFECSet matches turbine's 32:32 erasure batch.
	DataShredsPerFECSet = 32
	// FECSetsPerBatch is the close watermark: hold until two FEC sets are full.
	FECSetsPerBatch = 2
	// TypicalFECSetPayloadBytes is one full unsigned FEC set.
	TypicalFECSetPayloadBytes = DataShredsPerFECSet * TypicalDataShredPayloadBytes
	// DefaultTargetBatchBytes is two FEC sets. A short leftover is only
	// emitted at slot end (Freeze / ending tick).
	DefaultTargetBatchBytes = FECSetsPerBatch * TypicalFECSetPayloadBytes
)

// Limits configures per-slot cost and size budgets.
type Limits struct {
	BlockCost              uint64
	WritableAccountCost    uint64
	AllocatedDataSizeDelta uint64
	MaxBatchBytes          uint64
	MaxEntryBytes          uint64
}

func DefaultLimits() Limits {
	return Limits{
		BlockCost:              MaxBlockUnitsSIMD0256,
		WritableAccountCost:    MaxWritableAccountUnits,
		AllocatedDataSizeDelta: MaxBlockAccountsDataSizeDelta,
		MaxBatchBytes:          DefaultTargetBatchBytes,
		MaxEntryBytes:          DefaultPackEntryBytes(),
	}
}

// LimitsForSlot to apply slot-time reductions at the correct epoch boundary.
func LimitsForFeatures(feats *features.Features) Limits {
	limits := DefaultLimits()
	if feats != nil && feats.IsActive(features.RaiseBlockLimitsTo100m) {
		limits.BlockCost = MaxBlockUnitsSIMD0286
		limits.WritableAccountCost = 40_000_000
	}
	return limits
}

// LimitsForSlot mirrors Agave v4.3.0-rc.1 runtime/src/slot_params.rs.
// Reference: https://github.com/anza-xyz/agave/blob/v4.3.0-rc.1/runtime/src/slot_params.rs A slot-time
// gate takes effect in the epoch after activation; among effective gates the
// shortest duration wins, even if longer-duration gates activate later.
func LimitsForSlot(feats *features.Features, schedule *sealevel.SysvarEpochSchedule, slot uint64) (Limits, error) {
	limits := DefaultLimits()
	for _, transition := range []struct {
		gate                                  features.FeatureGate
		account, block, data, shreds, entries uint64
	}{
		{features.ReduceSlotTimeTo350ms, 21_000_000, 52_500_000, 87_500_000, 28_672, 18_350_080},
		{features.ReduceSlotTimeTo300ms, 18_000_000, 45_000_000, 75_000_000, 24_576, 15_728_640},
		{features.ReduceSlotTimeTo250ms, 15_000_000, 37_500_000, 62_500_000, 20_480, 13_107_200},
		{features.ReduceSlotTimeTo200ms, 12_000_000, 30_000_000, 50_000_000, 16_384, 10_485_760},
	} {
		if feats == nil {
			break
		}
		activation, active := feats.ActivationSlot(transition.gate)
		if !active {
			continue
		}
		if schedule == nil || schedule.SlotsPerEpoch == 0 {
			return Limits{}, fmt.Errorf("epoch schedule required for slot-time cost limits")
		}
		effective := schedule.FirstSlotInEpoch(safemath.SaturatingAddU64(schedule.GetEpoch(activation), 1))
		if effective > slot {
			continue
		}
		limits.WritableAccountCost = transition.account
		limits.BlockCost = transition.block
		limits.AllocatedDataSizeDelta = transition.data
		limits.MaxEntryBytes = packEntryBytes(transition.shreds, transition.entries)
	}
	if feats != nil && feats.IsActive(features.RaiseBlockLimitsTo100m) {
		limits.BlockCost = limits.BlockCost * 100 / 60
		limits.WritableAccountCost = limits.WritableAccountCost * 100 / 60
	}
	return limits, nil
}
