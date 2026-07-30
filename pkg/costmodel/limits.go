package costmodel

import (
	"github.com/Overclock-Validator/mithril/pkg/features"
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

// LimitsForFeatures returns the cost limits selected by the bank's feature set.
func LimitsForFeatures(feats *features.Features) Limits {
	limits := DefaultLimits()
	if feats != nil && feats.IsActive(features.RaiseBlockLimitsTo100m) {
		limits.BlockCost = MaxBlockUnitsSIMD0286
	}
	return limits
}
