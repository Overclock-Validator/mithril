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
	// FECSetsPerBatch is the close watermark: hold until one FEC set is full.
	FECSetsPerBatch = 1
	// TypicalFECSetPayloadBytes is one full unsigned FEC set.
	TypicalFECSetPayloadBytes = DataShredsPerFECSet * TypicalDataShredPayloadBytes
	// DefaultTargetBatchBytes is one FEC set. A short leftover is only
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

// LimitsForFeatures returns the cost limits selected by the bank's feature set.
func LimitsForFeatures(feats *features.Features) Limits {
	limits := DefaultLimits()
	if feats != nil && feats.IsActive(features.RaiseBlockLimitsTo100m) {
		limits.BlockCost = MaxBlockUnitsSIMD0286
	}
	return limits
}
