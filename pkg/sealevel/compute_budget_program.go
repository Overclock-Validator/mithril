package sealevel

import (
	"fmt"

	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/migration"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

const (
	MinHeapFrameBytes                    = (32 * 1024)
	MaxHeapFrameBytes                    = (256 * 1024)
	HeapFrameBytesMultiple               = 1024
	DefaultInstructionComputeUnitLimit   = 200000
	MaxComputeUnitLimit                  = 1400000
	MaxLoadedAccountsDataSizeBytes       = (64 * 1024 * 1024)
	MaxBuiltinAllocationComputeUnitLimit = 3000
)

type ComputeBudgetLimits struct {
	UpdatedHeapBytes          uint32
	ComputeUnitLimit          uint32
	ComputeUnitPrice          uint64
	LoadedAccountBytes        uint32
	DirectPriorityFeeLamports uint64
	UsesDirectPriorityFee     bool
}

const (
	ComputeBudgetInstrTypeRequestHeapFrame               = 1
	ComputeBudgetInstrTypeSetComputeUnitLimit            = 2
	ComputeBudgetInstrTypeSetComputeUnitPrice            = 3
	ComputeBudgetInstrTypeSetLoadedAccountsDataSizeLimit = 4
)

type ComputeBudgetInstrRequestHeapFrame struct {
	Bytes uint32
}

type ComputeBudgetInstrSetComputeUnitLimit struct {
	ComputeUnitLimit uint32
}

type ComputeBudgetInstrSetComputeUnitPrice struct {
	MicroLamports uint64
}

type ComputeBudgetInstrSetLoadedAccountsDataSizeLimit struct {
	Bytes uint32
}

func (requestHeapFrame *ComputeBudgetInstrRequestHeapFrame) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	requestHeapFrame.Bytes, err = decoder.ReadUint32(bin.LE)
	return err
}

func (requestHeapFrame *ComputeBudgetInstrRequestHeapFrame) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error

	err = encoder.WriteUint8(ComputeBudgetInstrTypeRequestHeapFrame)
	if err != nil {
		return err
	}

	err = encoder.WriteUint32(requestHeapFrame.Bytes, bin.LE)
	return err
}

func (setComputeUnitLimit *ComputeBudgetInstrSetComputeUnitLimit) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	setComputeUnitLimit.ComputeUnitLimit, err = decoder.ReadUint32(bin.LE)
	return err
}

func (setComputeUnitLimit *ComputeBudgetInstrSetComputeUnitLimit) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error

	err = encoder.WriteUint8(ComputeBudgetInstrTypeSetComputeUnitLimit)
	if err != nil {
		return err
	}

	err = encoder.WriteUint32(setComputeUnitLimit.ComputeUnitLimit, bin.LE)
	return err
}

func (setComputeUnitPrice *ComputeBudgetInstrSetComputeUnitPrice) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	setComputeUnitPrice.MicroLamports, err = decoder.ReadUint64(bin.LE)
	return err
}

func (setComputeUnitPrice *ComputeBudgetInstrSetComputeUnitPrice) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error

	err = encoder.WriteUint8(ComputeBudgetInstrTypeSetComputeUnitPrice)
	if err != nil {
		return err
	}

	err = encoder.WriteUint64(setComputeUnitPrice.MicroLamports, bin.LE)
	return err
}

func (setLoadedAccountsDataSizeLimit *ComputeBudgetInstrSetLoadedAccountsDataSizeLimit) UnmarshalWithDecoder(decoder *bin.Decoder) error {
	var err error
	setLoadedAccountsDataSizeLimit.Bytes, err = decoder.ReadUint32(bin.LE)
	return err
}

func (setLoadedAccountsDataSizeLimit *ComputeBudgetInstrSetLoadedAccountsDataSizeLimit) MarshalWithEncoder(encoder *bin.Encoder) error {
	var err error

	err = encoder.WriteUint8(ComputeBudgetInstrTypeSetLoadedAccountsDataSizeLimit)
	if err != nil {
		return err
	}

	err = encoder.WriteUint32(setLoadedAccountsDataSizeLimit.Bytes, bin.LE)
	return err
}

func sanitizeRequestedHeapSize(len uint32) bool {
	return len >= MinHeapFrameBytes && len <= MaxHeapFrameBytes && (len%HeapFrameBytesMultiple == 0)
}

func invalidInstructionDataErr(idx int) error {
	return fmt.Errorf("Error processing Instruction %d: %w", idx, InstrErrInvalidInstructionData)
}

func duplicateInstructionErr(idx int) error {
	return fmt.Errorf("Transaction contains a duplicate instruction (%d) that is not allowed", idx)
}

func calculateDefaultComputeUnitLimit(f *features.Features, numBuiltinInstrs uint32, numNonBuiltinInstrs uint32, numNonComputeBudgetInstrs uint32) uint32 {
	if f != nil && f.IsActive(features.ReserveMinimalCUsForBuiltinInstructions) {
		return numBuiltinInstrs*MaxBuiltinAllocationComputeUnitLimit + numNonBuiltinInstrs*DefaultInstructionComputeUnitLimit
	} else {
		return numNonComputeBudgetInstrs * DefaultInstructionComputeUnitLimit
	}
}

func isBuiltin(instr Instruction, f *features.Features) bool {
	programPubkey := instr.ProgramId
	// SIMD-0387: BLS proof-of-possession verification can consume 34,500 CUs,
	// so the vote program leaves the minimal builtin allocation once active.
	if programPubkey == a.VoteProgramAddr && f != nil && f.IsActive(features.BlsPubkeyManagementInVoteAccount) {
		return false
	}
	if migration.IsNonMigratingBuiltinProgram(programPubkey) {
		return true
	}

	if migration.IsMigratingBuiltinProgram(programPubkey) && !migration.HasBuiltinMigratedYet(programPubkey) {
		return true
	}

	return false
}

func ComputeBudgetExecuteInstructions(instructions []Instruction, f *features.Features) (*ComputeBudgetLimits, error) {
	var hasRequestedHeapSize bool
	var hasComputeUnitLimit bool
	var hasComputeUnitPrice bool
	var hasUpdatedLoadedAccountsDataSizeLimit bool

	var numNonComputeBudgetInstrs uint32
	var numBuiltinInstrs uint32
	var numNonBuiltinInstrs uint32

	var requestedHeapSize uint32
	var updatedComputeUnitLimit uint32
	var updatedLoadedAccountsDataSizeLimit uint32
	var updatedComputeUnitPrice uint64

	for idx, instr := range instructions {
		if isBuiltin(instr, f) {
			numBuiltinInstrs++
		} else {
			numNonBuiltinInstrs++
		}

		if instr.ProgramId != a.ComputeBudgetProgramAddr {
			numNonComputeBudgetInstrs++
			continue
		}

		instrData := instr.Data
		decoder := bin.NewBorshDecoder(instrData)

		instrType, err := decoder.ReadUint8()
		if err != nil {
			return nil, invalidInstructionDataErr(idx)
		}

		switch instrType {
		case ComputeBudgetInstrTypeRequestHeapFrame:
			{
				var requestHeapFrame ComputeBudgetInstrRequestHeapFrame
				err = requestHeapFrame.UnmarshalWithDecoder(decoder)
				if err != nil {
					return nil, invalidInstructionDataErr(idx)
				}

				if hasRequestedHeapSize {
					return nil, duplicateInstructionErr(idx)
				}

				requestedSize := requestHeapFrame.Bytes

				if sanitizeRequestedHeapSize(requestedSize) {
					requestedHeapSize = requestedSize
					hasRequestedHeapSize = true
				} else {
					return nil, invalidInstructionDataErr(idx)
				}
			}

		case ComputeBudgetInstrTypeSetComputeUnitLimit:
			{
				var setComputeUnitLimit ComputeBudgetInstrSetComputeUnitLimit
				err = setComputeUnitLimit.UnmarshalWithDecoder(decoder)
				if err != nil {
					return nil, invalidInstructionDataErr(idx)
				}

				if hasComputeUnitLimit {
					return nil, duplicateInstructionErr(idx)
				}

				updatedComputeUnitLimit = setComputeUnitLimit.ComputeUnitLimit
				hasComputeUnitLimit = true
			}

		case ComputeBudgetInstrTypeSetComputeUnitPrice:
			{
				var setComputeUnitPrice ComputeBudgetInstrSetComputeUnitPrice
				err = setComputeUnitPrice.UnmarshalWithDecoder(decoder)
				if err != nil {
					return nil, invalidInstructionDataErr(idx)
				}

				if hasComputeUnitPrice {
					return nil, duplicateInstructionErr(idx)
				}

				updatedComputeUnitPrice = setComputeUnitPrice.MicroLamports
				hasComputeUnitPrice = true
			}

		case ComputeBudgetInstrTypeSetLoadedAccountsDataSizeLimit:
			{
				var setLoadedAccountsDataSizeLimit ComputeBudgetInstrSetLoadedAccountsDataSizeLimit
				err = setLoadedAccountsDataSizeLimit.UnmarshalWithDecoder(decoder)
				if err != nil {
					return nil, invalidInstructionDataErr(idx)
				}

				if hasUpdatedLoadedAccountsDataSizeLimit {
					return nil, duplicateInstructionErr(idx)
				}

				updatedLoadedAccountsDataSizeLimit = setLoadedAccountsDataSizeLimit.Bytes
				hasUpdatedLoadedAccountsDataSizeLimit = true
			}

		default:
			{
				return nil, invalidInstructionDataErr(idx)
			}
		}
	}

	var updatedHeapBytes uint32
	if hasRequestedHeapSize {
		updatedHeapBytes = requestedHeapSize
	} else {
		updatedHeapBytes = MinHeapFrameBytes
	}
	if updatedHeapBytes > MaxHeapFrameBytes {
		updatedHeapBytes = MaxHeapFrameBytes
	}

	var computeUnitLimit uint32
	if hasComputeUnitLimit {
		computeUnitLimit = min(updatedComputeUnitLimit, MaxComputeUnitLimit)
	} else {
		computeUnitLimit = min(calculateDefaultComputeUnitLimit(f, numBuiltinInstrs, numNonBuiltinInstrs, numNonComputeBudgetInstrs), MaxComputeUnitLimit)
	}

	var computeUnitPrice uint64
	if hasComputeUnitPrice {
		computeUnitPrice = updatedComputeUnitPrice
	}

	var loadedAccountBytes uint32
	if hasUpdatedLoadedAccountsDataSizeLimit {
		loadedAccountBytes = min(updatedLoadedAccountsDataSizeLimit, MaxLoadedAccountsDataSizeBytes)
	} else {
		loadedAccountBytes = MaxLoadedAccountsDataSizeBytes
	}

	computeBudgetLimits := &ComputeBudgetLimits{UpdatedHeapBytes: updatedHeapBytes,
		ComputeUnitLimit: computeUnitLimit, ComputeUnitPrice: computeUnitPrice, LoadedAccountBytes: loadedAccountBytes}

	return computeBudgetLimits, nil
}

// ComputeBudgetLimitsForTransaction returns the transaction's effective
// resource limits. Legacy and v0 transactions configure their budget through
// ComputeBudget program instructions. SIMD-0385 v1 transactions carry the
// configuration inline; any ComputeBudget instructions in a v1 message are
// deliberately ignored here and execute later as ordinary 150-CU no-ops.
func ComputeBudgetLimitsForTransaction(tx *solana.Transaction, instructions []Instruction, f *features.Features) (*ComputeBudgetLimits, error) {
	if tx == nil || tx.Message.GetVersion() != solana.MessageVersionV1 {
		return ComputeBudgetExecuteInstructions(instructions, f)
	}

	config := tx.Message.TransactionConfig
	heapBytes := uint32(MinHeapFrameBytes)
	if config.HeapSize != nil {
		heapBytes = *config.HeapSize
		if !sanitizeRequestedHeapSize(heapBytes) {
			return nil, fmt.Errorf("invalid v1 heap size %d", heapBytes)
		}
	}

	var computeUnitLimit uint32
	if config.ComputeUnitLimit != nil {
		computeUnitLimit = min(*config.ComputeUnitLimit, uint32(MaxComputeUnitLimit))
	}

	var loadedAccountBytes uint32
	if config.LoadedAccountsDataSizeLimit != nil {
		loadedAccountBytes = min(*config.LoadedAccountsDataSizeLimit, uint32(MaxLoadedAccountsDataSizeBytes))
	}

	var priorityFee uint64
	if config.PriorityFee != nil {
		priorityFee = *config.PriorityFee
	}

	return &ComputeBudgetLimits{
		UpdatedHeapBytes:          heapBytes,
		ComputeUnitLimit:          computeUnitLimit,
		LoadedAccountBytes:        loadedAccountBytes,
		DirectPriorityFeeLamports: priorityFee,
		UsesDirectPriorityFee:     true,
	}, nil
}

func ComputeBudgetExecute(execCtx *ExecutionCtx) error {
	//mlog.Log.Debugf("ComputeBudget program")
	err := execCtx.ComputeMeter.Consume(cu.CUComputeBudgetProgramDefaultComputeUnits)
	return err
}
