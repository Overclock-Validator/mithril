package fees

import (
	"errors"
	"fmt"
	"math"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/wide"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

var microLamportsPerLamport = wide.Uint128FromUint64(1000000)
var microLamportsPerLamportMinus1 = wide.Uint128FromUint64(1000000 - 1)

func calculatePriorityFee(computeBudgetLimits *sealevel.ComputeBudgetLimits) uint64 {
	if computeBudgetLimits.UsesDirectPriorityFee {
		return computeBudgetLimits.DirectPriorityFeeLamports
	}
	computeUnitPrice := wide.Uint128FromUint64(computeBudgetLimits.ComputeUnitPrice)
	computeUnitLimit := wide.Uint128FromUint64(uint64(computeBudgetLimits.ComputeUnitLimit))

	microLamportFee := computeUnitPrice.Mul(computeUnitLimit)
	fee := microLamportFee.Add(microLamportsPerLamportMinus1).Div(microLamportsPerLamport)

	var priorityFee uint64
	if fee.IsUint64() {
		priorityFee = fee.Uint64()
	} else {
		priorityFee = math.MaxUint64
	}

	return priorityFee
}

// There are currently two aspects of the transaction fee model:
//  1. fee per signature (5k lamports/signature)
//  2. a prioritization fee, derived from SetComputeUnitPrice for legacy/v0 or
//     supplied directly as total lamports in a SIMD-0385 v1 header

const feePayerIdx = 0

type TxFeeInfo struct {
	ExecutionFee uint64
	PriorityFee  uint64
	TotalFee     uint64
}

type TxFeeInfoAccumulator struct {
	ExecutionFees uint64
	PriorityFees  uint64
	TotalFees     uint64
}

func (txFeeAccumulator *TxFeeInfoAccumulator) Add(txFeeInfo *TxFeeInfo) {
	var err error

	txFeeAccumulator.TotalFees, err = safemath.CheckedAddU64(txFeeAccumulator.TotalFees, txFeeInfo.TotalFee)
	if err != nil {
		panic("overflow in accumulating total tx fees - should be impossible")
	}

	txFeeAccumulator.PriorityFees, err = safemath.CheckedAddU64(txFeeAccumulator.PriorityFees, txFeeInfo.PriorityFee)
	if err != nil {
		panic("overflow in accumulating priority fees - should be impossible")
	}

	txFeeAccumulator.ExecutionFees, err = safemath.CheckedAddU64(txFeeAccumulator.ExecutionFees, txFeeInfo.ExecutionFee)
	if err != nil {
		panic("overflow in accumulating execution fees - should be impossible")
	}
}

// LeaderReward is the lamports credited to the slot leader for a transaction:
// full priority fee plus the unburned half of the signature (execution) fee.
func LeaderReward(feeInfo *TxFeeInfo) uint64 {
	if feeInfo == nil {
		return 0
	}
	unburnedSigFee := feeInfo.ExecutionFee - feeInfo.ExecutionFee/2
	return safemath.SaturatingAddU64(feeInfo.PriorityFee, unburnedSigFee)
}

func CalculateTxFees(tx *solana.Transaction, instrs []sealevel.Instruction, computeBudgetLimits *sealevel.ComputeBudgetLimits, f *features.Features) *TxFeeInfo {
	numSignatures := uint64(tx.Message.Header.NumRequiredSignatures)
	secp256r1PrecompiledEnabled := f.IsActive(features.EnableSecp256r1Precompile)

	// have to pay fees per signatures to these precompiles as well
	for _, instr := range instrs {
		if instr.ProgramId == a.Secp256kPrecompileAddr || instr.ProgramId == a.Ed25519PrecompileAddr ||
			(instr.ProgramId == a.Secp256r1PrecompileAddr && secp256r1PrecompiledEnabled) {
			if len(instr.Data) != 0 {
				numSignatures += uint64(instr.Data[0])
			}
		}
	}

	// basic tx fee. 5000 lamports per signature.
	baseTxFee := numSignatures * 5000

	// prioritization fees
	var priorityFee uint64
	if computeBudgetLimits.UsesDirectPriorityFee || computeBudgetLimits.ComputeUnitPrice != 0 {
		priorityFee = calculatePriorityFee(computeBudgetLimits)
	}

	totalTxFee := safemath.SaturatingAddU64(baseTxFee, priorityFee)
	return &TxFeeInfo{ExecutionFee: baseTxFee, PriorityFee: priorityFee, TotalFee: totalTxFee}
}

// TODO: implement new fee model
func CalculateAndDeductTxFees(tx *solana.Transaction, txMeta *rpc.TransactionMeta, instrs []sealevel.Instruction, transactionAccts *sealevel.TransactionAccounts, computeBudgetLimits *sealevel.ComputeBudgetLimits, f *features.Features, rent sealevel.SysvarRent, isSimulation bool) (*TxFeeInfo, uint64, error) {
	feePayerAcct, err := transactionAccts.GetAccount(feePayerIdx)
	if err != nil {
		if isSimulation {
			// RPC simulate must not crash the validator on user-submitted
			// input; surface the error to the client instead.
			return nil, 0, err
		}
		// Block-replay invariant: sanitize + loader guarantee feePayer at
		// AccountKeys[0] is loadable. Reaching here means our local state
		// or sanitize is broken — keep the loud signal.
		panic(fmt.Sprintf("CalculateAndDeductTxFees: feePayer GetAccount(0) failed: %v", err))
	}
	////mlog.Log.Debugf("feePayerAcct=%+v", feePayerAcct)

	defer transactionAccts.Unlock(feePayerIdx)

	numSignatures := uint64(tx.Message.Header.NumRequiredSignatures)
	secp256r1PrecompiledEnabled := f.IsActive(features.EnableSecp256r1Precompile)

	// have to pay fees per signatures to these precompiles as well
	for _, instr := range instrs {
		if instr.ProgramId == a.Secp256kPrecompileAddr || instr.ProgramId == a.Ed25519PrecompileAddr ||
			(instr.ProgramId == a.Secp256r1PrecompileAddr && secp256r1PrecompiledEnabled) {
			if len(instr.Data) != 0 {
				numSignatures += uint64(instr.Data[0])
			}
		}
	}

	// basic tx fee. 5000 lamports per signature.
	baseTxFee := numSignatures * 5000

	// prioritization fees
	var priorityFee uint64
	if computeBudgetLimits.UsesDirectPriorityFee || computeBudgetLimits.ComputeUnitPrice != 0 {
		priorityFee = calculatePriorityFee(computeBudgetLimits)
	}

	totalTxFee := safemath.SaturatingAddU64(baseTxFee, priorityFee)
	feeInfo := &TxFeeInfo{ExecutionFee: baseTxFee, PriorityFee: priorityFee, TotalFee: totalTxFee}

	if err := ValidateFeePayerWithFeatures(feePayerAcct, totalTxFee, rent, f); err != nil {
		return feeInfo, 0, err
	}
	////mlog.Log.Debugf("feePayerAcct.Lamports=%d totalTxFee=%d", feePayerAcct.Lamports, totalTxFee)

	feePayerAcct, err = transactionAccts.Touch(feePayerIdx)
	if err != nil {
		return feeInfo, 0, err
	}
	// Agave normalizes a rent-exempt payer before deducting its fee. Successful
	// execution commits this value; replay's failure publisher separately
	// restores the originally loaded epoch for ordinary-blockhash rollbacks.
	if feePayerAcct.RentEpoch != math.MaxUint64 && rent.IsExempt(feePayerAcct.Lamports, uint64(len(feePayerAcct.Data))) {
		feePayerAcct.RentEpoch = math.MaxUint64
	}
	feePayerAcct.Lamports -= totalTxFee

	return feeInfo, feePayerAcct.Lamports, nil
}

// DistributeTxFees credits the historical collector selected by replay. An
// invalid destination burns its share without publishing any account mutation.
// Agave v4.4.0-alpha.5 runtime/src/bank/fee_distribution.rs:248-299.
func DistributeTxFees(slotCtx *sealevel.SlotCtx, collector, leaderVote solana.PublicKey, txFeeAccumulator *TxFeeInfoAccumulator) (uint64, error) {
	var feesToBurn uint64
	var feesToLeader uint64

	if slotCtx.Features.IsActive(features.RewardFullPriorityFee) {
		halfFee := txFeeAccumulator.ExecutionFees / 2
		feesToLeader = safemath.SaturatingAddU64(txFeeAccumulator.PriorityFees, txFeeAccumulator.ExecutionFees-halfFee)
		feesToBurn = halfFee
	} else {
		feesToBurn = txFeeAccumulator.TotalFees / 2
		feesToLeader = txFeeAccumulator.TotalFees - feesToBurn
	}

	if feesToLeader == 0 {
		return feesToBurn, nil
	}
	acct, err := slotCtx.GetAccount(collector)
	if err != nil {
		// Read through the speculative overlay: prior unrooted fee credits may
		// not have reached AccountsDB yet.
		acct, err = slotCtx.GetAccountFromAccountsDb(collector)
		if errors.Is(err, accountsdb.ErrNoAccount) {
			acct = &accounts.Account{Key: collector, Owner: a.SystemProgramAddr}
		} else if err != nil {
			return 0, fmt.Errorf("load fee collector %s: %w", collector, err)
		}
		slotCtx.ParentAccts.SetAccountWithoutLock(collector, acct.Clone())
	}
	before := acct.Lamports
	after, err := safemath.CheckedAddU64(before, feesToLeader)
	burn := func() (uint64, error) { return safemath.SaturatingAddU64(feesToBurn, feesToLeader), nil }
	if err != nil {
		return burn()
	}
	rent := RentForSlot(slotCtx)
	custom := slotCtx.Features.IsActive(features.CustomCommissionCollector)
	relax := slotCtx.Features.IsActive(features.RelaxPostExecMinBalanceCheck)
	if !custom || collector != leaderVote {
		if acct.Owner != a.SystemProgramAddr {
			return burn()
		}
		if custom {
			// ReservedAccountKeys includes pending keys regardless of their gate.
			_, reserved := sealevel.NewReservedAcctsSet[collector]
			if reserved || sealevel.IsNativeProgram(collector) || sealevel.IsSysvar(collector) || collector == a.Secp256r1PrecompileAddr {
				return burn()
			}
			if collector != a.IncineratorAddr && !rent.IsExempt(after, uint64(len(acct.Data))) && (!relax || before == 0) {
				return burn()
			}
		} else if !rent.IsExempt(after, uint64(len(acct.Data))) && (!relax || before == 0) {
			// A positive deposit increases the balance, so under the old rent
			// transition rules a rent-paying post-state is never permitted.
			return burn()
		}
	}
	acct = acct.Clone()
	acct.Lamports = after
	if err := slotCtx.SetAccount(collector, acct); err != nil {
		return 0, fmt.Errorf("store fee collector %s: %w", collector, err)
	}
	slotCtx.RecordModifiedAcct(collector)
	return feesToBurn, nil
}
