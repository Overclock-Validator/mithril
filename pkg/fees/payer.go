package fees

import (
	"errors"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// NonceStateSize is Agave's solana_nonce::state::State::size().
const NonceStateSize = 80

var (
	ErrFeePayerNotFound         = errors.New("account not found")
	ErrInvalidAccountForFee     = errors.New("invalid account for fee")
	ErrInsufficientFundsForFee  = sealevel.InstrErrInsufficientFunds
	ErrInsufficientFundsForRent = errors.New("insufficient funds for rent")
)

const (
	systemAccountKindSystem = iota
	systemAccountKindNonce
)

// RentForSlot returns the bank-local Rent sysvar, then the process cache,
// then Agave's default (890880 lamports for a 0-byte account).
func RentForSlot(slotCtx *sealevel.SlotCtx) sealevel.SysvarRent {
	if slotCtx != nil {
		if bankSysvars := slotCtx.BankSysvars(); bankSysvars != nil {
			if rent, ok := bankSysvars.Rent(); ok {
				return rent
			}
		}
	}
	if sealevel.SysvarCache.Rent.Sysvar != nil {
		return *sealevel.SysvarCache.Rent.Sysvar
	}
	return sealevel.NewDefaultRentSysvar()
}

// ValidateFeePayer matches Agave svm::account_loader::validate_fee_payer
// without mutating the account: the payer must exist, be a system or nonce
// account, and cover the fee (plus the nonce rent reserve). A rent-exempt
// system payer may be drained to zero, but must not be left with a
// nonzero balance below the exemption minimum.
func ValidateFeePayer(payer *accounts.Account, fee uint64, rent sealevel.SysvarRent) error {
	return ValidateFeePayerWithFeatures(payer, fee, rent, nil)
}

// ValidateFeePayerWithFeatures applies pending fee-payer feature semantics.
// The nil feature set preserves the pre-activation behavior for callers that
// deliberately require strict block-production admission.
func ValidateFeePayerWithFeatures(payer *accounts.Account, fee uint64, rent sealevel.SysvarRent, feats *features.Features) error {
	if payer == nil || payer.Lamports == 0 {
		return ErrFeePayerNotFound
	}
	kind, ok := systemAccountKind(payer)
	if !ok {
		return ErrInvalidAccountForFee
	}
	minBalance := uint64(0)
	if kind == systemAccountKindNonce {
		minBalance = rent.MinimumBalance(NonceStateSize)
	}
	if payer.Lamports < minBalance || payer.Lamports-minBalance < fee {
		return ErrInsufficientFundsForFee
	}
	post := payer.Lamports - fee
	relaxPostExecMinBalance := feats != nil && feats.IsActive(features.RelaxPostExecMinBalanceCheck)
	if kind == systemAccountKindSystem && post != 0 && !rent.IsExempt(post, 0) {
		// Before SIMD-0392, an already-rent-paying system account may keep
		// decreasing while remaining rent-paying. After activation, Agave
		// treats the pre-state as rent-exempt but allows an otherwise unchanged
		// account to remain sub-exempt when its balance did not decrease. A fee
		// payer's owner and data cannot change during this phase, so only the
		// balance comparison remains here.
		if (relaxPostExecMinBalance && post < payer.Lamports) ||
			(!relaxPostExecMinBalance && rent.IsExempt(payer.Lamports, 0)) {
			return ErrInsufficientFundsForRent
		}
	}
	return nil
}

// PayerCanFund reports whether the current in-slot fee payer can pay this
// transaction's fee and still satisfy ValidateFeePayer. It does not execute.
func PayerCanFund(slotCtx *sealevel.SlotCtx, tx *solana.Transaction) error {
	if tx == nil || len(tx.Message.AccountKeys) == 0 {
		return ErrFeePayerNotFound
	}
	payer, err := loadPayer(slotCtx, tx.Message.AccountKeys[0])
	if err != nil {
		return ErrFeePayerNotFound
	}
	feats := features.NewFeaturesDefault()
	if slotCtx != nil && slotCtx.Features != nil {
		feats = slotCtx.Features
	}
	instrs, err := feeInstructions(tx)
	if err != nil {
		return err
	}
	limits, err := sealevel.ComputeBudgetLimitsForTransaction(tx, instrs, feats)
	if err != nil {
		return err
	}
	// Agave's block-production admission stays strict even after SIMD-0290;
	// only replay may turn a blockhash transaction into a committed no-op.
	feeInfo := CalculateTxFees(tx, instrs, limits, feats)
	return ValidateFeePayerWithFeatures(payer, feeInfo.TotalFee, RentForSlot(slotCtx), feats)
}

// ValidateTransactionFeePayer performs the fee-payer phase of Agave's
// transaction loader without mutating bank state. It deliberately runs before
// loading the remaining transaction accounts so fee-payer errors take
// precedence over account-load errors. The caller may subsequently deduct the
// returned fee from a transaction-local account clone, or publish it as the
// rollback state for a fees-only transaction.
func ValidateTransactionFeePayer(
	slotCtx *sealevel.SlotCtx,
	tx *solana.Transaction,
	instrs []sealevel.Instruction,
	limits *sealevel.ComputeBudgetLimits,
) (*TxFeeInfo, error) {
	if slotCtx == nil || tx == nil || len(tx.Message.AccountKeys) == 0 || limits == nil {
		return nil, ErrFeePayerNotFound
	}
	feats := slotCtx.Features
	if feats == nil {
		feats = features.NewFeaturesDefault()
	}
	feeInfo := CalculateTxFees(tx, instrs, limits, feats)
	payer, err := loadPayer(slotCtx, tx.Message.AccountKeys[0])
	if err != nil {
		return feeInfo, ErrFeePayerNotFound
	}
	if err := ValidateFeePayerWithFeatures(payer, feeInfo.TotalFee, RentForSlot(slotCtx), feats); err != nil {
		return feeInfo, err
	}
	return feeInfo, nil
}

func loadPayer(slotCtx *sealevel.SlotCtx, pk solana.PublicKey) (*accounts.Account, error) {
	if slotCtx == nil {
		return nil, ErrFeePayerNotFound
	}
	if acct, err := slotCtx.GetAccountShared(pk); err == nil && acct != nil {
		return acct, nil
	}
	if slotCtx.UnrootedRead == nil && slotCtx.AccountsDb == nil {
		return nil, ErrFeePayerNotFound
	}
	return slotCtx.GetAccountFromAccountsDb(pk)
}

func feeInstructions(tx *solana.Transaction) ([]sealevel.Instruction, error) {
	out := make([]sealevel.Instruction, 0, len(tx.Message.Instructions))
	for _, compiled := range tx.Message.Instructions {
		programID, err := tx.ResolveProgramIDIndex(compiled.ProgramIDIndex)
		if err != nil {
			return nil, err
		}
		out = append(out, sealevel.Instruction{
			ProgramId: programID,
			Data:      compiled.Data,
		})
	}
	return out, nil
}

func systemAccountKind(acct *accounts.Account) (int, bool) {
	if acct.Owner != a.SystemProgramAddr {
		return 0, false
	}
	if len(acct.Data) == 0 {
		return systemAccountKindSystem, true
	}
	if len(acct.Data) != NonceStateSize {
		return 0, false
	}
	nonceState, err := sealevel.UnmarshalNonceStateVersions(acct.Data)
	if err != nil || !nonceState.State().IsInitialized {
		return 0, false
	}
	return systemAccountKindNonce, true
}
