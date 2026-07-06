package replay

import (
	"encoding/json"
	"strings"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// LoadAndExecuteTransactionOutput is the output of loading and executing a sanitized transaction.
// This structure contains the complete result of transaction processing, including whether
// the transaction was successfully processed or encountered an error.
type LoadAndExecuteTransactionOutput struct {
	// Result of processing the transaction. Contains either ProcessedTransaction or TransactionError
	ProcessingResult TransactionProcessingResult
	// ExecutionResult contains all the state changes from executing the transaction
	// This is only populated if ProcessingResult contains a ProcessedTransaction
	ExecutionResult *TransactionExecutionResult

	// ExecCtx is the execution context after processing. Available when accounts were
	// loaded successfully. Used by ProcessTransaction for divergence checks and state application.
	ExecCtx *sealevel.ExecutionCtx
	// PreBalances contains lamport balances for each account BEFORE fee deduction.
	// Used by ProcessTransaction for pre-balance divergence checks.
	PreBalances []uint64
	// PreAccountSnapshots holds clones of every transaction account
	// captured before fee deduction and execution. The simulate handler
	// uses these to decode pre-execution token balances; block-replay
	// callers leave it nil.
	PreAccountSnapshots []*accounts.Account
	// FeeInfo contains the calculated fee info. Set whenever fees were calculated.
	FeeInfo *fees.TxFeeInfo
	// Instrs contains the parsed instructions from the transaction.
	Instrs []sealevel.Instruction
	// ComputeBudgetLimits contains the compute budget limits computed from instructions.
	ComputeBudgetLimits *sealevel.ComputeBudgetLimits
}

// TransactionProcessingResult represents the result of processing a transaction
type TransactionProcessingResult struct {
	// ProcessedTransaction contains the execution result if successful
	ProcessedTransaction *ProcessedTransaction
	// TransactionError contains the error if processing failed
	TransactionError *TransactionError
}

// TransactionError represents all possible transaction-level errors that can occur
// before or during transaction processing. These match the Solana error codes.
type TransactionError struct {
	ErrorType TransactionErrorType
	// InstructionIndex is set for InstructionError type
	InstructionIndex *uint8
	// InstructionError is set for InstructionError type
	InstructionError error
	// AccountIndex is set for errors that reference a specific account
	AccountIndex *uint8
}

// TransactionErrorType represents the type of transaction error
type TransactionErrorType int

const (
	// TransactionErrorAccountInUse - An account is already being processed in another transaction
	TransactionErrorAccountInUse TransactionErrorType = iota
	// TransactionErrorAccountLoadedTwice - A Pubkey appears twice in the transaction's account_keys
	TransactionErrorAccountLoadedTwice
	// TransactionErrorAccountNotFound - Attempt to debit an account but found no record of a prior credit
	TransactionErrorAccountNotFound
	// TransactionErrorProgramAccountNotFound - Attempt to load a program that does not exist
	TransactionErrorProgramAccountNotFound
	// TransactionErrorInsufficientFundsForFee - The from Pubkey does not have sufficient balance to pay the fee
	TransactionErrorInsufficientFundsForFee
	// TransactionErrorInvalidAccountForFee - This account may not be used to pay transaction fees
	TransactionErrorInvalidAccountForFee
	// TransactionErrorAlreadyProcessed - The bank has seen this transaction before
	TransactionErrorAlreadyProcessed
	// TransactionErrorBlockhashNotFound - The bank has not seen the given recent_blockhash
	TransactionErrorBlockhashNotFound
	// TransactionErrorInstructionError - An error occurred while processing an instruction
	TransactionErrorInstructionError
	// TransactionErrorCallChainTooDeep - Loader call chain is too deep
	TransactionErrorCallChainTooDeep
	// TransactionErrorMissingSignatureForFee - Transaction requires a fee but has no signature present
	TransactionErrorMissingSignatureForFee
	// TransactionErrorInvalidAccountIndex - Transaction contains an invalid account reference
	TransactionErrorInvalidAccountIndex
	// TransactionErrorSignatureFailure - Transaction did not pass signature verification
	TransactionErrorSignatureFailure
	// TransactionErrorInvalidProgramForExecution - This program may not be used for executing instructions
	TransactionErrorInvalidProgramForExecution
	// TransactionErrorSanitizeFailure - Transaction failed to sanitize accounts offsets correctly
	TransactionErrorSanitizeFailure
	// TransactionErrorClusterMaintenance - Cluster is in maintenance mode
	TransactionErrorClusterMaintenance
	// TransactionErrorAccountBorrowOutstanding - Transaction processing left an account with an outstanding borrowed reference
	TransactionErrorAccountBorrowOutstanding
	// TransactionErrorWouldExceedMaxBlockCostLimit - Transaction would exceed max Block Cost Limit
	TransactionErrorWouldExceedMaxBlockCostLimit
	// TransactionErrorUnsupportedVersion - Transaction version is unsupported
	TransactionErrorUnsupportedVersion
	// TransactionErrorInvalidWritableAccount - Transaction loads a writable account that cannot be written
	TransactionErrorInvalidWritableAccount
	// TransactionErrorWouldExceedMaxAccountCostLimit - Transaction would exceed max account limit within the block
	TransactionErrorWouldExceedMaxAccountCostLimit
	// TransactionErrorWouldExceedAccountDataBlockLimit - Transaction would exceed account data limit within the block
	TransactionErrorWouldExceedAccountDataBlockLimit
	// TransactionErrorTooManyAccountLocks - Transaction locked too many accounts
	TransactionErrorTooManyAccountLocks
	// TransactionErrorAddressLookupTableNotFound - Address lookup table not found
	TransactionErrorAddressLookupTableNotFound
	// TransactionErrorInvalidAddressLookupTableOwner - Attempted to lookup addresses from an account owned by the wrong program
	TransactionErrorInvalidAddressLookupTableOwner
	// TransactionErrorInvalidAddressLookupTableData - Attempted to lookup addresses from an invalid account
	TransactionErrorInvalidAddressLookupTableData
	// TransactionErrorInvalidAddressLookupTableIndex - Address table lookup uses an invalid index
	TransactionErrorInvalidAddressLookupTableIndex
	// TransactionErrorInvalidRentPayingAccount - Transaction leaves an account with a lower balance than rent-exempt minimum
	TransactionErrorInvalidRentPayingAccount
	// TransactionErrorWouldExceedMaxVoteCostLimit - Transaction would exceed max Vote Cost Limit
	TransactionErrorWouldExceedMaxVoteCostLimit
	// TransactionErrorWouldExceedAccountDataTotalLimit - Transaction would exceed total account data limit
	TransactionErrorWouldExceedAccountDataTotalLimit
	// TransactionErrorDuplicateInstruction - Transaction contains a duplicate instruction that is not allowed
	TransactionErrorDuplicateInstruction
	// TransactionErrorInsufficientFundsForRent - Transaction results in an account with insufficient funds for rent
	TransactionErrorInsufficientFundsForRent
	// TransactionErrorMaxLoadedAccountsDataSizeExceeded - Transaction exceeded max loaded accounts data size cap
	TransactionErrorMaxLoadedAccountsDataSizeExceeded
	// TransactionErrorInvalidLoadedAccountsDataSizeLimit - LoadedAccountsDataSizeLimit set for transaction must be greater than 0
	TransactionErrorInvalidLoadedAccountsDataSizeLimit
	// TransactionErrorResanitizationNeeded - Sanitized transaction differed before/after feature activation
	TransactionErrorResanitizationNeeded
	// TransactionErrorProgramExecutionTemporarilyRestricted - Program execution is temporarily restricted on an account
	TransactionErrorProgramExecutionTemporarilyRestricted
	// TransactionErrorUnbalancedTransaction - The total balance before the transaction does not equal the total balance after the transaction
	TransactionErrorUnbalancedTransaction
	// TransactionErrorProgramCacheHitMaxLimit - Program cache hit max limit
	TransactionErrorProgramCacheHitMaxLimit
)

// String returns the Agave-compatible TransactionError variant name (the
// const name with the TransactionError prefix stripped). Used by simulate
// callers that need a wire-format string when no InstructionError is set.
func (t TransactionErrorType) String() string {
	switch t {
	case TransactionErrorAccountInUse:
		return "AccountInUse"
	case TransactionErrorAccountLoadedTwice:
		return "AccountLoadedTwice"
	case TransactionErrorAccountNotFound:
		return "AccountNotFound"
	case TransactionErrorProgramAccountNotFound:
		return "ProgramAccountNotFound"
	case TransactionErrorInsufficientFundsForFee:
		return "InsufficientFundsForFee"
	case TransactionErrorInvalidAccountForFee:
		return "InvalidAccountForFee"
	case TransactionErrorAlreadyProcessed:
		return "AlreadyProcessed"
	case TransactionErrorBlockhashNotFound:
		return "BlockhashNotFound"
	case TransactionErrorInstructionError:
		return "InstructionError"
	case TransactionErrorCallChainTooDeep:
		return "CallChainTooDeep"
	case TransactionErrorMissingSignatureForFee:
		return "MissingSignatureForFee"
	case TransactionErrorInvalidAccountIndex:
		return "InvalidAccountIndex"
	case TransactionErrorSignatureFailure:
		return "SignatureFailure"
	case TransactionErrorInvalidProgramForExecution:
		return "InvalidProgramForExecution"
	case TransactionErrorSanitizeFailure:
		return "SanitizeFailure"
	case TransactionErrorClusterMaintenance:
		return "ClusterMaintenance"
	case TransactionErrorAccountBorrowOutstanding:
		return "AccountBorrowOutstanding"
	case TransactionErrorWouldExceedMaxBlockCostLimit:
		return "WouldExceedMaxBlockCostLimit"
	case TransactionErrorUnsupportedVersion:
		return "UnsupportedVersion"
	case TransactionErrorInvalidWritableAccount:
		return "InvalidWritableAccount"
	case TransactionErrorWouldExceedMaxAccountCostLimit:
		return "WouldExceedMaxAccountCostLimit"
	case TransactionErrorWouldExceedAccountDataBlockLimit:
		return "WouldExceedAccountDataBlockLimit"
	case TransactionErrorTooManyAccountLocks:
		return "TooManyAccountLocks"
	case TransactionErrorAddressLookupTableNotFound:
		return "AddressLookupTableNotFound"
	case TransactionErrorInvalidAddressLookupTableOwner:
		return "InvalidAddressLookupTableOwner"
	case TransactionErrorInvalidAddressLookupTableData:
		return "InvalidAddressLookupTableData"
	case TransactionErrorInvalidAddressLookupTableIndex:
		return "InvalidAddressLookupTableIndex"
	case TransactionErrorInvalidRentPayingAccount:
		return "InvalidRentPayingAccount"
	case TransactionErrorWouldExceedMaxVoteCostLimit:
		return "WouldExceedMaxVoteCostLimit"
	case TransactionErrorWouldExceedAccountDataTotalLimit:
		return "WouldExceedAccountDataTotalLimit"
	case TransactionErrorDuplicateInstruction:
		return "DuplicateInstruction"
	case TransactionErrorInsufficientFundsForRent:
		return "InsufficientFundsForRent"
	case TransactionErrorMaxLoadedAccountsDataSizeExceeded:
		return "MaxLoadedAccountsDataSizeExceeded"
	case TransactionErrorInvalidLoadedAccountsDataSizeLimit:
		return "InvalidLoadedAccountsDataSizeLimit"
	case TransactionErrorResanitizationNeeded:
		return "ResanitizationNeeded"
	case TransactionErrorProgramExecutionTemporarilyRestricted:
		return "ProgramExecutionTemporarilyRestricted"
	case TransactionErrorUnbalancedTransaction:
		return "UnbalancedTransaction"
	case TransactionErrorProgramCacheHitMaxLimit:
		return "ProgramCacheHitMaxLimit"
	}
	return "Unknown"
}

// ProcessedTransaction represents a transaction that was processed, either executed or fees-only
type ProcessedTransaction struct {
	// TransactionType indicates whether this is an Executed or FeesOnly transaction
	TransactionType ProcessedTransactionType
	// Executed contains the execution result if TransactionType is Executed
	Executed *ExecutedTransaction
	// FeesOnly contains the fees-only result if TransactionType is FeesOnly
	FeesOnly *FeesOnlyTransaction
}

// ProcessedTransactionType indicates the type of processed transaction
type ProcessedTransactionType int

const (
	// ProcessedTransactionTypeExecuted - Transaction was executed (may have failed but all instructions ran)
	ProcessedTransactionTypeExecuted ProcessedTransactionType = iota
	// ProcessedTransactionTypeFeesOnly - Transaction was not executed but fees were collected
	ProcessedTransactionTypeFeesOnly
)

// ExecutedTransaction represents a transaction that was executed.
// If execution failed, all account state changes will be rolled back except
// deducted fees and any advanced nonces.
type ExecutedTransaction struct {
	LoadedTransaction  LoadedTransaction
	ExecutionDetails   TransactionExecutionDetails
	ProgramsModified   map[solana.PublicKey]bool // Programs that were modified by this transaction
}

// FeesOnlyTransaction represents a transaction that was not able to be executed
// but fees are able to be collected and any nonces are advanceable
type FeesOnlyTransaction struct {
	LoadedTransaction LoadedTransaction
	ExecutionError    error // The error that prevented execution
}

// LoadedTransaction contains all the accounts and metadata loaded for a transaction
type LoadedTransaction struct {
	// ProgramIndices contains the indices of program accounts in the Accounts array
	ProgramIndices []uint16
	// FeeDetails contains the fee calculation for this transaction
	FeeDetails fees.TxFeeInfo
	// RollbackAccounts contains the accounts that need to be rolled back if the transaction fails
	RollbackAccounts RollbackAccounts
	// ComputeBudget contains the compute unit limits and heap size for this transaction
	ComputeBudget SVMTransactionExecutionBudget
	// LoadedAccountsDataSize is the total size of all loaded account data in bytes
	LoadedAccountsDataSize uint32
}

// KeyedAccountSharedData is a tuple of (Pubkey, AccountSharedData)
type KeyedAccountSharedData struct {
	Pubkey      solana.PublicKey
	AccountData AccountSharedData
}

// AccountSharedData represents the data stored in an account
type AccountSharedData struct {
	// Lamports in the account
	Lamports uint64
	// Data held in this account
	Data []byte
	// The program that owns this account
	Owner solana.PublicKey
	// This account's data contains a loaded program (and is now read-only)
	Executable bool
	// The epoch at which this account will next owe rent
	RentEpoch uint64
}

// RollbackAccounts represents the accounts that need to be saved for potential rollback
type RollbackAccounts struct {
	RollbackType RollbackAccountsType
	// FeePayer is set for FeePayerOnly and SeparateNonceAndFeePayer types
	FeePayer *KeyedAccountSharedData
	// Nonce is set for SameNonceAndFeePayer and SeparateNonceAndFeePayer types
	Nonce *KeyedAccountSharedData
}

// RollbackAccountsType indicates the type of rollback accounts
type RollbackAccountsType int

const (
	// RollbackAccountsTypeFeePayerOnly - Only the fee payer account needs rollback
	RollbackAccountsTypeFeePayerOnly RollbackAccountsType = iota
	// RollbackAccountsTypeSameNonceAndFeePayer - Nonce and fee payer are the same account
	RollbackAccountsTypeSameNonceAndFeePayer
	// RollbackAccountsTypeSeparateNonceAndFeePayer - Nonce and fee payer are separate accounts
	RollbackAccountsTypeSeparateNonceAndFeePayer
)

// SVMTransactionExecutionBudget contains the execution limits for a transaction
type SVMTransactionExecutionBudget struct {
	// ComputeUnitLimit is the number of compute units that a transaction is allowed to consume
	ComputeUnitLimit uint64
	// MaxInstructionStackDepth is the maximum program instruction invocation stack depth
	MaxInstructionStackDepth uint64
	// MaxInstructionTraceLength is the maximum cross-program invocation and instructions per transaction
	MaxInstructionTraceLength uint64
	// Sha256MaxSlices is the maximum number of slices hashed per syscall
	Sha256MaxSlices uint64
	// MaxCallDepth is the maximum SBF to BPF call depth
	MaxCallDepth uint64
	// StackFrameSize is the size of a stack frame in bytes
	StackFrameSize uint64
	// HeapSize is the program heap region size
	HeapSize uint32
}

// TransactionExecutionDetails contains the execution result and metadata
type TransactionExecutionDetails struct {
	// Status is the execution result (nil for success, error for failure)
	Status error
	// LogMessages contains the log messages produced during execution (if logging is enabled)
	LogMessages []string
	// InnerInstructions contains the inner instructions executed via CPI
	InnerInstructions []InnerInstructionsList
	// ReturnData contains the return data from the last instruction
	ReturnData *TransactionReturnData
	// ExecutedUnits is the number of compute units consumed
	ExecutedUnits uint64
	// AccountsDataLenDelta is the change in accounts data len for this transaction (only valid if Status is nil)
	AccountsDataLenDelta int64
}

// InnerInstructionsList represents the inner instructions for a single top-level instruction
type InnerInstructionsList struct {
	// Index is the index of the top-level instruction that produced these inner instructions
	Index uint8
	// Instructions contains the inner instructions
	Instructions []CompiledInstruction
}

// CompiledInstruction represents a compiled instruction.
// StackHeight is set for inner instructions to mirror Agave's
// UiCompiledInstruction; top-level top-level captures leave it zero.
type CompiledInstruction struct {
	// ProgramIdIndex is the index into the transaction's account keys of the program
	ProgramIdIndex uint8
	// Accounts contains the indices into the transaction's account keys
	Accounts []uint8
	// Data is the instruction data
	Data []byte
	// StackHeight is the depth at which this instruction executed.
	// 1 = top-level; >=2 = CPI invoked from within another instruction.
	StackHeight uint8
}

// TransactionReturnData represents data returned from a program
type TransactionReturnData struct {
	ProgramId solana.PublicKey
	Data      []byte
}

// AccountUpdate represents a single account update from transaction execution
type AccountUpdate struct {
	Pubkey  solana.PublicKey
	Account accounts.Account
}

// TransactionExecutionResult contains all the updates from executing a transaction
type TransactionExecutionResult struct {
	// AccountUpdates contains all account updates (modified accounts)
	AccountUpdates []AccountUpdate
	// WritableAccounts contains the pubkeys of all writable accounts (slice for ordered iteration)
	WritableAccounts []solana.PublicKey
	// WritableAccountSet is the same as WritableAccounts but as a set for O(1) lookups
	WritableAccountSet map[solana.PublicKey]struct{}
	// ModifiedStakeAccounts contains stake accounts that were modified
	ModifiedStakeAccounts []solana.PublicKey
	// ModifiedVoteAccounts contains vote accounts that were modified
	ModifiedVoteAccounts map[solana.PublicKey]*sealevel.VoteStateVersions
}

// agaveInstrErrName converts a Mithril InstrErr*/TxErr* sentinel into the
// Agave-format JSON value (bare string for unit variants, object for
// Custom and BorshIoError).
func agaveInstrErrName(err error) interface{} {
	if err == nil {
		return nil
	}
	// TODO: propagate the program-defined u32 through InstructionError
	// rather than emitting 0 as a placeholder.
	if err == sealevel.InstrErrCustom {
		return map[string]uint32{"Custom": 0}
	}
	if err == sealevel.InstrErrBorshIoError {
		return map[string]string{"BorshIoError": err.Error()}
	}
	name := err.Error()
	switch {
	case strings.HasPrefix(name, "InstrErr"):
		return strings.TrimPrefix(name, "InstrErr")
	case strings.HasPrefix(name, "TxErr"):
		return strings.TrimPrefix(name, "TxErr")
	}
	return name
}

// MarshalJSON renders TransactionError in Agave's wire format: bare string
// for unit variants, single-key object for tuple variants such as
// {"InstructionError":[idx, inner]}.
func (e *TransactionError) MarshalJSON() ([]byte, error) {
	if e == nil {
		return []byte("null"), nil
	}
	switch e.ErrorType {
	case TransactionErrorInstructionError:
		var idx uint8
		if e.InstructionIndex != nil {
			idx = *e.InstructionIndex
		}
		return json.Marshal(map[string]interface{}{
			"InstructionError": []interface{}{idx, agaveInstrErrName(e.InstructionError)},
		})
	case TransactionErrorInsufficientFundsForRent:
		var ai uint8
		if e.AccountIndex != nil {
			ai = *e.AccountIndex
		}
		return json.Marshal(map[string]interface{}{
			"InsufficientFundsForRent": map[string]uint8{"account_index": ai},
		})
	case TransactionErrorProgramExecutionTemporarilyRestricted:
		var ai uint8
		if e.AccountIndex != nil {
			ai = *e.AccountIndex
		}
		return json.Marshal(map[string]interface{}{
			"ProgramExecutionTemporarilyRestricted": map[string]uint8{"account_index": ai},
		})
	case TransactionErrorDuplicateInstruction:
		var idx uint8
		if e.InstructionIndex != nil {
			idx = *e.InstructionIndex
		}
		return json.Marshal(map[string]uint8{"DuplicateInstruction": idx})
	default:
		return json.Marshal(e.ErrorType.String())
	}
}
