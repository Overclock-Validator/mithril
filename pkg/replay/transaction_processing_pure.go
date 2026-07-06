package replay

import (
	"math"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/migration"
	"github.com/Overclock-Validator/mithril/pkg/rent"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

// LoadAndExecuteTransactionInput contains all the input state needed for pure transaction processing
type LoadAndExecuteTransactionInput struct {
	// SlotCtx provides access to accounts and slot state (used read-only)
	SlotCtx *sealevel.SlotCtx
	// Transaction is the transaction to process
	Transaction *solana.Transaction
	// Arena for borrowed accounts (optional)
	Arena *arena.Arena[sealevel.BorrowedAccount]
	// TxMeta is the on-chain transaction metadata (optional, used for fee calculation)
	TxMeta *rpc.TransactionMeta
	// IsSimulation indicates this is a simulation (no side effects on shared state)
	IsSimulation bool
	// RecordInnerInstructions enables CPI recording. The simulate handler
	// sets this when the RPC request asks for innerInstructions.
	RecordInnerInstructions bool
}

// LoadAndExecuteTransaction is a pure function that loads and executes a transaction.
// It takes all required state as input and returns all state changes as output without
// modifying the input SlotCtx. This function can be used for both actual execution
// and simulation.
// sanitizeFailureOutput returns the canonical SanitizeFailure response.
// InstructionError is left nil so the RPC renderer falls back to
// ErrorType.String() and emits Agave-format "SanitizeFailure".
func sanitizeFailureOutput() LoadAndExecuteTransactionOutput {
	return LoadAndExecuteTransactionOutput{
		ProcessingResult: TransactionProcessingResult{
			TransactionError: &TransactionError{
				ErrorType: TransactionErrorSanitizeFailure,
			},
		},
	}
}

func LoadAndExecuteTransaction(input LoadAndExecuteTransactionInput) LoadAndExecuteTransactionOutput {
	tx := input.Transaction
	slotCtx := input.SlotCtx

	if input.Arena != nil {
		input.Arena.Reset()
	}

	// Agave-style sanitize: reject malformed user txs before they
	// reach downstream index panics.
	hdr := tx.Message.Header
	numKeys := len(tx.Message.AccountKeys)
	if hdr.NumReadonlySignedAccounts >= hdr.NumRequiredSignatures ||
		int(hdr.NumRequiredSignatures) > len(tx.Signatures) ||
		len(tx.Signatures) > numKeys {
		return sanitizeFailureOutput()
	}
	// Mirror block-replay's StaticInstructionLimit cap so pre-activation
	// clusters fail mid-execution like Agave instead of SanitizeFailure.
	if slotCtx.Features.IsActive(features.StaticInstructionLimit) &&
		len(tx.Message.Instructions) > maxInstrTraceCapacity {
		return sanitizeFailureOutput()
	}
	for _, ci := range tx.Message.Instructions {
		if int(ci.ProgramIDIndex) >= numKeys {
			return sanitizeFailureOutput()
		}
		for _, idx := range ci.Accounts {
			if int(idx) >= numKeys {
				return sanitizeFailureOutput()
			}
		}
	}

	// Parse instructions and account metas
	start := time.Now()
	instrs, acctMetasPerInstr, err := instrsAndAcctMetasFromTx(tx, slotCtx.Features)
	if err != nil {
		return LoadAndExecuteTransactionOutput{
			ProcessingResult: TransactionProcessingResult{
				TransactionError: &TransactionError{
					ErrorType:        TransactionErrorSanitizeFailure,
					InstructionError: err,
				},
			},
		}
	}
	metrics.GlobalBlockReplay.InstructionsAndAccountMetasFromTx.AddTimingSince(start)

	// Compute budget limits
	start = time.Now()
	computeBudgetLimits, err := sealevel.ComputeBudgetExecuteInstructions(instrs, slotCtx.Features)
	if err != nil {
		return LoadAndExecuteTransactionOutput{
			ProcessingResult: TransactionProcessingResult{
				TransactionError: &TransactionError{
					ErrorType:        TransactionErrorInstructionError,
					InstructionError: err,
				},
			},
			Instrs:       instrs,
		}
	}
	metrics.GlobalBlockReplay.ComputeBudgetExecutionInstructions.AddTimingSince(start)

	// Validate transaction age
	if !sealevel.IsTransactionAgeValid(tx, instrs, slotCtx) {
		return LoadAndExecuteTransactionOutput{
			ProcessingResult: TransactionProcessingResult{
				TransactionError: &TransactionError{
					ErrorType: TransactionErrorBlockhashNotFound,
				},
			},
			Instrs:              instrs,
			ComputeBudgetLimits: computeBudgetLimits,
		}
	}

	// Load and validate accounts
	start = time.Now()
	instrsAcct := sealevel.MakeInstructionsSysvarAccount(instrs)
	var transactionAccts *sealevel.TransactionAccounts
	var txAcctMetas []*solana.AccountMeta

	if slotCtx.Features.IsActive(features.FormalizeLoadedTransactionDataSize) {
		transactionAccts, txAcctMetas, err = loadAndValidateTxAcctsSimd186(slotCtx, acctMetasPerInstr, tx, instrs, instrsAcct, computeBudgetLimits.LoadedAccountBytes)
	} else {
		transactionAccts, txAcctMetas, err = loadAndValidateTxAccts(slotCtx, acctMetasPerInstr, tx, instrs, instrsAcct, computeBudgetLimits.LoadedAccountBytes)
	}

	// Base output fields for all paths after parsing
	baseFields := func(out *LoadAndExecuteTransactionOutput) {
		out.Instrs = instrs
		out.ComputeBudgetLimits = computeBudgetLimits
	}

	if err == TxErrMaxLoadedAccountsDataSizeExceeded || err == TxErrInvalidProgramForExecution || err == TxErrProgramAccountNotFound {
		errType := mapLoadErrorType(err)
		out := LoadAndExecuteTransactionOutput{
			ProcessingResult: TransactionProcessingResult{
				TransactionError: &TransactionError{
					ErrorType:        errType,
					InstructionError: err,
				},
			},
		}
		baseFields(&out)
		return out
	} else if err != nil {
		out := LoadAndExecuteTransactionOutput{
			ProcessingResult: TransactionProcessingResult{
				TransactionError: &TransactionError{
					ErrorType:        TransactionErrorAccountNotFound,
					InstructionError: err,
				},
			},
		}
		baseFields(&out)
		return out
	}
	metrics.GlobalBlockReplay.AccountsFromTx.AddTimingSince(start)

	// Create execution context
	var log sealevel.LogRecorder
	execCtx := newExecCtx(slotCtx, transactionAccts, computeBudgetLimits, &log)
	execCtx.TransactionContext.AllInstructions = instrs
	execCtx.TransactionContext.Signature = tx.Signatures[0]
	execCtx.TransactionContext.BorrowedAccountArena = input.Arena
	execCtx.IsSimulation = input.IsSimulation
	execCtx.RecordInnerInstructions = input.RecordInnerInstructions

	// Capture pre-balance lamports (before fee deduction)
	preBalances := make([]uint64, len(tx.Message.AccountKeys))
	for i := range tx.Message.AccountKeys {
		acct := execCtx.TransactionContext.Accounts.Accounts[i]
		preBalances[i] = acct.Lamports
	}

	// Snapshot accounts so the simulate response can decode pre-state
	// token balances. Cloning is required because execution mutates
	// transactionAccts in place. Only the simulate handler reads this;
	// block-replay callers leave it nil and ignore.
	var preAccountSnapshots []*accounts.Account
	if input.IsSimulation {
		preAccountSnapshots = make([]*accounts.Account, len(execCtx.TransactionContext.Accounts.Accounts))
		for i, acct := range execCtx.TransactionContext.Accounts.Accounts {
			if acct == nil {
				continue
			}
			preAccountSnapshots[i] = acct.Clone()
		}
	}

	// Calculate and deduct fees
	start = time.Now()
	txFeeInfo, _, err := fees.CalculateAndDeductTxFees(tx, input.TxMeta, instrs, &execCtx.TransactionContext.Accounts, computeBudgetLimits, slotCtx.Features, input.IsSimulation)
	if err != nil {
		out := LoadAndExecuteTransactionOutput{
			ProcessingResult: TransactionProcessingResult{
				TransactionError: &TransactionError{
					ErrorType:        TransactionErrorInsufficientFundsForFee,
					InstructionError: err,
				},
			},
			ExecCtx:             execCtx,
			PreBalances:         preBalances,
			PreAccountSnapshots: preAccountSnapshots,
			FeeInfo:             txFeeInfo,
		}
		baseFields(&out)
		return out
	}
	metrics.GlobalBlockReplay.CalcAndDeductFees.AddTimingSince(start)

	// Read rent sysvar
	start = time.Now()
	rentSysvar, err := sealevel.ReadRentSysvar(execCtx)
	if err != nil {
		// Rent sysvar unreadable; return cleanly so the RPC worker
		// doesn't crash on local-state corruption.
		return LoadAndExecuteTransactionOutput{
			ProcessingResult: TransactionProcessingResult{
				TransactionError: &TransactionError{
					ErrorType:        TransactionErrorAccountNotFound,
					InstructionError: err,
				},
			},
			Instrs:              instrs,
			ComputeBudgetLimits: computeBudgetLimits,
		}
	}
	metrics.GlobalBlockReplay.ReadRentSysvar.AddTimingSince(start)

	// Set rent-exempt rent epoch max and compute pre-tx rent states
	start = time.Now()
	rent.MaybeSetRentExemptRentEpochMax(slotCtx, &rentSysvar, &execCtx.Features, &execCtx.TransactionContext.Accounts)
	preTxRentStates := rent.NewRentStateInfo(&rentSysvar, execCtx.TransactionContext, &execCtx.Features)
	metrics.GlobalBlockReplay.PreTxRentStates.AddTimingSince(start)

	// Execute all instructions
	var instrErr error
	writablePubkeys := make([]solana.PublicKey, 0, 64)

	start = time.Now()
	for instrIdx, instr := range tx.Message.Instructions {
		execCtx.SetCurrentTopLevelInstr(uint8(instrIdx))
		ixStart := time.Now()
		err = fixupInstructionsSysvarAcct(execCtx, uint16(instrIdx))
		if err != nil {
			instrErr = err
			break
		}
		metrics.GlobalBlockReplay.FixupInstructionsSysvarAccount.AddTimingSince(ixStart)

		ixStart = time.Now()
		acctMetas := acctMetasPerInstr[instrIdx]
		instructionAccts := sealevel.InstructionAcctsFromAccountMetas(acctMetas, *transactionAccts)
		metrics.GlobalBlockReplay.InstructionAccountsFromAccountMetas.AddTimingSince(ixStart)

		programId := tx.Message.AccountKeys[instr.ProgramIDIndex]
		migratingCus, isMigrating := migration.IsMigratingProgramAndGetCUs(programId)
		if isMigrating {
			err = execCtx.ComputeMeter.Consume(migratingCus)
			if err != nil {
				instrErr = err
				break
			}
			execCtx.ComputeMeter.Disable()
		}

		err = execCtx.ProcessInstruction(instr.Data, instructionAccts, programIndices(tx, instrIdx))
		if err == nil {
			for _, am := range acctMetas {
				if am.IsWritable {
					writablePubkeys = append(writablePubkeys, am.Pubkey)
				}
			}
			if isMigrating {
				execCtx.ComputeMeter.Enable()
			}
		} else {
			instrErr = err
			break
		}
	}
	metrics.GlobalBlockReplay.IxLoop.AddTimingSince(start)

	// Check rent state transitions
	start = time.Now()
	postTxRentStates := rent.NewRentStateInfo(&rentSysvar, execCtx.TransactionContext, &execCtx.Features)
	rentStateErr := rent.VerifyRentStateChanges(preTxRentStates, postTxRentStates, execCtx.TransactionContext)
	metrics.GlobalBlockReplay.PostTxRentStates.AddTimingSince(start)

	// If there was an error, return failed transaction result
	if instrErr != nil || rentStateErr != nil {
		var relevantErr error
		var errType TransactionErrorType
		if instrErr != nil {
			relevantErr = instrErr
			errType = TransactionErrorInstructionError
		} else {
			relevantErr = rentStateErr
			errType = TransactionErrorInsufficientFundsForRent
		}

		out := LoadAndExecuteTransactionOutput{
			ProcessingResult: TransactionProcessingResult{
				TransactionError: &TransactionError{
					ErrorType:        errType,
					InstructionError: relevantErr,
				},
			},
			ExecCtx:             execCtx,
			PreBalances:         preBalances,
			PreAccountSnapshots: preAccountSnapshots,
			FeeInfo:             txFeeInfo,
		}
		baseFields(&out)
		return out
	}

	// Success path: collect all writable accounts
	writablePubkeySet := make(map[solana.PublicKey]struct{}, len(txAcctMetas))
	for _, txAcctMeta := range txAcctMetas {
		if isWritable(txAcctMeta, &execCtx.Features) {
			writablePubkeys = append(writablePubkeys, txAcctMeta.PublicKey)
			writablePubkeySet[txAcctMeta.PublicKey] = struct{}{}
		}
	}

	// Add payer to writable sets
	payerAcct := execCtx.TransactionContext.Accounts.Accounts[0]
	writablePubkeys = append(writablePubkeys, payerAcct.Key)
	writablePubkeySet[payerAcct.Key] = struct{}{}

	// Collect modified accounts for simulation output
	accountUpdates := collectAccountUpdates(execCtx)

	// Collect modified vote accounts
	modifiedVoteAccounts := make(map[solana.PublicKey]*sealevel.VoteStateVersions)
	for pk, voteState := range execCtx.ModifiedVoteStates {
		modifiedVoteAccounts[pk] = voteState
	}

	// Calculate loaded accounts data size
	var loadedAccountsDataSize uint32
	for _, acct := range transactionAccts.Accounts {
		if !acct.IsDummy {
			loadedAccountsDataSize += uint32(len(acct.Data))
		}
	}

	// Collect return data
	var returnData *TransactionReturnData
	programId, data := execCtx.TransactionContext.ReturnData()
	if len(data) > 0 {
		returnData = &TransactionReturnData{
			ProgramId: programId,
			Data:      data,
		}
	}

	// Calculate accounts data len delta
	var accountsDataLenDelta int64
	for idx, acct := range execCtx.TransactionContext.Accounts.Accounts {
		if execCtx.TransactionContext.Accounts.Touched[idx] {
			originalAcct := transactionAccts.Accounts[idx]
			accountsDataLenDelta += int64(len(acct.Data)) - int64(len(originalAcct.Data))
		}
	}

	// Build compute budget
	computeBudget := SVMTransactionExecutionBudget{
		ComputeUnitLimit:          uint64(computeBudgetLimits.ComputeUnitLimit),
		MaxInstructionStackDepth:  maxStackCapacity,
		MaxInstructionTraceLength: maxInstrTraceCapacity,
		Sha256MaxSlices:           cu.CUSha256MaxSlices,
		MaxCallDepth:              64,
		StackFrameSize:            4096,
		HeapSize:                  computeBudgetLimits.UpdatedHeapBytes,
	}

	// Build loaded transaction
	loadedTransaction := LoadedTransaction{
		ProgramIndices:         []uint16{},
		FeeDetails:             *txFeeInfo,
		ComputeBudget:          computeBudget,
		LoadedAccountsDataSize: loadedAccountsDataSize,
	}

	// Build execution details
	executionDetails := TransactionExecutionDetails{
		Status:               nil,
		LogMessages:          log.Logs,
		InnerInstructions:    AssembleInnerInstructions(execCtx),
		ReturnData:           returnData,
		ExecutedUnits:        execCtx.ComputeMeter.Used(),
		AccountsDataLenDelta: accountsDataLenDelta,
	}

	// Build processed transaction
	processedTx := ProcessedTransaction{
		TransactionType: ProcessedTransactionTypeExecuted,
		Executed: &ExecutedTransaction{
			LoadedTransaction: loadedTransaction,
			ExecutionDetails:  executionDetails,
			ProgramsModified:  make(map[solana.PublicKey]bool),
		},
	}

	// Build execution result
	executionResult := &TransactionExecutionResult{
		AccountUpdates:        accountUpdates,
		WritableAccounts:      writablePubkeys,
		WritableAccountSet:    writablePubkeySet,
		ModifiedVoteAccounts:  modifiedVoteAccounts,
		ModifiedStakeAccounts: []solana.PublicKey{},
	}

	out := LoadAndExecuteTransactionOutput{
		ProcessingResult: TransactionProcessingResult{
			ProcessedTransaction: &processedTx,
		},
		ExecutionResult:     executionResult,
		ExecCtx:             execCtx,
		PreBalances:         preBalances,
		PreAccountSnapshots: preAccountSnapshots,
		FeeInfo:             txFeeInfo,
	}
	baseFields(&out)
	return out
}

// mapLoadErrorType maps account loading errors to TransactionErrorType
func mapLoadErrorType(err error) TransactionErrorType {
	switch err {
	case TxErrMaxLoadedAccountsDataSizeExceeded:
		return TransactionErrorMaxLoadedAccountsDataSizeExceeded
	case TxErrInvalidProgramForExecution:
		return TransactionErrorInvalidProgramForExecution
	case TxErrProgramAccountNotFound:
		return TransactionErrorProgramAccountNotFound
	default:
		return TransactionErrorAccountNotFound
	}
}

// AssembleInnerInstructions groups CPI captures from execCtx by their
// top-level instruction index and converts them into the response shape.
// Returns nil when no captures were recorded (e.g. simulate without
// innerInstructions=true, or block-replay).
//
// Exported so the simulate RPC handler can reach into a partially-executed
// execCtx on the InstructionError path — Agave wire format keeps captured
// CPIs on tx failure even though Mithril's bifurcated processing-result
// type discards them via the TransactionError early-return.
func AssembleInnerInstructions(execCtx *sealevel.ExecutionCtx) []InnerInstructionsList {
	if len(execCtx.InnerInstrs) == 0 {
		return nil
	}

	byTopLevel := make(map[uint8][]CompiledInstruction)
	order := make([]uint8, 0)
	for _, r := range execCtx.InnerInstrs {
		if _, seen := byTopLevel[r.TopLevelIdx]; !seen {
			order = append(order, r.TopLevelIdx)
		}
		byTopLevel[r.TopLevelIdx] = append(byTopLevel[r.TopLevelIdx], CompiledInstruction{
			ProgramIdIndex: r.ProgramIdIndex,
			Accounts:       append([]uint8{}, r.Accounts...),
			Data:           append([]byte{}, r.Data...),
			StackHeight:    r.StackHeight,
		})
	}

	result := make([]InnerInstructionsList, 0, len(order))
	for _, idx := range order {
		result = append(result, InnerInstructionsList{
			Index:        idx,
			Instructions: byTopLevel[idx],
		})
	}
	return result
}

// collectAccountUpdates collects all modified accounts from the execution context
func collectAccountUpdates(execCtx *sealevel.ExecutionCtx) []AccountUpdate {
	updates := make([]AccountUpdate, 0)

	for idx, newAcctState := range execCtx.TransactionContext.Accounts.Accounts {
		if execCtx.TransactionContext.Accounts.Touched[idx] {
			acct := *newAcctState
			if acct.Lamports == 0 {
				acct = accounts.Account{Key: acct.Key, RentEpoch: math.MaxUint64}
			}

			updates = append(updates, AccountUpdate{
				Pubkey:  acct.Key,
				Account: acct,
			})
		}
	}

	return updates
}

