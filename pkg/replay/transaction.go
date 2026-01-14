package replay

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/migration"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/rent"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/util"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

type TxErrInvalidSignature struct {
	msg string
}

func NewTxErrInvalidSignature(msg string) error {
	return &TxErrInvalidSignature{msg: msg}
}

func (err *TxErrInvalidSignature) Error() string {
	return err.msg
}

var (
	TxErrInsufficientFundsForRent          = errors.New("TxErrInsufficientFundsForRent")
	TxErrMaxLoadedAccountsDataSizeExceeded = errors.New("TxErrMaxLoadedAccountsDataSizeExceeded")
	TxErrProgramAccountNotFound            = errors.New("TxErrProgramAccountNotFound")
	TxErrInvalidProgramForExecution        = errors.New("TxErrInvalidProgramForExecution")
	TxErrInvalidBlockhash                  = errors.New("TxErrInvalidBlockhash")
)

func programIndices(tx *solana.Transaction, instrIdx int) []uint64 {
	idx := uint64(tx.Message.Instructions[instrIdx].ProgramIDIndex)
	return []uint64{idx}
}

const (
	maxStackCapacity      = 5
	maxInstrTraceCapacity = 64
)

func newExecCtx(slotCtx *sealevel.SlotCtx, transactionAccts *sealevel.TransactionAccounts, computeBudgetLimits *sealevel.ComputeBudgetLimits, log *sealevel.LogRecorder) *sealevel.ExecutionCtx {
	txCtx := sealevel.NewTransactionCtx(*transactionAccts, maxStackCapacity, maxInstrTraceCapacity)
	execCtx := &sealevel.ExecutionCtx{Log: log, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(uint64(computeBudgetLimits.ComputeUnitLimit)), PrevLamportsPerSignature: slotCtx.FeeRateGovernor.PrevLamportsPerSignature}

	execCtx.Features = *slotCtx.Features
	execCtx.Accounts = accounts.NewMemAccounts()
	execCtx.SlotCtx = slotCtx
	execCtx.TransactionContext.ComputeBudgetLimits = computeBudgetLimits
	execCtx.ModifiedVoteStates = make(map[solana.PublicKey]*sealevel.VoteStateVersions)
	return execCtx
}

func instrsAndAcctMetasFromTx(tx *solana.Transaction, f *features.Features) ([]sealevel.Instruction, [][]sealevel.AccountMeta, error) {
	instrs := make([]sealevel.Instruction, 0, len(tx.Message.Instructions))
	acctMetasPerInstr := make([][]sealevel.AccountMeta, 0, len(tx.Message.Instructions))

	for _, compiledInstr := range tx.Message.Instructions {
		programId, err := tx.ResolveProgramIDIndex(compiledInstr.ProgramIDIndex)
		if err != nil {
			return nil, nil, err
		}

		ams, err := compiledInstr.ResolveInstructionAccounts(&tx.Message)
		if err != nil {
			return nil, nil, err
		}

		var acctMetas []sealevel.AccountMeta
		for _, am := range ams {
			acctMeta := sealevel.AccountMeta{Pubkey: am.PublicKey, IsSigner: am.IsSigner, IsWritable: isWritable(tx, am, f)}
			acctMetas = append(acctMetas, acctMeta)
		}

		instr := sealevel.Instruction{Accounts: acctMetas, ProgramId: programId, Data: compiledInstr.Data}
		instrs = append(instrs, instr)
		acctMetasPerInstr = append(acctMetasPerInstr, acctMetas)
	}

	return instrs, acctMetasPerInstr, nil
}

func fixupInstructionsSysvarAcct(execCtx *sealevel.ExecutionCtx, instrIdx uint16) error {
	instructionsSysvarIdx, err := execCtx.TransactionContext.IndexOfAccount(sealevel.SysvarInstructionsAddr)
	if err == nil {
		instructionsAcct, err := execCtx.TransactionContext.AccountAtIndex(instructionsSysvarIdx)
		if err != nil {
			return err
		}

		lastIndex := len(instructionsAcct.Data) - 2
		binary.LittleEndian.PutUint16(instructionsAcct.Data[lastIndex:], instrIdx)
		//mlog.Log.Debugf("found instructions sysvar pubkey at instr idx %d", instrIdx)
	}
	return nil
}

var newReservedAccts = []solana.PublicKey{a.AddressLookupTableAddr, a.ComputeBudgetProgramAddr,
	a.Ed25519PrecompileAddr, a.LoaderV4Addr, a.Secp256kPrecompileAddr, a.ZkElgamalProofProgramAddr,
	a.ZkTokenProofProgramAddr, sealevel.SysvarEpochRewardsAddr, sealevel.SysvarLastRestartSlotAddr, a.SysvarOwnerAddr}

func isWritable(tx *solana.Transaction, am *solana.AccountMeta, f *features.Features) bool {
	if !am.IsWritable {
		return false
	}

	if isNativeProgram(am.PublicKey) || isSysvar(am.PublicKey) {
		return false
	}

	if f.IsActive(features.AddNewReservedAccountKeys) {
		if slices.Contains(newReservedAccts, am.PublicKey) {
			return false
		}
	}

	if f.IsActive(features.EnableSecp256r1Precompile) {
		if am.PublicKey == a.Secp256r1PrecompileAddr {
			return false
		}
	}

	programIds, err := tx.GetProgramIDs()
	if err != nil {
		panic(err)
	}

	for _, programId := range programIds {
		if am.PublicKey == programId {
			return false
		}
	}

	return true
}

func handleModifiedAccounts(slotCtx *sealevel.SlotCtx, execCtx *sealevel.ExecutionCtx) {
	// update account states in slotCtx for all accounts 'touched' during the tx's execution
	for idx, newAcctState := range execCtx.TransactionContext.Accounts.Accounts {
		if execCtx.TransactionContext.Accounts.Touched[idx] {
			// clean up accounts closed during the tx (garbage collection)
			if newAcctState.Lamports == 0 {
				newAcctState = &accounts.Account{Key: newAcctState.Key, RentEpoch: math.MaxUint64}
			}

			err := slotCtx.SetAccount(newAcctState.Key, newAcctState)
			if err != nil {
				panic(fmt.Sprintf("unable to set slot account for %s to update state: %s", newAcctState.Key, err))
			}
			slotCtx.RecordModifiedAcct(newAcctState.Key)
			//mlog.Log.Debugf("modified account %s after tx", newAcctState.Key)
		}
	}
}

func recordStakeDelegation(acct *accounts.Account) {
	isEmpty := acct.Lamports == 0
	isUninitialized := true

	if len(acct.Data) >= 4 {
		acctType := binary.LittleEndian.Uint32(acct.Data)
		isUninitialized = acctType == sealevel.StakeStateV2StatusUninitialized
	}

	if isEmpty || isUninitialized {
		global.DeleteStakeCacheItem(acct.Key)
	} else {
		stakeState, err := sealevel.UnmarshalStakeState(acct.Data)
		if err == nil {
			delegation := stakeState.Stake.Stake.Delegation
			delegation.CreditsObserved = stakeState.Stake.Stake.CreditsObserved
			global.PutStakeCacheItem(acct.Key, &delegation)
		}
	}
}

func recordVoteTimestampAndSlot(slotCtx *sealevel.SlotCtx, acct *accounts.Account) {
	voteStateVersioned := new(sealevel.VoteStateVersions)
	decoder := bin.NewBinDecoder(acct.Data)

	err := voteStateVersioned.UnmarshalWithDecoder(decoder)
	if err != nil {
		panic(fmt.Sprintf("unable to deserialize versioned vote state - shouldn't be possible. %s", err))
	}

	var timestamp sealevel.BlockTimestamp

	switch voteStateVersioned.Type {
	case sealevel.VoteStateVersionCurrent:
		timestamp = voteStateVersioned.Current.LastTimestamp

	case sealevel.VoteStateVersionV0_23_5:
		timestamp = voteStateVersioned.V0_23_5.LastTimestamp

	case sealevel.VoteStateVersionV1_14_11:
		timestamp = voteStateVersioned.V1_14_11.LastTimestamp
	}

	slotCtx.VoteTimestampMu.Lock()
	defer slotCtx.VoteTimestampMu.Unlock()
	slotCtx.VoteTimestamps[acct.Key] = timestamp
}

func recordStakeAndVoteAccounts(slotCtx *sealevel.SlotCtx, execCtx *sealevel.ExecutionCtx, writablePubkeys []solana.PublicKey) {
	modifiedVoteAccts := execCtx.TransactionContext.ModifiedVoteAccts

	for _, acct := range execCtx.TransactionContext.Accounts.Accounts {
		if !slices.Contains(writablePubkeys, acct.Key) {
			continue
		}

		if modifiedVoteAccts && acct.Owner == a.VoteProgramAddr {
			recordVoteTimestampAndSlot(slotCtx, acct)
			newVersionedVoteState, wasModified := execCtx.ModifiedVoteStates[acct.Key]
			if wasModified {
				global.PutVoteCacheItem(acct.Key, newVersionedVoteState)
			}
		}

		if acct.Owner == a.StakeProgramAddr {
			recordStakeDelegation(acct)
		}
	}
}

func handleFailedTx(slotCtx *sealevel.SlotCtx, tx *solana.Transaction, instrs []sealevel.Instruction, computeBudgetLimits *sealevel.ComputeBudgetLimits, instrErr error, rentStateErr error) (*fees.TxFeeInfo, error) {
	txFeeInfo := fees.CalculateTxFees(tx, instrs, computeBudgetLimits, slotCtx.Features)

	payerAcctKey := tx.Message.AccountKeys[0]
	p, err := slotCtx.GetAccount(payerAcctKey)
	if err != nil {
		panic(fmt.Sprintf("unable to get slot account to update payer acct state after failed tx: %s", err))
	}

	if txFeeInfo.TotalFee > p.Lamports {
		return nil, sealevel.InstrErrInsufficientFunds
	}

	p.Lamports -= txFeeInfo.TotalFee
	err = slotCtx.SetAccount(payerAcctKey, p)
	if err != nil {
		panic(fmt.Sprintf("unable to set slot account to update state of payer acct after failed t: %s", err))
	}
	slotCtx.RecordModifiedAcct(payerAcctKey)

	if len(instrs) >= 1 {
		instr := instrs[0]
		noncePubkey, didAdvanceNonceAcct := sealevel.MaybeAdvanceNonceAccountForFailedTx(slotCtx, tx, instr)
		if didAdvanceNonceAcct {
			slotCtx.RecordModifiedAcct(noncePubkey)
		}
	}

	var relevantErr error
	if instrErr != nil {
		relevantErr = instrErr
	} else {
		relevantErr = rentStateErr
	}

	return txFeeInfo, relevantErr
}

func verifySignatures(tx *solana.Transaction, slot uint64, sigverifyWg *sync.WaitGroup) {
	defer sigverifyWg.Done()
	start := time.Now()
	err := tx.VerifySignatures()
	if err != nil {
		panic(fmt.Sprintf("error - tx %s (version = %d) had an invalid signature: %s", tx.Signatures[0], tx.Message.GetVersion(), err))
	}
	metrics.GlobalBlockReplay.Sigverify.AddTimingSince(start)
}

func ProcessTransaction(slotCtx *sealevel.SlotCtx, sigverifyWg *sync.WaitGroup, tx *solana.Transaction, txMeta *rpc.TransactionMeta, dbgOpts *DebugOptions, arena *arena.Arena[sealevel.BorrowedAccount]) (*fees.TxFeeInfo, error) {
	if arena != nil {
		arena.Reset()
	}
	start := time.Now()
	sigverifyWg.Add(1)
	go verifySignatures(tx, slotCtx.Slot, sigverifyWg)

	if len(tx.Signatures) > 0 && dbgOpts.IsDebugTx(tx.Signatures[0]) {
		mlog.Log.Infof("Turning on debug logs while executing tx %s", tx.Signatures[0])
		mlog.Log.EnableInfLogging()
		defer mlog.Log.DisableInfLogging()
	}

	instrs, acctMetasPerInstr, err := instrsAndAcctMetasFromTx(tx, slotCtx.Features)
	if err != nil {
		return nil, err
	}
	metrics.GlobalBlockReplay.InstructionsAndAccountMetasFromTx.AddTimingSince(start)

	start = time.Now()
	computeBudgetLimits, err := sealevel.ComputeBudgetExecuteInstructions(instrs, slotCtx.Features)
	if err != nil {
		return nil, err
	}
	metrics.GlobalBlockReplay.ComputeBudgetExecutionInstructions.AddTimingSince(start)

	if !sealevel.IsTransactionAgeValid(tx, instrs, slotCtx) {
		return nil, TxErrInvalidBlockhash
	}

	instrsAcct := sealevel.MakeInstructionsSysvarAccount(instrs)

	start = time.Now()
	var transactionAccts *sealevel.TransactionAccounts

	if slotCtx.Features.IsActive(features.FormalizeLoadedTransactionDataSize) {
		transactionAccts, err = loadAndValidateTxAcctsSimd186(slotCtx, acctMetasPerInstr, tx, instrs, instrsAcct, computeBudgetLimits.LoadedAccountBytes)
	} else {
		transactionAccts, err = loadAndValidateTxAccts(slotCtx, acctMetasPerInstr, tx, instrs, instrsAcct, computeBudgetLimits.LoadedAccountBytes)
	}
	if err == TxErrMaxLoadedAccountsDataSizeExceeded || err == TxErrInvalidProgramForExecution || err == TxErrProgramAccountNotFound {
		return handleFailedTx(slotCtx, tx, instrs, computeBudgetLimits, err, nil)
	} else if err != nil {
		return nil, err
	}
	metrics.GlobalBlockReplay.AccountsFromTx.AddTimingSince(start)

	var log sealevel.LogRecorder
	execCtx := newExecCtx(slotCtx, transactionAccts, computeBudgetLimits, &log)
	execCtx.TransactionContext.AllInstructions = instrs
	execCtx.TransactionContext.Signature = tx.Signatures[0]
	execCtx.TransactionContext.BorrowedAccountArena = arena

	start = time.Now()
	// check for pre-balance divergences
	if txMeta != nil {
		for count := uint64(0); count < uint64(len(tx.Message.AccountKeys)); count++ {
			txAcct, err := execCtx.TransactionContext.Accounts.GetAccount(count)
			if err != nil {
				panic(fmt.Sprintf("unable to get tx acct %d whilst checking for pre-balances divergences", count))
			}
			if dbgOpts.IsDebugTx(tx.Signatures[0]) {
				// Avoid calling util.PrettyPrintAcct when not debug logging.
				////mlog.Log.Debugf("pre-balance account used in tx=%s: %s", tx.Signatures[0], util.PrettyPrintAcct(txAcct))
			}

			if !isNativeProgram(txAcct.Key) && !txAcct.IsDummy {
				if txAcct.Lamports != txMeta.PreBalances[count] {
					mlog.Log.Errorf("[run:%s] DIVERGENCE in slot %d: tx %s pre-balance mismatch for %s: mithril=%d, onchain=%d",
						CurrentRunID, slotCtx.Slot, tx.Signatures[0], txAcct.Key, txAcct.Lamports, txMeta.PreBalances[count])
					panic(fmt.Sprintf("tx %s pre-balance divergence: lamport balance for %s was %d but onchain lamport balance was %d\n%s", tx.Signatures[0], txAcct.Key, txAcct.Lamports, txMeta.PreBalances[count], util.PrettyPrintAcct(txAcct)))
				}
			}

			execCtx.TransactionContext.Accounts.Unlock(count)
		}
	}
	metrics.GlobalBlockReplay.PreBalanceDivergenceCheck.AddTimingSince(start)

	start = time.Now()
	txFeeInfo, _, err := fees.CalculateAndDeductTxFees(tx, txMeta, instrs, &execCtx.TransactionContext.Accounts, computeBudgetLimits, slotCtx.Features)
	if err != nil {
		return txFeeInfo, nil
	}

	metrics.GlobalBlockReplay.CalcAndDeductFees.AddTimingSince(start)

	// check for fee divergences
	if txMeta != nil && txFeeInfo.TotalFee != txMeta.Fee {
		mlog.Log.Errorf("[run:%s] DIVERGENCE in slot %d: tx %s fee mismatch: mithril=%d, onchain=%d",
			CurrentRunID, slotCtx.Slot, tx.Signatures[0], txFeeInfo.TotalFee, txMeta.Fee)
		panic(fmt.Sprintf("tx %s fee divergence: totalFee was %d, but onchain fee was %d", tx.Signatures[0], txFeeInfo.TotalFee, txMeta.Fee))
	}

	start = time.Now()
	rentSysvar, err := sealevel.ReadRentSysvar(execCtx)
	if err != nil {
		panic("failed to get and deserialize rent sysvar")
	}
	metrics.GlobalBlockReplay.ReadRentSysvar.AddTimingSince(start)

	start = time.Now()
	rent.MaybeSetRentExemptRentEpochMax(slotCtx, &rentSysvar, &execCtx.Features, &execCtx.TransactionContext.Accounts)
	preTxRentStates := rent.NewRentStateInfo(&rentSysvar, execCtx.TransactionContext, tx, &execCtx.Features)
	metrics.GlobalBlockReplay.PreTxRentStates.AddTimingSince(start)

	var instrErr error
	writablePubkeys := make([]solana.PublicKey, 0, 64)

	start = time.Now()
	for instrIdx, instr := range tx.Message.Instructions {
		ixStart := time.Now()
		err = fixupInstructionsSysvarAcct(execCtx, uint16(instrIdx))
		if err != nil {
			return txFeeInfo, err
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

	if dbgOpts.IsDebugTx(tx.Signatures[0]) {
		for _, l := range log.Logs {
			mlog.Log.Debugf("%s", l)
		}
	}

	// check for CU consumed divergences
	if instrErr == nil && *txMeta.ComputeUnitsConsumed != execCtx.ComputeMeter.Used() {
		discrepancy := max(execCtx.ComputeMeter.Used(), *txMeta.ComputeUnitsConsumed) - min(execCtx.ComputeMeter.Used(), *txMeta.ComputeUnitsConsumed)
		var sign byte
		if execCtx.ComputeMeter.Used() > *txMeta.ComputeUnitsConsumed {
			sign = '+'
		} else {
			sign = '-'
		}
		mlog.Log.Infof("tx %s CU divergence: used was %d but onchain CU consumed was %d (%c%d discrepancy) [non-failing]", tx.Signatures[0], execCtx.ComputeMeter.Used(), *txMeta.ComputeUnitsConsumed, sign, discrepancy)
	}

	start = time.Now()
	postTxRentStates := rent.NewRentStateInfo(&rentSysvar, execCtx.TransactionContext, tx, &execCtx.Features)
	rentStateErr := rent.VerifyRentStateChanges(preTxRentStates, postTxRentStates, execCtx.TransactionContext)
	metrics.GlobalBlockReplay.PostTxRentStates.AddTimingSince(start)

	start = time.Now()
	// check for post-balances divergences (but only if the tx succeeded)
	if txMeta != nil && instrErr == nil && rentStateErr == nil {
		var errBuf strings.Builder
		for count := uint64(0); count < uint64(len(tx.Message.AccountKeys)); count++ {
			txAcct, err := execCtx.TransactionContext.Accounts.GetAccount(count)
			if err != nil {
				panic(fmt.Sprintf("unable to get tx acct %d whilst checking for post-balances divergences", count))
			}

			if !isNativeProgram(txAcct.Key) && !txAcct.IsDummy {
				if txAcct.Lamports != txMeta.PostBalances[count] {
					errBuf.WriteString(fmt.Sprintf(
						" - lamport balance for %s was %d but onchain lamport balance was %d\n%s\n",
						txAcct.Key, txAcct.Lamports, txMeta.PostBalances[count], util.PrettyPrintAcct(txAcct)))
				}
			}
			execCtx.TransactionContext.Accounts.Unlock(count)
		}
		if errBuf.Len() > 0 {
			mlog.Log.Errorf("[run:%s] DIVERGENCE in slot %d: tx %s post-balance mismatches detected",
				CurrentRunID, slotCtx.Slot, tx.Signatures[0])
			msg := fmt.Sprintf("tx %s post-balance divergences:", tx.Signatures[0]) + errBuf.String()
			panic(msg)
		}
	}
	metrics.GlobalBlockReplay.PostBalanceDivergenceCheck.AddTimingSince(start)

	start = time.Now()
	payerAcct := execCtx.TransactionContext.Accounts.Accounts[0]

	// if there was an error in the tx, do not update account states, except for deducting the tx fee
	// from the payer account and advancing the nonce account if applicable
	if instrErr != nil || rentStateErr != nil {
		return handleFailedTx(slotCtx, tx, instrs, computeBudgetLimits, instrErr, rentStateErr)
	}

	txAcctMetas, err := tx.AccountMetaList()
	if err != nil {
		panic(err)
	}

	for _, txAcctMeta := range txAcctMetas {
		if isWritable(tx, txAcctMeta, &execCtx.Features) {
			writablePubkeys = append(writablePubkeys, txAcctMeta.PublicKey)
		}
	}

	for _, pk := range writablePubkeys {
		slotCtx.RecordWritableAcct(pk)
	}

	handleModifiedAccounts(slotCtx, execCtx)
	writablePubkeys = append(writablePubkeys, payerAcct.Key)
	recordStakeAndVoteAccounts(slotCtx, execCtx, writablePubkeys)
	metrics.GlobalBlockReplay.TxUpdateAccounts.AddTimingSince(start)

	return txFeeInfo, nil
}
