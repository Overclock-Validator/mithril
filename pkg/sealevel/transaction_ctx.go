package sealevel

import (
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
	"github.com/Overclock-Validator/wide"
	"github.com/gagliardetto/solana-go"
)

// One per tx processor.
var BorrowedAccountArenas []*arena.Arena[BorrowedAccount]

type TxReturnData struct {
	programId solana.PublicKey
	data      []byte
}

type TransactionAccounts struct {
	Accounts          []*accounts.Account
	Shared            []bool
	Locked            []bool
	Touched           []bool
	AcctMetas         []*AccountMeta
	OnFirstWriteClone func(*accounts.Account)
}

type TransactionCtx struct {
	InstructionTrace         []InstructionCtx
	InstructionStack         []uint64
	RetData                  TxReturnData
	AccountKeys              []solana.PublicKey
	AccountIndexLookup       map[solana.PublicKey]uint64
	Accounts                 TransactionAccounts
	ExecutableAccounts       []BorrowedAccount
	InstructionTraceCapacity uint64
	AccountsResizeDelta      int64
	Rent                     SysvarRent
	HeapSize                 uint32
	AllInstructions          []Instruction
	ComputeBudgetLimits      *ComputeBudgetLimits
	Signature                solana.Signature
	ModifiedStakeAccts       bool
	ModifiedVoteAccts        bool
	NonceAcctAdvanced        bool
	BorrowedAccountArena     *arena.Arena[BorrowedAccount]
}

func NewTransactionAccounts(accts []accounts.Account) *TransactionAccounts {
	transactionAccts := new(TransactionAccounts)

	transactionAccts.Accounts = make([]*accounts.Account, 0, len(accts))
	transactionAccts.Shared = make([]bool, len(accts))
	for _, acct := range accts {
		a := acct
		transactionAccts.Accounts = append(transactionAccts.Accounts, &a)
	}

	transactionAccts.Locked = make([]bool, len(accts), len(accts))
	transactionAccts.Touched = make([]bool, len(accts), len(accts))

	return transactionAccts
}

func NewTransactionAccountsFromRefs(accts []*accounts.Account, shared []bool) *TransactionAccounts {
	if len(accts) != len(shared) {
		panic("transaction accounts/shared flags length mismatch")
	}

	transactionAccts := new(TransactionAccounts)

	transactionAccts.Accounts = append(make([]*accounts.Account, 0, len(accts)), accts...)
	transactionAccts.Shared = append(make([]bool, 0, len(shared)), shared...)
	transactionAccts.Locked = make([]bool, len(accts), len(accts))
	transactionAccts.Touched = make([]bool, len(accts), len(accts))

	return transactionAccts
}

func NewTransactionCtx(txAccts TransactionAccounts, instrStackCapacity uint64, instrTraceCapacity uint64) *TransactionCtx {
	txCtx := new(TransactionCtx)

	txCtx.Accounts = txAccts
	txCtx.InstructionTraceCapacity = instrTraceCapacity
	txCtx.InstructionStack = make([]uint64, 0, instrStackCapacity)
	txCtx.InstructionTrace = append(txCtx.InstructionTrace, InstructionCtx{})
	txCtx.HeapSize = 32 * 1024

	txCtx.AccountKeys = make([]solana.PublicKey, 0, len(txAccts.Accounts))
	txCtx.AccountIndexLookup = make(map[solana.PublicKey]uint64, len(txAccts.Accounts))
	for idx, acct := range txAccts.Accounts {
		txCtx.AccountKeys = append(txCtx.AccountKeys, acct.Key)
		if _, exists := txCtx.AccountIndexLookup[acct.Key]; !exists {
			txCtx.AccountIndexLookup[acct.Key] = uint64(idx)
		}
	}

	return txCtx
}

func (txCtx *TransactionCtx) PushInstructionCtx(ixCtx InstructionCtx) {
	txCtx.InstructionTrace = append(txCtx.InstructionTrace, ixCtx)
}

func (txCtx *TransactionCtx) InstructionCtxStackHeight() uint64 {
	return uint64(len(txCtx.InstructionStack))
}

func (txCtx *TransactionCtx) CurrentInstructionCtx() (*InstructionCtx, error) {
	level, err := safemath.CheckedSubU64(txCtx.InstructionCtxStackHeight(), 1)
	if err != nil {
		return nil, InstrErrCallDepth
	}
	return txCtx.InstructionCtxAtNestingLevel(level)
}

func (txCtx *TransactionCtx) ReturnData() (solana.PublicKey, []byte) {
	return txCtx.RetData.programId, txCtx.RetData.data
}

func (txCtx *TransactionCtx) KeyOfAccountAtIndex(index uint64) (solana.PublicKey, error) {
	if len(txCtx.AccountKeys) == 0 || index > uint64(len(txCtx.AccountKeys)-1) {
		return solana.PublicKey{}, InstrErrNotEnoughAccountKeys
	}

	return txCtx.AccountKeys[index], nil
}

func (txCtx *TransactionCtx) SetReturnData(programId solana.PublicKey, data []byte) {
	txCtx.RetData.programId = programId
	txCtx.RetData.data = data
}

func (txCtx *TransactionCtx) IndexOfAccount(pubkey solana.PublicKey) (uint64, error) {
	if index, exists := txCtx.AccountIndexLookup[pubkey]; exists {
		return index, nil
	}
	return 0, InstrErrMissingAccount
}

func (txCtx *TransactionCtx) InstructionAcctsFromAccountMetas(instrAcctMetas []AccountMeta) []InstructionAccount {
	instrAccts := make([]InstructionAccount, 0, len(instrAcctMetas))
	idxInCalleeByTxIndex := make(map[uint64]uint64, len(instrAcctMetas))
	missingIndex := uint64(len(txCtx.Accounts.Accounts))

	for instrAcctIdx, accountMeta := range instrAcctMetas {
		idxInTx, exists := txCtx.AccountIndexLookup[accountMeta.Pubkey]
		if !exists {
			idxInTx = missingIndex
		}

		idxInCallee, exists := idxInCalleeByTxIndex[idxInTx]
		if !exists {
			idxInCallee = uint64(instrAcctIdx)
			idxInCalleeByTxIndex[idxInTx] = idxInCallee
		}

		instrAccts = append(instrAccts, InstructionAccount{
			IndexInTransaction: idxInTx,
			IndexInCaller:      idxInTx,
			IndexInCallee:      idxInCallee,
			IsSigner:           accountMeta.IsSigner,
			IsWritable:         accountMeta.IsWritable,
		})
	}

	return instrAccts
}

func (txCtx *TransactionCtx) NextInstructionCtx() (*InstructionCtx, error) {
	if len(txCtx.InstructionTrace) == 0 {
		return nil, InstrErrCallDepth
	}
	return &txCtx.InstructionTrace[len(txCtx.InstructionTrace)-1], nil
}

func (txCtx *TransactionCtx) InstructionCtxAtIndexInTrace(idxInTrace uint64) (*InstructionCtx, error) {
	if len(txCtx.InstructionTrace) == 0 || idxInTrace > uint64(len(txCtx.InstructionTrace)-1) {
		return nil, InstrErrCallDepth
	}
	return &txCtx.InstructionTrace[idxInTrace], nil
}

func (txCtx *TransactionCtx) InstructionTraceLength() uint64 {
	l := uint64(len(txCtx.InstructionTrace))
	return safemath.SaturatingSubU64(l, 1)
}

func (txCtx *TransactionCtx) InstructionCtxAtNestingLevel(nestingLevel uint64) (*InstructionCtx, error) {
	if len(txCtx.InstructionStack) == 0 || nestingLevel > uint64(len(txCtx.InstructionStack)-1) {
		return nil, InstrErrCallDepth
	}
	idxInTrace := txCtx.InstructionStack[nestingLevel]
	return txCtx.InstructionCtxAtIndexInTrace(idxInTrace)
}

func (txCtx *TransactionCtx) AccountAtIndex(idxInTx uint64) (*accounts.Account, error) {
	if len(txCtx.Accounts.Accounts) == 0 || idxInTx > uint64(len(txCtx.Accounts.Accounts)-1) {
		return nil, InstrErrNotEnoughAccountKeys
	}
	return txCtx.Accounts.Accounts[idxInTx], nil
}

func (txCtx *TransactionCtx) InstructionAccountsLamportSum(instrCtx *InstructionCtx) (wide.Uint128, error) {
	numInstrAccts := instrCtx.NumberOfInstructionAccounts()

	var instructionAcctsLamportSum wide.Uint128

	for instrAcctIdx := uint64(0); instrAcctIdx < numInstrAccts; instrAcctIdx++ {
		isDupe, _, err := instrCtx.IsInstructionAccountDuplicate(instrAcctIdx)
		if err != nil {
			return wide.NewUint128(0, 0), err
		}
		if isDupe {
			continue
		}

		idxInTx, err := instrCtx.IndexOfInstructionAccountInTransaction(instrAcctIdx)
		if err != nil {
			return wide.NewUint128(0, 0), err
		}

		acct, err := txCtx.AccountAtIndex(idxInTx)
		if err != nil {
			return wide.NewUint128(0, 0), err
		}

		lamportsUint128 := wide.Uint128FromUint64(acct.Lamports)
		instructionAcctsLamportSum, err = safemath.CheckedAddU128(instructionAcctsLamportSum, lamportsUint128)
		if err != nil {
			return wide.NewUint128(0, 0), InstrErrArithmeticOverflow
		}
	}

	return instructionAcctsLamportSum, nil
}

func (txCtx *TransactionCtx) Push() error {
	nestingLevel := txCtx.InstructionCtxStackHeight()

	if len(txCtx.InstructionTrace) == 0 {
		return InstrErrCallDepth
	}

	callerInstrCtx := txCtx.InstructionTrace[len(txCtx.InstructionTrace)-1]
	calleeInstructionAccountsLamportSum, err := txCtx.InstructionAccountsLamportSum(&callerInstrCtx)
	if err != nil {
		return err
	}

	if len(txCtx.InstructionStack) != 0 {
		callerInstrCtx, err := txCtx.CurrentInstructionCtx()
		if err != nil {
			return err
		}

		originalCallerInstrAcctsLamportSum := callerInstrCtx.InstructionAccountsLamportSum
		currentCallerInstructionAccountsLamportSum, err := txCtx.InstructionAccountsLamportSum(callerInstrCtx)
		if err != nil {
			return err
		}

		if originalCallerInstrAcctsLamportSum.Cmp(currentCallerInstructionAccountsLamportSum) != 0 {
			//mlog.Log.Debugf("unbalanced push")
			return InstrErrUnbalancedInstruction
		}
	}

	nextInstrCtx, err := txCtx.NextInstructionCtx()
	if err != nil {
		return err
	}
	nextInstrCtx.NestingLevel = nestingLevel
	nextInstrCtx.InstructionAccountsLamportSum = calleeInstructionAccountsLamportSum

	idxInTrace := txCtx.InstructionTraceLength()
	if idxInTrace >= txCtx.InstructionTraceCapacity {
		return InstrErrCallDepth
	}

	txCtx.InstructionTrace = append(txCtx.InstructionTrace, InstructionCtx{})
	txCtx.InstructionStack = append(txCtx.InstructionStack, idxInTrace)

	return nil
}

func (txCtx *TransactionCtx) Pop() error {
	if len(txCtx.InstructionStack) == 0 {
		return InstrErrCallDepth
	}

	currentInstrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	lamportsSum, err := txCtx.InstructionAccountsLamportSum(currentInstrCtx)
	if err != nil {
		return InstrErrUnbalancedInstruction
	}

	unbalanced := currentInstrCtx.InstructionAccountsLamportSum.Cmp(lamportsSum)

	// pop
	txCtx.InstructionStack = txCtx.InstructionStack[:len(txCtx.InstructionStack)-1]

	if unbalanced != 0 {
		return InstrErrUnbalancedInstruction
	}

	return nil
}

func (txAccounts *TransactionAccounts) GetAccount(idx uint64) (*accounts.Account, error) {
	if len(txAccounts.Accounts) == 0 || idx > (uint64(len(txAccounts.Accounts)-1)) {
		return nil, InstrErrMissingAccount
	}

	if txAccounts.IsLocked(idx) {
		return nil, InstrErrAccountBorrowFailed
	}

	txAccounts.Lock(idx)

	return txAccounts.Accounts[idx], nil
}

func (txAccounts *TransactionAccounts) IsLocked(idx uint64) bool {
	return txAccounts.Locked[idx]
}

func (txAccounts *TransactionAccounts) Lock(idx uint64) {
	txAccounts.Locked[idx] = true
}

func (txAccounts *TransactionAccounts) Unlock(idx uint64) {
	txAccounts.Locked[idx] = false
}

func (txAccounts *TransactionAccounts) Touch(idx uint64) (*accounts.Account, error) {
	if len(txAccounts.Touched) == 0 || idx > uint64(len(txAccounts.Touched)-1) {
		return nil, InstrErrNotEnoughAccountKeys
	}

	if txAccounts.Shared[idx] {
		clonedAcct := txAccounts.Accounts[idx].Clone()
		txAccounts.Accounts[idx] = clonedAcct
		txAccounts.Shared[idx] = false
		if txAccounts.OnFirstWriteClone != nil {
			txAccounts.OnFirstWriteClone(clonedAcct)
		}
	}

	txAccounts.Touched[idx] = true
	return txAccounts.Accounts[idx], nil
}
