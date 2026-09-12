package sealevel

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/arena"
	"github.com/Overclock-Validator/mithril/pkg/cu"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/lthash"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/gagliardetto/solana-go"
)

type ExecutionCtx struct {
	Log                      Logger
	Accounts                 accounts.Accounts
	TransactionContext       *TransactionCtx
	Features                 features.Features
	ComputeMeter             cu.ComputeMeter
	Blockhash                [32]byte
	PrevLamportsPerSignature uint64
	SlotCtx                  *SlotCtx
	ModifiedVoteStates       map[solana.PublicKey]*VoteStateVersions
	IsSimulation             bool

	// RecordInnerInstructions enables capture of CPI instructions into
	// InnerInstrs. Off by default; the simulate handler enables it when
	// the caller requests innerInstructions in the RPC response.
	RecordInnerInstructions bool
	currentTopLevelInstrIdx uint8
	InnerInstrs             []RecordedInnerInstr

	serializedAccountMetadataStack [][]serializedAcctMetadata
}

// RecordedInnerInstr is a CPI invocation captured during execution.
// Replay-layer code groups records by TopLevelIdx into the response shape.
type RecordedInnerInstr struct {
	TopLevelIdx    uint8
	StackHeight    uint8
	ProgramIdIndex uint8
	Accounts       []uint8
	Data           []byte
}

// SetCurrentTopLevelInstr is called by the replay loop before dispatching
// each top-level instruction so subsequent CPI captures know which entry
// in the response's innerInstructions array to belong to.
func (execCtx *ExecutionCtx) SetCurrentTopLevelInstr(idx uint8) {
	execCtx.currentTopLevelInstrIdx = idx
}

type SlotBank struct {
	PreviousSlot uint64
	BanksHash    [32]byte
}

// AccountReader is an optional slot-scoped read source that shadows AccountsDb.
// When set (rooted-durable mode), GetAccountFromAccountsDb resolves through it so
// execution sees confirmed-but-unrooted account versions, not rooted-only durable.
type AccountReader interface {
	GetAccount(slot uint64, pubkey solana.PublicKey) (*accounts.Account, error)
}

type SlotCtx struct {
	Accounts        accounts.Accounts
	ParentAccts     accounts.Accounts
	AccountsDb      *accountsdb.AccountsDb
	UnrootedRead    AccountReader // nil in legacy mode; shadows AccountsDb reads when set
	FeeRateGovernor *FeeRateGovernor
	Slot            uint64
	ParentSlot      uint64
	Epoch           uint64
	AcctMapsMu      *sync.Mutex // AcctMapsMu protects the next 2 maps
	ModifiedAccts   map[solana.PublicKey]bool
	WritableAccts   map[solana.PublicKey]bool
	NumSignatures   uint64 // signatures processed in this bank (resets for every child bank)
	Blockhash       [32]byte
	LastBlockhash   [32]byte
	SlotBank        SlotBank
	Features        *features.Features
	VoteTimestampMu *sync.Mutex
	// VoteTimestampsMu protects VoteTimestamps
	VoteTimestamps            map[solana.PublicKey]BlockTimestamp
	VoteAccts                 map[solana.PublicKey]uint64
	TotalEpochStake           uint64
	FinalBankhash             []byte
	AcctsLtHash               *lthash.LtHash
	EpochsAcctHash            []byte
	Replay                    bool
	LamportsBurnt             uint64
	TotalComputeUnitsConsumed uint64
	EahWorkaroundBankhash     []byte
	HasEahWorkaround          bool
	LatestEvictedBlockhash    [32]byte

	SerializedParameterArena *arena.Arena[byte]
	bankSysvars              atomic.Pointer[BankSysvars]

	TraceCtx context.Context
}

// BankSysvars returns the immutable sysvar snapshot owned by this bank.
func (slotCtx *SlotCtx) BankSysvars() *BankSysvars {
	if slotCtx == nil {
		return nil
	}
	return slotCtx.bankSysvars.Load()
}

// PublishBankSysvars atomically installs a complete sysvar generation. Bank
// construction and finalization publish only while transaction readers are
// quiescent; the atomic pointer also makes completed SlotCtx reads race-free.
func (slotCtx *SlotCtx) PublishBankSysvars(snapshot *BankSysvars) error {
	if slotCtx == nil {
		return fmt.Errorf("cannot publish bank sysvars to nil slot context")
	}
	if snapshot == nil {
		return fmt.Errorf("cannot publish nil bank sysvars for slot %d", slotCtx.Slot)
	}
	if snapshot.Slot() != slotCtx.Slot {
		return fmt.Errorf("bank sysvar slot %d does not match slot context %d", snapshot.Slot(), slotCtx.Slot)
	}
	slotCtx.bankSysvars.Store(snapshot)
	return nil
}

func (execCtx *ExecutionCtx) PrepareInstruction(ix Instruction, signers []solana.PublicKey) ([]InstructionAccount, []uint64, error) {
	txCtx := execCtx.TransactionContext

	ixCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return nil, nil, err
	}

	dedupInstructionAccounts := make([]InstructionAccount, 0, len(ix.Accounts))
	duplicateIndices := make([]uint64, 0, len(ix.Accounts))

	for instructionAcctIndex, accountMeta := range ix.Accounts {
		indexInTx, err := txCtx.IndexOfAccount(accountMeta.Pubkey)
		if err != nil {
			return nil, nil, err
		}

		duplicateIndex := -1
		for index, instrAcct := range dedupInstructionAccounts {
			if instrAcct.IndexInTransaction == indexInTx {
				duplicateIndex = index
				break
			}
		}

		if duplicateIndex != -1 {
			duplicateIndices = append(duplicateIndices, uint64(duplicateIndex))
			if duplicateIndex > len(dedupInstructionAccounts)-1 {
				return nil, nil, InstrErrNotEnoughAccountKeys
			}
			dedupInstructionAccounts[duplicateIndex].IsSigner = dedupInstructionAccounts[duplicateIndex].IsSigner || accountMeta.IsSigner
			dedupInstructionAccounts[duplicateIndex].IsWritable = dedupInstructionAccounts[duplicateIndex].IsWritable || accountMeta.IsWritable
		} else {
			indexInCaller, err := ixCtx.IndexOfInstructionAccount(txCtx, accountMeta.Pubkey)
			if err != nil {
				return nil, nil, err // InstructionError::MissingAccount
			}
			duplicateIndices = append(duplicateIndices, uint64(len(dedupInstructionAccounts)))

			instrAcct := InstructionAccount{IndexInTransaction: indexInTx,
				IndexInCaller: indexInCaller,
				IndexInCallee: uint64(instructionAcctIndex),
				IsSigner:      accountMeta.IsSigner,
				IsWritable:    accountMeta.IsWritable}

			dedupInstructionAccounts = append(dedupInstructionAccounts, instrAcct)
		}
	}

	for _, instructionAcct := range dedupInstructionAccounts {
		borrowedAcct, err := ixCtx.BorrowInstructionAccount(txCtx, instructionAcct.IndexInCaller)
		if err != nil {
			return nil, nil, err
		}

		// "Read-only in caller cannot become writable in callee"
		if instructionAcct.IsWritable && !borrowedAcct.IsWritable() {
			return nil, nil, InstrErrPrivilegeEscalation
		}

		// "To be signed in the callee,
		// it must be either signed in the caller or by the program"
		presentInSigners := false
		for _, addr := range signers {
			if addr == borrowedAcct.Key() {
				presentInSigners = true
				break
			}
		}
		if instructionAcct.IsSigner && !(borrowedAcct.IsSigner() || presentInSigners) {
			return nil, nil, InstrErrPrivilegeEscalation
		}
		borrowedAcct.Drop()
	}

	var instructionAccounts []InstructionAccount
	for _, duplicateIndex := range duplicateIndices {
		if duplicateIndex > uint64(len(dedupInstructionAccounts)-1) {
			return nil, nil, InstrErrNotEnoughAccountKeys
		}
		instrAcct := dedupInstructionAccounts[duplicateIndex]
		instructionAccounts = append(instructionAccounts, instrAcct)
	}

	// "Find and validate executables / program accounts"
	calleeProgramId := ix.ProgramId
	programAcctIdx, err := ixCtx.IndexOfInstructionAccount(txCtx, calleeProgramId)
	if err != nil {
		return nil, nil, err
	}

	borrowedProgramAcct, err := ixCtx.BorrowInstructionAccount(txCtx, programAcctIdx)
	if err != nil {
		return nil, nil, err
	}
	defer borrowedProgramAcct.Drop()

	if !borrowedProgramAcct.IsExecutable() {
		return nil, nil, InstrErrAccountNotExecutable
	}

	return instructionAccounts, []uint64{borrowedProgramAcct.IndexInTransaction}, nil
}

func (execCtx *ExecutionCtx) ProcessInstruction(instrData []byte, instructionAccts []InstructionAccount, programIndices []uint64) error {
	start := time.Now()
	nextInstrCtx, err := execCtx.TransactionContext.NextInstructionCtx()
	if err != nil {
		return err
	}
	metrics.GlobalBlockReplay.GetNextIxCtx.AddTimingSince(start)

	start = time.Now()
	nextInstrCtx.Configure(programIndices, instructionAccts, instrData)
	metrics.GlobalBlockReplay.NextIxCtxConfigure.AddTimingSince(start)

	// Capture this invocation as an inner instruction when nested under
	// an active top-level frame and recording is enabled. Stack height
	// before Push reflects parent depth: 0 = top-level, >=1 = CPI.
	if execCtx.RecordInnerInstructions && len(programIndices) > 0 && execCtx.StackHeight() >= 1 {
		acctIdxs := make([]uint8, 0, len(instructionAccts))
		for _, ia := range instructionAccts {
			acctIdxs = append(acctIdxs, uint8(ia.IndexInTransaction))
		}
		dataCopy := make([]byte, len(instrData))
		copy(dataCopy, instrData)
		execCtx.InnerInstrs = append(execCtx.InnerInstrs, RecordedInnerInstr{
			TopLevelIdx:    execCtx.currentTopLevelInstrIdx,
			StackHeight:    uint8(execCtx.StackHeight() + 1),
			ProgramIdIndex: uint8(programIndices[0]),
			Accounts:       acctIdxs,
			Data:           dataCopy,
		})
	}

	start = time.Now()
	err = execCtx.Push()
	if err != nil {
		return err
	}
	metrics.GlobalBlockReplay.IxPush.AddTimingSince(start)

	if len(programIndices) > 0 {
		programKey, keyErr := execCtx.TransactionContext.KeyOfAccountAtIndex(programIndices[len(programIndices)-1])
		if keyErr == nil {
			execCtx.TransactionContext.SetReturnData(programKey, []byte{})
		}
	}

	err1 := execCtx.ExecuteInstruction()

	start = time.Now()
	err2 := execCtx.Pop()
	metrics.GlobalBlockReplay.IxPop.AddTimingSince(start)

	if err1 != nil {
		return err1
	} else if err2 != nil {
		return err2
	}

	return nil
}

func (execCtx *ExecutionCtx) AddModifiedVoteState(pubkey solana.PublicKey, state *VoteStateVersions) {
	if execCtx.ModifiedVoteStates == nil {
		execCtx.ModifiedVoteStates = make(map[solana.PublicKey]*VoteStateVersions, 1)
	}
	execCtx.ModifiedVoteStates[pubkey] = state
}

func (execCtx *ExecutionCtx) ExecuteInstruction() error {
	start := time.Now()

	txCtx := execCtx.TransactionContext
	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return err
	}

	borrowedRootAccount, err := instrCtx.BorrowProgramAccount(txCtx, 0)
	if err != nil {
		return InstrErrUnsupportedProgramId
	}

	ownerId := borrowedRootAccount.Owner()
	rootAcctKey := borrowedRootAccount.Key()
	borrowedRootAccount.Drop()

	var builtinId solana.PublicKey
	if ownerId == a.NativeLoaderAddr {
		builtinId = rootAcctKey
	} else {
		builtinId = ownerId
	}

	nativeProgramFn, nativeProgramStr, err := resolveNativeProgramById(builtinId)
	if err != nil { // unrecognised builtin
		return err
	}
	metrics.GlobalBlockReplay.ExecIxResolveNativeProgram.AddTimingSince(start)

	start = time.Now()
	err = nativeProgramFn(execCtx)
	switch nativeProgramStr {
	case a.SystemProgramAddrStr:
		metrics.GlobalBlockReplay.ExecIxNativeProgramSystem.AddTimingSince(start)
	case a.StakeProgramAddrStr:
		metrics.GlobalBlockReplay.ExecIxNativeProgramStake.AddTimingSince(start)
	case a.VoteProgramAddrStr:
		metrics.GlobalBlockReplay.ExecIxNativeProgramVote.AddTimingSince(start)
	case a.ComputeBudgetProgramAddrStr:
		metrics.GlobalBlockReplay.ExecIxNativeProgramComputeBudget.AddTimingSince(start)
	case a.BpfLoader2AddrStr:
		metrics.GlobalBlockReplay.ExecIxNativeProgramBpfLoader2.AddTimingSince(start)
	case a.BpfLoaderDeprecatedAddrStr:
		metrics.GlobalBlockReplay.ExecIxNativeProgramBpfLoaderDeprecated.AddTimingSince(start)
	case a.BpfLoaderUpgradeableAddrStr:
		metrics.GlobalBlockReplay.ExecIxNativeProgramBpfLoaderUpgradeable.AddTimingSince(start)
	case a.ZkElgamalProofProgramAddrStr:
		metrics.GlobalBlockReplay.ExecIxNativeProgramZkElgamalProof.AddTimingSince(start)
	case a.Ed25519PrecompileAddrStr:
		metrics.GlobalBlockReplay.ExecIxNativeProgramEd25519Precompile.AddTimingSince(start)
	case a.Secp256kPrecompileAddrStr:
		metrics.GlobalBlockReplay.ExecIxNativeProgramSecp256kPrecompile.AddTimingSince(start)
	}

	return err
}

func (execCtx *ExecutionCtx) Push() error {
	txCtx := execCtx.TransactionContext

	idx := txCtx.InstructionTraceLength()
	instrCtx, err := txCtx.InstructionCtxAtIndexInTrace(idx)
	if err != nil {
		return err
	}

	programId, err := instrCtx.LastProgramKey(txCtx)
	if err != nil {
		return InstrErrUnsupportedProgramId
	}

	if txCtx.InstructionCtxStackHeight() != 0 {
		var contains bool
		for level := uint64(0); level < txCtx.InstructionCtxStackHeight(); level++ {
			ic, err := txCtx.InstructionCtxAtNestingLevel(level)
			if err == nil {
				programAcct, err := ic.BorrowLastProgramAccount(txCtx)
				if err == nil {
					programAcct.Drop()
					if programAcct.Key() == programId {
						contains = true
						break
					}
				}
			}
		}

		var isLast bool
		ic, err := txCtx.CurrentInstructionCtx()
		if err != nil {
			return err
		}
		programAcct, err := ic.BorrowLastProgramAccount(txCtx)
		if err == nil {
			if programAcct.Key() == programId {
				isLast = true
			}
			programAcct.Drop()
		}

		if contains && !isLast {
			return InstrErrReentrancyNotAllowed
		}
	}

	err = txCtx.Push()
	return err
}

func (execCtx *ExecutionCtx) Pop() error {
	return execCtx.TransactionContext.Pop()
}

func (execCtx *ExecutionCtx) StackHeight() uint64 {
	return execCtx.TransactionContext.InstructionCtxStackHeight()
}

func (execCtx *ExecutionCtx) NativeInvoke(instruction Instruction, signers []solana.PublicKey) error {
	instrAccts, programIndices, err := execCtx.PrepareInstruction(instruction, signers)
	if err != nil {
		return err
	}

	err = execCtx.ProcessInstruction(instruction.Data, instrAccts, programIndices)
	return err
}

func (execCtx *ExecutionCtx) CheckAligned() bool {
	txCtx := execCtx.TransactionContext
	instrCtx, err := txCtx.CurrentInstructionCtx()
	if err != nil {
		return true
	}

	programAcct, err := instrCtx.BorrowLastProgramAccount(txCtx)
	if err != nil {
		return true
	}
	defer programAcct.Drop()

	if programAcct.Owner() == a.BpfLoaderDeprecatedAddr {
		return false
	} else {
		return true
	}
}

func (slotCtx *SlotCtx) GetAccount(pubkey solana.PublicKey) (*accounts.Account, error) {
	pk := [32]byte(pubkey)
	acct, err := slotCtx.Accounts.GetAccount(&pk)
	if err != nil {
		return nil, err
	} else {
		return acct.Clone(), nil
	}
}

func (slotCtx *SlotCtx) GetAccountShared(pubkey solana.PublicKey) (*accounts.Account, error) {
	pk := [32]byte(pubkey)
	return slotCtx.Accounts.GetAccount(&pk)
}

func (slotCtx *SlotCtx) GetParentAccount(pubkey solana.PublicKey) (*accounts.Account, error) {
	acct, err := slotCtx.ParentAccts.GetAccountWithoutLock(pubkey)
	if err != nil {
		return nil, err
	} else {
		return acct, nil
	}
}

func (slotCtx *SlotCtx) GetAccountFromAccountsDb(pubkey solana.PublicKey) (*accounts.Account, error) {
	if bankSysvars := slotCtx.BankSysvars(); bankSysvars != nil && IsBankSysvarAccount(pubkey) {
		if acct, ok := bankSysvars.CloneAccount(pubkey); ok {
			return acct, nil
		}
		return nil, fmt.Errorf("bank sysvar account %s is absent at slot %d", pubkey, slotCtx.Slot)
	}
	if slotCtx.UnrootedRead != nil {
		return slotCtx.UnrootedRead.GetAccount(slotCtx.Slot, pubkey)
	}
	acct, err := slotCtx.AccountsDb.GetAccount(slotCtx.Slot, pubkey)
	if err != nil {
		return nil, err
	} else {
		return acct, nil
	}
}

func (slotCtx *SlotCtx) SetAccount(pubkey solana.PublicKey, acct *accounts.Account) error {
	pk := [32]byte(pubkey)
	err := slotCtx.Accounts.SetAccount(&pk, acct)
	return err
}

func (slotCtx *SlotCtx) RecordModifiedAcct(pubkey solana.PublicKey) {
	slotCtx.AcctMapsMu.Lock()
	defer slotCtx.AcctMapsMu.Unlock()
	if slotCtx.Features == nil || !slotCtx.Features.IsActive(features.RemoveAccountsDeltaHash) {
		slotCtx.WritableAccts[pubkey] = true
	}
	slotCtx.ModifiedAccts[pubkey] = true
}

func (slotCtx *SlotCtx) RecordModifiedAccountStates(accountStates []*accounts.Account, touched []bool) {
	if len(accountStates) != len(touched) {
		panic("account states/touched length mismatch")
	}
	slotCtx.AcctMapsMu.Lock()
	defer slotCtx.AcctMapsMu.Unlock()
	for idx, acct := range accountStates {
		if touched[idx] {
			slotCtx.ModifiedAccts[acct.Key] = true
		}
	}
}

func (slotCtx *SlotCtx) RecordWritableAcct(pubkey solana.PublicKey) {
	slotCtx.AcctMapsMu.Lock()
	defer slotCtx.AcctMapsMu.Unlock()
	slotCtx.WritableAccts[pubkey] = true
}

func (slotCtx *SlotCtx) RecordWritableAccts(pubkeys []solana.PublicKey) {
	slotCtx.AcctMapsMu.Lock()
	defer slotCtx.AcctMapsMu.Unlock()
	for _, pubkey := range pubkeys {
		slotCtx.WritableAccts[pubkey] = true
	}
}
