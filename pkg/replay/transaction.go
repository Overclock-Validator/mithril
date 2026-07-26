package replay

import (
	"encoding/binary"
	"errors"
	"fmt"
	"runtime/trace"
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
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/txverify"
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
	TxErrSanitizeFailure                   = errors.New("TxErrSanitizeFailure")
)

const (
	maxStackCapacity      = 5
	maxInstrTraceCapacity = 64
)

type discardLogger struct{}

func (discardLogger) Log(string) {}

func newExecCtx(slotCtx *sealevel.SlotCtx, transactionAccts *sealevel.TransactionAccounts, computeBudgetLimits *sealevel.ComputeBudgetLimits, log sealevel.Logger) *sealevel.ExecutionCtx {
	txCtx := sealevel.NewTransactionCtx(*transactionAccts, maxStackCapacity, maxInstrTraceCapacity)
	execCtx := &sealevel.ExecutionCtx{Log: log, TransactionContext: txCtx, ComputeMeter: cu.NewComputeMeter(uint64(computeBudgetLimits.ComputeUnitLimit)), PrevLamportsPerSignature: slotCtx.FeeRateGovernor.PrevLamportsPerSignature}

	execCtx.Features = *slotCtx.Features
	execCtx.Accounts = accounts.NewMemAccounts()
	execCtx.SlotCtx = slotCtx
	execCtx.TransactionContext.ComputeBudgetLimits = computeBudgetLimits
	return execCtx
}

type txAccountKeyInfo struct {
	firstIndex  uint64
	isProgramID bool
}

func instrsAndAcctMetasFromTx(tx *solana.Transaction, f *features.Features) ([]sealevel.Instruction, [][]sealevel.InstructionAccount, []*solana.AccountMeta, error) {
	instrs := make([]sealevel.Instruction, 0, len(tx.Message.Instructions))
	instructionAcctsPerInstr := make([][]sealevel.InstructionAccount, 0, len(tx.Message.Instructions))

	// AccountMetaList is relatively expensive and used to be rebuilt once per
	// instruction by ResolveInstructionAccounts, then once more by the account
	// loader. Build it once and share it with both parsing and loading.
	txAcctMetas, err := tx.AccountMetaList()
	if err != nil {
		return nil, nil, nil, err
	}

	keyInfo := make(map[solana.PublicKey]txAccountKeyInfo, len(txAcctMetas))
	for idx, acctMeta := range txAcctMetas {
		if _, exists := keyInfo[acctMeta.PublicKey]; !exists {
			keyInfo[acctMeta.PublicKey] = txAccountKeyInfo{firstIndex: uint64(idx)}
		}
	}

	// "write-demote" program IDs unless the upgradeable loader is present
	// in the transaction.
	for _, compiledInstr := range tx.Message.Instructions {
		if int(compiledInstr.ProgramIDIndex) >= len(tx.Message.AccountKeys) {
			return nil, nil, nil, TxErrSanitizeFailure
		}
		pid := tx.Message.AccountKeys[compiledInstr.ProgramIDIndex]
		info := keyInfo[pid]
		info.isProgramID = true
		keyInfo[pid] = info
	}
	upgradeableLoaderPresent := false
	for _, key := range tx.Message.AccountKeys {
		if key == a.BpfLoaderUpgradeableAddr {
			upgradeableLoaderPresent = true
			break
		}
	}
	demoteProgramIDs := !upgradeableLoaderPresent

	for _, compiledInstr := range tx.Message.Instructions {
		programId, err := tx.ResolveProgramIDIndex(compiledInstr.ProgramIDIndex)
		if err != nil {
			return nil, nil, nil, err
		}

		acctMetas := make([]sealevel.AccountMeta, len(compiledInstr.Accounts))
		instructionAccts := make([]sealevel.InstructionAccount, len(compiledInstr.Accounts))
		for instrAcctIdx, acctIdx := range compiledInstr.Accounts {
			if int(acctIdx) >= len(txAcctMetas) {
				return nil, nil, nil, TxErrSanitizeFailure
			}

			am := txAcctMetas[acctIdx]
			info := keyInfo[am.PublicKey]
			acctMeta := sealevel.AccountMeta{
				Pubkey:     am.PublicKey,
				IsSigner:   am.IsSigner,
				IsWritable: isWritableForInstr(am, info.isProgramID, demoteProgramIDs, f),
			}
			acctMetas[instrAcctIdx] = acctMeta

			idxInCallee := uint64(instrAcctIdx)
			for previousIdx := 0; previousIdx < instrAcctIdx; previousIdx++ {
				if instructionAccts[previousIdx].IndexInTransaction == info.firstIndex {
					idxInCallee = uint64(previousIdx)
					break
				}
			}
			instructionAccts[instrAcctIdx] = sealevel.InstructionAccount{
				IndexInTransaction: info.firstIndex,
				IndexInCaller:      info.firstIndex,
				IndexInCallee:      idxInCallee,
				IsSigner:           acctMeta.IsSigner,
				IsWritable:         acctMeta.IsWritable,
			}
		}

		instr := sealevel.Instruction{Accounts: acctMetas, ProgramId: programId, Data: compiledInstr.Data}
		instrs = append(instrs, instr)
		instructionAcctsPerInstr = append(instructionAcctsPerInstr, instructionAccts)
	}

	return instrs, instructionAcctsPerInstr, txAcctMetas, nil
}

func instructionsSysvarAccountIndex(tx *solana.Transaction) int {
	for idx, pubkey := range tx.Message.AccountKeys {
		if pubkey == sealevel.SysvarInstructionsAddr {
			return idx
		}
	}
	return -1
}

func fixupInstructionsSysvarAcct(execCtx *sealevel.ExecutionCtx, instructionsSysvarIdx int, instrIdx uint16) error {
	instructionsAcct, err := execCtx.TransactionContext.AccountAtIndex(uint64(instructionsSysvarIdx))
	if err != nil {
		return err
	}

	lastIndex := len(instructionsAcct.Data) - 2
	binary.LittleEndian.PutUint16(instructionsAcct.Data[lastIndex:], instrIdx)
	return nil
}

func isWritable(am *solana.AccountMeta, f *features.Features) bool {
	acctMeta := sealevel.AccountMeta{
		Pubkey:     am.PublicKey,
		IsSigner:   am.IsSigner,
		IsWritable: am.IsWritable,
	}
	return sealevel.IsWritable(&acctMeta, f)
}

func accountsDeltaHashRemoved(slotCtx *sealevel.SlotCtx) bool {
	return slotCtx != nil && slotCtx.Features != nil && slotCtx.Features.IsActive(features.RemoveAccountsDeltaHash)
}

func isWritableForInstr(am *solana.AccountMeta, isProgramID bool, demoteProgramIDs bool, f *features.Features) bool {
	// writability checks (native programs, sysvars, reserved keys, etc.)
	if !isWritable(am, f) {
		return false
	}

	if demoteProgramIDs && isProgramID {
		return false
	}

	return true
}

type transactionPublicationStats struct {
	touchedAccounts     uint64
	touchedAccountBytes uint64
}

func handleModifiedAccounts(slotCtx *sealevel.SlotCtx, execCtx *sealevel.ExecutionCtx) transactionPublicationStats {
	// update account states in slotCtx for all accounts 'touched' during the tx's execution
	transactionAccounts := &execCtx.TransactionContext.Accounts
	var stats transactionPublicationStats
	for idx, newAcctState := range transactionAccounts.Accounts {
		if transactionAccounts.Touched[idx] {
			// Track touched account stats for profiling
			stats.touchedAccounts++
			stats.touchedAccountBytes += uint64(len(newAcctState.Data))
		}
	}
	if err := accounts.SetTransactionAccounts(slotCtx.Accounts, transactionAccounts.Accounts, transactionAccounts.Touched); err != nil {
		panic(fmt.Sprintf("unable to publish transaction account states: %s", err))
	}
	slotCtx.RecordModifiedAccountStates(transactionAccounts.Accounts, transactionAccounts.Touched)

	// Record touched stats for clone optimization profiling
	TxAcctsTouched.Add(stats.touchedAccounts)
	TxAcctsTouchedBytes.Add(stats.touchedAccountBytes)
	return stats
}

func recordStakeDelegation(slot uint64, acct *accounts.Account) {
	isEmpty := acct.Lamports == 0
	isUninitialized := true

	if len(acct.Data) >= 4 {
		acctType := binary.LittleEndian.Uint32(acct.Data)
		isUninitialized = acctType == sealevel.StakeStateV2StatusUninitialized
	}

	if !isEmpty && !isUninitialized {
		// Slot-keyed enqueue: the entry reaches the durable index only when
		// this slot folds; an unwound wrong-fork slot drops it. Scans see it
		// from RAM meanwhile (StreamStakeAccounts merges pending entries).
		global.EnqueuePendingStakePubkey(slot, acct.Key)
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

	case sealevel.VoteStateVersionV4:
		timestamp = voteStateVersioned.V4.LastTimestamp
	}

	slotCtx.VoteTimestampMu.Lock()
	defer slotCtx.VoteTimestampMu.Unlock()
	slotCtx.VoteTimestamps[acct.Key] = timestamp
}

func recordStakeAndVoteAccount(slotCtx *sealevel.SlotCtx, execCtx *sealevel.ExecutionCtx, acct *accounts.Account, modifiedVoteAccts bool) {
	if acct.Lamports == 0 || acct.Owner != a.VoteProgramAddr {
		if global.VoteCacheItem(acct.Key) != nil {
			global.DeleteVoteCacheItem(acct.Key)
			markVoteStakeDirty(slotCtx.Slot) // global cache mutated — gates in-loop unwind
		}
	} else if modifiedVoteAccts {
		recordVoteTimestampAndSlot(slotCtx, acct)
		newVersionedVoteState, wasModified := execCtx.ModifiedVoteStates[acct.Key]
		if wasModified {
			global.PutVoteCacheItem(acct.Key, newVersionedVoteState)
		}
		markVoteStakeDirty(slotCtx.Slot)
	}

	if acct.Owner == a.StakeProgramAddr {
		recordStakeDelegation(slotCtx.Slot, acct)
		markVoteStakeDirty(slotCtx.Slot)
	}
}

func recordStakeAndVoteAccounts(slotCtx *sealevel.SlotCtx, execCtx *sealevel.ExecutionCtx, writablePubkeySet map[solana.PublicKey]struct{}) {
	modifiedVoteAccts := execCtx.TransactionContext.ModifiedVoteAccts

	for _, acct := range execCtx.TransactionContext.Accounts.Accounts {
		if _, isWritable := writablePubkeySet[acct.Key]; !isWritable {
			continue
		}
		recordStakeAndVoteAccount(slotCtx, execCtx, acct, modifiedVoteAccts)
	}
}

// transactionAccountIsWritable reproduces the writable-set union used by the
// materialized result without allocating a map. The payer is always included,
// and duplicate pubkeys inherit writability from any matching account meta.
func transactionAccountIsWritable(execCtx *sealevel.ExecutionCtx, accountIdx int) bool {
	txAccts := &execCtx.TransactionContext.Accounts
	if accountIdx < 0 || accountIdx >= len(txAccts.Accounts) || txAccts.Accounts[accountIdx] == nil {
		return false
	}

	key := txAccts.Accounts[accountIdx].Key
	if len(txAccts.Accounts) > 0 && txAccts.Accounts[0] != nil && key == txAccts.Accounts[0].Key {
		return true
	}

	for metaIdx, meta := range txAccts.AcctMetas {
		if meta == nil || meta.Pubkey != key || metaIdx >= len(txAccts.Accounts) {
			continue
		}
		if sealevel.IsWritable(meta, &execCtx.Features) {
			return true
		}
	}
	return false
}

func recordStakeAndVoteAccountsFromMetas(slotCtx *sealevel.SlotCtx, execCtx *sealevel.ExecutionCtx) {
	modifiedVoteAccts := execCtx.TransactionContext.ModifiedVoteAccts
	for idx, acct := range execCtx.TransactionContext.Accounts.Accounts {
		if !transactionAccountIsWritable(execCtx, idx) {
			continue
		}
		recordStakeAndVoteAccount(slotCtx, execCtx, acct, modifiedVoteAccts)
	}
}

func handleFailedTx(slotCtx *sealevel.SlotCtx, tx *solana.Transaction, instrs []sealevel.Instruction, computeBudgetLimits *sealevel.ComputeBudgetLimits, instrErr error, rentStateErr error) (*fees.TxFeeInfo, error) {
	recordMetrics := slotCtx != nil && slotCtx.Replay
	var totalStart time.Time
	var preparationStart time.Time
	if recordMetrics {
		totalStart = time.Now()
		preparationStart = totalStart
		defer func() {
			metrics.GlobalBlockReplay.TxFailedUpdateAccounts.AddTimingSince(totalStart)
		}()
	}

	txFeeInfo := fees.CalculateTxFees(tx, instrs, computeBudgetLimits, slotCtx.Features)

	payerAcctKey := tx.Message.AccountKeys[0]
	p, err := slotCtx.GetAccount(payerAcctKey)
	if err != nil {
		panic(fmt.Sprintf("unable to get slot account to update payer acct state after failed tx: %s", err))
	}

	if txFeeInfo.TotalFee > p.Lamports {
		if recordMetrics {
			metrics.GlobalBlockReplay.TxFailedPublicationPreparation.AddTimingSince(preparationStart)
		}
		return nil, sealevel.InstrErrInsufficientFunds
	}

	if recordMetrics {
		metrics.GlobalBlockReplay.TxFailedPublicationPreparation.AddTimingSince(preparationStart)
	}
	var payerStart time.Time
	if recordMetrics {
		payerStart = time.Now()
	}
	p.Lamports -= txFeeInfo.TotalFee
	err = slotCtx.SetAccount(payerAcctKey, p)
	if err != nil {
		panic(fmt.Sprintf("unable to set slot account to update state of payer acct after failed t: %s", err))
	}
	slotCtx.RecordModifiedAcct(payerAcctKey)
	if recordMetrics {
		metrics.GlobalBlockReplay.TxFailedPayerPublication.AddTimingSince(payerStart)
	}

	if len(instrs) >= 1 {
		instr := instrs[0]
		recordNonce := recordMetrics && sealevel.IsNonceInstr(instr)
		var nonceStart time.Time
		if recordNonce {
			nonceStart = time.Now()
		}
		noncePubkey, didAdvanceNonceAcct := sealevel.MaybeAdvanceNonceAccountForFailedTx(slotCtx, tx, instr)
		if didAdvanceNonceAcct {
			slotCtx.RecordModifiedAcct(noncePubkey)
			if recordNonce {
				metrics.GlobalBlockReplay.TxFailedNoncePublication.AddTimingSince(nonceStart)
			}
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

type sigverifySnapshot struct {
	slot             uint64
	version          solana.MessageVersion
	resolved         bool
	requiredSigs     uint8
	readonlySigned   uint8
	readonlyUnsigned uint8
	staticKeys       int
	totalKeys        int
	lookups          int
	firstKeys        []solana.PublicKey
	signers          []solana.PublicKey
	signatures       []solana.Signature
	message          []byte
}

// buildSigverifySnapshot captures what verification needs, and NOTHING
// rendered: it runs on the execution path for every transaction, and the
// base58 strings the failure diagnostics want (tx signature, signer and key
// previews) used to be built eagerly here — ~10 encodings per tx paid on
// data only ever read when a signature fails, which is a halt. They render
// lazily in diagContext now.
func buildSigverifySnapshot(tx *solana.Transaction, slot uint64) (*sigverifySnapshot, error) {
	message, err := txverify.MessageBytes(tx)
	if err != nil {
		return nil, err
	}

	numStaticKeys := len(tx.Message.AccountKeys)
	if tx.Message.IsResolved() {
		numStaticKeys -= tx.Message.AddressTableLookups.NumLookups()
	}

	signers := tx.Message.Signers()
	snapshot := &sigverifySnapshot{
		slot:             slot,
		version:          tx.Message.GetVersion(),
		resolved:         tx.Message.IsResolved(),
		requiredSigs:     tx.Message.Header.NumRequiredSignatures,
		readonlySigned:   tx.Message.Header.NumReadonlySignedAccounts,
		readonlyUnsigned: tx.Message.Header.NumReadonlyUnsignedAccounts,
		staticKeys:       numStaticKeys,
		totalKeys:        len(tx.Message.AccountKeys),
		lookups:          tx.Message.AddressTableLookups.NumLookups(),
		firstKeys:        append([]solana.PublicKey(nil), tx.Message.AccountKeys[:min(6, len(tx.Message.AccountKeys))]...),
		signers:          append([]solana.PublicKey(nil), signers...),
		signatures:       append([]solana.Signature(nil), tx.Signatures...),
		message:          message,
	}

	return snapshot, nil
}

// txSigString is the transaction's primary signature, rendered on demand
// (failure paths only).
func (s *sigverifySnapshot) txSigString() string {
	if len(s.signatures) == 0 {
		return "<missing>"
	}
	return s.signatures[0].String()
}

// diagContext renders the failure diagnostics from the raw snapshot data.
func (s *sigverifySnapshot) diagContext() string {
	firstSigners := make([]string, 0, min(4, len(s.signers)))
	for i := 0; i < min(4, len(s.signers)); i++ {
		firstSigners = append(firstSigners, s.signers[i].String())
	}
	firstKeys := make([]string, 0, len(s.firstKeys))
	for _, key := range s.firstKeys {
		firstKeys = append(firstKeys, key.String())
	}
	return fmt.Sprintf("slot=%d tx=%s version=%d resolved=%t required_sigs=%d readonly_signed=%d readonly_unsigned=%d static_keys=%d total_keys=%d lookups=%d signers=%v first_keys=%v",
		s.slot, s.txSigString(), s.version, s.resolved, s.requiredSigs, s.readonlySigned, s.readonlyUnsigned,
		s.staticKeys, s.totalKeys, s.lookups, firstSigners, firstKeys)
}

func verifySignatures(snapshot *sigverifySnapshot, sigverifyWg *sync.WaitGroup) {
	defer sigverifyWg.Done()
	start := time.Now()

	if len(snapshot.signers) != len(snapshot.signatures) {
		mlog.Log.Errorf("sigverify context: %s", snapshot.diagContext())
		panic(fmt.Sprintf("error - tx %s (version = %d) had mismatched signers/signatures: got %d signers, but %d signatures",
			snapshot.txSigString(), snapshot.version, len(snapshot.signers), len(snapshot.signatures)))
	}

	for i, sig := range snapshot.signatures {
		if snapshot.signers[i].Verify(snapshot.message, sig) {
			continue
		}
		mlog.Log.Errorf("sigverify context: %s", snapshot.diagContext())
		panic(fmt.Sprintf("error - tx %s (version = %d) had an invalid signature: invalid signature by %s",
			snapshot.txSigString(), snapshot.version, snapshot.signers[i]))
	}
	metrics.GlobalBlockReplay.Sigverify.AddTimingSince(start)
}

func processTransactionComputeUnits(execCtx *sealevel.ExecutionCtx) uint64 {
	if execCtx == nil {
		return 0
	}
	return execCtx.ComputeMeter.Used()
}

func ProcessTransaction(slotCtx *sealevel.SlotCtx, sigverifyWg *sync.WaitGroup, tx *solana.Transaction, txMeta *rpc.TransactionMeta, dbgOpts *DebugOptions, arena *arena.Arena[sealevel.BorrowedAccount], shouldVerifySignatures bool) (*fees.TxFeeInfo, uint64, error) {
	if trace.IsEnabled() && slotCtx.TraceCtx != nil {
		regionType := "ProcessTransaction"
		if tx.IsVote() {
			regionType = "ProcessVote"
		}
		region := trace.StartRegion(slotCtx.TraceCtx, regionType)
		defer region.End()

		if len(tx.Signatures) > 0 {
			trace.Log(slotCtx.TraceCtx, "signature", tx.Signatures[0].String())
		}
		if len(tx.Message.Instructions) > 0 {
			progIdx := tx.Message.Instructions[0].ProgramIDIndex
			if int(progIdx) < len(tx.Message.AccountKeys) {
				trace.Log(slotCtx.TraceCtx, "program", tx.Message.AccountKeys[progIdx].String())
			}
		}
	}

	if slotCtx.Features.IsActive(features.StaticInstructionLimit) {
		if len(tx.Message.Instructions) > maxInstrTraceCapacity {
			return nil, 0, TxErrSanitizeFailure
		}
	}

	if shouldVerifySignatures {
		sigverifySnapshot, err := buildSigverifySnapshot(tx, slotCtx.Slot)
		if err != nil {
			return nil, 0, err
		}
		sigverifyWg.Add(1)
		enqueueSigverify(sigverifySnapshot, sigverifyWg)
	}

	debugTx := len(tx.Signatures) > 0 && dbgOpts != nil && dbgOpts.IsDebugTx(tx.Signatures[0])
	captureTx := len(tx.Signatures) > 0 && txCaptureActive()
	if debugTx {
		mlog.Log.Infof("Turning on debug logs while executing tx %s", tx.Signatures[0])
		mlog.Log.EnableInfLogging()
		defer mlog.Log.DisableInfLogging()
	}

	// Execute via pure function
	input := LoadAndExecuteTransactionInput{
		SlotCtx:     slotCtx,
		Transaction: tx,
		Arena:       arena,
		TxMeta:      txMeta,
		LeanResult:  true,
		// On-chain metadata and the trailing verifier are the only replay
		// consumers of pre-balances. Ordinary production transactions skip it.
		CapturePreBalances: txMeta != nil || captureTx,
		RecordLogs:         debugTx,
	}
	output := LoadAndExecuteTransaction(input)

	execCtx := output.ExecCtx
	instrs := output.Instrs
	computeBudgetLimits := output.ComputeBudgetLimits

	// Pre-balance divergence check (uses pre-fee-deduction lamports from pure function output)
	start := time.Now()
	if txMeta != nil && output.PreBalances != nil && execCtx != nil {
		for count := uint64(0); count < uint64(len(tx.Message.AccountKeys)); count++ {
			txAcct := execCtx.TransactionContext.Accounts.Accounts[count]
			if debugTx {
				// Avoid calling util.PrettyPrintAcct when not debug logging.
				////mlog.Log.Debugf("pre-balance account used in tx=%s: %s", tx.Signatures[0], util.PrettyPrintAcct(txAcct))
			}

			if !isNativeProgram(txAcct.Key) && !txAcct.IsDummy {
				if output.PreBalances[count] != txMeta.PreBalances[count] {
					mlog.Log.Errorf("[run:%s] DIVERGENCE in slot %d: tx %s pre-balance mismatch for %s: mithril=%d, onchain=%d",
						CurrentRunID, slotCtx.Slot, tx.Signatures[0], txAcct.Key, output.PreBalances[count], txMeta.PreBalances[count])
					panic(fmt.Sprintf("tx %s pre-balance divergence: lamport balance for %s was %d but onchain lamport balance was %d\n%s", tx.Signatures[0], txAcct.Key, output.PreBalances[count], txMeta.PreBalances[count], util.PrettyPrintAcct(txAcct)))
				}
			}
		}
	}
	metrics.GlobalBlockReplay.PreBalanceDivergenceCheck.AddTimingSince(start)

	// Handle transaction errors from the pure function
	if output.ProcessingResult.TransactionError != nil {
		txErr := output.ProcessingResult.TransactionError
		// Trailing-verifier capture (failed tx): fee + status + pre-balances.
		// Post-balances are never compared for failed txs (mirrors the
		// RPC-mode checks, which only compare post on success). Capture is a
		// replay-side observation; the execution context is not modified.
		if captureTx {
			var fee uint64
			if output.FeeInfo != nil {
				fee = output.FeeInfo.TotalFee
			}
			recordTxExecCapture(slotCtx.Slot, tx.Signatures[0], &txExecRecord{
				Fee:      fee,
				Failed:   true,
				Pre:      output.PreBalances,
				SkipMask: txComparabilityMask(execCtx, len(output.PreBalances)),
			})
		}
		if debugTx && execCtx != nil {
			if logRecorder, ok := execCtx.Log.(*sealevel.LogRecorder); ok {
				for _, l := range logRecorder.Logs {
					mlog.Log.Debugf("%s", l)
				}
			}
		}

		switch txErr.ErrorType {
		case TransactionErrorSanitizeFailure:
			return nil, processTransactionComputeUnits(execCtx), txErr.InstructionError

		case TransactionErrorBlockhashNotFound:
			return nil, processTransactionComputeUnits(execCtx), TxErrInvalidBlockhash

		case TransactionErrorMaxLoadedAccountsDataSizeExceeded,
			TransactionErrorInvalidProgramForExecution,
			TransactionErrorProgramAccountNotFound:
			txFeeInfo, err := handleFailedTx(slotCtx, tx, instrs, computeBudgetLimits, txErr.InstructionError, nil)
			return txFeeInfo, processTransactionComputeUnits(execCtx), err

		case TransactionErrorInsufficientFundsForFee:
			// CalculateAndDeductTxFees failed - return fee info with nil error (matches original behavior)
			return output.FeeInfo, processTransactionComputeUnits(execCtx), nil

		case TransactionErrorInstructionError:
			txFeeInfo, err := handleFailedTx(slotCtx, tx, instrs, computeBudgetLimits, txErr.InstructionError, nil)
			return txFeeInfo, processTransactionComputeUnits(execCtx), err

		case TransactionErrorInsufficientFundsForRent:
			txFeeInfo, err := handleFailedTx(slotCtx, tx, instrs, computeBudgetLimits, nil, txErr.InstructionError)
			return txFeeInfo, processTransactionComputeUnits(execCtx), err

		default:
			return nil, processTransactionComputeUnits(execCtx), txErr.InstructionError
		}
	}

	// Successful execution path
	txFeeInfo := output.FeeInfo

	// Fee divergence check
	if txMeta != nil && txFeeInfo.TotalFee != txMeta.Fee {
		mlog.Log.Errorf("[run:%s] DIVERGENCE in slot %d: tx %s fee mismatch: mithril=%d, onchain=%d",
			CurrentRunID, slotCtx.Slot, tx.Signatures[0], txFeeInfo.TotalFee, txMeta.Fee)
		panic(fmt.Sprintf("tx %s fee divergence: totalFee was %d, but onchain fee was %d", tx.Signatures[0], txFeeInfo.TotalFee, txMeta.Fee))
	}

	// Debug logging
	if debugTx && execCtx != nil {
		if logRecorder, ok := execCtx.Log.(*sealevel.LogRecorder); ok {
			for _, l := range logRecorder.Logs {
				mlog.Log.Debugf("%s", l)
			}
		}
	}

	// CU divergence check (non-failing)
	if txMeta != nil && txMeta.ComputeUnitsConsumed != nil {
		cuUsed := execCtx.ComputeMeter.Used()
		if *txMeta.ComputeUnitsConsumed != cuUsed {
			discrepancy := max(cuUsed, *txMeta.ComputeUnitsConsumed) - min(cuUsed, *txMeta.ComputeUnitsConsumed)
			var sign byte
			if cuUsed > *txMeta.ComputeUnitsConsumed {
				sign = '+'
			} else {
				sign = '-'
			}
			mlog.Log.Infof("tx %s CU divergence: used was %d but onchain CU consumed was %d (%c%d discrepancy) [non-failing]", tx.Signatures[0], cuUsed, *txMeta.ComputeUnitsConsumed, sign, discrepancy)
		}
	}

	// Post-balance divergence check (only if tx succeeded)
	start = time.Now()
	if txMeta != nil {
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

	// Trailing-verifier capture (successful tx): fee + status + pre AND post
	// balances, with the native/dummy comparability mask. Read-only walk;
	// the execution context is not modified.
	if captureTx {
		n := len(tx.Message.AccountKeys)
		post := make([]uint64, n)
		for count := 0; count < n; count++ {
			txAcct, aerr := execCtx.TransactionContext.Accounts.GetAccount(uint64(count))
			if aerr != nil {
				continue
			}
			post[count] = txAcct.Lamports
			execCtx.TransactionContext.Accounts.Unlock(uint64(count))
		}
		var fee uint64
		if txFeeInfo != nil {
			fee = txFeeInfo.TotalFee
		}
		recordTxExecCapture(slotCtx.Slot, tx.Signatures[0], &txExecRecord{
			Fee:      fee,
			Failed:   false,
			Pre:      output.PreBalances,
			Post:     post,
			SkipMask: txComparabilityMask(execCtx, n),
		})
	}

	// Apply state changes to slotCtx
	if slotCtx.Replay {
		start = time.Now()
	}
	if err := applySuccessfulTransactionState(slotCtx, execCtx, output.ExecutionResult); err != nil {
		panic(fmt.Sprintf("unable to apply successful transaction %s in slot %d: %v", tx.Signatures[0], slotCtx.Slot, err))
	}
	if slotCtx.Replay {
		metrics.GlobalBlockReplay.TxUpdateAccounts.AddTimingSince(start)
	}

	return txFeeInfo, processTransactionComputeUnits(execCtx), nil
}

// txComparabilityMask marks account indices the trailing verifier must not
// compare: native programs (mithril models their account bodies differently)
// and dummy placeholders for accounts that did not exist. Mirrors the skip
// rules of the RPC-mode pre/post-balance divergence checks.
func txComparabilityMask(execCtx *sealevel.ExecutionCtx, numAccts int) []byte {
	mask := make([]byte, (numAccts+7)/8)
	if execCtx == nil {
		for i := range mask {
			mask[i] = 0xFF
		}
		return mask
	}
	accts := execCtx.TransactionContext.Accounts.Accounts
	for i := 0; i < numAccts; i++ {
		if i >= len(accts) || accts[i] == nil {
			setMaskBit(mask, i)
			continue
		}
		if isNativeProgram(accts[i].Key) || accts[i].IsDummy {
			setMaskBit(mask, i)
		}
	}
	return mask
}
