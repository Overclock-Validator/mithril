package blockprod

import (
	"errors"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/costmodel"
	"github.com/Overclock-Validator/mithril/pkg/replay"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// PreparedBundle is a block bundle checked for forging, with its address-table
// lookups resolved, so its account set is final. A bundle that is not revert
// protected is a single transaction that lands even if it fails, as long as
// the failure is one the protocol records.
type PreparedBundle struct {
	txs             []*solana.Transaction
	candidates      []forgeCandidate
	revertProtected bool
}

// PrepareBundle does the lock-free checks of a block bundle's transactions.
// Unless the result is ForgeAccepted, the bundle cannot be forged.
func (b *WorkingBank) PrepareBundle(txs []*solana.Transaction, revertProtected bool) (*PreparedBundle, BundleResult) {
	if len(txs) == 0 || (!revertProtected && len(txs) != 1) {
		return nil, BundleResult{Result: ForgeDroppedParse, Index: -1}
	}
	p := &PreparedBundle{txs: txs, candidates: make([]forgeCandidate, len(txs)), revertProtected: revertProtected}
	seen := make(map[[32]byte]struct{}, len(txs))
	for idx, tx := range txs {
		c, result := b.prepareForge(tx, 0, nil)
		if result != ForgeAccepted {
			return nil, BundleResult{Result: result, Index: idx}
		}
		if _, dup := seen[c.messageHash]; dup {
			return nil, BundleResult{Result: ForgeDroppedAlreadyProcessed, Index: idx}
		}
		seen[c.messageHash] = struct{}{}
		p.candidates[idx] = c
	}
	return p, BundleResult{Result: ForgeAccepted, Index: -1}
}

// Accounts calls fn once for each account the bundle loads, with whether any
// of its transactions write-locks it. Bundles that share an account one of
// them writes conflict and must not execute concurrently.
func (p *PreparedBundle) Accounts(fn func(key solana.PublicKey, writable bool)) {
	if len(p.candidates) == 1 {
		lockedAccounts(&p.candidates[0].execTx.Message, fn)
		return
	}
	writable := make(map[solana.PublicKey]bool)
	var order []solana.PublicKey
	for _, c := range p.candidates {
		lockedAccounts(&c.execTx.Message, func(key solana.PublicKey, w bool) {
			prev, ok := writable[key]
			if !ok {
				order = append(order, key)
			}
			writable[key] = prev || w
		})
	}
	for _, key := range order {
		fn(key, writable[key])
	}
}

// lockedAccounts calls fn for each account m loads, with whether it is write
// locked. Lookups in m must be resolved, if it has any.
func lockedAccounts(m *solana.Message, fn func(key solana.PublicKey, writable bool)) {
	keys := m.AccountKeys
	numStatic := len(keys)
	if m.IsResolved() {
		numStatic -= m.NumLookups()
	}
	h := m.Header
	signers := int(h.NumRequiredSignatures)
	writableSigners := max(signers-int(h.NumReadonlySignedAccounts), 0)
	writableUnsigned := max(numStatic-signers-int(h.NumReadonlyUnsignedAccounts), 0)
	writableLookups := m.AddressTableLookups.NumWritableLookups()
	for i, key := range keys {
		var writable bool
		switch {
		case i >= numStatic:
			writable = i-numStatic < writableLookups
		case i >= signers:
			writable = i-signers < writableUnsigned
		default:
			writable = i < writableSigners
		}
		fn(key, writable)
	}
}

// ExecutedBundle is a bundle executed on its own view of the bank, not yet
// committed. Commit it with CommitBundle or drop it with Discard.
type ExecutedBundle struct {
	prepared *PreparedBundle
	scratch  *sealevel.SlotCtx
	outputs  []replay.LoadAndExecuteTransactionOutput
	actual   []costmodel.TransactionCost
	// result is set when the bundle cannot commit whatever the bank's state.
	result BundleResult
}

// Discard drops an executed bundle that will not be committed.
func (e *ExecutedBundle) Discard() {
	if e.scratch != nil {
		evictProgramCacheAdds(e.scratch)
		e.scratch = nil
	}
}

// ExecuteBundle executes a prepared bundle on a scratch view of the bank,
// without the bank lock: nothing lands until CommitBundle. Bundles may execute
// concurrently only if their account sets do not conflict, and a bundle must
// not execute before a conflicting earlier one has committed. Anything that
// forges into the bank concurrently, such as the TPU, must respect the same
// account locks.
func (b *WorkingBank) ExecuteBundle(p *PreparedBundle) *ExecutedBundle {
	e := &ExecutedBundle{prepared: p, result: BundleResult{Result: ForgeAccepted, Index: -1}}
	b.mu.Lock()
	accepting := b.accepting
	for idx, c := range p.candidates {
		if !accepting {
			break
		}
		// Copies of one transaction always conflict on their fee payer, so a
		// copy committed earlier is already recorded here.
		if result := b.statusLocked(c); result != ForgeAccepted {
			e.result = BundleResult{Result: result, Index: idx}
			break
		}
	}
	b.mu.Unlock()
	if !accepting {
		e.result = BundleResult{Result: ForgeDroppedNoLeader, Index: -1}
	}
	if e.result.Result != ForgeAccepted {
		return e
	}

	e.scratch = sealevel.NewScratchSlotCtx(b.slotCtx)
	if p.revertProtected {
		e.outputs, e.actual, e.result = b.executeBundleOn(e.scratch, p.candidates)
	} else {
		e.outputs, e.actual, e.result = b.executeStandaloneOn(e.scratch, p.candidates[0])
	}
	if e.result.Result != ForgeAccepted {
		e.Discard()
	}
	return e
}

// executeBundleOn executes a revert-protected bundle's transactions in order
// on scratch, each seeing the writes of the ones before it. Any failure stops
// the bundle.
func (b *WorkingBank) executeBundleOn(scratch *sealevel.SlotCtx, candidates []forgeCandidate) ([]replay.LoadAndExecuteTransactionOutput, []costmodel.TransactionCost, BundleResult) {
	outputs := make([]replay.LoadAndExecuteTransactionOutput, len(candidates))
	actual := make([]costmodel.TransactionCost, len(candidates))
	for idx, c := range candidates {
		if err := b.preparer.PayerCanFund(scratch, c.execTx, nil); err != nil {
			return nil, nil, BundleResult{Result: ForgeDroppedExecution, Index: idx, Err: err}
		}
		// Every output must outlive the next execution, so none may use an
		// arena that is reset per transaction.
		output := b.preparer.LoadAndExecute(replay.LoadAndExecuteTransactionInput{
			SlotCtx:           scratch,
			Transaction:       c.execTx,
			LeanResult:        true,
			SkipTimingMetrics: true,
		}, nil)
		if txErr := output.ProcessingResult.TransactionError; txErr != nil {
			return nil, nil, BundleResult{Result: ForgeDroppedExecution, Index: idx, Err: transactionErr(txErr)}
		}
		if output.ExecCtx == nil {
			return nil, nil, BundleResult{Result: ForgeDroppedExecution, Index: idx, Err: errors.New("missing execution context")}
		}
		txAccounts := &output.ExecCtx.TransactionContext.Accounts
		if err := accounts.SetTransactionAccounts(scratch.Accounts, txAccounts.Accounts, txAccounts.Touched); err != nil {
			return nil, nil, BundleResult{Result: ForgeDroppedExecution, Index: idx, Err: err}
		}
		execCU, loadedCost := actualExecutionUsage(output)
		actual[idx] = c.cost.WithActualUsage(execCU, loadedCost)
		outputs[idx] = output
	}
	return outputs, actual, BundleResult{Result: ForgeAccepted, Index: -1}
}

// executeStandaloneOn executes a transaction that is not revert protected on
// scratch. A failed execution still commits if the protocol records the
// failure; CommitBundle decides.
func (b *WorkingBank) executeStandaloneOn(scratch *sealevel.SlotCtx, c forgeCandidate) ([]replay.LoadAndExecuteTransactionOutput, []costmodel.TransactionCost, BundleResult) {
	if err := b.preparer.PayerCanFund(scratch, c.execTx, nil); err != nil {
		return nil, nil, BundleResult{Result: ForgeDroppedExecution, Index: 0, Err: err}
	}
	output := b.preparer.LoadAndExecute(replay.LoadAndExecuteTransactionInput{
		SlotCtx:           scratch,
		Transaction:       c.execTx,
		LeanResult:        true,
		SkipTimingMetrics: true,
	}, nil)
	execCU, loadedCost := actualExecutionUsage(output)
	return []replay.LoadAndExecuteTransactionOutput{output},
		[]costmodel.TransactionCost{c.cost.WithActualUsage(execCU, loadedCost)},
		BundleResult{Result: ForgeAccepted, Index: -1}
}

// CommitBundle lands an executed bundle under the bank lock, checking block
// limits against the units it actually used. Unless it returns ForgeAccepted,
// nothing from the bundle landed.
func (b *WorkingBank) CommitBundle(e *ExecutedBundle) BundleResult {
	if e.result.Result != ForgeAccepted {
		return e.result
	}
	committed := false
	defer func() {
		if !committed {
			e.Discard()
		}
	}()
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.accepting {
		return BundleResult{Result: ForgeDroppedNoLeader, Index: -1}
	}
	p := e.prepared
	if reason := b.costs.WouldExceedAll(e.actual); reason != costmodel.ExceedNone {
		return BundleResult{Result: ForgeDroppedCost, Reason: reason, Index: -1}
	}
	sizes := make([]int, len(p.candidates))
	for idx, c := range p.candidates {
		sizes[idx] = c.serializedSize
	}
	if b.entries.wouldOverflowBatch(sizes[0]) {
		b.flushEntriesLocked()
	}
	if b.entries.wouldExceedSlotN(sizes) {
		return BundleResult{Result: ForgeDroppedCost, Reason: costmodel.ExceedBatchBytes, Index: -1}
	}

	if !p.revertProtected && e.outputs[0].ProcessingResult.TransactionError != nil {
		feeInfo, err := replay.ApplyFailedTransaction(b.slotCtx, p.candidates[0].execTx, e.outputs[0])
		if err != nil {
			return BundleResult{Result: ForgeDroppedExecution, Index: 0, Err: err}
		}
		e.outputs[0].FeeInfo = feeInfo
	} else if err := replay.ApplySuccessfulTransactions(b.slotCtx, e.outputs); err != nil {
		return BundleResult{Result: ForgeDroppedExecution, Index: -1, Err: err}
	}
	committed = true
	b.recordForgedLocked(p.txs, p.candidates, e.outputs, e.actual)
	return BundleResult{Result: ForgeAccepted, Index: -1}
}

// recordForgedLocked records committed transactions: their statuses, fees,
// signatures and costs, and their entries, in order.
func (b *WorkingBank) recordForgedLocked(txs []*solana.Transaction, candidates []forgeCandidate, outputs []replay.LoadAndExecuteTransactionOutput, actual []costmodel.TransactionCost) {
	if b.seenMessages == nil {
		b.seenMessages = make(map[[32]byte]struct{})
	}
	for idx, tx := range txs {
		b.seenMessages[candidates[idx].messageHash] = struct{}{}
		if outputs[idx].FeeInfo != nil {
			b.txFees.Add(outputs[idx].FeeInfo)
		}
		b.forgedTxs = append(b.forgedTxs, tx)
		b.numSigs += uint64(tx.Message.Header.NumRequiredSignatures)
		// Equivalent to recording the requested cost and rebating it.
		b.costs.Record(actual[idx])
		size := candidates[idx].serializedSize
		if flushed, batchBytes, didFlush := b.entries.appendSerialized(*tx, size, size); didFlush {
			b.entryHash = b.entries.CurrentEntryHash()
			b.sink.OnEntryBatch(flushed, batchBytes)
		}
	}
}
