package replay

import (
	"encoding/json"
	"errors"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
)

// TransactionResultSession collects lightweight outcomes independently of full
// history. Its methods belong to the replay owner; workers write indexed records.
// Root/recovery publication and RPC activation remain the caller's responsibility.
type TransactionResultSession struct {
	cache    *txstatus.ResultCache
	current  *blockResultCapture
	disabled bool
}

// TransactionResultSeed uses the already reconciled replay parent. It does not
// infer a blockhash from a slot or repair a missing resume-state identity.
func TransactionResultSeed(s *state.MithrilState, resume *ResumeState) (txstatus.ResultBankID, error) {
	if resume != nil {
		parent := txstatus.ResultBankID{Slot: resume.ParentSlot, Blockhash: resume.LastBlockhash}
		if parent.Blockhash.IsZero() {
			return txstatus.ResultBankID{}, txstatus.ErrResultLineage
		}
		return parent, nil
	}
	hash, err := manifestLastBlockhash(s)
	if err != nil {
		return txstatus.ResultBankID{}, err
	}
	return txstatus.ResultBankID{Slot: s.ManifestParentSlot, Blockhash: hash}, nil
}

// NewTransactionResultSession borrows a cache for explicitly non-voting replay.
// A nil cache keeps the path disabled. Mode must remain unchanged during replay.
func NewTransactionResultSession(cache *txstatus.ResultCache, mode string) (*TransactionResultSession, error) {
	if cache == nil {
		return nil, nil
	}
	if mode != "verifying" {
		return nil, errors.New("transaction results require verifying mode")
	}
	return &TransactionResultSession{cache: cache}, nil
}

func (s *TransactionResultSession) begin() *blockResultCapture {
	if s == nil || s.disabled {
		return nil
	}
	if s.current != nil {
		s.disable()
		return nil
	}
	s.current = &blockResultCapture{}
	return s.current
}

func (s *TransactionResultSession) discardCapture(capture *blockResultCapture) {
	if s == nil || capture == nil || s.current != capture {
		return
	}
	capture.records, capture.bank.Results = nil, nil
	s.current = nil
}

func (s *TransactionResultSession) discard() {
	if s != nil {
		s.discardCapture(s.current)
	}
}

// acceptBlock is only called after the common synchronous consensus acceptance
// boundary. A failed capture publishes a gap, never an invented success record.
func (s *TransactionResultSession) acceptBlock(block *b.Block) error {
	if s == nil || s.disabled {
		return nil
	}
	if block == nil {
		s.disable()
		return txstatus.ErrResultLineage
	}
	if s.current == nil {
		return s.cache.Publish(txstatus.ResultBank{ID: resultBlockID(block),
			Parent: txstatus.ResultBankID{Slot: block.ParentSlot, Blockhash: block.LastBlockhash}})
	}
	capture := s.current
	if !capture.sealed || capture.bank.ID != resultBlockID(block) ||
		capture.bank.Parent != (txstatus.ResultBankID{Slot: block.ParentSlot, Blockhash: block.LastBlockhash}) {
		s.disable()
		return txstatus.ErrResultLineage
	}
	defer s.discardCapture(capture)
	return s.cache.Publish(capture.bank)
}

func (s *TransactionResultSession) disable() {
	if s == nil {
		return
	}
	s.disabled = true
	s.discard()
	s.cache.Invalidate()
}

// Invalidate permanently disables this session without changing replay state.
func (s *TransactionResultSession) Invalidate() { s.disable() }

// Unwind removes an abandoned accepted suffix after account recovery selected
// this exact parent. It does not erase retained capture gaps or recover a latch.
func (s *TransactionResultSession) Unwind(parent txstatus.ResultBankID) error {
	if s == nil {
		return nil
	}
	s.discard()
	if s.disabled {
		return txstatus.ErrResultUnavailable
	}
	return s.cache.Unwind(parent)
}

// Reset starts cold only after a validated durable rebase. It never re-enables
// a session disabled by unexpected production or broken capture ownership.
func (s *TransactionResultSession) Reset(parent txstatus.ResultBankID) error {
	if s == nil {
		return nil
	}
	s.discard()
	if s.disabled {
		return txstatus.ErrResultUnavailable
	}
	return s.cache.Reset(parent)
}

func (s *TransactionResultSession) checkParent(parent txstatus.ResultBankID) error {
	if s == nil {
		return nil
	}
	view, err := s.cache.Lookup(nil)
	if err != nil {
		return err
	}
	if view.Tip != parent || view.Root != parent || parent.Blockhash.IsZero() {
		s.cache.Invalidate()
		return txstatus.ErrResultLineage
	}
	return nil
}

func (s *TransactionResultSession) root(through uint64, rooted *state.ResumeContext) error {
	if s == nil {
		return nil
	}
	if s.disabled {
		return txstatus.ErrResultUnavailable
	}
	if rooted == nil || rooted.Slot != through {
		s.cache.Invalidate()
		return txstatus.ErrResultLineage
	}
	hash, err := solana.HashFromBase58(rooted.Blockhash)
	if err != nil || hash.IsZero() {
		s.cache.Invalidate()
		return txstatus.ErrResultLineage
	}
	return s.cache.Root(txstatus.ResultBankID{Slot: rooted.Slot, Blockhash: hash})
}

func setResultRecoveryPending(session *TransactionResultSession, publisher SlotCtxSetter, pending bool) {
	if session != nil {
		if gate, ok := publisher.(interface{ SetHistoryRecoveryPending(bool) }); ok {
			gate.SetHistoryRecoveryPending(pending)
		}
	}
}

func (s *TransactionResultSession) finishUnwind(publisher SlotCtxSetter, parent *ResumeState) {
	if s == nil {
		return
	}
	if err := s.Unwind(txstatus.ResultBankID{Slot: parent.ParentSlot, Blockhash: parent.LastBlockhash}); err != nil {
		mlog.Log.Debugf("transaction results unavailable after account unwind: %v", err)
	}
	setResultRecoveryPending(s, publisher, false)
}

func (s *TransactionResultSession) replayStopped(publisher SlotCtxSetter, err error) {
	if s == nil || err == nil {
		return
	}
	var switchErr *CertifiedSwitch
	var mismatch *AlpenglowFinalityMismatch
	if errors.As(err, &switchErr) || errors.As(err, &mismatch) {
		setResultRecoveryPending(s, publisher, true)
		return
	}
	s.disable()
}

func (s *TransactionResultSession) endReplay(publisher SlotCtxSetter, completed bool) {
	s.discard()
	if s != nil && !completed {
		s.disable()
		setResultRecoveryPending(s, publisher, true)
	}
}

func publishRootedTransactionResults(session *TransactionResultSession, publisher SlotCtxSetter, through uint64, rooted *state.ResumeContext) {
	if err := session.root(through, rooted); err != nil {
		mlog.Log.Debugf("transaction results unavailable at durable root %d: %v", through, err)
	}
	if publisher != nil {
		var count uint64
		if rooted.TransactionCount != nil {
			count = *rooted.TransactionCount
		}
		publisher.SetRootedBankState(through, rooted.BlockHeight, count)
	}
}

func (s *TransactionResultSession) validateReplay(opts *ConsensusOpts, fetch *BlockFetchOpts) error {
	if s == nil {
		return nil
	}
	if s.cache == nil || !initializedObserverReplay(opts) || observerReplayHasProduction(opts, fetch) {
		return errors.New("transaction results require an initialized Alpenglow observer session")
	}
	return nil
}

func initializedObserverReplay(opts *ConsensusOpts) bool {
	return opts != nil && opts.Alpenglow && opts.Engine != nil
}

func observerReplayHasProduction(opts *ConsensusOpts, fetch *BlockFetchOpts) bool {
	return opts.Engine.Snapshot().Voting != nil || (fetch != nil && (fetch.LocalBlocks != nil || fetch.LocalLeaderForSlot != nil))
}

type transactionResultCapture struct {
	result   txstatus.SignatureResult
	recorded bool
	err      error
}

func (capture *transactionResultCapture) finish(tx *solana.Transaction, result *TransactionError) {
	if tx == nil || len(tx.Signatures) == 0 {
		capture.err = txstatus.ErrResultInput
		return
	}
	var encoded json.RawMessage
	if result != nil {
		invalid := result.ErrorType.String() == "Unknown"
		switch result.ErrorType {
		case TransactionErrorInstructionError:
			invalid = result.InstructionIndex == nil || result.InstructionError == nil
		case TransactionErrorInsufficientFundsForRent, TransactionErrorProgramExecutionTemporarilyRestricted:
			invalid = result.AccountIndex == nil
		case TransactionErrorDuplicateInstruction:
			invalid = result.InstructionIndex == nil
		}
		if invalid {
			capture.err = txstatus.ErrResultInput
			return
		}
		if result.ErrorType == TransactionErrorInstructionError && result.InstructionError != nil && len(result.InstructionError.Error()) > metadataErrorTextLimit {
			capture.err = txstatus.ErrResultInput
			return
		}
		var err error
		encoded, err = json.Marshal(result)
		if err != nil || len(encoded) > txstatus.MaxResultErrorBytes {
			capture.err = txstatus.ErrResultInput
			return
		}
	}
	capture.result = txstatus.SignatureResult{Signature: tx.Signatures[0], Error: encoded}
	capture.recorded = true
}

type transactionResultGroup []transactionResultCapture

func (group transactionResultGroup) transaction(index int) *transactionResultCapture {
	if group == nil {
		return nil
	}
	return &group[index]
}

type blockResultCapture struct {
	records transactionResultGroup
	bank    txstatus.ResultBank
	err     error
	sealed  bool
}

func (capture *blockResultCapture) beginGroup(count int) transactionResultGroup {
	if capture == nil || capture.err != nil {
		return nil
	}
	if count < 0 || count > txstatus.MaxResultBankEntries-len(capture.records) {
		capture.fail(txstatus.ErrResultUnavailable)
		return nil
	}
	start := len(capture.records)
	capture.records = append(capture.records, make(transactionResultGroup, count)...)
	return capture.records[start:]
}

func (capture *blockResultCapture) seal(block *b.Block) {
	if capture == nil {
		return
	}
	capture.sealed = true
	capture.bank = txstatus.ResultBank{ID: resultBlockID(block),
		Parent: txstatus.ResultBankID{Slot: block.ParentSlot, Blockhash: block.LastBlockhash}}
	if capture.err != nil {
		return
	}
	if len(capture.records) != len(block.Transactions) {
		capture.fail(txstatus.ErrResultUnavailable)
		return
	}
	for _, record := range capture.records {
		if record.err != nil || !record.recorded {
			capture.fail(txstatus.ErrResultUnavailable)
			return
		}
	}
	capture.bank.Results = make([]txstatus.SignatureResult, len(capture.records))
	for i, record := range capture.records {
		capture.bank.Results[i] = record.result
	}
	capture.bank.Complete = true
	capture.records = nil
}

func (capture *blockResultCapture) fail(err error) {
	if capture.err == nil {
		capture.err = err
	}
	capture.records, capture.bank.Results = nil, nil
	capture.bank.Complete = false
}

func resultBlockID(block *b.Block) txstatus.ResultBankID {
	return txstatus.ResultBankID{Slot: block.Slot, Blockhash: block.Blockhash}
}
