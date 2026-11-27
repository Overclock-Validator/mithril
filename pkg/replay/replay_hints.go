package replay

import (
	"encoding/binary"
	"os"
	"sync"
	"sync/atomic"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// Replay hints: what executing a block taught this node, kept so that its own
// later replay of the same block (a private replay from a checkpoint) need not
// rediscover it.
//
// For every transaction the node records whether it failed, the compute it
// consumed, and which of its writable accounts a successful one really
// changed. A later replay of the block then
//
//   - plans on the real dependency graph: a writable account the transaction
//     did not change is planned as a read, so transactions that only declared
//     a common account run side by side;
//   - orders ready work by the compute really consumed;
//   - does not execute a transaction recorded as failed: its only effects are
//     the fee and a nonce advance, which the ordinary failure path applies.
//
// Hints are this node's own observations of a block it has already validated.
// They are never taken from another node: a claim that a transaction failed
// cannot be checked without running it. Each use is still checked:
//
//   - the hints are bound to the block's hash and are ignored for any other
//     block at the slot;
//   - at every commit the transaction's real changes are compared with its
//     hint, and a transaction that fails although its hint says it succeeded
//     is noticed (hintUse.violated);
//   - the caller compares the bank hash with the recorded one.
//
// On any disagreement the caller replays again without hints.
//
// MITHRIL_REPLAY_HINTS=1 turns recording on (experimental).
var replayHintsEnabled = os.Getenv("MITHRIL_REPLAY_HINTS") == "1"

// replayHintSlots bounds the hints kept in memory.
const replayHintSlots = 1024

type txHint struct {
	failed bool
	// kcu is the consumed compute in thousands of CU, plus one.
	kcu int32
	// touched lists the non-payer writable accounts a successful transaction
	// changed.
	touched []solana.PublicKey
}

type blockHints struct {
	blockhash [32]byte
	bySig     map[solana.Signature]txHint
}

// hintRecorder collects one block's hints while the node replays it.
type hintRecorder struct {
	touched sync.Map // solana.Signature -> []solana.PublicKey, until the outcome arrives
	done    sync.Map // solana.Signature -> txHint
}

// hintUse applies one block's hints to a private replay of it.
type hintUse struct {
	hints    *blockHints
	violated atomic.Bool
	skipped  atomic.Int64
}

type replayHintStore struct {
	mu     sync.Mutex
	bySlot map[uint64]*blockHints
	order  []uint64
}

var replayHints = replayHintStore{bySlot: make(map[uint64]*blockHints)}

func (store *replayHintStore) put(slot uint64, hints *blockHints) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if _, replaced := store.bySlot[slot]; !replaced {
		store.order = append(store.order, slot)
		if len(store.order) > replayHintSlots {
			delete(store.bySlot, store.order[0])
			store.order = store.order[1:]
		}
	}
	store.bySlot[slot] = hints
}

func (store *replayHintStore) get(slot uint64) *blockHints {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.bySlot[slot]
}

// hintsFor returns the stored hints for exactly this block, or nil.
func hintsFor(block *b.Block) *blockHints {
	hints := replayHints.get(block.Slot)
	if hints == nil || hints.blockhash != block.Blockhash || hints.blockhash == ([32]byte{}) {
		return nil
	}
	return hints
}

func hintRecorderOf(slotCtx *sealevel.SlotCtx) *hintRecorder {
	if slotCtx == nil || slotCtx.ReplayHints == nil {
		return nil
	}
	recorder, _ := slotCtx.ReplayHints.(*hintRecorder)
	return recorder
}

func hintUseOf(slotCtx *sealevel.SlotCtx) *hintUse {
	if slotCtx == nil || slotCtx.ReplayHints == nil {
		return nil
	}
	use, _ := slotCtx.ReplayHints.(*hintUse)
	return use
}

func blockHintUse(block *b.Block) *hintUse {
	if block == nil || block.ReplayHints == nil {
		return nil
	}
	use, _ := block.ReplayHints.(*hintUse)
	return use
}

// touchedWritable lists the non-payer writable accounts the transaction
// changed, as its commit sees them.
func touchedWritable(execCtx *sealevel.ExecutionCtx, visit func(solana.PublicKey) bool) {
	txCtx := execCtx.TransactionContext
	if txCtx == nil {
		return
	}
	accts := &txCtx.Accounts
	for idx := 1; idx < len(accts.Accounts); idx++ {
		if idx >= len(accts.AcctMetas) || accts.AcctMetas[idx] == nil || !accts.AcctMetas[idx].IsWritable {
			continue
		}
		if idx < len(accts.Touched) && accts.Touched[idx] {
			if !visit(solana.PublicKey(accts.Accounts[idx].Key)) {
				return
			}
		}
	}
}

// replayHintsAtCommit runs at a successful commit: it records what the
// transaction changed (the node's own replay) or checks it against the hint
// (a private replay).
func replayHintsAtCommit(slotCtx *sealevel.SlotCtx, execCtx *sealevel.ExecutionCtx) {
	if slotCtx == nil || slotCtx.ReplayHints == nil || execCtx == nil || execCtx.TransactionContext == nil {
		return
	}
	signature := execCtx.TransactionContext.Signature
	switch hints := slotCtx.ReplayHints.(type) {
	case *hintRecorder:
		var touched []solana.PublicKey
		touchedWritable(execCtx, func(key solana.PublicKey) bool {
			touched = append(touched, key)
			return true
		})
		hints.touched.Store(signature, touched)
	case *hintUse:
		hint, known := hints.hints.bySig[signature]
		if !known {
			return
		}
		if hint.failed {
			hints.violated.Store(true)
			return
		}
		touchedWritable(execCtx, func(key solana.PublicKey) bool {
			for _, expected := range hint.touched {
				if expected == key {
					return true
				}
			}
			hints.violated.Store(true)
			return false
		})
	}
}

// replayHintsAtOutcome runs when a transaction's processing is over.
func replayHintsAtOutcome(slotCtx *sealevel.SlotCtx, tx *solana.Transaction, computeUnits uint64, failed bool) {
	if len(tx.Signatures) == 0 {
		return
	}
	signature := tx.Signatures[0]
	switch hints := slotCtx.ReplayHints.(type) {
	case *hintRecorder:
		hint := txHint{failed: failed, kcu: int32(computeUnits/1000) + 1}
		if touched, ok := hints.touched.LoadAndDelete(signature); ok && !failed {
			hint.touched = touched.([]solana.PublicKey)
		}
		hints.done.Store(signature, hint)
	case *hintUse:
		if hint, known := hints.hints.bySig[signature]; known && failed && !hint.failed {
			hints.violated.Store(true)
		}
	}
}

// sealReplayHints stores a finished block's hints. A transaction with no
// outcome has no hint and is later planned and executed as declared.
func sealReplayHints(slotCtx *sealevel.SlotCtx, block *b.Block) {
	recorder := hintRecorderOf(slotCtx)
	if recorder == nil {
		return
	}
	hints := &blockHints{blockhash: block.Blockhash, bySig: make(map[solana.Signature]txHint, len(block.Transactions))}
	recorder.done.Range(func(signature, hint any) bool {
		hints.bySig[signature.(solana.Signature)] = hint.(txHint)
		return true
	})
	replayHints.put(block.Slot, hints)
}

// knownFailure reports whether the node recorded that tx failed.
func (use *hintUse) knownFailure(tx *solana.Transaction) bool {
	if use == nil || len(tx.Signatures) == 0 {
		return false
	}
	hint, known := use.hints.bySig[tx.Signatures[0]]
	return known && hint.failed
}

// advancedNonce returns the nonce account a failed transaction still
// advances, and whether the transaction is a nonce transaction at all.
// resolved is false when it is one but its nonce account cannot be named from
// the message.
func advancedNonce(tx *solana.Transaction) (nonce solana.PublicKey, isNonce, resolved bool) {
	keys := tx.Message.AccountKeys
	if len(tx.Message.Instructions) == 0 {
		return nonce, false, true
	}
	ix := tx.Message.Instructions[0]
	if int(ix.ProgramIDIndex) >= len(keys) || keys[ix.ProgramIDIndex] != solana.SystemProgramID ||
		len(ix.Data) < 4 || binary.LittleEndian.Uint32(ix.Data) != 4 {
		return nonce, false, true
	}
	if len(ix.Accounts) == 0 || int(ix.Accounts[0]) >= len(keys) {
		return nonce, true, false
	}
	return keys[ix.Accounts[0]], true, true
}

// applyGraph narrows each hinted transaction's writable accounts to the ones
// it really changes: for a failed transaction its fee payer and the nonce
// account it advances, for a successful one its fee payer and what it
// changed. Transactions without a hint keep their declared accounts.
func (use *hintUse) applyGraph(txs []*solana.Transaction, accounts []plannerTransactionAccounts) {
	for i, tx := range txs {
		if i >= len(accounts) || len(tx.Signatures) == 0 || len(tx.Message.AccountKeys) == 0 {
			continue
		}
		hint, known := use.hints.bySig[tx.Signatures[0]]
		if !known {
			continue
		}
		payer := tx.Message.AccountKeys[0]
		var nonce solana.PublicKey
		hasNonce := false
		if hint.failed {
			var resolved bool
			if nonce, hasNonce, resolved = advancedNonce(tx); !resolved {
				continue
			}
		}
		acc := accounts[i].accounts
		for j := range acc {
			if !acc[j].writable || acc[j].publicKey == payer || (hasNonce && acc[j].publicKey == nonce) {
				continue
			}
			real := false
			if !hint.failed {
				for _, key := range hint.touched {
					if key == acc[j].publicKey {
						real = true
						break
					}
				}
			}
			acc[j].writable = real
		}
		readonlyEnd := 0
		for j := range acc {
			if acc[j].writable {
				continue
			}
			acc[readonlyEnd], acc[j] = acc[j], acc[readonlyEnd]
			readonlyEnd++
		}
		accounts[i].readonlyEnd = readonlyEnd
	}
}

// schedule orders ready work by the compute each transaction really used. A
// recorded failure is not executed, so it weighs next to nothing.
func (use *hintUse) schedule(txs []*solana.Transaction, sched planSchedule) planSchedule {
	sched.priority = true
	sched.weight = func(tx int) int32 {
		if len(txs[tx].Signatures) > 0 {
			if hint, known := use.hints.bySig[txs[tx].Signatures[0]]; known && hint.kcu > 0 {
				if hint.failed {
					return 1
				}
				return hint.kcu
			}
		}
		return requestedKCU(txs[tx])
	}
	return sched
}

// hintedSchedule is sched, or the hint-weighted schedule in a private replay
// that has hints.
func hintedSchedule(slotCtx *sealevel.SlotCtx, txs []*solana.Transaction, sched planSchedule) planSchedule {
	if use := hintUseOf(slotCtx); use != nil {
		return use.schedule(txs, sched)
	}
	return sched
}
