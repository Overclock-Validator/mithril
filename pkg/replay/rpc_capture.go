package replay

import (
	"encoding/json"
	"sync"
	"sync/atomic"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/fees"
	"github.com/Overclock-Validator/mithril/pkg/rpcindex"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/watch"
	"github.com/gagliardetto/solana-go"
)

// rpcWatch is the watch registry of the attached RPC server, or nil. With
// nil, transaction processing pays one atomic load.
var rpcWatch atomic.Pointer[watch.Registry]

// rpcCapture collects one watched transaction's metadata across execution.
type rpcCapture struct {
	errJSON  []byte
	pre      []uint64
	preAccts []*accounts.Account
	logs     []string
	inner    []rpcindex.InnerInstructions
}

func startRPCCapture(tx *solana.Transaction) *rpcCapture {
	reg := rpcWatch.Load()
	if reg == nil || len(tx.Signatures) == 0 {
		return nil
	}
	snap := reg.Current()
	if snap == nil || snap.Match == nil {
		return nil
	}
	// Replay resolves lookup tables before processing, so AccountKeys holds
	// the loaded keys too.
	if !snap.Match.MatchTransaction(tx.Message.AccountKeys, &tx.Signatures[0]) {
		return nil
	}
	return &rpcCapture{}
}

func (c *rpcCapture) observe(output *LoadAndExecuteTransactionOutput) {
	c.pre = output.PreBalances
	c.preAccts = output.PreAccountSnapshots
	if te := output.ProcessingResult.TransactionError; te != nil {
		c.errJSON, _ = json.Marshal(te)
	}
	execCtx := output.ExecCtx
	if execCtx == nil {
		return
	}
	if rec, ok := execCtx.Log.(*sealevel.LogRecorder); ok && rec != nil {
		c.logs = append([]string(nil), rec.Logs...)
	}
	for _, list := range AssembleInnerInstructions(execCtx) {
		out := rpcindex.InnerInstructions{Index: list.Index, Instructions: make([]rpcindex.InnerInstruction, len(list.Instructions))}
		for i, ix := range list.Instructions {
			out.Instructions[i] = rpcindex.InnerInstruction{
				ProgramIDIndex: ix.ProgramIdIndex,
				Accounts:       append([]uint8(nil), ix.Accounts...),
				Data:           append([]byte(nil), ix.Data...),
				StackHeight:    uint32(ix.StackHeight),
			}
		}
		c.inner = append(c.inner, out)
	}
}

// finish reads post-state after the transaction committed. The calling worker
// still holds the transaction's account locks, so no other transaction has
// changed these accounts yet.
func (c *rpcCapture) finish(slotCtx *sealevel.SlotCtx, tx *solana.Transaction, feeInfo *fees.TxFeeInfo, computeUnits uint64, err error) {
	keys := tx.Message.AccountKeys
	captured := &rpcindex.Captured{
		Signature:    tx.Signatures[0],
		Keys:         append([]solana.PublicKey(nil), keys...),
		Err:          err,
		ErrJSON:      c.errJSON,
		ComputeUnits: computeUnits,
		PreBalances:  c.pre,
		PostBalances: make([]uint64, len(keys)),
		Logs:         c.logs,
		Inner:        c.inner,
	}
	if feeInfo != nil {
		captured.Fee = feeInfo.TotalFee
	}
	decimals := map[solana.PublicKey]uint8{}
	mintDecimals := func(mint solana.PublicKey) uint8 {
		if d, ok := decimals[mint]; ok {
			return d
		}
		var d uint8
		if a, err := slotCtx.GetAccountShared(mint); err == nil {
			d, _ = rpcindex.MintDecimals(a)
		}
		decimals[mint] = d
		return d
	}
	tokenBalance := func(i int, a *accounts.Account) (rpcindex.TokenBalance, bool) {
		mint, owner, amount, program, ok := rpcindex.TokenAccountState(a)
		if !ok || i > 255 {
			return rpcindex.TokenBalance{}, false
		}
		return rpcindex.TokenBalance{AccountIndex: uint8(i), Mint: mint, Owner: owner, ProgramID: program, Amount: amount, Decimals: mintDecimals(mint)}, true
	}
	for i, key := range keys {
		post, err := slotCtx.GetAccountShared(key)
		if err == nil && post != nil {
			captured.PostBalances[i] = post.Lamports
			if tb, ok := tokenBalance(i, post); ok {
				captured.PostTokens = append(captured.PostTokens, tb)
			}
		}
		if i < len(c.preAccts) {
			if tb, ok := tokenBalance(i, c.preAccts[i]); ok {
				captured.PreTokens = append(captured.PreTokens, tb)
			}
		}
	}
	rpcCaptures.add(slotCtx, captured)
}

// discoverTokenAccounts runs when a bank's writes are buffered, with history
// on. A written token account owned by a watched wallet joins the watch set
// before the next block, so later transactions that name only the token
// account are captured. A watched token account that was closed or
// reassigned is reported too. Cost: one owner comparison per written
// account, and a filter probe for token accounts.
func discoverTokenAccounts(slotCtx *sealevel.SlotCtx, written []*accounts.Account) {
	reg := rpcWatch.Load()
	if reg == nil {
		return
	}
	snap := reg.Current()
	if snap == nil || snap.Owners == nil {
		return
	}
	var changes []rpcindex.TokenAccountChange
	var keys []watch.Key
	for _, a := range written {
		if a == nil {
			continue
		}
		program := solana.PublicKey(a.Owner)
		isToken := program == rpcindex.SPLTokenProgram || program == rpcindex.Token2022Program
		mint, owner, _, _, ok := rpcindex.TokenAccountState(a)
		if isToken && ok && snap.Owners.HasKey(&owner) {
			changes = append(changes, rpcindex.TokenAccountChange{Owner: owner, Mint: mint, Account: a.Key, Program: program})
			keys = append(keys, watch.Key{Key: a.Key, Kind: watch.TokenAccount})
			continue
		}
		// No longer a token account of a watched wallet: closed, or its
		// owner changed. Only previously watched token accounts matter.
		if snap.Match.HasKey(&a.Key) && reg.Kinds(a.Key)&watch.TokenAccount != 0 {
			changes = append(changes, rpcindex.TokenAccountChange{Account: a.Key, Closed: true})
		}
	}
	if len(keys) > 0 {
		reg.AddKeys(keys...)
	}
	if len(changes) > 0 {
		rpcCaptures.addTokenChanges(slotCtx, changes)
	}
}

// rpcCaptures buffers captures per executing bank until the block completes.
// Streaming replay may discard a speculative bank and execute the slot again,
// so buffers are keyed by bank, not slot.
var rpcCaptures = captureBuffers{byBank: map[*sealevel.SlotCtx]*bankCaptures{}}

type bankCaptures struct {
	captured []*rpcindex.Captured
	tokens   []rpcindex.TokenAccountChange
	// feeTxs are the bank's executed transactions, with lookup tables
	// resolved, for the prioritization fee record.
	feeTxs []*solana.Transaction
}

// hasHistory reports whether the bank produced anything for the indexer.
func (bc *bankCaptures) hasHistory() bool {
	return len(bc.captured) > 0 || len(bc.tokens) > 0
}

// rpcFeeRecording is set while an RPC server records prioritization fees.
// Without one, each executed group pays one atomic load.
var rpcFeeRecording atomic.Bool

// recordFeeTransactions keeps a group of transactions, about to execute with
// lookup tables resolved, for the bank's prioritization fee record. The
// streaming path executes resolved copies while the completed block keeps
// the unresolved originals, so the record must come from execution. Every
// transaction of a valid block executes; a bank that fails is discarded with
// its buffer. Called once per group, not per transaction, to stay off the
// workers' path.
func recordFeeTransactions(slotCtx *sealevel.SlotCtx, txs []*solana.Transaction) {
	if !rpcFeeRecording.Load() || len(txs) == 0 {
		return
	}
	rpcCaptures.mu.Lock()
	bc := rpcCaptures.bankLocked(slotCtx)
	bc.feeTxs = append(bc.feeTxs, txs...)
	rpcCaptures.mu.Unlock()
}

type captureBuffers struct {
	mu     sync.Mutex
	byBank map[*sealevel.SlotCtx]*bankCaptures
}

func (b *captureBuffers) bankLocked(slotCtx *sealevel.SlotCtx) *bankCaptures {
	bc := b.byBank[slotCtx]
	if bc == nil {
		bc = &bankCaptures{}
		b.byBank[slotCtx] = bc
	}
	return bc
}

func (b *captureBuffers) add(slotCtx *sealevel.SlotCtx, c *rpcindex.Captured) {
	b.mu.Lock()
	bc := b.bankLocked(slotCtx)
	bc.captured = append(bc.captured, c)
	b.mu.Unlock()
}

func (b *captureBuffers) addTokenChanges(slotCtx *sealevel.SlotCtx, changes []rpcindex.TokenAccountChange) {
	b.mu.Lock()
	bc := b.bankLocked(slotCtx)
	bc.tokens = append(bc.tokens, changes...)
	b.mu.Unlock()
}

// take returns the completed bank's captures and drops every buffer at or
// below its slot, including those of discarded speculative banks.
func (b *captureBuffers) take(slotCtx *sealevel.SlotCtx) *bankCaptures {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := b.byBank[slotCtx]
	for bank := range b.byBank {
		if bank.Slot <= slotCtx.Slot {
			delete(b.byBank, bank)
		}
	}
	return out
}

func (b *captureBuffers) reset() {
	b.mu.Lock()
	b.byBank = map[*sealevel.SlotCtx]*bankCaptures{}
	b.mu.Unlock()
}
