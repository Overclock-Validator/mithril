package rpcindex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/rpcdb"
	"github.com/Overclock-Validator/mithril/pkg/watch"
	"github.com/gagliardetto/solana-go"
	"github.com/mr-tron/base58"
)

// queueDepth bounds the blocks waiting for the indexer. Under the default
// keep-up-with-tip policy, a full queue drops captures and records a gap
// instead of slowing replay.
const queueDepth = 1024

// Indexer builds records from replayed blocks off the replay goroutine. It
// keeps unfinalized blocks in memory, where processed and confirmed reads see
// them, and writes a block to rpcdb only once it is finalized, so abandoned
// forks never reach disk.
type Indexer struct {
	store    *rpcdb.Store
	registry *watch.Registry
	events   chan event

	mu      sync.RWMutex
	pending map[uint64]*pendingBlock // unfinalized blocks with records, by slot

	dropped atomic.Uint64 // blocks whose captures were dropped on a full queue
	// droppedPersisted is the drop count the store last accounted for. A
	// higher count clears the persisted enumeration markers at the next
	// finality write. Used only by the event handler.
	droppedPersisted uint64
	// completedQueued counts accepted CompletedBlock events and
	// completedApplied the ones handled, in queue order. Replay hands a block
	// over before publishing its bank view, so a reader that waits for
	// completedApplied to reach completedQueued sees the discoveries of every
	// bank it can select.
	completedQueued  atomic.Uint64
	completedApplied atomic.Uint64
	err              atomic.Pointer[error]
	done             chan struct{}
}

type pendingBlock struct {
	slot    uint64
	blockID solana.Hash
	records []*rpcdb.TxRecord
	tokens  []TokenAccountChange
}

type eventKind uint8

const (
	evCompleted eventKind = iota
	evFinalized
	evAbandon
	evSync
)

type event struct {
	kind     eventKind
	slot     uint64
	blockID  solana.Hash
	block    *b.Block
	captured []*Captured
	tokens   []TokenAccountChange
	synced   chan struct{}
}

// New returns an indexer writing to store and matching with registry.
func New(store *rpcdb.Store, registry *watch.Registry) *Indexer {
	return &Indexer{
		store:    store,
		registry: registry,
		events:   make(chan event, queueDepth),
		pending:  map[uint64]*pendingBlock{},
		done:     make(chan struct{}),
	}
}

// Registry is the watch registry replay matches against.
func (ix *Indexer) Registry() *watch.Registry { return ix.registry }

// Store is the finalized index store.
func (ix *Indexer) Store() *rpcdb.Store { return ix.store }

// Run processes events until ctx ends.
func (ix *Indexer) Run(ctx context.Context) {
	defer close(ix.done)
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-ix.events:
			if err := ix.handle(ev); err != nil {
				ix.err.Store(&err)
			}
		}
	}
}

// Err reports the first persistent write failure, if any.
func (ix *Indexer) Err() error {
	if p := ix.err.Load(); p != nil {
		return *p
	}
	return nil
}

// Dropped is the number of blocks whose captures were lost to a full queue.
func (ix *Indexer) Dropped() uint64 { return ix.dropped.Load() }

// CompletedBlock hands over a replayed block's captures and token account
// changes. Replay calls it only when there are some, and it never blocks.
func (ix *Indexer) CompletedBlock(slot uint64, blockID solana.Hash, block *b.Block, captured []*Captured, tokens []TokenAccountChange) {
	select {
	case ix.events <- event{kind: evCompleted, slot: slot, blockID: blockID, block: block, captured: captured, tokens: tokens}:
		ix.completedQueued.Add(1)
	default:
		ix.dropped.Add(1)
	}
}

// ErrIndexerBehind reports that queued blocks were not handled in time.
var ErrIndexerBehind = errors.New("rpc indexer is behind replay")

// WaitCompleted waits until every block handed over so far has been handled,
// polling for at most timeout.
func (ix *Indexer) WaitCompleted(ctx context.Context, timeout time.Duration) error {
	target := ix.completedQueued.Load()
	deadline := time.Now().Add(timeout)
	for ix.completedApplied.Load() < target {
		if time.Now().After(deadline) {
			return ErrIndexerBehind
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond):
		}
	}
	return nil
}

// Finalized records that every block up to slot on the followed chain is
// final. It never blocks; a dropped notice is repaired by the next one.
func (ix *Indexer) Finalized(slot uint64) {
	select {
	case ix.events <- event{kind: evFinalized, slot: slot}:
	default:
	}
}

// Abandon drops unfinalized blocks at or above fromSlot after a fork switch.
// It blocks if the queue is full: losing it would let abandoned records be
// finalized.
func (ix *Indexer) Abandon(fromSlot uint64) {
	ix.events <- event{kind: evAbandon, slot: fromSlot}
}

// BeginReplay drops every unfinalized block above root, the bank a replay run
// starts from. Replay calls it at the start of each run, including a restart
// from a checkpoint after recovery: blocks the previous attempt completed
// above root may belong to an abandoned fork, and the new run reports again
// the ones it selects. Like Abandon it blocks rather than drop the event.
func (ix *Indexer) BeginReplay(root uint64) {
	ix.events <- event{kind: evAbandon, slot: root + 1}
}

// Sync waits until every event queued before it has been handled (tests).
func (ix *Indexer) Sync() {
	ch := make(chan struct{})
	ix.events <- event{kind: evSync, synced: ch}
	<-ch
}

func (ix *Indexer) handle(ev event) error {
	switch ev.kind {
	case evCompleted:
		defer ix.completedApplied.Add(1)
		records := ix.buildRecords(ev)
		ix.mu.Lock()
		if len(records) == 0 && len(ev.tokens) == 0 {
			delete(ix.pending, ev.slot)
		} else {
			ix.pending[ev.slot] = &pendingBlock{slot: ev.slot, blockID: ev.blockID, records: records, tokens: ev.tokens}
		}
		ix.mu.Unlock()
	case evAbandon:
		ix.mu.Lock()
		for slot := range ix.pending {
			if slot >= ev.slot {
				delete(ix.pending, slot)
			}
		}
		ix.mu.Unlock()
	case evFinalized:
		return ix.persistThrough(ev.slot)
	case evSync:
		close(ev.synced)
	}
	return nil
}

func (ix *Indexer) persistThrough(slot uint64) error {
	ix.mu.RLock()
	var final []*pendingBlock
	for s, pb := range ix.pending {
		if s <= slot {
			final = append(final, pb)
		}
	}
	ix.mu.RUnlock()
	watermark, err := ix.store.Watermark()
	if err != nil {
		return err
	}
	dropped := ix.dropped.Load()
	if len(final) == 0 && slot <= watermark && dropped == ix.droppedPersisted {
		return nil
	}
	sort.Slice(final, func(i, j int) bool { return final[i].slot < final[j].slot })
	batch := ix.store.NewBatch()
	written := map[solana.PublicKey]*rpcdb.TokenAccount{} // token rows set or deleted in this batch
	for _, pb := range final {
		for _, rec := range pb.records {
			if err := batch.PutTransaction(rec); err != nil {
				return err
			}
		}
		if err := ix.applyTokenChanges(batch, pb, written); err != nil {
			return err
		}
	}
	if slot > watermark {
		if err := batch.SetWatermark(slot); err != nil {
			return err
		}
	}
	// A dropped block may have held a token account discovery. The watermark
	// can pass over it, so a restart must not trust markers set before it:
	// clear them, and every enumerated wallet is scanned again.
	if dropped != ix.droppedPersisted {
		markers, err := ix.store.Enumerated()
		if err != nil {
			return err
		}
		for owner := range markers {
			if err := batch.DeleteEnumerated(owner); err != nil {
				return err
			}
		}
	}
	if err := batch.Commit(); err != nil {
		return fmt.Errorf("persist rpc history through slot %d: %w", slot, err)
	}
	ix.droppedPersisted = dropped
	ix.mu.Lock()
	for _, pb := range final {
		delete(ix.pending, pb.slot)
	}
	ix.mu.Unlock()
	return nil
}

func (ix *Indexer) buildRecords(ev event) []*rpcdb.TxRecord {
	if ev.block == nil || len(ev.captured) == 0 {
		return nil
	}
	position := make(map[solana.Signature]int, len(ev.block.Transactions))
	for i, tx := range ev.block.Transactions {
		if len(tx.Signatures) > 0 {
			position[tx.Signatures[0]] = i
		}
	}
	snap := ix.registry.Current()
	records := make([]*rpcdb.TxRecord, 0, len(ev.captured))
	for _, c := range ev.captured {
		pos, ok := position[c.Signature]
		if !ok {
			continue // not in the accepted block
		}
		tx := ev.block.Transactions[pos]
		wire, err := tx.MarshalBinary()
		if err != nil {
			continue
		}
		rec := &rpcdb.TxRecord{
			Signature: c.Signature,
			Slot:      ev.slot,
			Position:  uint32(pos),
			BlockTime: ev.block.UnixTimestamp,
			BlockID:   ev.blockID,
			Wire:      wire,
			Meta:      EncodeMeta(c, tx),
		}
		if c.Err != nil {
			rec.Flags |= rpcdb.Failed
		}
		seen := map[solana.PublicKey]bool{}
		for i := range c.Keys {
			if !seen[c.Keys[i]] && snap != nil && snap.Match.HasKey(&c.Keys[i]) {
				seen[c.Keys[i]] = true
				rec.Addresses = append(rec.Addresses, c.Keys[i])
			}
		}
		records = append(records, rec)
	}
	return records
}

// applyTokenChanges writes a finalized block's token account changes in slot
// order, so a later close overrides an earlier creation. written tracks rows
// set (non-nil) or deleted (nil) earlier in the same uncommitted batch, which
// the store cannot see yet.
func (ix *Indexer) applyTokenChanges(batch *rpcdb.Batch, pb *pendingBlock, written map[solana.PublicKey]*rpcdb.TokenAccount) error {
	for _, ch := range pb.tokens {
		if ch.Closed {
			row, inBatch := written[ch.Account]
			if !inBatch {
				ta, ok, err := ix.store.TokenAccountByAccount(ch.Account)
				if err != nil {
					return err
				}
				if ok {
					row = &ta
				}
			}
			if row != nil {
				if err := batch.DeleteTokenAccount(*row); err != nil {
					return err
				}
			}
			written[ch.Account] = nil
			continue
		}
		row := tokenRow(ch)
		prev, inBatch := written[ch.Account]
		if !inBatch {
			ta, ok, err := ix.store.TokenAccountByAccount(ch.Account)
			if err != nil {
				return err
			}
			if ok {
				prev = &ta
			}
		}
		if prev != nil && *prev != row { // reassigned: drop the old owner's row
			if err := batch.DeleteTokenAccount(*prev); err != nil {
				return err
			}
		}
		if err := batch.PutTokenAccount(row); err != nil {
			return err
		}
		written[ch.Account] = &row
		if err := batch.PutWatch(0, ch.Account, uint8(watch.TokenAccount), pb.slot); err != nil {
			return err
		}
	}
	return nil
}

func tokenRow(ch TokenAccountChange) rpcdb.TokenAccount {
	ta := rpcdb.TokenAccount{Owner: ch.Owner, Mint: ch.Mint, Account: ch.Account}
	if ch.Program == Token2022Program {
		ta.Program = 1
	}
	return ta
}

// PendingTokenAccounts returns unfinalized token account changes for owner
// at or below maxSlot, oldest first. Closes carry only the account.
func (ix *Indexer) PendingTokenAccounts(maxSlot uint64) []TokenAccountChange {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	slots := make([]uint64, 0, len(ix.pending))
	for slot, pb := range ix.pending {
		if slot <= maxSlot && len(pb.tokens) > 0 {
			slots = append(slots, slot)
		}
	}
	sort.Slice(slots, func(i, j int) bool { return slots[i] < slots[j] })
	var out []TokenAccountChange
	for _, slot := range slots {
		out = append(out, ix.pending[slot].tokens...)
	}
	return out
}

// Pending returns unfinalized records for addr at or below maxSlot, newest
// first, for processed and confirmed history reads.
func (ix *Indexer) Pending(addr solana.PublicKey, maxSlot uint64) []*rpcdb.TxRecord {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	var out []*rpcdb.TxRecord
	for slot, pb := range ix.pending {
		if slot > maxSlot {
			continue
		}
		for _, rec := range pb.records {
			for _, a := range rec.Addresses {
				if a == addr {
					out = append(out, rec)
					break
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Slot != out[j].Slot {
			return out[i].Slot > out[j].Slot
		}
		return out[i].Position > out[j].Position
	})
	return out
}

// PendingTransaction returns an unfinalized record by signature at or below
// maxSlot.
func (ix *Indexer) PendingTransaction(sig solana.Signature, maxSlot uint64) (*rpcdb.TxRecord, bool) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()
	for slot, pb := range ix.pending {
		if slot > maxSlot {
			continue
		}
		for _, rec := range pb.records {
			if rec.Signature == sig {
				return rec, true
			}
		}
	}
	return nil, false
}

// Meta is a transaction's status metadata in Solana's getTransaction JSON
// form.
type Meta struct {
	Err                  json.RawMessage       `json:"err"`
	Status               map[string]any        `json:"status"`
	Fee                  uint64                `json:"fee"`
	PreBalances          []uint64              `json:"preBalances"`
	PostBalances         []uint64              `json:"postBalances"`
	InnerInstructions    []innerInstructionsJS `json:"innerInstructions"`
	LogMessages          []string              `json:"logMessages"`
	PreTokenBalances     []tokenBalanceJS      `json:"preTokenBalances"`
	PostTokenBalances    []tokenBalanceJS      `json:"postTokenBalances"`
	Rewards              []any                 `json:"rewards"`
	LoadedAddresses      loadedAddressesJS     `json:"loadedAddresses"`
	ComputeUnitsConsumed uint64                `json:"computeUnitsConsumed"`
}

type innerInstructionsJS struct {
	Index        uint8           `json:"index"`
	Instructions []instructionJS `json:"instructions"`
}

type instructionJS struct {
	ProgramIDIndex uint8    `json:"programIdIndex"`
	Accounts       []uint16 `json:"accounts"`
	Data           string   `json:"data"`
	StackHeight    *uint32  `json:"stackHeight"`
}

type uiTokenAmountJS struct {
	Amount         string   `json:"amount"`
	Decimals       uint8    `json:"decimals"`
	UIAmount       *float64 `json:"uiAmount"`
	UIAmountString string   `json:"uiAmountString"`
}

type tokenBalanceJS struct {
	AccountIndex  uint8           `json:"accountIndex"`
	Mint          string          `json:"mint"`
	Owner         string          `json:"owner"`
	ProgramID     string          `json:"programId"`
	UITokenAmount uiTokenAmountJS `json:"uiTokenAmount"`
}

type loadedAddressesJS struct {
	Writable []string `json:"writable"`
	Readonly []string `json:"readonly"`
}

// EncodeMeta renders c as getTransaction metadata. original is the
// transaction as it appears in the block, before lookup tables resolve.
func EncodeMeta(c *Captured, original *solana.Transaction) []byte {
	m := Meta{
		Err:                  json.RawMessage("null"),
		Status:               map[string]any{"Ok": nil},
		Fee:                  c.Fee,
		PreBalances:          nonNil(c.PreBalances),
		PostBalances:         nonNil(c.PostBalances),
		LogMessages:          append([]string{}, c.Logs...),
		PreTokenBalances:     tokenBalancesJS(c.PreTokens),
		PostTokenBalances:    tokenBalancesJS(c.PostTokens),
		Rewards:              []any{},
		ComputeUnitsConsumed: c.ComputeUnits,
		InnerInstructions:    []innerInstructionsJS{},
		LoadedAddresses:      loadedAddressesJS{Writable: []string{}, Readonly: []string{}},
	}
	if c.ErrJSON != nil {
		m.Err = json.RawMessage(c.ErrJSON)
		m.Status = map[string]any{"Err": json.RawMessage(c.ErrJSON)}
	}
	for _, list := range c.Inner {
		js := innerInstructionsJS{Index: list.Index, Instructions: make([]instructionJS, len(list.Instructions))}
		for i, ix := range list.Instructions {
			accts := make([]uint16, len(ix.Accounts))
			for j, a := range ix.Accounts {
				accts[j] = uint16(a)
			}
			height := ix.StackHeight
			js.Instructions[i] = instructionJS{ProgramIDIndex: ix.ProgramIDIndex, Accounts: accts, Data: base58.Encode(ix.Data), StackHeight: &height}
		}
		m.InnerInstructions = append(m.InnerInstructions, js)
	}
	if original != nil {
		// Whole-block replay resolves the block's own transactions in place,
		// so their AccountKeys may already end with the loaded addresses.
		static := len(original.Message.AccountKeys)
		if original.Message.IsResolved() {
			static -= original.Message.NumLookups()
		}
		var writable int
		for _, l := range original.Message.AddressTableLookups {
			writable += len(l.WritableIndexes)
		}
		for i := static; i < len(c.Keys); i++ {
			if i-static < writable {
				m.LoadedAddresses.Writable = append(m.LoadedAddresses.Writable, c.Keys[i].String())
			} else {
				m.LoadedAddresses.Readonly = append(m.LoadedAddresses.Readonly, c.Keys[i].String())
			}
		}
	}
	out, _ := json.Marshal(m)
	return out
}

func nonNil(v []uint64) []uint64 {
	if v == nil {
		return []uint64{}
	}
	return v
}

func tokenBalancesJS(in []TokenBalance) []tokenBalanceJS {
	out := make([]tokenBalanceJS, len(in))
	for i, tb := range in {
		out[i] = tokenBalanceJS{
			AccountIndex: tb.AccountIndex,
			Mint:         tb.Mint.String(),
			Owner:        tb.Owner.String(),
			ProgramID:    tb.ProgramID.String(),
			UITokenAmount: uiTokenAmountJS{
				Amount:         strconv.FormatUint(tb.Amount, 10),
				Decimals:       tb.Decimals,
				UIAmount:       uiAmount(tb.Amount, tb.Decimals),
				UIAmountString: UIAmountString(tb.Amount, tb.Decimals),
			},
		}
	}
	return out
}

func uiAmount(amount uint64, decimals uint8) *float64 {
	f, _ := strconv.ParseFloat(UIAmountString(amount, decimals), 64)
	return &f
}

// UIAmountString formats amount with decimals places and no trailing zeros,
// as Agave's real_number_string_trimmed does.
func UIAmountString(amount uint64, decimals uint8) string {
	s := strconv.FormatUint(amount, 10)
	if decimals == 0 {
		return s
	}
	d := int(decimals)
	if len(s) <= d {
		s = fmt.Sprintf("%0*s", d+1, s)
	}
	whole, frac := s[:len(s)-d], s[len(s)-d:]
	for len(frac) > 0 && frac[len(frac)-1] == '0' {
		frac = frac[:len(frac)-1]
	}
	if frac == "" {
		return whole
	}
	return whole + "." + frac
}
