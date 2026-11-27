// Package bankview publishes immutable replayed-bank views to RPC readers.
//
// Replay publishes one View per completed bank and marks which view each
// commitment level currently selects. Readers pick a level once per request
// and read every field and account through that view. Account data is not
// part of the view: it lives in the working set and the durable store, which
// keep changing, so every read is validated against two seqlock counters and
// retried as a whole when either moved.
package bankview

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// Level is a commitment level a request can select.
type Level uint8

const (
	Processed Level = iota // replayed and completed on the chosen fork
	Notarized              // verified notarization certificate; alias "confirmed"
	Finalized              // finalized and verified by replay's fold gate
	Durable                // folded into the on-disk accounts store
	levelCount
)

func (l Level) String() string {
	switch l {
	case Processed:
		return "processed"
	case Notarized:
		return "notarized"
	case Finalized:
		return "finalized"
	case Durable:
		return "durable"
	}
	return fmt.Sprintf("level(%d)", uint8(l))
}

// ParseCommitment maps a request's commitment string to a level. An empty
// string selects finalized, the Solana RPC default.
func ParseCommitment(commitment string) (Level, error) {
	switch commitment {
	case "", "finalized", "max", "root":
		return Finalized, nil
	case "confirmed", "notarized", "singleGossip":
		return Notarized, nil
	case "processed", "recent", "single":
		return Processed, nil
	case "durable":
		return Durable, nil
	}
	return 0, fmt.Errorf("invalid commitment %q", commitment)
}

// View is one completed bank. It is immutable after publication.
type View struct {
	Slot             uint64
	ParentSlot       uint64
	BlockHeight      uint64
	TransactionCount uint64

	// BlockID is the Alpenglow block identity; it distinguishes forks that
	// reuse a slot number.
	BlockID    solana.Hash
	HasBlockID bool

	Blockhash            solana.Hash
	Bankhash             solana.Hash
	LamportsPerSignature uint64

	// Features is the bank's feature set. Replay replaces the set at epoch
	// boundaries rather than mutating it, so views can share it.
	//
	// Views deliberately hold no slot context or sysvar snapshot: many views
	// stay retained until the next fold, and those structures would keep
	// per-slot execution state alive. Sysvars are accounts; read them through
	// Read.Account at the view's slot.
	Features *features.Features

	// Execution parameters a simulation needs besides accounts and sysvars.
	// FeeRateGovernor is the bank's fully derived governor (a copy).
	// LatestEvictedBlockhash is the newest hash the bank's blockhash queue has
	// dropped, still accepted as recent by replay's age check. EpochStakes is
	// the epoch's vote stake map, which replay replaces at epoch boundaries and
	// never mutates, so views share it.
	FeeRateGovernor        sealevel.FeeRateGovernor
	LatestEvictedBlockhash solana.Hash
	EpochStakes            map[solana.PublicKey]uint64
	TotalEpochStake        uint64
}

// Accounts is the account state behind every view: the unrooted working set
// layered over the durable store.
type Accounts interface {
	// Generation is the working-set seqlock counter; odd during an unwind.
	Generation() uint64
	// LookupAt returns the newest unrooted version written at or below maxSlot.
	LookupAt(pubkey [32]byte, maxSlot uint64) (*accounts.Account, bool)
	// ReadPublication returns the durable read epoch and the slot it reaches.
	// The epoch changes with every fold, rewind or recovery of the durable
	// store. stable is false while such a change is in progress; no view may
	// read the store then.
	ReadPublication() (epoch, through uint64, stable bool)
	// GetDurable reads the durable store; a missing account is (nil, nil).
	GetDurable(pubkey solana.PublicKey) (*accounts.Account, error)
	// GetDurableBatch reads keys from the durable store in order; missing
	// accounts are nil entries. It refuses with accountsdb.ErrAccountDataLimit
	// before copying more than maxDataBytes of account data; NoDataLimit
	// disables the check.
	GetDurableBatch(ctx context.Context, pubkeys []solana.PublicKey, maxDataBytes uint64) ([]*accounts.Account, error)
}

// LookupErrAccounts is implemented by account state whose unrooted lookups
// can fail, such as a reader that is not an in-memory working set. Reads use
// LookupAtErr in place of LookupAt so a failure is returned, never read past.
type LookupErrAccounts interface {
	LookupAtErr(pubkey [32]byte, maxSlot uint64) (*accounts.Account, bool, error)
}

var (
	// ErrNotReady means replay has not published a bank at the requested level.
	ErrNotReady = errors.New("node has not published a bank at this commitment yet")
	// ErrInconsistent means every attempt overlapped a fork switch or fold.
	ErrInconsistent = errors.New("bank state changed during every read attempt; retry")
)

// MinContextSlotError reports a selected bank below the request's minContextSlot.
type MinContextSlotError struct {
	ContextSlot uint64
}

func (e *MinContextSlotError) Error() string {
	return "Minimum context slot has not been reached"
}

type snapshot struct {
	views [levelCount]*View
	// foldBound[level] is the highest fold watermark that cannot contain a
	// write newer than that level's view: the slot before the next retained
	// view, or MaxUint64 for the newest one. Skipped slots between banks make
	// a watermark past view.Slot harmless.
	foldBound [levelCount]uint64
	gen       uint64
	accounts  Accounts
}

func (p *Publisher) storeLocked(next *snapshot) {
	for level, v := range next.views {
		next.foldBound[level] = 0
		if v == nil {
			continue
		}
		next.foldBound[level] = math.MaxUint64
		for _, r := range p.recent {
			if r.Slot > v.Slot {
				next.foldBound[level] = r.Slot - 1
				break
			}
		}
	}
	p.cur.Store(next)
}

// Publisher holds the current views. Replay is the only writer; readers never
// block it.
type Publisher struct {
	mu       sync.Mutex // serializes writers
	cur      atomic.Pointer[snapshot]
	recent   []*View // published processed views at or above the durable view, ascending
	listener func(Level, *View)
	// rootOnly marks views that PublishFallback installed before replay
	// published any bank of its own.
	rootOnly bool
}

// SetListener registers fn to run after a level selects a new view (and
// after an unwind republishes the processed view). It runs on the writer's
// goroutine under the publisher lock, so it must not block.
func (p *Publisher) SetListener(fn func(Level, *View)) {
	p.mu.Lock()
	p.listener = fn
	p.mu.Unlock()
}

func (p *Publisher) notifyLocked(level Level, v *View) {
	if p.listener != nil && v != nil {
		p.listener(level, v)
	}
}

const maxRecentViews = 4096

// Reset forgets every view and binds the publisher to a new account state,
// such as at the start of a replay run.
func (p *Publisher) Reset(accts Accounts) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.recent = nil
	p.rootOnly = false
	if accts == nil {
		p.cur.Store(nil)
		return
	}
	p.storeLocked(&snapshot{gen: accts.Generation(), accounts: accts})
}

// PublishProcessed makes view the processed bank.
func (p *Publisher) PublishProcessed(view *View) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cur := p.cur.Load()
	if cur == nil || view == nil {
		return
	}
	p.rootOnly = false
	p.recent = append(p.recent, view)
	if len(p.recent) > maxRecentViews {
		p.recent = append([]*View(nil), p.recent[len(p.recent)-maxRecentViews:]...)
	}
	next := *cur
	next.views[Processed] = view
	next.gen = cur.accounts.Generation()
	p.storeLocked(&next)
	p.notifyLocked(Processed, view)
}

// PublishFallback serves banks that replay did not publish itself: root as
// the durable (so finalized and notarized) bank and processed, when not nil,
// as the processed bank. With no processed bank, processed reads are refused. It applies only while replay has published no bank
// of its own since the last Reset, such as at node startup before replay
// runs; afterwards it does nothing. Each call replaces the previous fallback
// banks and binds accts, whose current generation the views are valid for. A
// nil root withdraws the fallback banks.
func (p *Publisher) PublishFallback(accts Accounts, root, processed *View) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cur := p.cur.Load()
	if cur != nil && cur.views[Processed] != nil && !p.rootOnly {
		return
	}
	if accts == nil || root == nil {
		// The node withdrew its root: serve nothing until it publishes again.
		p.recent = nil
		p.cur.Store(nil)
		return
	}
	p.rootOnly = true
	p.recent = []*View{root}
	next := &snapshot{gen: accts.Generation(), accounts: accts}
	for level := Notarized; level < levelCount; level++ {
		next.views[level] = root
	}
	// Without a processed bank the processed level stays empty: replay may
	// already be past the root, so the root must not stand in for it.
	if processed != nil && processed.Slot >= root.Slot {
		if processed.Slot > root.Slot {
			p.recent = append(p.recent, processed)
		}
		next.views[Processed] = processed
	}
	p.storeLocked(next)
	for level := Processed; level < levelCount; level++ {
		p.notifyLocked(level, next.views[level])
	}
}

// MarkNotarized selects the view with this slot and block ID as notarized.
func (p *Publisher) MarkNotarized(slot uint64, blockID solana.Hash) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cur := p.cur.Load()
	if cur == nil {
		return
	}
	if old := cur.views[Notarized]; old != nil && old.Slot >= slot {
		return
	}
	for i := len(p.recent) - 1; i >= 0; i-- {
		v := p.recent[i]
		if v.Slot == slot && v.HasBlockID && v.BlockID == blockID {
			next := *cur
			next.views[Notarized] = v
			p.storeLocked(&next)
			p.notifyLocked(Notarized, v)
			return
		}
	}
}

// MarkFinalized selects the newest retained view at or below slot as
// finalized. Replay calls it with a slot its fold gate already verified.
func (p *Publisher) MarkFinalized(slot uint64) {
	p.markAtOrBelow(Finalized, slot)
}

// MarkDurable selects the newest retained view at or below the fold watermark.
func (p *Publisher) MarkDurable(slot uint64) {
	p.markAtOrBelow(Durable, slot)
}

func (p *Publisher) markAtOrBelow(level Level, slot uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cur := p.cur.Load()
	if cur == nil {
		return
	}
	v := p.atOrBelowLocked(slot)
	if v == nil {
		return
	}
	if old := cur.views[level]; old != nil && old.Slot >= v.Slot {
		return
	}
	next := *cur
	next.views[level] = v
	// Durability implies finality, and finality implies notarization.
	for l := level - 1; l >= Notarized; l-- {
		if lower := next.views[l]; lower == nil || lower.Slot < v.Slot {
			next.views[l] = v
		}
	}
	p.storeLocked(&next)
	for l := Notarized; l <= level; l++ {
		if next.views[l] == v && cur.views[l] != v {
			p.notifyLocked(l, v)
		}
	}
	if level == Durable {
		p.pruneBelowLocked(v.Slot)
	}
}

func (p *Publisher) atOrBelowLocked(slot uint64) *View {
	for i := len(p.recent) - 1; i >= 0; i-- {
		if p.recent[i].Slot <= slot {
			return p.recent[i]
		}
	}
	return nil
}

func (p *Publisher) pruneBelowLocked(slot uint64) {
	i := 0
	for i < len(p.recent) && p.recent[i].Slot < slot {
		i++
	}
	if i > 0 {
		p.recent = append([]*View(nil), p.recent[i:]...)
	}
}

// Abandon drops views at or above fromSlot after replay evicted that suffix,
// and republishes the surviving views under the new working-set generation.
// Call it after the eviction finished.
func (p *Publisher) Abandon(fromSlot uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cur := p.cur.Load()
	if cur == nil {
		return
	}
	keep := len(p.recent)
	for keep > 0 && p.recent[keep-1].Slot >= fromSlot {
		keep--
	}
	p.recent = p.recent[:keep]
	next := *cur
	for level, v := range next.views {
		if v != nil && v.Slot >= fromSlot {
			next.views[level] = nil
		}
	}
	if keep > 0 {
		next.views[Processed] = p.recent[keep-1]
	}
	if next.views[Notarized] == nil {
		next.views[Notarized] = next.views[Finalized]
	}
	next.gen = cur.accounts.Generation()
	p.storeLocked(&next)
	p.notifyLocked(Processed, next.views[Processed])
}

// Read is one validated attempt to read through a view.
type Read struct {
	View     *View
	Level    Level // the level actually served; Notarized may fall back to Finalized
	accounts Accounts
	gen      uint64
	epoch    uint64
	durable  uint64 // the slot the durable store reaches for this read
}

// DurableSlot returns the slot the durable store reaches for this read. Slots
// after it are still held in the working set.
func (r *Read) DurableSlot() uint64 { return r.durable }

// Account returns the account as of the view's slot, or nil if it does not
// exist there. Zero-lamport accounts are reported as missing.
func (r *Read) Account(pubkey solana.PublicKey) (*accounts.Account, error) {
	if a, ok, err := r.lookup(pubkey); err != nil {
		return nil, err
	} else if ok {
		return liveOrNil(a), nil
	}
	a, err := r.accounts.GetDurable(pubkey)
	if err != nil {
		return nil, err
	}
	return liveOrNil(a), nil
}

// NoDataLimit disables the account data budget of a batch read.
const NoDataLimit = ^uint64(0)

// Accounts returns accounts as of the view's slot, in order, with nil for
// missing ones.
func (r *Read) Accounts(ctx context.Context, pubkeys []solana.PublicKey) ([]*accounts.Account, error) {
	return r.AccountsWithDataLimit(ctx, pubkeys, NoDataLimit)
}

// AccountsWithDataLimit is Accounts with a budget on the total account data
// returned, duplicates included. It fails with accountsdb.ErrAccountDataLimit
// once the budget is exceeded; durable reads are refused before their data
// is copied.
func (r *Read) AccountsWithDataLimit(ctx context.Context, pubkeys []solana.PublicKey, maxDataBytes uint64) ([]*accounts.Account, error) {
	return r.AccountsAtSlot(ctx, pubkeys, r.View.Slot, maxDataBytes)
}

// AccountsAtSlot is AccountsWithDataLimit as of an earlier slot of the view's
// chain, at or after DurableSlot: the working set still holds every version
// written since then. Slots at or before DurableSlot read the durable store's
// current values.
func (r *Read) AccountsAtSlot(ctx context.Context, pubkeys []solana.PublicKey, slot uint64, maxDataBytes uint64) ([]*accounts.Account, error) {
	if slot > r.View.Slot {
		return nil, fmt.Errorf("slot %d is after the view's slot %d", slot, r.View.Slot)
	}
	out := make([]*accounts.Account, len(pubkeys))
	var misses []solana.PublicKey
	var missIdx []int
	remaining := maxDataBytes
	for i, pk := range pubkeys {
		a, ok, err := r.lookupAt(pk, slot)
		if err != nil {
			return nil, err
		}
		if ok {
			out[i] = liveOrNil(a)
			if out[i] != nil && maxDataBytes != NoDataLimit {
				if uint64(len(a.Data)) > remaining {
					return nil, fmt.Errorf("%w: unrooted account data exceeds %d bytes", accountsdb.ErrAccountDataLimit, maxDataBytes)
				}
				remaining -= uint64(len(a.Data))
			}
			continue
		}
		misses = append(misses, pk)
		missIdx = append(missIdx, i)
	}
	if len(misses) == 0 {
		return out, nil
	}
	durable, err := r.accounts.GetDurableBatch(ctx, misses, remaining)
	if err != nil {
		return nil, err
	}
	if len(durable) != len(misses) {
		return nil, fmt.Errorf("durable batch returned %d accounts for %d keys", len(durable), len(misses))
	}
	for j, a := range durable {
		out[missIdx[j]] = liveOrNil(a)
	}
	return out, nil
}

// lookup finds an unrooted version at or below the view's slot.
func (r *Read) lookup(pubkey solana.PublicKey) (*accounts.Account, bool, error) {
	return r.lookupAt(pubkey, r.View.Slot)
}

func (r *Read) lookupAt(pubkey solana.PublicKey, slot uint64) (*accounts.Account, bool, error) {
	if accts, ok := r.accounts.(LookupErrAccounts); ok {
		return accts.LookupAtErr([32]byte(pubkey), slot)
	}
	a, ok := r.accounts.LookupAt([32]byte(pubkey), slot)
	return a, ok, nil
}

func liveOrNil(a *accounts.Account) *accounts.Account {
	if a == nil || a.Lamports == 0 {
		return nil
	}
	return a
}

// begin selects a view and records the counters a read must not see change.
// ok=false means the state is mid-change and the caller should retry.
func (p *Publisher) begin(level Level) (r *Read, ok bool, err error) {
	snap := p.cur.Load()
	if snap == nil {
		return nil, false, ErrNotReady
	}
	served := selectLevel(snap, level)
	view := snap.views[served]
	if view == nil {
		return nil, false, ErrNotReady
	}
	gen := snap.accounts.Generation()
	if gen%2 == 1 || gen != snap.gen {
		return nil, false, nil
	}
	epoch, through, stable := snap.accounts.ReadPublication()
	if !stable || through > snap.foldBound[served] {
		return nil, false, nil // a fold already holds writes newer than this view
	}
	// A publication and fold may both complete between loading snap and
	// sampling the counters, leaving a new epoch paired with snap's old fold
	// bound. Only a snapshot still current after sampling bounds them; a fold
	// after this point changes the epoch, which valid catches.
	if p.cur.Load() != snap {
		return nil, false, nil
	}
	return &Read{View: view, Level: served, accounts: snap.accounts, gen: gen, epoch: epoch, durable: through}, true, nil
}

func (r *Read) valid() bool {
	if r.accounts.Generation() != r.gen {
		return false
	}
	epoch, _, stable := r.accounts.ReadPublication()
	return stable && epoch == r.epoch
}

// MaxAttempts bounds how many times a request is retried against fresh views.
const MaxAttempts = 3

const retryPause = 2 * time.Millisecond

// Do runs fn against the view selected for level and retries the whole call
// when a fork switch or fold overlapped it. fn must be free of side effects
// other than building its result, since it can run more than once.
// minContextSlot is ignored when zero.
func (p *Publisher) Do(ctx context.Context, level Level, minContextSlot uint64, fn func(*Read) error) error {
	for attempt := 0; attempt < MaxAttempts; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retryPause):
			}
		}
		r, ok, err := p.begin(level)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if minContextSlot != 0 && r.View.Slot < minContextSlot {
			return &MinContextSlotError{ContextSlot: r.View.Slot}
		}
		if err := fn(r); err != nil {
			if r.valid() {
				return err
			}
			continue // the error may come from a torn read
		}
		if r.valid() {
			return nil
		}
	}
	return ErrInconsistent
}

// Accounts returns the account store replay installed, or nil before replay
// starts.
func (p *Publisher) Accounts() Accounts {
	snap := p.cur.Load()
	if snap == nil {
		return nil
	}
	return snap.accounts
}

// Current returns the view selected for level without account reads, for
// methods that only need bank scalars. It applies the same Notarized
// fallback as Do.
func (p *Publisher) Current(level Level) (*View, Level, error) {
	snap := p.cur.Load()
	if snap == nil {
		return nil, level, ErrNotReady
	}
	served := selectLevel(snap, level)
	view := snap.views[served]
	if view == nil {
		return nil, level, ErrNotReady
	}
	return view, served, nil
}

// selectLevel serves notarized from the finalized view when no notarization
// source is newer, as on a non-voting node without forwarded certificates.
func selectLevel(snap *snapshot, level Level) Level {
	if level != Notarized {
		return level
	}
	n, fin := snap.views[Notarized], snap.views[Finalized]
	if n == nil || (fin != nil && fin.Slot >= n.Slot) {
		return Finalized
	}
	return Notarized
}
