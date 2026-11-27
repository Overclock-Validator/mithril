package txstatus

import (
	"bytes"
	"encoding/json"
	"errors"
	"sync"

	"github.com/gagliardetto/solana-go"
)

const (
	resultRoots       = 300
	resultActiveBanks = 512 + 1 // Completed-bank margin before replay checks its 512-slot cap.
	resultBankSlots   = resultRoots + resultActiveBanks
	resultCacheBytes  = 128 << 20
	resultScaffold    = resultBankSlots * 128
	resultEntryBytes  = 160
	resultErrorBytes  = 2048
	resultBankEntries = 16384
)

// MaxResultBankEntries bounds both pending capture and retained bank outcomes.
const MaxResultBankEntries = resultBankEntries

// MaxResultErrorBytes bounds one owned typed-error encoding.
const MaxResultErrorBytes = resultErrorBytes

var (
	ErrResultUnavailable = errors.New("transaction results unavailable")
	ErrResultLineage     = errors.New("transaction result lineage mismatch")
	ErrResultInput       = errors.New("invalid transaction result input")
)

// ResultBankID identifies a produced bank, not a numeric slot watermark.
type ResultBankID struct {
	Slot      uint64
	Blockhash solana.Hash
}

// SignatureResult contains a first signature and its actual committed outcome.
// Error must come from the typed execution-error encoder; nil means success.
// The cache checks its envelope, not the truth of the supplied execution result.
type SignatureResult struct {
	Signature solana.Signature
	Error     json.RawMessage
}

// ResultBank is published only after replay accepts the complete bank. Complete
// is false when its outcome capture failed; no supplied results are then kept.
type ResultBank struct {
	ID, Parent ResultBankID
	Results    []SignatureResult
	Complete   bool
}

// CachedResult is an owned lookup result. Found=false is an ordinary cache miss.
type CachedResult struct {
	Slot      uint64
	Error     json.RawMessage
	Found     bool
	Finalized bool
}

// ResultView describes one cache mutation generation. It does not certify the
// caller's external canonical publication or replace its recovery guard.
type ResultView struct {
	Tip, Root  ResultBankID
	Generation uint64
	Results    []CachedResult
	lineage    *resultLineage
}

// resultLineage is a non-zero-sized identity retained by outstanding views.
type resultLineage byte

type resultBank struct {
	id, parent ResultBankID
	signatures []solana.Signature
	charge     uint64
	complete   bool
}

type storedResult struct {
	slot uint64
	err  string
}

// ResultCache uses one selected-signature map and per-bank deletion keys,
// independently of the consensus duplicate-message cache. Logical charges are
// not heap/RSS limits. The fixed node scaffold is reserved before outcome maps.
type ResultCache struct {
	mu          sync.RWMutex
	results     map[solana.Signature]storedResult
	banks       [resultBankSlots]resultBank
	count       int
	rooted      int
	base, root  ResultBankID
	generation  uint64
	lineage     *resultLineage
	charged     uint64
	limit       uint64
	unavailable bool
}

// NewResultCache starts cold at an authoritative selected parent. Missing older
// results are ordinary misses; no 300-root startup barrier is imposed.
func NewResultCache(parent ResultBankID) (*ResultCache, error) {
	if parent.Blockhash.IsZero() {
		return nil, ErrResultLineage
	}
	return &ResultCache{base: parent, root: parent, lineage: new(resultLineage), charged: resultScaffold, limit: resultCacheBytes}, nil
}

// Invalidate closes optional result availability until authoritative Reset.
// It preserves retained state and advances the mutation generation.
func (c *ResultCache) Invalidate() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.invalidate(ErrResultUnavailable)
}

// Publish appends one accepted bank. Capture/data refusal appends an incomplete
// identity-only node and returns ErrResultUnavailable, preserving known results.
// A conflicting identity or overflow beyond the active-bank allowance latches unavailable
// until an authoritative Reset; neither is silently treated as a cold restart.
func (c *ResultCache) Publish(bank ResultBank) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.unavailable {
		return ErrResultUnavailable
	}
	if bank.ID.Blockhash.IsZero() || bank.ID.Slot <= bank.Parent.Slot {
		return c.invalidate(ErrResultLineage)
	}
	for i := range c.count {
		old := &c.banks[i]
		if old.id != bank.ID {
			continue
		}
		if old.parent != bank.Parent {
			return c.invalidate(ErrResultLineage)
		}
		if !old.complete {
			return ErrResultUnavailable
		}
		if !bank.Complete || len(bank.Results) != len(old.signatures) {
			return c.invalidate(ErrResultInput)
		}
		seen := make(map[solana.Signature]struct{}, len(bank.Results))
		for _, result := range bank.Results {
			value, ok := c.results[result.Signature]
			if _, duplicate := seen[result.Signature]; duplicate || !ok || value.slot != old.id.Slot || len(result.Error) != len(value.err) || value.err != string(result.Error) {
				return c.invalidate(ErrResultInput)
			}
			seen[result.Signature] = struct{}{}
		}
		return nil
	}
	if bank.Parent != c.tip() {
		return c.invalidate(ErrResultLineage)
	}
	if c.count-c.rooted == resultActiveBanks || c.count == len(c.banks) {
		return c.invalidate(ErrResultUnavailable)
	}
	node := resultBank{id: bank.ID, parent: bank.Parent}
	err := ErrResultUnavailable
	if bank.Complete {
		var charge uint64
		charge, err = resultCharge(bank.Results)
		if err == nil && charge > c.limit-c.charged {
			err = ErrResultUnavailable
		}
		if err == nil {
			seen := make(map[solana.Signature]struct{}, len(bank.Results))
			for _, result := range bank.Results {
				_, existing := c.results[result.Signature]
				if _, duplicate := seen[result.Signature]; duplicate || existing {
					return c.invalidate(ErrResultInput)
				}
				seen[result.Signature] = struct{}{}
			}
			if len(bank.Results) > 0 {
				node.signatures = make([]solana.Signature, len(bank.Results))
				if c.results == nil {
					c.results = make(map[solana.Signature]storedResult, len(bank.Results))
				}
			}
			for i, result := range bank.Results {
				node.signatures[i] = result.Signature
				c.results[result.Signature] = storedResult{slot: bank.ID.Slot, err: string(result.Error)}
			}
			node.complete, node.charge = true, charge
			c.charged += charge
		}
	}
	c.banks[c.count] = node
	c.count++
	c.generation++
	return err
}

// Root marks an exact retained selected bank rooted and prunes older complete
// and incomplete nodes together. Repeating the current root is a no-op.
// An invalid authoritative notification latches this optional cache unavailable.
func (c *ResultCache) Root(id ResultBankID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.unavailable {
		return ErrResultUnavailable
	}
	if id == c.root {
		return nil
	}
	index := c.index(id)
	if id.Slot <= c.root.Slot || index < 0 {
		return c.invalidate(ErrResultLineage)
	}
	c.root, c.rooted = id, index+1
	remove := max(0, c.rooted-resultRoots)
	if remove > 0 {
		c.base = c.banks[remove-1].id
		for i := range remove {
			c.charged -= c.banks[i].charge
			for _, signature := range c.banks[i].signatures {
				delete(c.results, signature)
			}
		}
		copy(c.banks[:], c.banks[remove:c.count])
		clear(c.banks[c.count-remove : c.count])
		c.count -= remove
		c.rooted -= remove
	}
	c.generation++
	return nil
}

// Unwind removes the suffix after an exact retained selected parent, never a
// rooted bank. It drops outcomes and incomplete markers in the same mutation.
// Invalid authoritative notifications require Reset before further lookups.
func (c *ResultCache) Unwind(parent ResultBankID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.unavailable {
		return ErrResultUnavailable
	}
	index := c.index(parent)
	if parent.Slot < c.root.Slot || (index < 0 && parent != c.base) {
		return c.invalidate(ErrResultLineage)
	}
	keep := index + 1
	if keep == c.count {
		return nil
	}
	for i := keep; i < c.count; i++ {
		c.charged -= c.banks[i].charge
		for _, signature := range c.banks[i].signatures {
			delete(c.results, signature)
		}
	}
	clear(c.banks[keep:c.count])
	c.count = keep
	c.lineage = new(resultLineage)
	c.generation++
	return nil
}

// Reset is only for authoritative startup/recovery rebase, not byte pressure.
// The caller must hold its recovery guard while invalidating external views.
// An invalid authoritative parent also latches this optional cache unavailable.
func (c *ResultCache) Reset(parent ResultBankID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if parent.Blockhash.IsZero() {
		return c.invalidate(ErrResultLineage)
	}
	clear(c.banks[:c.count])
	c.results = nil
	c.count, c.rooted, c.base, c.root = 0, 0, parent, parent
	c.charged, c.unavailable = resultScaffold, false
	c.lineage = new(resultLineage)
	c.generation++
	return nil
}

// Lookup returns at most 256 owned results from one cache view. Known-positive
// batches remain available across a gap; a missing position plus any retained
// incomplete node fails the whole batch instead of inventing a clean miss.
// Root is an exact selected identity, and orphan keys are removed during Unwind.
// Therefore slot comparison is sufficient only for the already selected map.
func (c *ResultCache) Lookup(signatures []solana.Signature) (ResultView, error) {
	view, uncertain, err := c.LookupForHistory(signatures)
	if err != nil {
		return ResultView{}, err
	}
	if uncertain {
		return ResultView{}, ErrResultUnavailable
	}
	return view, nil
}

// LookupForHistory returns an owned cache view for canonical history lookup.
// The boolean marks misses made uncertain by retained incomplete banks. Every
// such miss must be resolved from history before the caller returns the batch.
func (c *ResultCache) LookupForHistory(signatures []solana.Signature) (ResultView, bool, error) {
	if len(signatures) > 256 {
		return ResultView{}, false, ErrResultInput
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.unavailable {
		return ResultView{}, false, ErrResultUnavailable
	}
	view := ResultView{Tip: c.tip(), Root: c.root, Generation: c.generation, Results: make([]CachedResult, len(signatures)), lineage: c.lineage}
	gap, missing := false, false
	for i := range c.count {
		gap = gap || !c.banks[i].complete
	}
	for i, signature := range signatures {
		value, ok := c.results[signature]
		if ok {
			view.Results[i] = CachedResult{Slot: value.slot, Found: true, Finalized: value.slot <= c.root.Slot}
			if value.err != "" {
				view.Results[i].Error = json.RawMessage(value.err)
			}
		}
		missing = missing || !ok
	}
	return view, gap && missing, nil
}

// ChangedOnlyForward reports a changed view produced only by accepted appends
// or root advances. Recovery, unwind and invalidation never qualify for retry.
func (c *ResultCache) ChangedOnlyForward(view ResultView) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	tip := c.tip()
	return !c.unavailable && view.lineage != nil && view.lineage == c.lineage &&
		c.generation != view.Generation && (tip.Slot > view.Tip.Slot || c.root.Slot > view.Root.Slot) &&
		(tip.Slot > view.Tip.Slot || tip == view.Tip) &&
		(c.root.Slot > view.Root.Slot || c.root == view.Root)
}

func resultCharge(results []SignatureResult) (uint64, error) {
	if len(results) > resultBankEntries {
		return 0, ErrResultUnavailable
	}
	charge := uint64(len(results)) * resultEntryBytes
	for _, result := range results {
		if len(result.Error) > resultErrorBytes {
			return 0, ErrResultUnavailable
		}
		if len(result.Error) > 0 {
			value := bytes.TrimSpace(result.Error)
			if !json.Valid(value) || len(value) <= 2 || (value[0] != '"' && value[0] != '{') {
				return 0, ErrResultInput
			}
		}
		charge += uint64(len(result.Error))
	}
	return charge, nil
}

func (c *ResultCache) tip() ResultBankID {
	if c.count == 0 {
		return c.base
	}
	return c.banks[c.count-1].id
}

func (c *ResultCache) index(id ResultBankID) int {
	for i := range c.count {
		if c.banks[i].id == id {
			return i
		}
	}
	return -1
}

func (c *ResultCache) invalidate(err error) error {
	c.unavailable = true
	c.lineage = new(resultLineage)
	c.generation++
	return err
}
