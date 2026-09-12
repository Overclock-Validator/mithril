package replay

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/state"
	"github.com/Overclock-Validator/mithril/pkg/txstatus"
	"github.com/gagliardetto/solana-go"
)

// Agave keeps one status-cache root for every recent blockhash. MAX_RECENT_BLOCKHASHES
// is 300; MAX_PROCESSING_AGE (150) is a separate transaction-admission limit.
const maxTransactionStatusRoots = 300

// Agave deliberately stores only a 20-byte slice of a transaction key. The
// blockhash group carries the slice offset so a status-cache snapshot can use
// the same representation without expanding every key back to 32 bytes.
const transactionStatusKeySize = 20

const maxAncestorAlreadyProcessedOccurrences = 16

var transactionStatusSnapshotMagic = [4]byte{'M', 'T', 'S', '2'}

type transactionStatusKey [transactionStatusKeySize]byte

type transactionStatusGroup struct {
	keyIndex uint8
	keys     map[transactionStatusKey]struct{}
}

type transactionStatusDelta map[solana.Hash]*transactionStatusGroup

// transactionStatusNode is immutable after publication. Keeping immutable
// parent-linked deltas lets a producer bank pin its exact parent view even if
// replay subsequently unwinds that branch.
type transactionStatusNode struct {
	slot       uint64
	blockID    solana.Hash
	hasBlockID bool
	parent     *transactionStatusNode
	delta      transactionStatusDelta
}

type visibleTransactionStatusGroup struct {
	keyIndex uint8
	keys     map[transactionStatusKey]uint16
}

// TransactionStatusCache is replay's authoritative, fork-aware
// AlreadyProcessed cache. Replay itself follows one selected branch at a time;
// Unwind removes the abandoned suffix and immutable TransactionStatusViews
// preserve any concurrently closing producer bank's original parent basis.
type TransactionStatusCache struct {
	mu sync.RWMutex

	tip     *transactionStatusNode
	visible map[solana.Hash]*visibleTransactionStatusGroup

	rootedThrough    uint64
	rootedSinceSeed  uint16
	coverageComplete bool
	// coverageFromGenesis proves that a short (<300-root) complete window began
	// from a known-empty genesis cache. Without this bit, completeness requires
	// the full 300 retained roots; a serialized boolean alone is not evidence.
	coverageFromGenesis bool
}

// TransactionStatusView is an immutable view of one bank lineage. It lazily
// flattens only the recent-blockhash groups a producer actually queries, rather
// than copying a million-key snapshot at every leader-bank start.
type TransactionStatusView struct {
	tip           *transactionStatusNode
	complete      bool
	rootedThrough uint64

	mu     sync.Mutex
	groups map[solana.Hash]*transactionStatusGroup
}

// IncompleteTransactionStatusCoverageError fails closed when snapshot-origin
// status roots are missing or stale. Such a cache cannot authorize replay,
// voting, or block production merely because a lookup returned absent.
type IncompleteTransactionStatusCoverageError struct {
	CachedRoot uint64
}

func (e *IncompleteTransactionStatusCoverageError) Error() string {
	return fmt.Sprintf("transaction status cache coverage is incomplete through rooted bank %d; refusing to authorize replay or block production", e.CachedRoot)
}

// AncestorAlreadyProcessedOccurrence identifies a transaction whose message
// was already committed by an ancestor bank.
type AncestorAlreadyProcessedOccurrence struct {
	Index         int
	ProcessedSlot uint64
}

// AncestorAlreadyProcessedTransactionMessagesError is distinct from an
// intra-block duplicate: it means the candidate repeats a committed ancestor
// transaction under Agave's (recent_blockhash, message_hash) lookup.
type AncestorAlreadyProcessedTransactionMessagesError struct {
	Slot                  uint64
	AlreadyProcessedCount uint64
	Occurrences           []AncestorAlreadyProcessedOccurrence
}

// TransactionStatusLineageError prevents status publication or validation
// against a cache view that belongs to a different replay parent.
type TransactionStatusLineageError struct {
	Slot                 uint64
	ParentSlot           uint64
	ExpectedSlot         uint64
	ParentBlockID        solana.Hash
	ExpectedBlockID      solana.Hash
	BlockIDMismatch      bool
	ParentBlockIDMissing bool
}

// TransactionStatusRootedUnwindError rejects attempts to abandon status
// history that is already part of the durable account root.
type TransactionStatusRootedUnwindError struct {
	FromSlot      uint64
	RootedThrough uint64
}

func (e *TransactionStatusRootedUnwindError) Error() string {
	return fmt.Sprintf("cannot unwind transaction statuses from slot %d at or below durable root %d", e.FromSlot, e.RootedThrough)
}

func (e *TransactionStatusLineageError) Error() string {
	if e.ParentBlockIDMissing {
		return fmt.Sprintf("slot %d omits transaction-status parent block id for selected parent %s at slot %d",
			e.Slot, e.ExpectedBlockID, e.ParentSlot)
	}
	if e.BlockIDMismatch {
		return fmt.Sprintf("slot %d transaction-status parent block id %s does not match selected parent %s at slot %d",
			e.Slot, e.ParentBlockID, e.ExpectedBlockID, e.ParentSlot)
	}
	return fmt.Sprintf("slot %d transaction-status parent slot %d does not match selected replay parent slot %d",
		e.Slot, e.ParentSlot, e.ExpectedSlot)
}

func (e *AncestorAlreadyProcessedTransactionMessagesError) Error() string {
	if e == nil {
		return "transaction messages already processed by an ancestor bank"
	}
	var details bytes.Buffer
	for i, occurrence := range e.Occurrences {
		if i > 0 {
			details.WriteString(", ")
		}
		_, _ = fmt.Fprintf(&details, "%d->slot %d", occurrence.Index, occurrence.ProcessedSlot)
	}
	suffix := ""
	if uint64(len(e.Occurrences)) < e.AlreadyProcessedCount {
		suffix = fmt.Sprintf(" (showing first %d)", len(e.Occurrences))
	}
	if details.Len() == 0 {
		return fmt.Sprintf("slot %d contains %d transaction messages already processed by an ancestor bank (AlreadyProcessed)",
			e.Slot, e.AlreadyProcessedCount)
	}
	return fmt.Sprintf("slot %d contains %d transaction messages already processed by an ancestor bank (AlreadyProcessed); indexes (transaction->processed bank): %s%s",
		e.Slot, e.AlreadyProcessedCount, details.String(), suffix)
}

// IsAlreadyProcessedTransactionError reports both invalid forms: duplicate
// messages inside one candidate bank and messages already committed in an
// ancestor bank.
func IsAlreadyProcessedTransactionError(err error) bool {
	var duplicate *DuplicateTransactionMessagesError
	var ancestor *AncestorAlreadyProcessedTransactionMessagesError
	return errors.As(err, &duplicate) || errors.As(err, &ancestor)
}

// NewTransactionStatusCache constructs a known-empty cache (genesis/tests).
// Snapshot-based replay must use one of the seed constructors below and must
// not infer complete coverage from absence.
func NewTransactionStatusCache() *TransactionStatusCache {
	return newTransactionStatusCache(true)
}

func newTransactionStatusCache(complete bool) *TransactionStatusCache {
	return &TransactionStatusCache{
		visible:             make(map[solana.Hash]*visibleTransactionStatusGroup),
		coverageComplete:    complete,
		coverageFromGenesis: complete,
	}
}

// NewTransactionStatusCacheFromSnapshot restores the compact blob attached to
// a rooted resume context. An empty blob deliberately creates an incomplete
// cache instead of pretending that snapshot-origin statuses do not exist.
func NewTransactionStatusCacheFromSnapshot(data []byte) (*TransactionStatusCache, error) {
	cache := newTransactionStatusCache(false)
	if len(data) == 0 {
		return cache, nil
	}
	if err := cache.restore(data); err != nil {
		return nil, err
	}
	return cache, nil
}

// NewTransactionStatusCacheFromAgaveSnapshot builds complete rooted coverage
// from Agave's snapshots/status_cache member. Root deltas are serialized from
// a HashSet and therefore sorted here; non-root fork deltas are deliberately
// excluded from the canonical parent lineage.
func NewTransactionStatusCacheFromAgaveSnapshot(deltas []txstatus.SnapshotSlotDelta, expectedRoot uint64) (*TransactionStatusCache, error) {
	roots := make([]txstatus.SnapshotSlotDelta, 0, len(deltas))
	for _, delta := range deltas {
		if delta.IsRoot {
			roots = append(roots, delta)
		}
	}
	sort.Slice(roots, func(i, j int) bool { return roots[i].Slot < roots[j].Slot })
	if len(roots) == 0 {
		return nil, fmt.Errorf("Agave transaction status snapshot contains no rooted bank deltas")
	}
	for index := 1; index < len(roots); index++ {
		if roots[index].Slot == roots[index-1].Slot {
			return nil, fmt.Errorf("Agave transaction status roots repeat slot %d", roots[index].Slot)
		}
	}
	if roots[len(roots)-1].Slot != expectedRoot {
		return nil, fmt.Errorf("Agave transaction status snapshot latest root %d does not match replay parent %d",
			roots[len(roots)-1].Slot, expectedRoot)
	}
	if len(roots) > maxTransactionStatusRoots {
		return nil, fmt.Errorf("Agave transaction status snapshot has %d rooted bank deltas, expected at most %d",
			len(roots), maxTransactionStatusRoots)
	}
	if len(roots) != maxTransactionStatusRoots && roots[0].Slot != 0 {
		return nil, fmt.Errorf("Agave transaction status snapshot has only %d rooted bank deltas and does not retain genesis root 0; need %d for complete coverage",
			len(roots), maxTransactionStatusRoots)
	}

	cache := NewTransactionStatusCache()
	var parent *transactionStatusNode
	for _, root := range roots {
		delta := make(transactionStatusDelta, len(root.Statuses))
		for statusIndex, status := range root.Statuses {
			if status.KeyIndex > txstatus.MaxCachedKeyIndex {
				return nil, fmt.Errorf("Agave transaction status root %d status %d key index %d exceeds %d",
					root.Slot, statusIndex, status.KeyIndex, txstatus.MaxCachedKeyIndex)
			}
			blockhash := solana.Hash(status.RecentBlockhash)
			group := delta[blockhash]
			if group == nil {
				group = &transactionStatusGroup{
					keyIndex: uint8(status.KeyIndex),
					keys:     make(map[transactionStatusKey]struct{}, len(status.Keys)),
				}
				delta[blockhash] = group
			} else if group.keyIndex != uint8(status.KeyIndex) {
				return nil, fmt.Errorf("Agave transaction status root %d repeats blockhash %s with key indexes %d and %d",
					root.Slot, blockhash, group.keyIndex, status.KeyIndex)
			}
			for _, importedKey := range status.Keys {
				var key transactionStatusKey
				copy(key[:], importedKey[:])
				group.keys[key] = struct{}{}
			}
		}
		if err := cache.addDeltaVisibleLocked(delta); err != nil {
			return nil, fmt.Errorf("Agave transaction status root %d: %w", root.Slot, err)
		}
		parent = &transactionStatusNode{slot: root.Slot, parent: parent, delta: delta}
	}
	cache.tip = parent
	cache.rootedThrough = expectedRoot
	cache.rootedSinceSeed = uint16(len(roots))
	cache.coverageComplete = true
	cache.coverageFromGenesis = roots[0].Slot == 0
	return cache, nil
}

// loadTransactionStatusCacheForReplay restores the status lineage selected by
// the durable account frontier. Before the first fold, that is Agave's raw
// snapshots/status_cache seed. After a fold, the manifest-selected immutable
// sidecar is authoritative; rereading the original seed would silently forget
// every transaction processed since bootstrap.
func loadTransactionStatusCacheForReplay(
	accountsDbPath string,
	expectedRoot uint64,
	snapshotRoot uint64,
	checkpoint *state.TransactionStatusCheckpointRef,
	parentBlockID solana.Hash,
	hasParentBlockID bool,
) (*TransactionStatusCache, error) {
	if checkpoint != nil {
		if err := ValidateTransactionStatusCheckpointRef(checkpoint, expectedRoot); err != nil {
			return nil, fmt.Errorf("transaction status checkpoint reference at durable replay parent %d is invalid: %w", expectedRoot, err)
		}
		data, err := ReadTransactionStatusCheckpoint(accountsDbPath, checkpoint)
		if err != nil {
			return nil, fmt.Errorf("read transaction status checkpoint at durable replay parent %d: %w", expectedRoot, err)
		}
		cache, err := NewTransactionStatusCacheFromSnapshot(data)
		if err != nil {
			return nil, fmt.Errorf("decode transaction status checkpoint at durable replay parent %d: %w", expectedRoot, err)
		}
		if err := validateRestoredTransactionStatusCache(cache, expectedRoot, parentBlockID, hasParentBlockID); err != nil {
			return nil, fmt.Errorf("transaction status checkpoint does not match durable replay parent %d: %w", expectedRoot, err)
		}
		return cache, nil
	}

	if expectedRoot != snapshotRoot {
		return nil, fmt.Errorf("durable replay parent %d is past snapshot root %d but its committed transaction status checkpoint reference is missing; re-bootstrap from a fresh snapshot", expectedRoot, snapshotRoot)
	}

	seedPath := filepath.Join(accountsDbPath, txstatus.SnapshotSeedFileName)
	data, err := os.ReadFile(seedPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && expectedRoot == 0 {
			return NewTransactionStatusCache(), nil
		}
		return nil, fmt.Errorf("load transaction status cache seed %s for replay parent %d: %w (a fresh snapshot or matching durable status checkpoint is required)",
			seedPath, expectedRoot, err)
	}
	deltas, err := txstatus.DecodeAgaveSnapshot(data)
	if err != nil {
		return nil, fmt.Errorf("decode transaction status cache seed %s: %w", seedPath, err)
	}
	cache, err := NewTransactionStatusCacheFromAgaveSnapshot(deltas, expectedRoot)
	if err != nil {
		return nil, fmt.Errorf("transaction status cache seed does not match durable replay parent %d: %w", expectedRoot, err)
	}
	if err := validateRestoredTransactionStatusCache(cache, expectedRoot, parentBlockID, hasParentBlockID); err != nil {
		return nil, fmt.Errorf("Agave transaction status seed does not match durable replay parent %d: %w", expectedRoot, err)
	}
	return cache, nil
}

func validateRestoredTransactionStatusCache(cache *TransactionStatusCache, expectedRoot uint64, parentBlockID solana.Hash, hasParentBlockID bool) error {
	if cache == nil || !cache.CoverageComplete() {
		cachedRoot := uint64(0)
		if cache != nil {
			cachedRoot = cache.RootedThrough()
		}
		return &IncompleteTransactionStatusCoverageError{CachedRoot: cachedRoot}
	}
	if cache.RootedThrough() != expectedRoot {
		return fmt.Errorf("rooted watermark is %d", cache.RootedThrough())
	}
	if tipSlot, ok := cache.TipSlot(); !ok || tipSlot != expectedRoot {
		return fmt.Errorf("selected tip is %d (present=%t)", tipSlot, ok)
	}
	if hasParentBlockID {
		if err := cache.BindTipBlockID(expectedRoot, parentBlockID); err != nil {
			return fmt.Errorf("bind transaction status replay parent identity: %w", err)
		}
	}
	return nil
}

// CoverageComplete reports whether every still-retained root is represented.
func (c *TransactionStatusCache) CoverageComplete() bool {
	if c == nil {
		return false
	}
	c.mu.RLock()
	complete := c.coverageComplete
	c.mu.RUnlock()
	return complete
}

// RootedThrough reports the durable status watermark represented by the cache.
func (c *TransactionStatusCache) RootedThrough() uint64 {
	if c == nil {
		return 0
	}
	c.mu.RLock()
	rooted := c.rootedThrough
	c.mu.RUnlock()
	return rooted
}

// TipSlot reports the currently selected executed parent bank.
func (c *TransactionStatusCache) TipSlot() (uint64, bool) {
	if c == nil {
		return 0, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.tip == nil {
		return 0, false
	}
	return c.tip.slot, true
}

// BindTipBlockID attaches resume-context lineage identity to an Agave seed,
// whose status-cache format contains slots but not Alpenglow block IDs.
func (c *TransactionStatusCache) BindTipBlockID(slot uint64, blockID solana.Hash) error {
	if c == nil {
		return &IncompleteTransactionStatusCoverageError{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tip == nil || c.tip.slot != slot {
		return fmt.Errorf("cannot bind transaction status block id at slot %d: selected tip is not that slot", slot)
	}
	if c.tip.hasBlockID && c.tip.blockID != blockID {
		return fmt.Errorf("transaction status tip at slot %d has block id %s, cannot bind %s", slot, c.tip.blockID, blockID)
	}
	c.tip = &transactionStatusNode{
		slot: slot, blockID: blockID, hasBlockID: true,
		parent: c.tip.parent, delta: c.tip.delta,
	}
	return nil
}

// View pins the selected parent bank's exact status lineage.
func (c *TransactionStatusCache) View() *TransactionStatusView {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	view := &TransactionStatusView{
		tip:           c.tip,
		complete:      c.coverageComplete,
		rootedThrough: c.rootedThrough,
		groups:        make(map[solana.Hash]*transactionStatusGroup),
	}
	c.mu.RUnlock()
	return view
}

// ContainsTransaction implements block production's immutable ancestor lookup.
func (v *TransactionStatusView) ContainsTransaction(tx *solana.Transaction) (bool, error) {
	if v == nil || tx == nil {
		return false, nil
	}
	messageHash, err := TransactionMessageHash(tx)
	if err != nil {
		return false, err
	}
	return v.ContainsMessage(tx.Message.RecentBlockhash, messageHash)
}

// CoverageComplete reports whether this pinned view contains every status root
// needed to authorize transaction admission.
func (v *TransactionStatusView) CoverageComplete() bool {
	return v != nil && v.complete
}

// ContainsMessage avoids hashing a message a second time when block production
// already computed its hash for same-bank duplicate detection.
func (v *TransactionStatusView) ContainsMessage(recentBlockhash solana.Hash, messageHash [32]byte) (bool, error) {
	if v == nil {
		return false, &IncompleteTransactionStatusCoverageError{}
	}
	if !v.complete {
		return false, &IncompleteTransactionStatusCoverageError{CachedRoot: v.rootedThrough}
	}
	group := v.group(recentBlockhash)
	if group == nil {
		return false, nil
	}
	key := sliceTransactionStatusKey(messageHash, group.keyIndex)
	_, ok := group.keys[key]
	return ok, nil
}

func (v *TransactionStatusView) group(blockhash solana.Hash) *transactionStatusGroup {
	v.mu.Lock()
	defer v.mu.Unlock()
	if group, ok := v.groups[blockhash]; ok {
		return group
	}
	group := &transactionStatusGroup{keys: make(map[transactionStatusKey]struct{})}
	found := false
	for node := v.tip; node != nil; node = node.parent {
		deltaGroup := node.delta[blockhash]
		if deltaGroup == nil {
			continue
		}
		if !found {
			group.keyIndex = deltaGroup.keyIndex
			found = true
		}
		for key := range deltaGroup.keys {
			group.keys[key] = struct{}{}
		}
	}
	if !found {
		group = nil
	}
	v.groups[blockhash] = group
	return group
}

// ValidateBlock rejects both duplicate messages in this bank and ancestor
// AlreadyProcessed hits. It is safe to call before any account access.
func (c *TransactionStatusCache) ValidateBlock(block *b.Block) error {
	if block == nil {
		return errors.New("nil block")
	}
	plan, err := planBlockTransactionExecution(block)
	if err != nil {
		return err
	}
	return c.validateBlockWithPlan(block, plan)
}

// validateBlockWithPlan preserves the status-cache checks while letting
// replay reuse the exact immutable identities used for execution planning.
func (c *TransactionStatusCache) validateBlockWithPlan(block *b.Block, plan blockTransactionExecutionPlan) error {
	if block == nil {
		return errors.New("nil block")
	}
	if plan.messageIdentities == nil || !plan.messageIdentities.MatchesBlock(block) {
		return errors.New("prepared transaction message identities do not match block")
	}
	if c == nil {
		return &IncompleteTransactionStatusCoverageError{}
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.coverageComplete {
		return &IncompleteTransactionStatusCoverageError{CachedRoot: c.rootedThrough}
	}
	if err := c.validateParentLocked(block); err != nil {
		return err
	}
	return c.validateAncestorTransactionsLocked(block.Slot, plan.messageIdentities)
}

func (c *TransactionStatusCache) validateAncestorTransactionsLocked(slot uint64, identities *b.PreparedTransactionMessageIdentities) error {
	var already *AncestorAlreadyProcessedTransactionMessagesError
	for index := 0; index < identities.Len(); index++ {
		identity := identities.Identity(index)
		group := c.visible[identity.RecentBlockhash]
		if group == nil {
			continue
		}
		key := sliceTransactionStatusKey(identity.MessageHash, group.keyIndex)
		if group.keys[key] == 0 {
			continue
		}
		if already == nil {
			already = &AncestorAlreadyProcessedTransactionMessagesError{Slot: slot}
		}
		already.AlreadyProcessedCount++
		if len(already.Occurrences) < maxAncestorAlreadyProcessedOccurrences {
			already.Occurrences = append(already.Occurrences, AncestorAlreadyProcessedOccurrence{
				Index:         index,
				ProcessedSlot: c.processedSlotLocked(identity.RecentBlockhash, key),
			})
		}
	}
	if already != nil {
		return already
	}
	return nil
}

// CommitBlock publishes every transaction recorded in a successfully committed
// bank. Recorded instruction failures are processed transactions in Agave and
// therefore belong here too; callers must not invoke this for a rejected or
// noncommitted bank.
func (c *TransactionStatusCache) CommitBlock(block *b.Block) error {
	if c == nil {
		return &IncompleteTransactionStatusCoverageError{}
	}
	if block == nil {
		return errors.New("commit transaction statuses: nil block")
	}
	plan, err := planBlockTransactionExecution(block)
	if err != nil {
		return err
	}
	return c.commitBlockWithPlan(block, plan)
}

// commitBlockWithPlan atomically rechecks the mutable lineage/status state and
// publishes the already-prepared immutable transaction identities.
func (c *TransactionStatusCache) commitBlockWithPlan(block *b.Block, plan blockTransactionExecutionPlan) error {
	if block == nil || plan.messageIdentities == nil || !plan.messageIdentities.MatchesBlock(block) {
		return errors.New("prepared transaction message identities do not match block")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.coverageComplete {
		return &IncompleteTransactionStatusCoverageError{CachedRoot: c.rootedThrough}
	}
	// Parent lineage and ancestor status are mutable, so both remain under the
	// publication lock even when hashing and same-bank deduplication happened
	// earlier. This keeps commit safe across a concurrent branch transition.
	if err := c.validateParentLocked(block); err != nil {
		return err
	}
	if err := c.validateAncestorTransactionsLocked(block.Slot, plan.messageIdentities); err != nil {
		return err
	}

	delta := make(transactionStatusDelta)
	for index := 0; index < plan.messageIdentities.Len(); index++ {
		identity := plan.messageIdentities.Identity(index)
		blockhash := identity.RecentBlockhash
		group := delta[blockhash]
		if group == nil {
			keyIndex := uint8(0)
			if visible := c.visible[blockhash]; visible != nil {
				keyIndex = visible.keyIndex
			}
			group = &transactionStatusGroup{
				keyIndex: keyIndex,
				keys:     make(map[transactionStatusKey]struct{}),
			}
			delta[blockhash] = group
		}
		group.keys[sliceTransactionStatusKey(identity.MessageHash, group.keyIndex)] = struct{}{}
	}

	if err := c.addDeltaVisibleLocked(delta); err != nil {
		return err
	}
	c.tip = &transactionStatusNode{
		slot:       block.Slot,
		blockID:    solana.Hash(block.AlpenglowBlockID),
		hasBlockID: block.HasAlpenglowBlockID,
		parent:     c.tip,
		delta:      delta,
	}
	return nil
}

// Unwind removes the abandoned, unrooted executed suffix. Immutable views held
// by closing producer banks remain pinned to their old nodes.
func (c *TransactionStatusCache) Unwind(fromSlot uint64) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if fromSlot <= c.rootedThrough {
		return &TransactionStatusRootedUnwindError{FromSlot: fromSlot, RootedThrough: c.rootedThrough}
	}
	for c.tip != nil && c.tip.slot >= fromSlot {
		c.removeDeltaVisibleLocked(c.tip.delta)
		c.tip = c.tip.parent
	}
	return nil
}

// Root marks the executed banks through a durable watermark and prunes all but
// MAX_RECENT_BLOCKHASHES rooted banks. It returns true on the transition from
// incomplete snapshot coverage to complete locally reconstructed coverage.
func (c *TransactionStatusCache) Root(through uint64) bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	wasComplete := c.coverageComplete
	newlyRooted := c.countNodesBetweenLocked(c.rootedThrough, through)
	if through > c.rootedThrough {
		c.rootedThrough = through
	}
	// A genesis cache has complete coverage from the outset, but still needs
	// to count its rooted banks. Otherwise its own serialized checkpoint fails
	// the complete-window validation when reopened after the first root.
	rooted := uint32(c.rootedSinceSeed) + uint32(newlyRooted)
	c.rootedSinceSeed = uint16(min(rooted, maxTransactionStatusRoots))
	c.coverageComplete = c.coverageComplete || rooted >= maxTransactionStatusRoots
	c.pruneLocked(through)
	return !wasComplete && c.coverageComplete
}

// SnapshotThrough serializes only the rooted lineage needed at through. It is
// called while constructing a fold job, so the blob rides in that exact durable
// manifest without being copied into every speculative ResumeContext.
func (c *TransactionStatusCache) SnapshotThrough(through uint64) ([]byte, error) {
	if c == nil {
		return nil, nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	nodes := c.nodesThroughLocked(through)
	if len(nodes) > maxTransactionStatusRoots {
		nodes = nodes[len(nodes)-maxTransactionStatusRoots:]
	}
	newlyRooted := c.countNodesBetweenLocked(c.rootedThrough, through)
	rootedSinceSeed := uint32(c.rootedSinceSeed) + uint32(newlyRooted)
	complete := c.coverageComplete || rootedSinceSeed >= maxTransactionStatusRoots
	if rootedSinceSeed > maxTransactionStatusRoots {
		rootedSinceSeed = maxTransactionStatusRoots
	}
	return marshalTransactionStatusNodes(nodes, uint16(rootedSinceSeed), complete, c.coverageFromGenesis)
}

func (c *TransactionStatusCache) processedSlotLocked(blockhash solana.Hash, key transactionStatusKey) uint64 {
	for node := c.tip; node != nil; node = node.parent {
		group := node.delta[blockhash]
		if group == nil {
			continue
		}
		if _, ok := group.keys[key]; ok {
			return node.slot
		}
	}
	return 0
}

func (c *TransactionStatusCache) validateParentLocked(block *b.Block) error {
	if c.tip == nil {
		return nil
	}
	if block.ParentSlot != c.tip.slot {
		return &TransactionStatusLineageError{
			Slot:         block.Slot,
			ParentSlot:   block.ParentSlot,
			ExpectedSlot: c.tip.slot,
		}
	}
	if c.tip.hasBlockID {
		if !block.HasAlpenglowParentBlockID {
			return &TransactionStatusLineageError{
				Slot: block.Slot, ParentSlot: block.ParentSlot, ExpectedSlot: c.tip.slot,
				ExpectedBlockID: c.tip.blockID, ParentBlockIDMissing: true,
			}
		}
		if solana.Hash(block.AlpenglowParentBlockID) != c.tip.blockID {
			return &TransactionStatusLineageError{
				Slot:            block.Slot,
				ParentSlot:      block.ParentSlot,
				ExpectedSlot:    c.tip.slot,
				ParentBlockID:   solana.Hash(block.AlpenglowParentBlockID),
				ExpectedBlockID: c.tip.blockID,
				BlockIDMismatch: true,
			}
		}
	}
	return nil
}

func (c *TransactionStatusCache) addDeltaVisibleLocked(delta transactionStatusDelta) error {
	for blockhash, deltaGroup := range delta {
		if group := c.visible[blockhash]; group != nil && group.keyIndex != deltaGroup.keyIndex {
			return fmt.Errorf("transaction status blockhash %s uses inconsistent key indexes %d and %d",
				blockhash, group.keyIndex, deltaGroup.keyIndex)
		}
	}
	for blockhash, deltaGroup := range delta {
		group := c.visible[blockhash]
		if group == nil {
			group = &visibleTransactionStatusGroup{
				keyIndex: deltaGroup.keyIndex,
				keys:     make(map[transactionStatusKey]uint16),
			}
			c.visible[blockhash] = group
		}
		for key := range deltaGroup.keys {
			group.keys[key]++
		}
	}
	return nil
}

func (c *TransactionStatusCache) removeDeltaVisibleLocked(delta transactionStatusDelta) {
	for blockhash, deltaGroup := range delta {
		group := c.visible[blockhash]
		if group == nil {
			continue
		}
		for key := range deltaGroup.keys {
			if group.keys[key] <= 1 {
				delete(group.keys, key)
			} else {
				group.keys[key]--
			}
		}
		if len(group.keys) == 0 {
			delete(c.visible, blockhash)
		}
	}
}

func (c *TransactionStatusCache) countNodesBetweenLocked(after, through uint64) uint16 {
	var count uint16
	for node := c.tip; node != nil; node = node.parent {
		if node.slot <= after {
			break
		}
		if node.slot <= through && count < maxTransactionStatusRoots {
			count++
		}
	}
	return count
}

func (c *TransactionStatusCache) nodesThroughLocked(through uint64) []*transactionStatusNode {
	var reverse []*transactionStatusNode
	for node := c.tip; node != nil; node = node.parent {
		if node.slot <= through {
			reverse = append(reverse, node)
		}
	}
	nodes := make([]*transactionStatusNode, len(reverse))
	for i := range reverse {
		nodes[len(reverse)-1-i] = reverse[i]
	}
	return nodes
}

func (c *TransactionStatusCache) pruneLocked(through uint64) {
	var reverse []*transactionStatusNode
	for node := c.tip; node != nil; node = node.parent {
		reverse = append(reverse, node)
	}
	if len(reverse) == 0 {
		return
	}
	nodes := make([]*transactionStatusNode, len(reverse))
	for i := range reverse {
		nodes[len(reverse)-1-i] = reverse[i]
	}
	rootedCount := 0
	for _, node := range nodes {
		if node.slot <= through {
			rootedCount++
		}
	}
	drop := rootedCount - maxTransactionStatusRoots
	if drop <= 0 {
		return
	}
	for _, node := range nodes[:drop] {
		c.removeDeltaVisibleLocked(node.delta)
	}
	retained := nodes[drop:]
	var parent *transactionStatusNode
	for _, old := range retained {
		parent = &transactionStatusNode{
			slot: old.slot, blockID: old.blockID, hasBlockID: old.hasBlockID,
			parent: parent, delta: old.delta,
		}
	}
	c.tip = parent
}

func sliceTransactionStatusKey(messageHash [32]byte, keyIndex uint8) transactionStatusKey {
	// Match Agave's saturating_sub(CACHED_KEY_SIZE + 1), including its
	// deliberate exclusion of the final possible starting offset.
	maxIndex := len(messageHash) - transactionStatusKeySize - 1
	index := int(keyIndex)
	if index > maxIndex {
		index = maxIndex
	}
	var key transactionStatusKey
	copy(key[:], messageHash[index:index+transactionStatusKeySize])
	return key
}

func marshalTransactionStatusNodes(nodes []*transactionStatusNode, rootedSinceSeed uint16, complete bool, coverageFromGenesis bool) ([]byte, error) {
	var buf bytes.Buffer
	buf.Write(transactionStatusSnapshotMagic[:])
	flags := byte(0)
	if complete {
		flags = 1
	}
	if coverageFromGenesis {
		flags |= 2
	}
	buf.WriteByte(flags)
	_ = binary.Write(&buf, binary.LittleEndian, rootedSinceSeed)
	_ = binary.Write(&buf, binary.LittleEndian, uint16(len(nodes)))
	for _, node := range nodes {
		_ = binary.Write(&buf, binary.LittleEndian, node.slot)
		nodeFlags := byte(0)
		if node.hasBlockID {
			nodeFlags = 1
		}
		buf.WriteByte(nodeFlags)
		if node.hasBlockID {
			buf.Write(node.blockID[:])
		}
		blockhashes := make([]solana.Hash, 0, len(node.delta))
		for blockhash := range node.delta {
			blockhashes = append(blockhashes, blockhash)
		}
		sort.Slice(blockhashes, func(i, j int) bool {
			return bytes.Compare(blockhashes[i][:], blockhashes[j][:]) < 0
		})
		_ = binary.Write(&buf, binary.LittleEndian, uint32(len(blockhashes)))
		for _, blockhash := range blockhashes {
			group := node.delta[blockhash]
			buf.Write(blockhash[:])
			buf.WriteByte(group.keyIndex)
			keys := make([]transactionStatusKey, 0, len(group.keys))
			for key := range group.keys {
				keys = append(keys, key)
			}
			sort.Slice(keys, func(i, j int) bool {
				return bytes.Compare(keys[i][:], keys[j][:]) < 0
			})
			_ = binary.Write(&buf, binary.LittleEndian, uint32(len(keys)))
			for _, key := range keys {
				buf.Write(key[:])
			}
		}
	}
	return buf.Bytes(), nil
}

func (c *TransactionStatusCache) restore(data []byte) error {
	reader := bytes.NewReader(data)
	var magic [4]byte
	if _, err := io.ReadFull(reader, magic[:]); err != nil {
		return fmt.Errorf("read transaction status snapshot magic: %w", err)
	}
	if magic != transactionStatusSnapshotMagic {
		return fmt.Errorf("invalid transaction status snapshot magic %q", magic)
	}
	flags, err := reader.ReadByte()
	if err != nil {
		return fmt.Errorf("read transaction status snapshot flags: %w", err)
	}
	if flags&^3 != 0 {
		return fmt.Errorf("unsupported transaction status snapshot flags %#x", flags)
	}
	var rootedSinceSeed, nodeCount uint16
	if err := binary.Read(reader, binary.LittleEndian, &rootedSinceSeed); err != nil {
		return fmt.Errorf("read transaction status rooted count: %w", err)
	}
	if err := binary.Read(reader, binary.LittleEndian, &nodeCount); err != nil {
		return fmt.Errorf("read transaction status node count: %w", err)
	}
	if rootedSinceSeed > maxTransactionStatusRoots {
		return fmt.Errorf("transaction status snapshot rooted count %d exceeds max %d", rootedSinceSeed, maxTransactionStatusRoots)
	}
	if nodeCount > maxTransactionStatusRoots {
		return fmt.Errorf("transaction status snapshot has %d rooted banks, max %d", nodeCount, maxTransactionStatusRoots)
	}
	complete := flags&1 != 0
	fromGenesis := flags&2 != 0
	if fromGenesis && !complete {
		return fmt.Errorf("transaction status snapshot claims genesis coverage without complete coverage")
	}
	if complete && nodeCount < maxTransactionStatusRoots && !fromGenesis {
		return fmt.Errorf("transaction status snapshot claims complete coverage with only %d rooted banks and no genesis-origin proof", nodeCount)
	}
	if complete {
		if nodeCount == maxTransactionStatusRoots && rootedSinceSeed != maxTransactionStatusRoots {
			return fmt.Errorf("complete transaction status snapshot has rooted count %d for %d retained banks", rootedSinceSeed, nodeCount)
		}
		if nodeCount < maxTransactionStatusRoots && rootedSinceSeed != nodeCount {
			return fmt.Errorf("genesis-complete transaction status snapshot has rooted count %d for %d retained banks", rootedSinceSeed, nodeCount)
		}
	}
	var parent *transactionStatusNode
	var previousSlot uint64
	for i := uint16(0); i < nodeCount; i++ {
		var slot uint64
		var groupCount uint32
		var blockID solana.Hash
		hasBlockID := false
		if err := binary.Read(reader, binary.LittleEndian, &slot); err != nil {
			return fmt.Errorf("read transaction status bank %d slot: %w", i, err)
		}
		if i > 0 && slot <= previousSlot {
			return fmt.Errorf("transaction status bank slots are not strictly increasing: %d after %d", slot, previousSlot)
		}
		previousSlot = slot
		nodeFlags, err := reader.ReadByte()
		if err != nil {
			return fmt.Errorf("read transaction status bank %d flags: %w", i, err)
		}
		if nodeFlags&^1 != 0 {
			return fmt.Errorf("transaction status bank %d has unsupported flags %#x", i, nodeFlags)
		}
		if nodeFlags&1 != 0 {
			if _, err := io.ReadFull(reader, blockID[:]); err != nil {
				return fmt.Errorf("read transaction status bank %d block id: %w", i, err)
			}
			hasBlockID = true
		}
		if err := binary.Read(reader, binary.LittleEndian, &groupCount); err != nil {
			return fmt.Errorf("read transaction status bank %d group count: %w", i, err)
		}
		if uint64(groupCount) > uint64(reader.Len())/37+1 {
			return fmt.Errorf("transaction status bank %d group count %d exceeds remaining snapshot", i, groupCount)
		}
		delta := make(transactionStatusDelta, groupCount)
		for groupIndex := uint32(0); groupIndex < groupCount; groupIndex++ {
			var blockhash solana.Hash
			if _, err := io.ReadFull(reader, blockhash[:]); err != nil {
				return fmt.Errorf("read transaction status bank %d group %d blockhash: %w", i, groupIndex, err)
			}
			keyIndex, err := reader.ReadByte()
			if err != nil {
				return fmt.Errorf("read transaction status bank %d group %d key index: %w", i, groupIndex, err)
			}
			if int(keyIndex) > 32-transactionStatusKeySize-1 {
				return fmt.Errorf("transaction status bank %d group %d key index %d is invalid", i, groupIndex, keyIndex)
			}
			var keyCount uint32
			if err := binary.Read(reader, binary.LittleEndian, &keyCount); err != nil {
				return fmt.Errorf("read transaction status bank %d group %d key count: %w", i, groupIndex, err)
			}
			if uint64(keyCount)*transactionStatusKeySize > uint64(reader.Len()) {
				return fmt.Errorf("transaction status bank %d group %d key count %d exceeds remaining snapshot", i, groupIndex, keyCount)
			}
			if _, duplicate := delta[blockhash]; duplicate {
				return fmt.Errorf("transaction status bank %d repeats blockhash %s", i, blockhash)
			}
			group := &transactionStatusGroup{keyIndex: keyIndex, keys: make(map[transactionStatusKey]struct{}, keyCount)}
			for keyIndexInGroup := uint32(0); keyIndexInGroup < keyCount; keyIndexInGroup++ {
				var key transactionStatusKey
				if _, err := io.ReadFull(reader, key[:]); err != nil {
					return fmt.Errorf("read transaction status bank %d group %d key %d: %w", i, groupIndex, keyIndexInGroup, err)
				}
				group.keys[key] = struct{}{}
			}
			delta[blockhash] = group
		}
		if err := c.addDeltaVisibleLocked(delta); err != nil {
			return fmt.Errorf("transaction status bank %d: %w", slot, err)
		}
		parent = &transactionStatusNode{slot: slot, blockID: blockID, hasBlockID: hasBlockID, parent: parent, delta: delta}
	}
	if reader.Len() != 0 {
		return fmt.Errorf("transaction status snapshot has %d trailing bytes", reader.Len())
	}
	c.tip = parent
	c.rootedThrough = previousSlot
	c.rootedSinceSeed = rootedSinceSeed
	c.coverageComplete = complete
	c.coverageFromGenesis = fromGenesis
	return nil
}
