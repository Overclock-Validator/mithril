package turbine

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/Overclock-Validator/mithril/pkg/statsd"
	"github.com/gagliardetto/solana-go"
	"github.com/klauspost/reedsolomon"
)

var (
	ErrDuplicateShred               = errors.New("duplicate data shred")
	ErrSlotIncomplete               = errors.New("slot incomplete")
	ErrSlotOverflow                 = errors.New("slot has too many data shreds")
	ErrNonCanonicalAlpenglowBlockID = errors.New("non-canonical alpenglow block id")
)

const (
	maxDataShredsPerSlot         = 64 * 1024
	maxRetainedIncompleteSlotLag = uint64(512)
	// The emission-gating head slot may list this many missing indices per
	// scan — enough to keep its admission SHARE full at any latency (the
	// repair client bounds actual sends to that share; a total head
	// monopoly was tried live and lowered emission throughput). Window
	// slots stay at the caller's per-slot cap. Sized to cover a whole monster
	// slot's missing set (a 65k-tx block is ~15k data shreds): the head is the
	// slot that gates replay, so it must never be listing-capped below what its
	// admission share and the peer set can actually fetch — that was the real
	// bottleneck a 256/1024 cap left in place.
	repairHeadMaxMissing         = 16384
	maxRetainedIncompleteSlotCap = 1024
	maxRetainedCompletedSlotLag  = uint64(512)
	repairObservedSlotLag        = uint64(1)
	repairScanSlotWindow         = uint64(96)
	maxPriorityRepairSlots       = 512
	maxPriorityRepairRange       = uint64(64)
)

type SlotAssembler struct {
	mu                   sync.Mutex
	slots                map[uint64]*slotState
	completedSlots       map[uint64]struct{}
	knownBlockIDs        map[uint64]solana.Hash
	rejectedBlockIDs     map[uint64]map[solana.Hash]struct{}
	protectedKnownIDs    map[uint64]struct{}
	protectedBlockIDs    map[uint64]struct{}
	priorityRepairSlots  map[uint64]struct{}
	priorityRepairOrder  []uint64
	encoders             map[fecLayout]reedsolomon.Encoder
	partialShredObs      map[uint64]PartialShredObservation // shreds seen for slots that never became full (retained for skip observability)
	retentionFloor       uint64                             // when non-zero, slots >= floor are never "too old" (repair catchup holds a window far behind the live edge)
	edgeScanLag          uint64                             // how far behind the shred edge the freshness-repair scan reaches (0 = repairScanSlotWindow)
	maxObservedSlot      uint64
	highestFullSlot      uint64 // monotonic: highest slot reconstructed from shreds ("full", Agave SlotMeta/is_full sense)
	recoveredDataShreds  uint64
	usefulRepairShreds   uint64 // distinct data shreds delivered BY repair (the throughput signal)
	nonCanonicalBlockIDs uint64
	lastNonCanonicalSlot uint64
	lastNonCanonicalGot  solana.Hash
	lastNonCanonicalWant solana.Hash
	evictedSlots         uint64
	ignoredOldShreds     uint64
	// onComplete fires when a slot fully assembles (all data shreds
	// 0..lastIndex held) — the completeness signal the shred spool journals.
	onComplete func(slot uint64, lastIndex uint32, shreds uint32)
	// Configured before ingestion; tests may replace it with a blocking probe.
	// Production uses the process-wide bounded transaction verifier.
	verifyTransactions func(context.Context, *block.Block) error
}

type SlotRepairRequest struct {
	Slot                  uint64
	MissingDataShreds     []uint32
	NeedHighestDataShred  bool
	HighestDataShredIndex uint32
}

// PartialShredObservation records what arrived for a slot that never became
// full — the operator signal distinguishing "leader sent some shreds then
// stopped" from "leader never transmitted" when a slot ends up skipped.
type PartialShredObservation struct {
	DataShreds     int   // distinct data shreds received
	RepairedShreds int   // of those, delivered via repair
	FirstNanos     int64 // wall clock (unix nanos) of the first accepted shred
}

type slotState struct {
	slot        uint64
	parentSlot  uint64
	shreds      map[uint32]*Shred
	fecSets     map[uint32]*fecState
	lastIndex   uint32
	haveLast    bool
	shredVer    uint16
	firstParent bool

	// Observability: when the slot's first shred was accepted, and how many of
	// its shreds arrived via repair rather than turbine.
	firstShredAt   time.Time
	fullAt         time.Time
	repairedShreds int
	// completing makes the immutable full state a single-owner generation token.
	completing bool
	// Assembly failures for this slot (mixed variants/signatures, FEC layout
	// conflicts, ...). A slot frozen below completion while repair responses
	// flow is usually poisoned state — the latest error names the poison.
	errCount int
	lastErr  string
}

func (s *slotState) noteError(err error) {
	s.errCount++
	s.lastErr = err.Error()
}

type slotCompletionWork struct {
	state              *slotState
	queuedAt           time.Time
	observeCollection  bool
	reportNonCanonical bool
	verifyTransactions func(context.Context, *block.Block) error
}

type processedSlotCompletion struct {
	block             *block.Block
	parentInfo        *AlpenglowParentInfo
	roots             []solana.Hash
	err               error
	canceled          bool
	completionReadyAt time.Time
	timings           block.TurbineIngressTimings
}

type slotCompletionResult struct {
	block    *block.Block
	err      error
	hydrated bool
	pending  bool
}

type fecLayout struct {
	dataShreds   uint16
	codingShreds uint16
	shardSize    int
}

type fecState struct {
	slot        uint64
	fecSetIndex uint32
	data        map[uint32]*Shred
	coding      map[uint16]*Shred
	layout      fecLayout
	haveLayout  bool
	signature   [64]byte
	haveSig     bool
	dataVariant byte
	codeVariant byte
}

func NewSlotAssembler() *SlotAssembler {
	return &SlotAssembler{
		slots:               make(map[uint64]*slotState),
		completedSlots:      make(map[uint64]struct{}),
		knownBlockIDs:       make(map[uint64]solana.Hash),
		rejectedBlockIDs:    make(map[uint64]map[solana.Hash]struct{}),
		protectedKnownIDs:   make(map[uint64]struct{}),
		protectedBlockIDs:   make(map[uint64]struct{}),
		priorityRepairSlots: make(map[uint64]struct{}),
		partialShredObs:     make(map[uint64]PartialShredObservation),
		encoders:            make(map[fecLayout]reedsolomon.Encoder),
		verifyTransactions:  validateBlockTransactionsContext,
	}
}

// recordPartialObsLocked snapshots a never-completed slot's shred arrivals
// before its state is dropped (reset or pruned), so skip reporting can still
// say what the leader managed to send.
func (a *SlotAssembler) recordPartialObsLocked(state *slotState) {
	if state == nil || len(state.shreds) == 0 {
		return
	}
	a.partialShredObs[state.slot] = PartialShredObservation{
		DataShreds:     len(state.shreds),
		RepairedShreds: state.repairedShreds,
		FirstNanos:     state.firstShredAt.UnixNano(),
	}
}

// ShredObservation reports what has been seen for a slot that did not (or has
// not yet) become full: live partial state first, then retained observations
// from reset/pruned slots. ok is false when no shred was ever accepted.
func (a *SlotAssembler) ShredObservation(slot uint64) (PartialShredObservation, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if state := a.slots[slot]; state != nil && len(state.shreds) > 0 {
		return PartialShredObservation{
			DataShreds:     len(state.shreds),
			RepairedShreds: state.repairedShreds,
			FirstNanos:     state.firstShredAt.UnixNano(),
		}, true
	}
	obs, ok := a.partialShredObs[slot]
	return obs, ok
}

func (a *SlotAssembler) AddPacket(packet []byte) (*block.Block, error) {
	shred, err := ParseShred(packet)
	if err != nil {
		if errors.Is(err, ErrCodingShredIgnored) {
			return nil, nil
		}
		return nil, err
	}
	return a.AddShred(shred)
}

func (a *SlotAssembler) AddShred(shred *Shred) (*block.Block, error) {
	return a.AddShredFrom(shred, false)
}

// AddShredFrom ingests a shred, recording whether it arrived via repair (for
// per-slot observability) rather than turbine.
func (a *SlotAssembler) AddShredFrom(shred *Shred, fromRepair bool) (*block.Block, error) {
	work, err := a.addShredFrom(shred, fromRepair)
	if err != nil || work == nil {
		return nil, err
	}
	processed := a.processCompletion(context.Background(), work)
	return a.finalizeCompletion(work, processed)
}

// addShredFrom owns only mutable shred/FEC state. Once a slot first becomes
// reconstructable it returns a single immutable completion token; decoding,
// parsing, and signature verification must happen after this method unlocks.
func (a *SlotAssembler) addShredFrom(shred *Shred, fromRepair bool) (*slotCompletionWork, error) {
	if shred == nil {
		return nil, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if shred.Slot > a.maxObservedSlot {
		a.maxObservedSlot = shred.Slot
	}
	a.pruneOldSlotsLocked()
	if a.slotTooOldLocked(shred.Slot) {
		a.ignoredOldShreds++
		return nil, nil
	}
	if _, completed := a.completedSlots[shred.Slot]; completed {
		a.ignoredOldShreds++
		return nil, nil
	}

	state := a.slotState(shred.Slot, shred.Version)
	if state.completing {
		a.ignoredOldShreds++
		return nil, nil
	}
	var err error
	switch shred.Type {
	case ShredTypeData:
		err = state.addDataShred(shred)
		// Count repair deliveries only for DISTINCT data shreds (after the
		// duplicate check) so "repaired" can never exceed the distinct-shred
		// count it is reported alongside. The global tally drives the
		// useful-distinct-shreds/sec rate — the true throughput signal
		// (raw req/s says nothing about whether requests fetch new data).
		if err == nil && fromRepair {
			state.repairedShreds++
			a.usefulRepairShreds++
		}
	case ShredTypeCode:
		err = state.addCodingShred(shred)
	default:
		return nil, nil
	}
	if err != nil {
		if errors.Is(err, ErrDuplicateShred) {
			return nil, nil
		}
		state.noteError(err)
		return nil, err
	}
	if state.firstShredAt.IsZero() {
		state.firstShredAt = time.Now()
	}

	recovered, err := a.recoverFEC(state, shred.FECSetIndex)
	if err != nil {
		state.noteError(err)
		return nil, err
	}
	for _, recoveredShred := range recovered {
		err := state.addDataShred(recoveredShred)
		if err != nil && !errors.Is(err, ErrDuplicateShred) {
			state.noteError(err)
			return nil, err
		}
		if err == nil {
			a.recoveredDataShreds++
		}
	}

	if !state.complete() {
		return nil, nil
	}
	return a.claimCompletionLocked(state, false), nil
}

func (a *SlotAssembler) claimCompletionLocked(state *slotState, reportNonCanonical bool) *slotCompletionWork {
	if state == nil || state.completing || !state.complete() {
		return nil
	}
	now := time.Now()
	observeCollection := state.fullAt.IsZero()
	if observeCollection {
		state.fullAt = now
		if state.slot > a.highestFullSlot {
			a.highestFullSlot = state.slot
		}
	}
	state.completing = true
	verifyTransactions := a.verifyTransactions
	if verifyTransactions == nil {
		verifyTransactions = validateBlockTransactionsContext
	}
	return &slotCompletionWork{
		state:              state,
		queuedAt:           now,
		observeCollection:  observeCollection,
		reportNonCanonical: reportNonCanonical,
		verifyTransactions: verifyTransactions,
	}
}

// abortCompletion releases a token that could not be handed to a completion
// worker (normally only receiver shutdown). Pointer identity prevents an old
// token from changing replacement state installed by ResetSlot.
func (a *SlotAssembler) abortCompletion(work *slotCompletionWork) {
	if work == nil || work.state == nil {
		return
	}
	a.mu.Lock()
	if a.slots[work.state.slot] == work.state && work.state.completing {
		work.state.completing = false
	}
	a.mu.Unlock()
}

func (a *SlotAssembler) processCompletion(ctx context.Context, work *slotCompletionWork) processedSlotCompletion {
	if work == nil || work.state == nil {
		return processedSlotCompletion{}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if ctx.Err() != nil {
		return processedSlotCompletion{canceled: true}
	}
	startedAt := time.Now()
	timings := block.TurbineIngressTimings{CompletionQueueDelay: startedAt.Sub(work.queuedAt)}
	_ = statsd.Duration(statsd.TurbineBlockCompletionQueueDelay, timings.CompletionQueueDelay, nil)
	if !work.state.firstShredAt.IsZero() && !work.state.fullAt.IsZero() {
		timings.ShredCollection = work.state.fullAt.Sub(work.state.firstShredAt)
		if work.observeCollection {
			_ = statsd.Duration(statsd.TurbineShredCollection, timings.ShredCollection, nil)
		}
	}

	decodeStartedAt := time.Now()
	var decodeTimings entryDecodeTimings
	blk, parentInfo, roots, err := work.state.decodeBlock(&decodeTimings)
	decodeTotal := time.Since(decodeStartedAt)
	decodeOnly := decodeTotal - decodeTimings.transactionParse
	if decodeOnly < 0 {
		decodeOnly = 0
	}
	timings.BlockDecode = decodeOnly
	timings.TransactionParse = decodeTimings.transactionParse
	_ = statsd.Duration(statsd.TurbineBlockDecode, timings.BlockDecode, nil)
	_ = statsd.Duration(statsd.TurbineTransactionParse, timings.TransactionParse, nil)
	processed := processedSlotCompletion{block: blk, parentInfo: parentInfo, roots: roots, err: err, timings: timings}
	if err != nil {
		return processed
	}
	if ctx.Err() != nil {
		processed.canceled = true
		return processed
	}

	sigverifyStartedAt := time.Now()
	processed.err = work.verifyTransactions(ctx, blk)
	processed.timings.TransactionSigverify = time.Since(sigverifyStartedAt)
	_ = statsd.Duration(statsd.TurbineTransactionSigverify, processed.timings.TransactionSigverify, nil)
	if ctx.Err() != nil {
		processed.canceled = true
		processed.err = nil
		return processed
	}
	if processed.err == nil {
		blk.MarkTransactionSignaturesVerified()
		processed.completionReadyAt = time.Now()
	}
	return processed
}

func (a *SlotAssembler) finalizeCompletion(work *slotCompletionWork, processed processedSlotCompletion) (*block.Block, error) {
	if work == nil || work.state == nil {
		return nil, processed.err
	}
	state := work.state
	a.mu.Lock()
	if a.slots[state.slot] != state || !state.completing {
		a.mu.Unlock()
		return nil, nil
	}
	if processed.err != nil {
		// Retain a deterministic decode/verification failure on the live full
		// state so catchup diagnostics report poison instead of a missing slot.
		state.noteError(processed.err)
		state.completing = false
		a.mu.Unlock()
		return nil, processed.err
	}

	blk := processed.block
	a.attachAlpenglowIdentityLocked(blk, state, processed.parentInfo, processed.roots)
	if !a.acceptAlpenglowBlockIDLocked(blk) {
		a.trackNonCanonicalBlockIDLocked(blk)
		a.recordPartialObsLocked(state)
		delete(a.slots, state.slot)
		a.mu.Unlock()
		if work.reportNonCanonical {
			return nil, ErrNonCanonicalAlpenglowBlockID
		}
		return nil, nil
	}

	delete(a.slots, state.slot)
	a.completedSlots[state.slot] = struct{}{}
	a.trackBlockIDLocked(blk)
	if !state.firstShredAt.IsZero() {
		blk.ShredFirstNanos = state.firstShredAt.UnixNano()
	}
	if !state.fullAt.IsZero() {
		blk.ShredFullNanos = state.fullAt.UnixNano()
	}
	blk.RepairedShreds = state.repairedShreds
	onComplete := a.onComplete
	lastIndex := state.lastIndex
	a.mu.Unlock()

	// The spool callback can take its own lock and perform journal I/O. Keep it
	// outside the global assembler mutex, but finish it before block delivery.
	if onComplete != nil {
		onComplete(state.slot, lastIndex, lastIndex+1)
	}
	blk.MarkTurbineIngressTimings(processed.timings)
	admissionStart := processed.completionReadyAt
	if admissionStart.IsZero() {
		admissionStart = time.Now()
	}
	blk.MarkTurbineReplayAdmissionStart(admissionStart)
	return blk, nil
}

func (a *SlotAssembler) attachAlpenglowIdentityLocked(blk *block.Block, state *slotState, parentInfo *AlpenglowParentInfo, roots []solana.Hash) {
	if blk == nil || state == nil {
		return
	}
	parentSlot := state.parentSlot
	parentBlockID, parentKnown := a.knownBlockIDs[parentSlot]
	if parentInfo != nil {
		parentSlot = parentInfo.ParentSlot
		parentBlockID = parentInfo.ParentBlockID
		parentKnown = true
	}
	blk.SourceParentSlot = parentSlot
	// Preserve an explicit slot-0 header's zero ID: genesis's certificate names
	// (0, 0). Replay still validates it against its verified bootstrap state.
	// Ordinary zero parent IDs retain their existing absent/invalid behavior.
	if (parentInfo != nil && parentSlot == 0) || (parentKnown && parentBlockID != (solana.Hash{})) {
		blk.AlpenglowParentBlockID = parentBlockID
		blk.HasAlpenglowParentBlockID = true
	}
	if parentKnown && len(roots) > 0 {
		blk.AlpenglowBlockID = DoubleMerkleBlockID(parentSlot, parentBlockID, roots)
		blk.HasAlpenglowBlockID = true
		blk.AlpenglowLastChainedRoot = roots[len(roots)-1]
		blk.HasAlpenglowLastChainedRoot = true
	}
}

// ShredEdges reports the monotonic shred frontier: the highest slot any
// accepted shred has been seen for, and the highest slot that became full
// (reconstructable). Both only ever advance.
func (a *SlotAssembler) ShredEdges() (latestShredSlot, highestFullSlot uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.maxObservedSlot, a.highestFullSlot
}

func (a *SlotAssembler) SetKnownAlpenglowBlockID(slot uint64, blockID solana.Hash) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, rejected := a.rejectedBlockIDs[slot][blockID]; rejected {
		return
	}
	a.knownBlockIDs[slot] = blockID
}

// RejectAlpenglowBlockID permanently rejects one objectively invalid identity
// for the assembler's bounded retention window. If ordinary assembly had
// learned that identity as the slot hint, unpin it so a different sibling can
// assemble; a distinct certificate-derived hint remains intact.
func (a *SlotAssembler) RejectAlpenglowBlockID(slot uint64, blockID solana.Hash) {
	if slot == 0 || blockID == (solana.Hash{}) {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.rejectedBlockIDs == nil {
		a.rejectedBlockIDs = make(map[uint64]map[solana.Hash]struct{})
	}
	ids := a.rejectedBlockIDs[slot]
	if ids == nil {
		ids = make(map[solana.Hash]struct{})
		a.rejectedBlockIDs[slot] = ids
	}
	ids[blockID] = struct{}{}
	if a.knownBlockIDs[slot] == blockID {
		delete(a.knownBlockIDs, slot)
	}
}

func (a *SlotAssembler) ResetSlot(slot uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.recordPartialObsLocked(a.slots[slot])
	delete(a.slots, slot)
	delete(a.completedSlots, slot)
}

func (a *SlotAssembler) PrioritizeRepairSlot(slot uint64) {
	a.PrioritizeRepairRange(slot, slot)
}

func (a *SlotAssembler) PrioritizeRepairRange(start, end uint64) {
	if start == 0 {
		return
	}
	if end < start {
		end = start
	}
	if end-start > maxPriorityRepairRange {
		end = start + maxPriorityRepairRange
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	for slot := start; ; slot++ {
		if _, completed := a.completedSlots[slot]; !completed {
			if _, exists := a.priorityRepairSlots[slot]; !exists {
				a.priorityRepairSlots[slot] = struct{}{}
				a.priorityRepairOrder = append(a.priorityRepairOrder, slot)
			}
		}
		if slot == end {
			break
		}
	}
	a.prunePriorityRepairSlotsLocked()
}

func (a *SlotAssembler) slotState(slot uint64, version uint16) *slotState {
	state := a.slots[slot]
	if state != nil {
		return state
	}
	state = &slotState{
		slot:      slot,
		shreds:    make(map[uint32]*Shred),
		fecSets:   make(map[uint32]*fecState),
		shredVer:  version,
		lastIndex: ^uint32(0),
	}
	a.slots[slot] = state
	return state
}

// SetOnComplete registers the slot-completion hook (spool journaling).
// Must be set before ingestion starts.
func (a *SlotAssembler) SetOnComplete(fn func(slot uint64, lastIndex uint32, shreds uint32)) {
	a.onComplete = fn
}

// SetEdgeRepairLag bounds the freshness-repair scan to slots within lag of
// the shred edge. The spool's RAM policy only assembles the last
// spoolLiveAssemblyLag slots in memory — scanning further back would emit
// repair requests whose responses cannot assemble (they spool to disk and
// the request repeats forever).
func (a *SlotAssembler) SetEdgeRepairLag(lag uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.edgeScanLag = lag
}

// SetRetentionFloor pins the assembler's age cutoff: while floor is non-zero,
// slots >= floor are accepted even when they trail the live edge by more than
// the normal lag window. Repair catchup uses this so shreds for a gap far
// behind the edge are not discarded as "too old". Advance the floor as replay
// progresses (releasing state behind it) and clear it (0) when caught up. The
// absolute incomplete-slot cap still applies as the memory backstop, but its
// eviction policy hard-protects the floor itself and repair-priority slots.
func (a *SlotAssembler) SetRetentionFloor(slot uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.retentionFloor = slot
}

func (a *SlotAssembler) slotTooOldLocked(slot uint64) bool {
	if a.retentionFloor > 0 && slot >= a.retentionFloor {
		return false
	}
	if a.maxObservedSlot <= maxRetainedIncompleteSlotLag {
		return false
	}
	return slot < a.maxObservedSlot-maxRetainedIncompleteSlotLag
}

func (a *SlotAssembler) pruneOldSlotsLocked() {
	if len(a.slots) > 0 && a.maxObservedSlot > maxRetainedIncompleteSlotLag {
		minSlot := a.maxObservedSlot - maxRetainedIncompleteSlotLag
		if a.retentionFloor > 0 && a.retentionFloor < minSlot {
			minSlot = a.retentionFloor // repair catchup: keep the whole window behind the edge
		}
		for slot, state := range a.slots {
			if slot < minSlot && !state.completing {
				a.recordPartialObsLocked(state)
				delete(a.slots, slot)
				a.evictedSlots++
			}
		}
	}
	if a.maxObservedSlot > maxRetainedCompletedSlotLag {
		minSlot := a.maxObservedSlot - maxRetainedCompletedSlotLag
		if a.retentionFloor > 0 && a.retentionFloor < minSlot {
			minSlot = a.retentionFloor // keep completed markers + block-id hints for the catchup window
		}
		for slot := range a.completedSlots {
			if slot < minSlot {
				delete(a.completedSlots, slot)
			}
		}
		clear(a.protectedKnownIDs)
		clear(a.protectedBlockIDs)
		for _, state := range a.slots {
			if !state.completing {
				continue
			}
			a.protectedKnownIDs[state.slot] = struct{}{}
			a.protectedBlockIDs[state.slot] = struct{}{}
			a.protectedKnownIDs[state.parentSlot] = struct{}{}
		}
		for slot := range a.knownBlockIDs {
			if _, protected := a.protectedKnownIDs[slot]; slot < minSlot && !protected {
				delete(a.knownBlockIDs, slot)
			}
		}
		for slot := range a.rejectedBlockIDs {
			if _, protected := a.protectedBlockIDs[slot]; slot < minSlot && !protected {
				delete(a.rejectedBlockIDs, slot)
			}
		}
		for slot := range a.partialShredObs {
			if slot < minSlot {
				delete(a.partialShredObs, slot)
			}
		}
	}
	a.prunePriorityRepairSlotsLocked()

	if len(a.slots) == 0 {
		return
	}
	for len(a.slots) > maxRetainedIncompleteSlotCap {
		victim, ok := a.capEvictionCandidateLocked()
		if !ok {
			return
		}
		a.recordPartialObsLocked(a.slots[victim])
		delete(a.slots, victim)
		a.evictedSlots++
	}
}

// capEvictionCandidateLocked chooses state furthest ahead of replay, rather
// than the numerically oldest state. During a large repair catchup the oldest
// slot is the emission-gating head; evicting it on every repair response makes
// completion impossible once live ingress fills the cap. Prefer non-priority
// future state, preserve every explicit repair pin, and never evict the exact
// retention floor. The priority set is bounded below the assembler cap, so the
// fallback should only be needed if an invariant changes in the future.
func (a *SlotAssembler) capEvictionCandidateLocked() (uint64, bool) {
	var highest uint64
	found := false
	for slot := range a.slots {
		if a.slots[slot].completing {
			continue
		}
		if a.retentionFloor > 0 && slot == a.retentionFloor {
			continue
		}
		if _, priority := a.priorityRepairSlots[slot]; priority {
			continue
		}
		if !found || slot > highest {
			highest = slot
			found = true
		}
	}
	if found {
		return highest, true
	}

	for slot := range a.slots {
		if a.slots[slot].completing {
			continue
		}
		if a.retentionFloor > 0 && slot == a.retentionFloor {
			continue
		}
		if !found || slot > highest {
			highest = slot
			found = true
		}
	}
	return highest, found
}

func (a *SlotAssembler) prunePriorityRepairSlotsLocked() {
	if len(a.priorityRepairOrder) == 0 {
		return
	}

	minSlot := uint64(0)
	if a.maxObservedSlot > maxRetainedIncompleteSlotLag {
		minSlot = a.maxObservedSlot - maxRetainedIncompleteSlotLag
	}
	if a.retentionFloor > 0 && a.retentionFloor < minSlot {
		minSlot = a.retentionFloor // repair catchup: keep priority slots across the whole window
	}

	filtered := a.priorityRepairOrder[:0]
	for _, slot := range a.priorityRepairOrder {
		if _, exists := a.priorityRepairSlots[slot]; !exists {
			continue
		}
		if slot < minSlot {
			delete(a.priorityRepairSlots, slot)
			continue
		}
		if _, completed := a.completedSlots[slot]; completed {
			delete(a.priorityRepairSlots, slot)
			continue
		}
		filtered = append(filtered, slot)
	}
	a.priorityRepairOrder = filtered

	for len(a.priorityRepairOrder) > maxPriorityRepairSlots {
		slot := a.priorityRepairOrder[0]
		a.priorityRepairOrder = a.priorityRepairOrder[1:]
		delete(a.priorityRepairSlots, slot)
	}
}

func (s *slotState) addDataShred(shred *Shred) error {
	if _, exists := s.shreds[shred.Index]; exists {
		return ErrDuplicateShred
	}
	if shred.Index >= maxDataShredsPerSlot {
		return fmt.Errorf("%w: slot %d shred index %d", ErrSlotOverflow, shred.Slot, shred.Index)
	}
	if s.shredVer != shred.Version {
		return fmt.Errorf("mixed shred versions for slot %d: %d and %d", shred.Slot, s.shredVer, shred.Version)
	}
	if !s.firstParent {
		s.parentSlot = shred.ParentSlot()
		s.firstParent = true
	} else if s.parentSlot != shred.ParentSlot() {
		return fmt.Errorf("mixed parent slots for slot %d: %d and %d", shred.Slot, s.parentSlot, shred.ParentSlot())
	}
	if err := s.addDataToFEC(shred); err != nil {
		return err
	}
	s.shreds[shred.Index] = shred
	if shred.LastInSlot() {
		s.haveLast = true
		s.lastIndex = shred.Index
	}
	return nil
}

func (s *slotState) repairRequest(maxMissing int) (SlotRepairRequest, bool) {
	req := SlotRepairRequest{Slot: s.slot}

	var maxObserved uint32
	haveData := false
	for index := range s.shreds {
		if !haveData || index > maxObserved {
			maxObserved = index
			haveData = true
		}
	}

	req.NeedHighestDataShred = !s.haveLast
	if haveData && maxObserved < maxDataShredsPerSlot-1 {
		req.HighestDataShredIndex = maxObserved + 1
	}

	req.MissingDataShreds = s.missingDataForRepair(maxObserved, maxMissing)

	if len(req.MissingDataShreds) == 0 && !req.NeedHighestDataShred {
		return SlotRepairRequest{}, false
	}
	return req, true
}

// codedSpan is one FEC set with a KNOWN layout (at least one verified coding
// shred held): its data-index span, how many more shreds it needs before
// Reed-Solomon recovery fires, and which of its data indices are missing.
type codedSpan struct {
	start, end uint32 // data index span [start, end)
	deficit    int    // shreds still needed to cross the recovery threshold
	missing    []uint32
}

// requestsToUnlock is how many repair round trips fill the whole span: the
// deficit normally (recovery supplies the rest), or everything missing when
// the set already held enough to recover and errored anyway — direct fetch
// is the bypass around poisoned coding state.
func (span codedSpan) requestsToUnlock() int {
	if span.deficit > 0 && span.deficit <= len(span.missing) {
		return span.deficit
	}
	return len(span.missing)
}

// missingDataForRepair selects WHICH missing data indices are worth a repair
// round trip. The repair protocol serves data shreds only — coding arrives
// exclusively via live broadcast — so the only recovery leverage available
// is the coding already held: a set missing 12 data shreds while holding 8
// coding shreds needs just 4 repairs (any 4) before recovery yields the
// rest; a plain low-to-high walk would request all 12 and waste two thirds
// of the budget. Selection:
//
//   - a set with a known layout contributes only requestsToUnlock indices
//     (lowest missing first, deterministic across scans);
//   - sets go cheapest-unlock-first, so a truncated budget crosses recovery
//     thresholds instead of spreading beneath them;
//   - indices under no known layout (zero coding held: zero leverage) are
//     requested in full, ascending — the old linear behavior, which is also
//     the cold pre-join regime where every data shred must be fetched.
//
// A known layout also proves data extends through the span's end, so the
// scan reaches past the highest RECEIVED index when a partially-heard set
// promises more — without that, the tail waits on a HighestWindowIndex
// round trip to be discovered.
func (s *slotState) missingDataForRepair(maxObserved uint32, maxMissing int) []uint32 {
	spans := make([]codedSpan, 0, len(s.fecSets))
	for _, fec := range s.fecSets {
		if !fec.haveLayout || fec.layout.dataShreds == 0 {
			continue
		}
		spans = append(spans, codedSpan{
			start:   fec.fecSetIndex,
			end:     fec.fecSetIndex + uint32(fec.layout.dataShreds),
			deficit: int(fec.layout.dataShreds) - len(fec.data) - len(fec.coding),
		})
	}
	sort.Slice(spans, func(i, j int) bool { return spans[i].start < spans[j].start })

	missingThrough := maxObserved
	if s.haveLast {
		missingThrough = s.lastIndex
	} else {
		for _, span := range spans {
			if span.end-1 > missingThrough {
				missingThrough = span.end - 1
			}
		}
	}
	if missingThrough > maxDataShredsPerSlot-1 {
		missingThrough = maxDataShredsPerSlot - 1
	}

	var uncovered []uint32
	spanIdx := 0
	for index := uint32(0); index <= missingThrough; index++ {
		if s.shreds[index] == nil {
			for spanIdx < len(spans) && spans[spanIdx].end <= index {
				spanIdx++
			}
			if spanIdx < len(spans) && spans[spanIdx].start <= index {
				spans[spanIdx].missing = append(spans[spanIdx].missing, index)
			} else {
				uncovered = append(uncovered, index)
			}
		}
		if index == maxDataShredsPerSlot-1 {
			break
		}
	}

	order := make([]int, 0, len(spans))
	for i := range spans {
		if len(spans[i].missing) > 0 {
			order = append(order, i)
		}
	}
	sort.Slice(order, func(a, b int) bool {
		na, nb := spans[order[a]].requestsToUnlock(), spans[order[b]].requestsToUnlock()
		if na != nb {
			return na < nb
		}
		return spans[order[a]].start < spans[order[b]].start
	})

	missing := make([]uint32, 0, min(maxMissing, 64))
	for _, i := range order {
		span := spans[i]
		for _, index := range span.missing[:span.requestsToUnlock()] {
			if len(missing) >= maxMissing {
				return missing
			}
			missing = append(missing, index)
		}
	}
	for _, index := range uncovered {
		if len(missing) >= maxMissing {
			return missing
		}
		missing = append(missing, index)
	}
	return missing
}

func (s *slotState) addCodingShred(shred *Shred) error {
	if shred.NumDataShreds == 0 || shred.NumCodingShreds == 0 {
		return fmt.Errorf("invalid coding shred FEC layout for slot %d fec_set=%d: data=%d coding=%d", shred.Slot, shred.FECSetIndex, shred.NumDataShreds, shred.NumCodingShreds)
	}
	if shred.Position >= shred.NumCodingShreds {
		return fmt.Errorf("invalid coding shred position for slot %d fec_set=%d: position=%d coding=%d", shred.Slot, shred.FECSetIndex, shred.Position, shred.NumCodingShreds)
	}
	if s.shredVer != shred.Version {
		return fmt.Errorf("mixed shred versions for slot %d: %d and %d", shred.Slot, s.shredVer, shred.Version)
	}
	fec := s.fecSet(shred.FECSetIndex)
	if _, exists := fec.coding[shred.Position]; exists {
		return ErrDuplicateShred
	}
	if err := fec.acceptShred(shred); err != nil {
		return err
	}
	layout, err := shred.fecLayout()
	if err != nil {
		return err
	}
	if err := fec.setLayout(layout); err != nil {
		return err
	}
	fec.coding[shred.Position] = shred
	return nil
}

func (a *SlotAssembler) CompleteSlot(slot uint64) (*block.Block, error) {
	a.mu.Lock()
	state := a.slots[slot]
	work := a.claimCompletionLocked(state, true)
	a.mu.Unlock()
	if work == nil {
		return nil, ErrSlotIncomplete
	}
	processed := a.processCompletion(context.Background(), work)
	return a.finalizeCompletion(work, processed)
}

func (a *SlotAssembler) acceptAlpenglowBlockIDLocked(blk *block.Block) bool {
	if blk == nil || !blk.HasAlpenglowBlockID {
		return true
	}
	blockID := solana.Hash(blk.AlpenglowBlockID)
	if _, rejected := a.rejectedBlockIDs[blk.Slot][blockID]; rejected {
		return false
	}
	known, ok := a.knownBlockIDs[blk.Slot]
	if !ok || known == (solana.Hash{}) {
		return true
	}
	return blockID == known
}

func (a *SlotAssembler) trackBlockIDLocked(blk *block.Block) {
	if blk == nil || !blk.HasAlpenglowBlockID {
		return
	}
	blockID := solana.Hash(blk.AlpenglowBlockID)
	if _, rejected := a.rejectedBlockIDs[blk.Slot][blockID]; rejected {
		return
	}
	if known, ok := a.knownBlockIDs[blk.Slot]; ok && known != (solana.Hash{}) && known != blockID {
		return
	}
	a.knownBlockIDs[blk.Slot] = blockID
}

func (a *SlotAssembler) trackNonCanonicalBlockIDLocked(blk *block.Block) {
	if blk == nil || !blk.HasAlpenglowBlockID {
		return
	}
	want, ok := a.knownBlockIDs[blk.Slot]
	if !ok || want == (solana.Hash{}) {
		return
	}
	a.nonCanonicalBlockIDs++
	a.lastNonCanonicalSlot = blk.Slot
	a.lastNonCanonicalGot = solana.Hash(blk.AlpenglowBlockID)
	a.lastNonCanonicalWant = want
}

func (a *SlotAssembler) ActiveSlots() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.slots)
}

func (a *SlotAssembler) RecoveredDataShreds() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.recoveredDataShreds
}

// UsefulRepairShreds is the cumulative count of distinct data shreds
// delivered by repair — the numerator of useful-shreds/sec.
func (a *SlotAssembler) UsefulRepairShreds() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.usefulRepairShreds
}

func (a *SlotAssembler) EvictedSlots() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.evictedSlots
}

func (a *SlotAssembler) IgnoredOldShreds() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.ignoredOldShreds
}

// HeadShredDetail is the completion picture for one incomplete slot: what is
// held, where the contiguous prefix ends, whether the block-end shred was
// seen, and the flags of the highest-index held shred. Both observed
// stuck-head incidents held ~the whole block with haveLast=false — this
// detail distinguishes "missing tail" from "end flag never registered".
type HeadShredDetail struct {
	DataShreds        int
	MaxIndex          uint32
	ContiguousThrough int64 // highest i with 0..i all held; -1 when index 0 missing
	HaveLast          bool
	LastIndex         uint32 // meaningful when HaveLast
	TopFlags          byte   // flags byte of the highest-index held shred
	TopRecovered      bool   // that shred came from FEC recovery
}

// HeadShredDetail reports the completion picture for a slot still being
// assembled; ok is false when no live state exists.
func (a *SlotAssembler) HeadShredDetail(slot uint64) (HeadShredDetail, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	state := a.slots[slot]
	if state == nil || len(state.shreds) == 0 {
		return HeadShredDetail{}, false
	}
	d := HeadShredDetail{DataShreds: len(state.shreds), HaveLast: state.haveLast, LastIndex: state.lastIndex}
	first := true
	for idx, sh := range state.shreds {
		if first || idx > d.MaxIndex {
			d.MaxIndex = idx
			d.TopFlags = sh.Flags
			d.TopRecovered = sh.Recovered
			first = false
		}
	}
	d.ContiguousThrough = -1
	for idx := uint32(0); state.shreds[idx] != nil; idx++ {
		d.ContiguousThrough = int64(idx)
	}
	return d, true
}

// SlotCompleted reports whether the completed-slot marker is set. A marked
// slot ignores all further shreds AND is skipped by repair-request
// generation — correct after a block was truly emitted, catastrophic if the
// marker is stale while the emitter still waits for the slot.
func (a *SlotAssembler) SlotCompleted(slot uint64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.completedSlots[slot]
	return ok
}

// SlotAssemblyErrors reports the failure count and latest failure text for a
// slot's live assembly state (0/"" once the slot completes or is reset).
func (a *SlotAssembler) SlotAssemblyErrors(slot uint64) (int, string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if state := a.slots[slot]; state != nil {
		return state.errCount, state.lastErr
	}
	return 0, ""
}

// IsPrioritySlot reports whether slot is currently priority-pinned for
// repair (catchup window or live-gap pin) — such slots always assemble in
// RAM regardless of the spool's assembly-window policy.
func (a *SlotAssembler) IsPrioritySlot(slot uint64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ok := a.priorityRepairSlots[slot]
	return ok
}

func (a *SlotAssembler) PriorityRepairSlots() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.prunePriorityRepairSlotsLocked()
	return len(a.priorityRepairSlots)
}

func (a *SlotAssembler) NonCanonicalBlockIDStats() (count uint64, slot uint64, got solana.Hash, want solana.Hash) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.nonCanonicalBlockIDs, a.lastNonCanonicalSlot, a.lastNonCanonicalGot, a.lastNonCanonicalWant
}

// RepairRequestsTiered returns two request tiers: PRIORITY (catchup window
// and live-gap pins — what unblocks replay) and EDGE (freshness scan just
// behind the shred edge — holes are dramatically cheaper to patch while
// peers still hold those shreds hot than at hydration time minutes later).
// The caller allocates budget between them; scarce tokens go priority-first.
func (a *SlotAssembler) RepairRequestsTiered(maxSlots int, maxMissingPerSlot int) (priority, edge []SlotRepairRequest) {
	if maxSlots <= 0 {
		return nil, nil
	}
	if maxMissingPerSlot <= 0 {
		maxMissingPerSlot = 1
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Freshness repair deliberately trails the observed edge: broadcast/FEC
	// should get the first chance to finish a slot before we spend repair
	// bandwidth on it. Explicit priority pins are different. They are installed
	// by replay for a slot that is already gating progress, and may legitimately
	// name the newest observed slot (or an entirely absent slot ahead of it when
	// live ingress has gone quiet). Applying the edge grace to those pins makes
	// the catchup head impossible to repair precisely when repair is most needed.
	haveRepairThrough := a.maxObservedSlot > repairObservedSlotLag
	repairThrough := uint64(0)
	if haveRepairThrough {
		repairThrough = a.maxObservedSlot - repairObservedSlotLag
	}
	scanLag := a.edgeScanLag
	if scanLag == 0 {
		scanLag = repairScanSlotWindow
	}
	start := uint64(0)
	if repairThrough > scanLag {
		start = repairThrough - scanLag
	}
	if a.maxObservedSlot > maxRetainedIncompleteSlotLag {
		minRetained := a.maxObservedSlot - maxRetainedIncompleteSlotLag
		if start < minRetained {
			start = minRetained
		}
	}

	seen := make(map[uint64]struct{}, maxSlots)
	appendRequest := func(dst []SlotRepairRequest, slot uint64, maxMissing int, priorityPin bool) []SlotRepairRequest {
		if len(dst) >= maxSlots {
			return dst
		}
		if _, alreadySeen := seen[slot]; alreadySeen {
			return dst
		}
		if _, completed := a.completedSlots[slot]; completed {
			return dst
		}
		if a.slotTooOldLocked(slot) || (!priorityPin && (!haveRepairThrough || slot > repairThrough)) {
			return dst
		}
		state := a.slots[slot]
		if state == nil {
			seen[slot] = struct{}{}
			return append(dst, SlotRepairRequest{
				Slot:                  slot,
				NeedHighestDataShred:  true,
				HighestDataShredIndex: 0,
			})
		}
		if req, ok := state.repairRequest(maxMissing); ok {
			seen[slot] = struct{}{}
			return append(dst, req)
		}
		return dst
	}

	a.prunePriorityRepairSlotsLocked()
	for _, slot := range a.priorityRepairOrder {
		// HEAD FIRST: the first priority slot — the one gating emission —
		// may list up to repairHeadMaxMissing, several times the per-slot
		// cap, so its admission share stays full at any response latency.
		// Without this, degraded latency capped the head at 256 in flight
		// and the surplus drained into the window while emission starved.
		// The send side bounds the head to a SHARE of the admission window
		// (repair.go boundHeadToAdmissionShare): a total monopoly was
		// tried live and lowered throughput — no prefill, no freshness
		// repair, every slot re-paying full serial fill latency.
		maxMissing := maxMissingPerSlot
		if len(priority) == 0 {
			maxMissing = repairHeadMaxMissing
		}
		priority = appendRequest(priority, slot, maxMissing, true)
		if len(priority) >= maxSlots {
			break
		}
	}
	for slot := start; haveRepairThrough && slot <= repairThrough && len(edge) < maxSlots; slot++ {
		edge = appendRequest(edge, slot, maxMissingPerSlot, false)
	}
	return priority, edge
}

// RepairRequests preserves the flat view (priority first) for callers that
// do not budget tiers.
func (a *SlotAssembler) RepairRequests(maxSlots int, maxMissingPerSlot int) []SlotRepairRequest {
	priority, edge := a.RepairRequestsTiered(maxSlots, maxMissingPerSlot)
	out := append(priority, edge...)
	if len(out) > maxSlots {
		out = out[:maxSlots]
	}
	return out
}

func (a *SlotAssembler) recoverFEC(state *slotState, fecSetIndex uint32) ([]*Shred, error) {
	fec := state.fecSets[fecSetIndex]
	if fec == nil || !fec.haveLayout {
		return nil, nil
	}
	layout := fec.layout
	if int(layout.dataShreds)+int(layout.codingShreds) == len(fec.data)+len(fec.coding) {
		return nil, nil
	}
	if len(fec.data)+len(fec.coding) < int(layout.dataShreds) {
		return nil, nil
	}

	shards := make([][]byte, int(layout.dataShreds)+int(layout.codingShreds))
	for idx, shred := range fec.data {
		if idx >= uint32(layout.dataShreds) {
			continue
		}
		shard, err := shred.erasureShard()
		if err != nil {
			return nil, err
		}
		// ReconstructSome treats present shards as read-only and only fills the
		// requested missing entries, so the owned shred payload can be passed
		// directly without another full-shard copy.
		shards[int(idx)] = shard
	}
	for pos, shred := range fec.coding {
		if pos >= layout.codingShreds {
			continue
		}
		shard, err := shred.erasureShard()
		if err != nil {
			return nil, err
		}
		shards[int(layout.dataShreds)+int(pos)] = shard
	}
	encoder, err := a.fecEncoder(layout)
	if err != nil {
		return nil, err
	}
	required := make([]bool, int(layout.dataShreds)+int(layout.codingShreds))
	var missingData int
	for idx := 0; idx < int(layout.dataShreds); idx++ {
		if fec.data[uint32(idx)] == nil {
			required[idx] = true
			missingData++
		}
	}
	if missingData == 0 {
		return nil, nil
	}
	if err := encoder.ReconstructSome(shards, required); err != nil {
		if errors.Is(err, reedsolomon.ErrTooFewShards) {
			return nil, nil
		}
		return nil, fmt.Errorf("recover FEC set slot %d fec_set=%d: %w", state.slot, fecSetIndex, err)
	}

	var recovered []*Shred
	for idx := 0; idx < int(layout.dataShreds); idx++ {
		if !required[idx] {
			continue
		}
		shard := shards[idx]
		if len(shard) != layout.shardSize {
			return nil, fmt.Errorf("recover FEC set slot %d fec_set=%d: recovered shard %d size %d, want %d", state.slot, fecSetIndex, idx, len(shard), layout.shardSize)
		}
		shred, err := fec.recoveredDataShred(uint32(idx), shard)
		if err != nil {
			return nil, err
		}
		recovered = append(recovered, shred)
	}
	return recovered, nil
}

func (a *SlotAssembler) fecEncoder(layout fecLayout) (reedsolomon.Encoder, error) {
	encoder := a.encoders[layout]
	if encoder != nil {
		return encoder, nil
	}
	encoder, err := reedsolomon.New(
		int(layout.dataShreds),
		int(layout.codingShreds),
		reedsolomon.WithInversionCache(false),
	)
	if err != nil {
		return nil, err
	}
	a.encoders[layout] = encoder
	return encoder, nil
}

func (s *slotState) fecSet(fecSetIndex uint32) *fecState {
	fec := s.fecSets[fecSetIndex]
	if fec != nil {
		return fec
	}
	fec = &fecState{
		slot:        s.slot,
		fecSetIndex: fecSetIndex,
		data:        make(map[uint32]*Shred),
		coding:      make(map[uint16]*Shred),
	}
	s.fecSets[fecSetIndex] = fec
	return fec
}

func (s *slotState) addDataToFEC(shred *Shred) error {
	if !isMerkleVariant(shred.Variant) || len(shred.Payload) < dataPayloadSize {
		return nil
	}
	if shred.Index < shred.FECSetIndex {
		return nil
	}
	fec := s.fecSet(shred.FECSetIndex)
	if err := fec.acceptShred(shred); err != nil {
		return err
	}
	fec.data[shred.Index-shred.FECSetIndex] = shred
	return nil
}

func (f *fecState) acceptShred(shred *Shred) error {
	if !f.haveSig {
		copy(f.signature[:], shred.Signature[:])
		f.haveSig = true
	} else if f.signature != shred.Signature {
		return fmt.Errorf("mixed FEC signatures for slot %d fec_set=%d", f.slot, f.fecSetIndex)
	}

	switch shred.Type {
	case ShredTypeData:
		expected, ok := merkleCounterpartVariant(shred.Variant, ShredTypeCode)
		if !ok {
			return fmt.Errorf("unsupported data shred variant for slot %d fec_set=%d: 0x%02x", f.slot, f.fecSetIndex, shred.Variant)
		}
		if f.dataVariant == 0 {
			f.dataVariant = shred.Variant
		} else if f.dataVariant != shred.Variant {
			return fmt.Errorf("mixed data shred variants for slot %d fec_set=%d: 0x%02x and 0x%02x", f.slot, f.fecSetIndex, f.dataVariant, shred.Variant)
		}
		if f.codeVariant != 0 && f.codeVariant != expected {
			return fmt.Errorf("mixed data/code shred variants for slot %d fec_set=%d: data=0x%02x code=0x%02x", f.slot, f.fecSetIndex, shred.Variant, f.codeVariant)
		}
	case ShredTypeCode:
		expected, ok := merkleCounterpartVariant(shred.Variant, ShredTypeData)
		if !ok {
			return fmt.Errorf("unsupported coding shred variant for slot %d fec_set=%d: 0x%02x", f.slot, f.fecSetIndex, shred.Variant)
		}
		if f.codeVariant == 0 {
			f.codeVariant = shred.Variant
		} else if f.codeVariant != shred.Variant {
			return fmt.Errorf("mixed coding shred variants for slot %d fec_set=%d: 0x%02x and 0x%02x", f.slot, f.fecSetIndex, f.codeVariant, shred.Variant)
		}
		if f.dataVariant != 0 && f.dataVariant != expected {
			return fmt.Errorf("mixed data/code shred variants for slot %d fec_set=%d: data=0x%02x code=0x%02x", f.slot, f.fecSetIndex, f.dataVariant, shred.Variant)
		}
	}
	return nil
}

func (f *fecState) setLayout(layout fecLayout) error {
	if !f.haveLayout {
		f.layout = layout
		f.haveLayout = true
		return nil
	}
	if f.layout != layout {
		return fmt.Errorf("mixed FEC layouts for slot %d fec_set=%d: %+v and %+v", f.slot, f.fecSetIndex, f.layout, layout)
	}
	return nil
}

func (s *Shred) fecLayout() (fecLayout, error) {
	shard, err := s.erasureShard()
	if err != nil {
		return fecLayout{}, err
	}
	return fecLayout{
		dataShreds:   s.NumDataShreds,
		codingShreds: s.NumCodingShreds,
		shardSize:    len(shard),
	}, nil
}

func (f *fecState) recoveredDataShred(dataIndex uint32, shard []byte) (*Shred, error) {
	code := f.firstCodingShred()
	if code == nil {
		return nil, fmt.Errorf("recover FEC set slot %d fec_set=%d: no coding shred template", f.slot, f.fecSetIndex)
	}
	payload := make([]byte, dataPayloadSize)
	copy(payload[:shredSignatureSize], code.Signature[:])
	copy(payload[shredSignatureSize:], shard)
	shred, err := ParseShred(payload)
	if err != nil {
		return nil, fmt.Errorf("parse recovered data shred slot %d fec_set=%d data_index=%d: %w", f.slot, f.fecSetIndex, dataIndex, err)
	}
	expectedVariant, ok := merkleCounterpartVariant(code.Variant, ShredTypeData)
	if !ok {
		return nil, fmt.Errorf("recover FEC set slot %d fec_set=%d: unsupported coding variant 0x%02x", f.slot, f.fecSetIndex, code.Variant)
	}
	if shred.Type != ShredTypeData ||
		shred.Variant != expectedVariant ||
		shred.Slot != f.slot ||
		shred.FECSetIndex != f.fecSetIndex ||
		shred.Index != f.fecSetIndex+dataIndex ||
		shred.Version != code.Version {
		return nil, fmt.Errorf("recover FEC set slot %d fec_set=%d: invalid recovered data shred index=%d slot=%d variant=0x%02x version=%d", f.slot, f.fecSetIndex, shred.Index, shred.Slot, shred.Variant, shred.Version)
	}
	shred.Recovered = true
	return shred, nil
}

func (f *fecState) firstCodingShred() *Shred {
	for _, shred := range f.coding {
		return shred
	}
	return nil
}

func (s *slotState) complete() bool {
	if !s.haveLast {
		return false
	}
	for idx := uint32(0); idx <= s.lastIndex; idx++ {
		if s.shreds[idx] == nil {
			return false
		}
	}
	return true
}

func (s *slotState) orderedShreds() []*Shred {
	indexes := make([]int, 0, len(s.shreds))
	for idx := range s.shreds {
		indexes = append(indexes, int(idx))
	}
	sort.Ints(indexes)
	out := make([]*Shred, 0, len(indexes))
	for _, idx := range indexes {
		out = append(out, s.shreds[uint32(idx)])
	}
	return out
}

// decodeBlock performs every immutable, CPU-heavy completion step except
// transaction signature verification. Parent/child identity hints are applied
// later under the assembler lock so hints learned while this runs still win.
func (s *slotState) decodeBlock(timings *entryDecodeTimings) (*block.Block, *AlpenglowParentInfo, []solana.Hash, error) {
	entries, parentInfo, footer, err := decodeEntriesAndAlpenglowMarkersFromDataShreds(s.orderedShreds(), timings)
	if err != nil {
		return nil, nil, nil, err
	}
	effectiveParentSlot := s.parentSlot
	if parentInfo != nil {
		if parentInfo.FromUpdateParent && s.slot%defaultBroadcastLeaderSlots != 0 {
			return nil, nil, nil, fmt.Errorf("slot %d alpenglow update-parent marker is not in the first slot of its leader window", s.slot)
		}
		if parentInfo.ParentSlot >= s.slot {
			return nil, nil, nil, fmt.Errorf("slot %d alpenglow parent marker points to non-ancestor slot %d", s.slot, parentInfo.ParentSlot)
		}
		effectiveParentSlot = parentInfo.ParentSlot
	}

	blk := BlockFromEntries(s.slot, effectiveParentSlot, entries)
	blk.AlpenglowShredVersion = s.shredVer
	if footer != nil {
		blk.HasAlpenglowFooter = true
		blk.SkipRewardCert = append([]byte(nil), footer.SkipRewardCert...)
		blk.NotarRewardCert = append([]byte(nil), footer.NotarRewardCert...)
		blk.BlockFinalCert = append([]byte(nil), footer.BlockFinalCert...)
		blk.AlpenglowFinalCert = append([]byte(nil), footer.BlockFinalCert...)
		blk.FooterProducerTimeNanos = footer.BlockProducerTimeNanos
		if footer.BankHash != (solana.Hash{}) {
			blk.ExpectedBankhash = footer.BankHash
			blk.HasExpectedBankhash = true
		}
	}
	roots, err := s.optionalFECSetMerkleRoots()
	if err != nil {
		return nil, nil, nil, err
	}
	return blk, parentInfo, roots, nil
}

// block is retained as the synchronous construction helper used by focused
// generator tests. Production assembly uses decodeBlock plus final-time hint
// resolution in finalizeCompletion.
func (s *slotState) block(parentBlockID solana.Hash, parentKnown bool) (*block.Block, error) {
	blk, parentInfo, roots, err := s.decodeBlock(nil)
	if err != nil {
		return nil, err
	}
	parentSlot := s.parentSlot
	if parentInfo != nil {
		parentSlot = parentInfo.ParentSlot
		parentBlockID = parentInfo.ParentBlockID
		parentKnown = true
	}
	if (parentInfo != nil && parentSlot == 0) || (parentKnown && parentBlockID != (solana.Hash{})) {
		blk.AlpenglowParentBlockID = parentBlockID
		blk.HasAlpenglowParentBlockID = true
	}
	if parentKnown && len(roots) > 0 {
		blk.AlpenglowBlockID = DoubleMerkleBlockID(parentSlot, parentBlockID, roots)
		blk.HasAlpenglowBlockID = true
		blk.AlpenglowLastChainedRoot = roots[len(roots)-1]
		blk.HasAlpenglowLastChainedRoot = true
	}
	if err := validateBlockTransactions(blk); err != nil {
		return nil, err
	}
	blk.MarkTransactionSignaturesVerified()
	return blk, nil
}

// optionalFECSetMerkleRoots accepts a fully legacy slot with no Merkle roots,
// but rejects a partially rooted Alpenglow slot. That lets decode run before a
// parent hint is known without weakening mixed/missing-root validation.
func (s *slotState) optionalFECSetMerkleRoots() ([]solana.Hash, error) {
	if !s.haveLast || len(s.fecSets) == 0 {
		return nil, nil
	}
	fecSetCount := s.lastIndex/dataShredsPerFECBlock + 1
	roots := make([]solana.Hash, fecSetCount)
	haveRoot := make([]bool, fecSetCount)
	anyRoot := false
	for fecSetNumber := uint32(0); fecSetNumber < fecSetCount; fecSetNumber++ {
		fecSetIndex := fecSetNumber * dataShredsPerFECBlock
		fec := s.fecSets[fecSetIndex]
		if fec == nil {
			return nil, fmt.Errorf("slot %d alpenglow block id: missing FEC set %d", s.slot, fecSetIndex)
		}
		root, ok, err := fec.merkleRoot()
		if err != nil {
			return nil, fmt.Errorf("slot %d fec_set=%d alpenglow block id: %w", s.slot, fecSetIndex, err)
		}
		if ok {
			roots[fecSetNumber] = root
			haveRoot[fecSetNumber] = true
			anyRoot = true
		}
	}
	if !anyRoot {
		return nil, nil
	}
	for fecSetNumber, ok := range haveRoot {
		if !ok {
			return nil, fmt.Errorf("slot %d fec_set=%d alpenglow block id: missing Merkle root", s.slot, uint32(fecSetNumber)*dataShredsPerFECBlock)
		}
	}
	return roots, nil
}

func (s *slotState) alpenglowBlockID(parentSlot uint64, parentBlockID solana.Hash, parentKnown bool) (solana.Hash, bool, error) {
	if !parentKnown {
		return solana.Hash{}, false, nil
	}

	roots, err := s.fecSetMerkleRoots()
	if err != nil {
		return solana.Hash{}, false, err
	}
	if len(roots) == 0 {
		return solana.Hash{}, false, nil
	}
	return DoubleMerkleBlockID(parentSlot, parentBlockID, roots), true, nil
}

func (s *slotState) fecSetMerkleRoots() ([]solana.Hash, error) {
	if !s.haveLast {
		return nil, nil
	}

	fecSetCount := s.lastIndex/dataShredsPerFECBlock + 1
	roots := make([]solana.Hash, 0, fecSetCount)
	for fecSetNumber := uint32(0); fecSetNumber < fecSetCount; fecSetNumber++ {
		fecSetIndex := fecSetNumber * dataShredsPerFECBlock
		fec := s.fecSets[fecSetIndex]
		if fec == nil {
			return nil, fmt.Errorf("slot %d alpenglow block id: missing FEC set %d", s.slot, fecSetIndex)
		}
		root, ok, err := fec.merkleRoot()
		if err != nil {
			return nil, fmt.Errorf("slot %d fec_set=%d alpenglow block id: %w", s.slot, fecSetIndex, err)
		}
		if !ok {
			return nil, fmt.Errorf("slot %d fec_set=%d alpenglow block id: missing Merkle root", s.slot, fecSetIndex)
		}
		roots = append(roots, root)
	}
	return roots, nil
}

func (f *fecState) merkleRoot() (solana.Hash, bool, error) {
	for _, idx := range sortedUint32Keys(f.data) {
		shred := f.data[idx]
		if shred == nil || shred.Recovered {
			continue
		}
		root, err := shred.MerkleRoot()
		if err != nil {
			if errors.Is(err, ErrUnsupportedShred) {
				continue
			}
			return solana.Hash{}, false, err
		}
		return root, true, nil
	}
	for _, pos := range sortedUint16Keys(f.coding) {
		shred := f.coding[pos]
		if shred == nil {
			continue
		}
		root, err := shred.MerkleRoot()
		if err != nil {
			if errors.Is(err, ErrUnsupportedShred) {
				continue
			}
			return solana.Hash{}, false, err
		}
		return root, true, nil
	}
	return solana.Hash{}, false, nil
}

func merkleTreeRoot(leaves []solana.Hash) solana.Hash {
	if len(leaves) == 0 {
		return solana.Hash{}
	}

	nodes := make([]solana.Hash, 0, merkleTreeSize(len(leaves)))
	nodes = append(nodes, leaves...)
	for size := len(leaves); size > 1; size = (size + 1) >> 1 {
		offset := len(nodes) - size
		end := offset + size
		for idx := offset; idx < end; idx += 2 {
			other := idx + 1
			if other >= end {
				other = end - 1
			}
			nodes = append(nodes, merkleHashNode(nodes[idx][:merkleProofEntrySize], nodes[other][:merkleProofEntrySize]))
		}
	}
	return nodes[len(nodes)-1]
}

func merkleTreeSize(leaves int) int {
	if leaves <= 0 {
		return 0
	}
	size := leaves
	for width := leaves; width > 1; width = (width + 1) >> 1 {
		size += (width + 1) >> 1
	}
	return size
}

func sortedUint32Keys[V any](m map[uint32]V) []uint32 {
	keys := make([]uint32, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}

func sortedUint16Keys[V any](m map[uint16]V) []uint16 {
	keys := make([]uint16, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	return keys
}
