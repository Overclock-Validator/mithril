package turbine

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/block"
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
	priorityRepairSlots  map[uint64]struct{}
	priorityRepairOrder  []uint64
	encoders             map[fecLayout]reedsolomon.Encoder
	partialShredObs      map[uint64]PartialShredObservation // shreds seen for slots that never became full (retained for skip observability)
	maxObservedSlot      uint64
	highestFullSlot      uint64 // monotonic: highest slot reconstructed from shreds ("full", Agave SlotMeta/is_full sense)
	recoveredDataShreds  uint64
	nonCanonicalBlockIDs uint64
	lastNonCanonicalSlot uint64
	lastNonCanonicalGot  solana.Hash
	lastNonCanonicalWant solana.Hash
	evictedSlots         uint64
	ignoredOldShreds     uint64
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
	repairedShreds int
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
		priorityRepairSlots: make(map[uint64]struct{}),
		partialShredObs:     make(map[uint64]PartialShredObservation),
		encoders:            make(map[fecLayout]reedsolomon.Encoder),
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
	if fromRepair {
		state.repairedShreds++
	}
	var err error
	switch shred.Type {
	case ShredTypeData:
		err = state.addDataShred(shred)
	case ShredTypeCode:
		err = state.addCodingShred(shred)
	default:
		return nil, nil
	}
	if err != nil {
		if errors.Is(err, ErrDuplicateShred) {
			return nil, nil
		}
		return nil, err
	}

	recovered, err := a.recoverFEC(state, shred.FECSetIndex)
	if err != nil {
		return nil, err
	}
	for _, recoveredShred := range recovered {
		err := state.addDataShred(recoveredShred)
		if err != nil && !errors.Is(err, ErrDuplicateShred) {
			return nil, err
		}
		if err == nil {
			a.recoveredDataShreds++
		}
	}

	if !state.complete() {
		return nil, nil
	}

	parentBlockID, parentKnown := a.knownBlockIDs[state.parentSlot]
	blk, err := state.block(parentBlockID, parentKnown)
	if err != nil {
		return nil, err
	}
	if !a.acceptAlpenglowBlockIDLocked(blk) {
		a.trackNonCanonicalBlockIDLocked(blk)
		a.recordPartialObsLocked(state) // shreds DID arrive; useful if the slot ends up skipped
		delete(a.slots, shred.Slot)
		return nil, nil
	}
	delete(a.slots, shred.Slot)
	a.completedSlots[shred.Slot] = struct{}{}
	a.trackBlockIDLocked(blk)
	// Shred-path observability: stamp when the slot's shreds started arriving
	// and when it became full ("full" = reconstructable, Agave is_full sense).
	if !state.firstShredAt.IsZero() {
		blk.ShredFirstNanos = state.firstShredAt.UnixNano()
	}
	blk.ShredFullNanos = time.Now().UnixNano()
	blk.RepairedShreds = state.repairedShreds
	if shred.Slot > a.highestFullSlot {
		a.highestFullSlot = shred.Slot
	}
	return blk, nil
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

	a.knownBlockIDs[slot] = blockID
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
		slot:         slot,
		shreds:       make(map[uint32]*Shred),
		fecSets:      make(map[uint32]*fecState),
		shredVer:     version,
		lastIndex:    ^uint32(0),
		firstShredAt: time.Now(),
	}
	a.slots[slot] = state
	return state
}

func (a *SlotAssembler) slotTooOldLocked(slot uint64) bool {
	if a.maxObservedSlot <= maxRetainedIncompleteSlotLag {
		return false
	}
	return slot < a.maxObservedSlot-maxRetainedIncompleteSlotLag
}

func (a *SlotAssembler) pruneOldSlotsLocked() {
	if len(a.slots) > 0 && a.maxObservedSlot > maxRetainedIncompleteSlotLag {
		minSlot := a.maxObservedSlot - maxRetainedIncompleteSlotLag
		for slot, state := range a.slots {
			if slot < minSlot {
				a.recordPartialObsLocked(state)
				delete(a.slots, slot)
				a.evictedSlots++
			}
		}
	}
	if a.maxObservedSlot > maxRetainedCompletedSlotLag {
		minSlot := a.maxObservedSlot - maxRetainedCompletedSlotLag
		for slot := range a.completedSlots {
			if slot < minSlot {
				delete(a.completedSlots, slot)
			}
		}
		for slot := range a.knownBlockIDs {
			if slot < minSlot {
				delete(a.knownBlockIDs, slot)
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
		var oldest uint64
		first := true
		for slot := range a.slots {
			if first || slot < oldest {
				oldest = slot
				first = false
			}
		}
		if first {
			return
		}
		a.recordPartialObsLocked(a.slots[oldest])
		delete(a.slots, oldest)
		a.evictedSlots++
	}
}

func (a *SlotAssembler) prunePriorityRepairSlotsLocked() {
	if len(a.priorityRepairOrder) == 0 {
		return
	}

	minSlot := uint64(0)
	if a.maxObservedSlot > maxRetainedIncompleteSlotLag {
		minSlot = a.maxObservedSlot - maxRetainedIncompleteSlotLag
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

	missingThrough := maxObserved
	if s.haveLast {
		missingThrough = s.lastIndex
	}
	for index := uint32(0); index <= missingThrough && len(req.MissingDataShreds) < maxMissing; index++ {
		if s.shreds[index] == nil {
			req.MissingDataShreds = append(req.MissingDataShreds, index)
		}
		if index == maxDataShredsPerSlot-1 {
			break
		}
	}

	if len(req.MissingDataShreds) == 0 && !req.NeedHighestDataShred {
		return SlotRepairRequest{}, false
	}
	return req, true
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
	defer a.mu.Unlock()

	state := a.slots[slot]
	if state == nil || !state.complete() {
		return nil, ErrSlotIncomplete
	}
	parentBlockID, parentKnown := a.knownBlockIDs[state.parentSlot]
	blk, err := state.block(parentBlockID, parentKnown)
	if err != nil {
		return nil, err
	}
	if !a.acceptAlpenglowBlockIDLocked(blk) {
		a.trackNonCanonicalBlockIDLocked(blk)
		delete(a.slots, slot)
		return nil, ErrNonCanonicalAlpenglowBlockID
	}
	delete(a.slots, slot)
	a.completedSlots[slot] = struct{}{}
	a.trackBlockIDLocked(blk)
	return blk, nil
}

func (a *SlotAssembler) acceptAlpenglowBlockIDLocked(blk *block.Block) bool {
	if blk == nil || !blk.HasAlpenglowBlockID {
		return true
	}
	known, ok := a.knownBlockIDs[blk.Slot]
	if !ok || known == (solana.Hash{}) {
		return true
	}
	return solana.Hash(blk.AlpenglowBlockID) == known
}

func (a *SlotAssembler) trackBlockIDLocked(blk *block.Block) {
	if blk == nil || !blk.HasAlpenglowBlockID {
		return
	}
	blockID := solana.Hash(blk.AlpenglowBlockID)
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

func (a *SlotAssembler) RepairRequests(maxSlots int, maxMissingPerSlot int) []SlotRepairRequest {
	if maxSlots <= 0 {
		return nil
	}
	if maxMissingPerSlot <= 0 {
		maxMissingPerSlot = 1
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.maxObservedSlot <= repairObservedSlotLag {
		return nil
	}
	repairThrough := a.maxObservedSlot - repairObservedSlotLag
	start := uint64(0)
	if repairThrough > repairScanSlotWindow {
		start = repairThrough - repairScanSlotWindow
	}
	if a.maxObservedSlot > maxRetainedIncompleteSlotLag {
		minRetained := a.maxObservedSlot - maxRetainedIncompleteSlotLag
		if start < minRetained {
			start = minRetained
		}
	}

	requests := make([]SlotRepairRequest, 0, maxSlots)
	seen := make(map[uint64]struct{}, maxSlots)
	appendRequest := func(slot uint64) {
		if len(requests) >= maxSlots {
			return
		}
		if _, alreadySeen := seen[slot]; alreadySeen {
			return
		}
		if _, completed := a.completedSlots[slot]; completed {
			return
		}
		if a.slotTooOldLocked(slot) || slot > repairThrough {
			return
		}
		state := a.slots[slot]
		if state == nil {
			requests = append(requests, SlotRepairRequest{
				Slot:                  slot,
				NeedHighestDataShred:  true,
				HighestDataShredIndex: 0,
			})
			seen[slot] = struct{}{}
			return
		}
		if req, ok := state.repairRequest(maxMissingPerSlot); ok {
			requests = append(requests, req)
			seen[slot] = struct{}{}
		}
	}

	a.prunePriorityRepairSlotsLocked()
	for _, slot := range a.priorityRepairOrder {
		appendRequest(slot)
		if len(requests) >= maxSlots {
			return requests
		}
	}

	for slot := start; slot <= repairThrough && len(requests) < maxSlots; slot++ {
		appendRequest(slot)
	}
	return requests
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
		shards[int(idx)] = append([]byte(nil), shard...)
	}
	for pos, shred := range fec.coding {
		if pos >= layout.codingShreds {
			continue
		}
		shard, err := shred.erasureShard()
		if err != nil {
			return nil, err
		}
		shards[int(layout.dataShreds)+int(pos)] = append([]byte(nil), shard...)
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

func (s *slotState) block(parentBlockID solana.Hash, parentKnown bool) (*block.Block, error) {
	entries, parentInfo, err := DecodeEntriesAndAlpenglowParentInfoFromDataShreds(s.orderedShreds())
	if err != nil {
		return nil, err
	}
	effectiveParentSlot := s.parentSlot
	effectiveParentBlockID := parentBlockID
	effectiveParentKnown := parentKnown
	if parentInfo != nil {
		if parentInfo.ParentSlot >= s.slot {
			return nil, fmt.Errorf("slot %d alpenglow parent marker points to non-ancestor slot %d", s.slot, parentInfo.ParentSlot)
		}
		effectiveParentSlot = parentInfo.ParentSlot
		effectiveParentBlockID = parentInfo.ParentBlockID
		effectiveParentKnown = true
	}

	blk := BlockFromEntries(s.slot, effectiveParentSlot, entries)
	if blockID, ok, err := s.alpenglowBlockID(effectiveParentSlot, effectiveParentBlockID, effectiveParentKnown); err != nil {
		return nil, err
	} else if ok {
		blk.AlpenglowBlockID = blockID
		blk.HasAlpenglowBlockID = true
	}
	if effectiveParentKnown {
		blk.AlpenglowParentBlockID = effectiveParentBlockID
		blk.HasAlpenglowParentBlockID = true
	}
	// Pull the footer's finalization cert (for an earlier slot) if present; replay
	// decodes + verifies it to drive Alpenglow finality without needing Votor QUIC.
	if fc := s.footerFinalCert(); len(fc) > 0 {
		blk.AlpenglowFinalCert = fc
	}
	if err := validateBlockTransactions(blk); err != nil {
		return nil, err
	}
	return blk, nil
}

// footerFinalCert returns the raw final_cert bytes from the block footer, or nil.
// Each batch is classified by its first shred, so entry batches (the common case)
// are skipped without copying — this stays off the classic hot path.
func (s *slotState) footerFinalCert() []byte {
	batchStart := true
	isMarker := false
	var batch []byte
	for _, shred := range s.orderedShreds() {
		if shred == nil || shred.Type != ShredTypeData {
			continue
		}
		if batchStart {
			isMarker = InferIsBlockMarker(shred.Data)
			batchStart = false
		}
		if isMarker {
			batch = append(batch, shred.Data...)
		}
		if !shred.DataComplete() {
			continue
		}
		if isMarker {
			if comp, err := UnmarshalBlockComponent(batch); err == nil && comp.Marker != nil &&
				comp.Marker.Kind == MarkerBlockFooter && comp.Marker.Footer != nil {
				return comp.Marker.Footer.BlockFinalCert
			}
		}
		batch = nil
		batchStart = true
	}
	return nil
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
	fecSetCount := uint32(s.lastIndex/dataShredsPerFECBlock + 1)

	var parentSlotBytes [8]byte
	binary.LittleEndian.PutUint64(parentSlotBytes[:], parentSlot)
	var fecSetCountBytes [4]byte
	binary.LittleEndian.PutUint32(fecSetCountBytes[:], fecSetCount)
	parentInfoLeaf := hashv([][]byte{parentSlotBytes[:], parentBlockID[:], fecSetCountBytes[:]})
	roots = append(roots, parentInfoLeaf)

	return merkleTreeRoot(roots), true, nil
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
