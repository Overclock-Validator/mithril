package alpenglow

import (
	"bytes"
	"fmt"
	"sort"
)

const (
	// Alpenglow leader windows contain four consecutive slots. ParentReady is
	// emitted only for the first slot in a window, matching Agave.
	LeaderWindowSlots = uint64(4)
	// MAX_NOTAR_FALLBACK_BLOCKS in Agave. Reaching this bound is treated as
	// invalid consensus input instead of panicking the process.
	maxNotarFallbackBlocks = 7
)

type BlockProductionParentKind uint8

const (
	BlockProductionParentNotReady BlockProductionParentKind = iota
	BlockProductionParentMissedWindow
	BlockProductionParentReady
)

type BlockProductionParent struct {
	Kind   BlockProductionParentKind
	Parent BlockID
}

type parentReadyStatus struct {
	skip           bool
	notarFallbacks []BlockID
	parentsReady   []BlockID
}

// ParentReadyTracker mirrors Agave's parent-ready state machine. A block is a
// valid parent for slot s when it has a notarize-fallback-or-stronger
// certificate and every intervening slot is skip-certified.
type ParentReadyTracker struct {
	root                   uint64
	highestWithParentReady uint64
	slots                  map[uint64]*parentReadyStatus
}

func NewParentReadyTracker(root BlockID) *ParentReadyTracker {
	t := &ParentReadyTracker{slots: make(map[uint64]*parentReadyStatus)}
	t.Restore(root.Slot+1, root)
	return t
}

// Restore installs a restart/checkpoint parent-ready boundary. parent must be
// earlier than slot; skipped slots between the two are reconstructed.
func (t *ParentReadyTracker) Restore(slot uint64, parent BlockID) {
	if t.slots == nil {
		t.slots = make(map[uint64]*parentReadyStatus)
	}
	t.slots = make(map[uint64]*parentReadyStatus)
	t.root = parent.Slot
	t.highestWithParentReady = slot
	t.status(parent.Slot).notarFallbacks = []BlockID{parent}
	for s := parent.Slot + 1; s < slot; s++ {
		st := t.status(s)
		st.skip = true
		st.parentsReady = []BlockID{parent}
	}
	t.status(slot).parentsReady = []BlockID{parent}
}

func (t *ParentReadyTracker) SetRoot(root uint64) {
	if root < t.root {
		return
	}
	t.root = root
	for slot := range t.slots {
		if slot < root {
			delete(t.slots, slot)
		}
	}
}

func (t *ParentReadyTracker) Root() uint64 { return t.root }

func (t *ParentReadyTracker) AddNotarFallbackOrStronger(block BlockID) ([]ConsensusEvent, error) {
	if block.Slot <= t.root {
		return nil, nil
	}
	return t.addNotarFallbackOrStronger(block)
}

// AddGenesis permits the migration genesis block at the current root. This is
// the behavior fixed after Agave v4.2.0-beta.0.
func (t *ParentReadyTracker) AddGenesis(block BlockID) ([]ConsensusEvent, error) {
	if block.Slot < t.root {
		return nil, nil
	}
	return t.addNotarFallbackOrStronger(block)
}

func (t *ParentReadyTracker) addNotarFallbackOrStronger(block BlockID) ([]ConsensusEvent, error) {
	status := t.status(block.Slot)
	if containsBlock(status.notarFallbacks, block) {
		return nil, nil
	}
	if len(status.notarFallbacks) >= maxNotarFallbackBlocks {
		return nil, fmt.Errorf("alpenglow parent-ready: slot %d exceeds %d notarize-fallback blocks", block.Slot, maxNotarFallbackBlocks)
	}
	status.notarFallbacks = append(status.notarFallbacks, block)

	var events []ConsensusEvent
	for slot := block.Slot + 1; ; slot++ {
		status := t.status(slot)
		if !containsBlock(status.parentsReady, block) {
			status.parentsReady = append(status.parentsReady, block)
			if isLeaderWindowStart(slot) {
				events = append(events, ConsensusEvent{Kind: ConsensusEventParentReady, Slot: slot, Block: block})
			}
			if slot > t.highestWithParentReady {
				t.highestWithParentReady = slot
			}
		}
		if !status.skip {
			break
		}
	}
	return events, nil
}

func (t *ParentReadyTracker) AddSkip(slot uint64) []ConsensusEvent {
	if slot <= t.root {
		return nil
	}
	t.status(slot).skip = true

	future := []uint64{slot + 1}
	for next := slot + 1; t.status(next).skip; next++ {
		future = append(future, next+1)
	}

	if slot == 0 {
		return nil
	}
	previous, ok := t.slots[slot-1]
	if !ok {
		return nil
	}
	potential := append([]BlockID(nil), previous.notarFallbacks...)
	if previous.skip {
		for _, parent := range previous.parentsReady {
			if !containsBlock(potential, parent) {
				potential = append(potential, parent)
			}
		}
	}
	if len(potential) == 0 {
		return nil
	}

	var events []ConsensusEvent
	for _, next := range future {
		status := t.status(next)
		for _, parent := range potential {
			if containsBlock(status.parentsReady, parent) {
				continue
			}
			status.parentsReady = append(status.parentsReady, parent)
			if isLeaderWindowStart(next) {
				events = append(events, ConsensusEvent{Kind: ConsensusEventParentReady, Slot: next, Block: parent})
			}
		}
		if next > t.highestWithParentReady {
			t.highestWithParentReady = next
		}
	}
	return events
}

func (t *ParentReadyTracker) HasNotarFallbackOrStronger(block BlockID) bool {
	status := t.slots[block.Slot]
	return status != nil && containsBlock(status.notarFallbacks, block)
}

func (t *ParentReadyTracker) Parents(slot uint64) []BlockID {
	status := t.slots[slot]
	if status == nil {
		return nil
	}
	parents := append([]BlockID(nil), status.parentsReady...)
	sort.Slice(parents, func(i, j int) bool { return blockLess(parents[i], parents[j]) })
	return parents
}

func (t *ParentReadyTracker) BlockProductionParent(slot uint64) BlockProductionParent {
	if t.highestWithParentReady > slot {
		return BlockProductionParent{Kind: BlockProductionParentMissedWindow}
	}
	parents := t.Parents(slot)
	if len(parents) == 0 {
		return BlockProductionParent{Kind: BlockProductionParentNotReady}
	}
	return BlockProductionParent{Kind: BlockProductionParentReady, Parent: parents[0]}
}

func (t *ParentReadyTracker) status(slot uint64) *parentReadyStatus {
	status := t.slots[slot]
	if status == nil {
		status = &parentReadyStatus{}
		t.slots[slot] = status
	}
	return status
}

func isLeaderWindowStart(slot uint64) bool { return slot%LeaderWindowSlots == 0 }

func containsBlock(blocks []BlockID, block BlockID) bool {
	for _, existing := range blocks {
		if existing == block {
			return true
		}
	}
	return false
}

func blockLess(a, b BlockID) bool {
	if a.Slot != b.Slot {
		return a.Slot < b.Slot
	}
	return bytes.Compare(a.Hash[:], b.Hash[:]) < 0
}
