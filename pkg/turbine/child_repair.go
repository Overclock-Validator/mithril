package turbine

import "time"

const childRepairLimit = 4
const childRepairLifetime = 2 * time.Second

// SetStreamRepairParent anchors lookahead to the generation currently executing.
// Zero clears the hint on discard/finalize. This grants fetching, never execution
// or fork choice: the child's parent block ID may not be verifiable until full.
func (a *SlotAssembler) SetStreamRepairParent(g StreamGeneration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.streamRepairInvalidChild = nil
	a.streamRepairParent = nil
	a.streamRepairChild = nil
	slot := g.Slot()
	if g.IsZero() || slot == 0 || a.streamSubscriber == nil {
		return
	}
	p := g.state
	if a.streamStatusLocked(g) == StreamGone {
		return
	}
	a.streamRepairParent = p
	a.streamRepairUntil = time.Now().Add(childRepairLifetime)
	// Reconcile a header published before replay installed the anchor.
	if slot == ^uint64(0) {
		return
	}
	c := a.slots[slot+1]
	if c == nil || c.prefetch == nil {
		return
	}
	b := c.prefetch.batches[0]
	if b == nil || b.ready == nil {
		return
	}
	select {
	case <-b.ready:
		a.noteChildRepairHeaderLocked(c, b)
	default:
	}
}

// Called by the asynchronous decoder, not replay's busy execution goroutine.
func (a *SlotAssembler) noteChildRepairHeaderLocked(s *slotState, b *prefetchedShredBatch) {
	p := a.streamRepairParent
	if p == nil || s == nil || a.slots[s.slot] != s || b == nil || !b.marker || b.parent == nil {
		return
	}
	if s == p && b.parent.FromUpdateParent {
		a.streamRepairParent = nil
		a.streamRepairChild = nil
		return
	}
	if p.slot == ^uint64(0) || s.slot != p.slot+1 {
		return
	}
	if b.parent.FromUpdateParent || b.parent.ParentSlot != p.slot {
		a.streamRepairInvalidChild = s
		a.streamRepairChild = nil
		return
	}
	if a.streamRepairInvalidChild == s || b.start != 0 || b.err != nil || b.parent.ParentSlot != p.slot || a.streamRepairChild == s {
		return
	}
	if time.Now().After(a.streamRepairUntil) || a.streamStatusLocked(StreamGeneration{slot: p.slot, state: p}) == StreamGone {
		return
	}
	a.streamRepairChild = s
	// Nonblocking channel send has no callback or lock acquisition. It uses the
	// existing coalesced/minimum-spacing repair scheduler, including under a.mu.
	select {
	case a.streamRepairWake <- struct{}{}:
	default:
	}
}

func (a *SlotAssembler) childRepairRequest(now time.Time) (SlotRepairRequest, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	p, c := a.streamRepairParent, a.streamRepairChild
	if a.streamSubscriber == nil || p == nil || c == nil || now.After(a.streamRepairUntil) {
		return SlotRepairRequest{}, false
	}
	if a.streamStatusLocked(StreamGeneration{slot: p.slot, state: p}) == StreamGone || a.slots[c.slot] != c || c.completing {
		return SlotRepairRequest{}, false
	}
	req, ok := c.repairRequestWithPrefix(childRepairLimit, true)
	if !ok || len(req.MissingDataShreds) == 0 {
		return SlotRepairRequest{}, false
	}
	// Only the earliest span, not four unrelated holes or highest-index probes.
	first := req.MissingDataShreds[0]
	end := first + 1
	for _, f := range c.fecSets {
		if f.haveLayout && f.fecSetIndex <= first && first < f.fecSetIndex+uint32(f.layout.dataShreds) {
			end = f.fecSetIndex + uint32(f.layout.dataShreds)
			break
		}
	}
	n := 1
	for n < len(req.MissingDataShreds) && req.MissingDataShreds[n] < end {
		n++
	}
	req.MissingDataShreds = req.MissingDataShreds[:n]
	req.NeedHighestDataShred = false
	return req, true
}
