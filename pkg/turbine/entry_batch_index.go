package turbine

import "math/bits"

// Three levels cover all 65,536 permitted data-shred indexes. Successor and
// predecessor queries touch a bounded number of words, even in adversarial order.
// The index is bounded (~24 KiB per retained slot) and allocated only when
// streaming preparation is enabled. It never rescans a slot's retained map.
const batchIndexWords = maxDataShredsPerSlot / 64

type shredIndexBits struct {
	words  [batchIndexWords]uint64
	groups [batchIndexWords / 64]uint64
	top    uint64
}

func (b *shredIndexBits) set(i uint32) {
	w, g := i/64, i/4096
	b.words[w] |= uint64(1) << (i % 64)
	b.groups[g] |= uint64(1) << (w % 64)
	b.top |= uint64(1) << g
}
func (b *shredIndexBits) clear(i uint32) {
	w, g := i/64, i/4096
	b.words[w] &^= uint64(1) << (i % 64)
	if b.words[w] == 0 {
		b.groups[g] &^= uint64(1) << (w % 64)
		if b.groups[g] == 0 {
			b.top &^= uint64(1) << g
		}
	}
}
func (b *shredIndexBits) next(i uint32) (uint32, bool) {
	if i >= maxDataShredsPerSlot {
		return 0, false
	}
	w, g := i/64, i/4096
	if x := b.words[w] & (^uint64(0) << (i % 64)); x != 0 {
		return w*64 + uint32(bits.TrailingZeros64(x)), true
	}
	x := b.groups[g] & (^uint64(0) << (w%64 + 1))
	if x == 0 {
		top := b.top & (^uint64(0) << (g + 1))
		if top == 0 {
			return 0, false
		}
		g = uint32(bits.TrailingZeros64(top))
		x = b.groups[g]
	}
	w = g*64 + uint32(bits.TrailingZeros64(x))
	return w*64 + uint32(bits.TrailingZeros64(b.words[w])), true
}
func (b *shredIndexBits) previous(i uint32) (uint32, bool) {
	if i >= maxDataShredsPerSlot {
		i = maxDataShredsPerSlot - 1
	}
	w, g := i/64, i/4096
	if x := b.words[w] & (^uint64(0) >> (63 - i%64)); x != 0 {
		return w*64 + uint32(63-bits.LeadingZeros64(x)), true
	}
	x := b.groups[g] & ((uint64(1) << (w % 64)) - 1)
	if x == 0 {
		top := b.top & ((uint64(1) << g) - 1)
		if top == 0 {
			return 0, false
		}
		g = uint32(63 - bits.LeadingZeros64(top))
		x = b.groups[g]
	}
	w = g*64 + uint32(63-bits.LeadingZeros64(x))
	return w*64 + uint32(63-bits.LeadingZeros64(b.words[w])), true
}

type entryBatchIndex struct {
	missing shredIndexBits
	ends    shredIndexBits
	emitted [batchIndexWords]uint64
}

func newEntryBatchIndex() *entryBatchIndex {
	b := new(entryBatchIndex)
	for i := range b.missing.words {
		b.missing.words[i] = ^uint64(0)
	}
	for i := range b.missing.groups {
		b.missing.groups[i] = ^uint64(0)
	}
	b.missing.top = (uint64(1) << len(b.missing.groups)) - 1
	return b
}

// One insertion can complete its containing batch, and, if it provides a new
// DATA_COMPLETE boundary, the immediately following batch. All other batches
// are unchanged. A range is emitted once, only when every data index is present.
// The preceding boundary is mandatory unless the range starts at index zero.
func (b *entryBatchIndex) add(i uint32, dataComplete bool) (ready [2]shredBatchRange, n int) {
	if i >= maxDataShredsPerSlot {
		return ready, 0
	}
	b.missing.clear(i)
	if dataComplete {
		b.ends.set(i)
	}
	if end, ok := b.ends.next(i); ok {
		if r, ok := b.complete(end); ok {
			ready[n] = r
			n++
		}
	}
	if dataComplete {
		if end, ok := b.ends.next(i + 1); ok {
			if r, ok := b.complete(end); ok {
				ready[n] = r
				n++
			}
		}
	}
	return
}
func (b *entryBatchIndex) complete(end uint32) (shredBatchRange, bool) {
	if b.emitted[end/64]&(uint64(1)<<(end%64)) != 0 {
		return shredBatchRange{}, false
	}
	start := uint32(0)
	if end > 0 {
		if prev, ok := b.ends.previous(end - 1); ok {
			start = prev + 1
		}
	}
	if missing, ok := b.missing.next(start); ok && missing <= end {
		return shredBatchRange{}, false
	}
	b.emitted[end/64] |= uint64(1) << (end % 64)
	return shredBatchRange{start, end}, true
}

func (s *slotState) discoverEntryBatch(sh *Shred) {
	ready, n := s.batchIndex.add(sh.Index, sh.DataComplete())
	for _, r := range ready[:n] {
		s.completeBatches = append(s.completeBatches, r)
		if s.pipelineTrace != nil && !s.pipelineTrace.sealed {
			s.pipelineTrace.discovered[r.start] = entryTraceNow()
		}
	}
}

// Seed once if preparation is installed on an assembler with existing data.
// Normal ingress creates the index with the first accepted data shred.
func (a *SlotAssembler) notePrefetchShredLocked(s *slotState, sh *Shred) {
	p := a.entryPrefetch
	if p == nil || p.closed || p.ctx.Err() != nil || sh.Type != ShredTypeData {
		return
	}
	if s.batchIndex == nil {
		s.batchIndex = newEntryBatchIndex()
		for _, existing := range s.shreds {
			s.discoverEntryBatch(existing)
		}
	} else {
		s.discoverEntryBatch(sh)
	}
}
