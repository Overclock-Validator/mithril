package costmodel

// PackEntryBytesMax is the shred-safe entry-byte bound: we close a batch when
// the next microblock would not fit in one FEC set, so padding is at most one
// microblock. The slot cap is still min(this, SIMD-0525), decided at schedule
// time like Firedancer pack.
//
// https://github.com/firedancer-io/firedancer/pull/10791
func PackEntryBytesMax(slotMaxDataShreds, maxMicroblock uint64) uint64 {
	c := uint64(TypicalFECSetPayloadBytes)
	wmark := uint64(FECSetsPerBatch)*c - 8
	if maxMicroblock >= wmark {
		return 0
	}
	fecSets := slotMaxDataShreds / DataShredsPerFECSet
	first := (8 + maxMicroblock + c - 1) / c
	if first < 1 {
		first = 1
	}
	const lastFEC = 2 // leftover plus resigned ending tick
	if fecSets < first+lastFEC {
		return 0
	}
	middle := fecSets - first - lastFEC
	minBatch := wmark - maxMicroblock
	return middle * minBatch
}

// DefaultPackEntryBytes is min(shred-safe, SIMD-0525) minus one ending tick.
func DefaultPackEntryBytes() uint64 {
	shredSafe := PackEntryBytesMax(DefaultMaxDataShredsPerSlot, MaxMicroblockBytes)
	cap := uint64(DefaultMaxEntryBytesPerSlot)
	if shredSafe > 0 && shredSafe < cap {
		cap = shredSafe
	}
	if cap > EntryHeaderBytes {
		cap -= EntryHeaderBytes
	}
	return cap
}
