package replay

// optionalUnrootedState preserves a nil interface for ordinary durable replay.
// Converting a nil *unrootedTail directly would incorrectly enable overlay reads.
func optionalUnrootedState(tail *unrootedTail) unrootedState {
	if tail == nil {
		return nil
	}
	return tail
}
