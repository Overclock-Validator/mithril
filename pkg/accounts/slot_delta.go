package accounts

// SlotDelta is one executed slot's account writes. Batch durable folds consume
// strictly ascending deltas and keep only the newest version of each key.
type SlotDelta struct {
	Slot  uint64
	Delta []*Account
}
