package turbine

import (
	"fmt"
	"sort"
)

// DecodeComponentsFromDataShreds deshreds DATA_COMPLETE batches into block components.
func DecodeComponentsFromDataShreds(shreds []*Shred) ([]BlockComponent, error) {
	if len(shreds) == 0 {
		return nil, nil
	}
	sort.Slice(shreds, func(i, j int) bool {
		return shreds[i].Index < shreds[j].Index
	})

	var components []BlockComponent
	var batchBytes []byte
	for _, shred := range shreds {
		if shred == nil || shred.Type != ShredTypeData {
			continue
		}
		batchBytes = append(batchBytes, shred.Data...)
		if !shred.DataComplete() {
			continue
		}
		component, err := unmarshalBlockComponentFromShredBatch(batchBytes)
		if err != nil {
			return nil, fmt.Errorf("decode component ending at shred %d: %w", shred.Index, err)
		}
		components = append(components, component)
		batchBytes = nil
	}
	if len(batchBytes) != 0 {
		return nil, fmt.Errorf("slot ended with %d undecoded component bytes", len(batchBytes))
	}
	return components, nil
}

// SlotComponents collects ordered block components recovered from a slot's shreds.
type SlotComponents struct {
	Slot       uint64
	ParentSlot uint64
	Components []BlockComponent
}

// AssembleSlotComponents rebuilds the Alpenglow component stream for a completed slot.
func AssembleSlotComponents(shreds []*Shred) (SlotComponents, error) {
	if len(shreds) == 0 {
		return SlotComponents{}, nil
	}
	components, err := DecodeComponentsFromDataShreds(shreds)
	if err != nil {
		return SlotComponents{}, err
	}
	var parentSlot uint64
	for _, shred := range shreds {
		if shred != nil && shred.Type == ShredTypeData {
			parentSlot = shred.ParentSlot()
			break
		}
	}
	return SlotComponents{
		Slot:       shreds[0].Slot,
		ParentSlot: parentSlot,
		Components: components,
	}, nil
}

// EntriesFromComponents returns ledger entries, skipping markers and abort sentinels.
func EntriesFromComponents(components []BlockComponent) []Entry {
	var entries []Entry
	for _, component := range components {
		if component.IsEntryBatch() {
			entries = append(entries, component.EntryBatch...)
		}
	}
	return entries
}
