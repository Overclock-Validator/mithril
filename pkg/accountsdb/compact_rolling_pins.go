package accountsdb

import (
	"fmt"
)

type retainedManifest struct {
	name, path string
	hdr        ManifestHeader
	parked     bool
}

type pinManifestIdentity struct {
	header      ManifestHeader
	size, mtime int64
	crc         uint32
	parked      bool
}

type pinLocation struct {
	file, offset uint64
}

// The ordinary pin maps already encode the first reference. Store a count
// only when another source, including a parked manifest, pins the same target.
type pinExtraRefs struct {
	whole   map[uint64]uint64
	records map[pinLocation]uint64
}

func newPinRefs() *pinExtraRefs {
	return &pinExtraRefs{make(map[uint64]uint64), make(map[pinLocation]uint64)}
}

func newCompactionPins() *compactionPins {
	return &compactionPins{whole: make(map[uint64]struct{}), records: make(map[uint64]map[uint64][32]byte)}
}

func (pins *compactionPins) addWhole(file uint64, refs *pinExtraRefs) {
	if refs != nil {
		if _, present := pins.whole[file]; present {
			refs.whole[file]++
		}
	}
	pins.whole[file] = struct{}{}
}

func (pins *compactionPins) readPinManifests(manifests []retainedManifest, floor uint64, scan *RewindScanStats, refs *pinExtraRefs) error {
	var scratch pinManifestScratch
	for _, k := range manifests {
		if k.parked || k.hdr.BatchSeq >= floor {
			pins.addWhole(k.hdr.FileId, refs)
		}
		err := scratch.visit(k.path, func(r ManifestRecord) error {
			if (!k.parked && k.hdr.BatchSeq <= floor) || !r.PrevValid {
				return nil
			}
			file, offset := r.Prev.FileId, r.Prev.Offset
			if k.parked {
				pins.addWhole(file, refs)
			}
			records := pins.records[file]
			if records == nil {
				records = make(map[uint64][32]byte)
				pins.records[file] = records
			}
			if old, exists := records[offset]; exists {
				if old != r.Pubkey {
					return fmt.Errorf("conflicting undo pointers for file %d offset %d", file, offset)
				}
				if refs != nil {
					refs.records[pinLocation{file, offset}]++
				}
			}
			records[offset] = r.Pubkey
			return nil
		})
		if err != nil {
			return fmt.Errorf("cannot establish rewind pins from %s: %w", k.name, err)
		}
		scan.DecodedManifests++
		scan.DecodedRecords += int64(scratch.records)
	}
	return nil
}

type rollingPinState struct {
	horizon, floor, validationFloor uint64
	identities                      map[string]pinManifestIdentity
	refs                            *pinExtraRefs
}

// Reclamation can remove manifests that no longer require validation after the
// verified floor was persisted. Every other old identity must still match.
func (old *rollingPinState) sameHistory(ids map[string]pinManifestIdentity, newHead string, validationFloor uint64) bool {
	if validationFloor < old.validationFloor {
		return false
	}
	for name, id := range old.identities {
		if next, exists := ids[name]; exists {
			if next != id || name == newHead {
				return false
			}
		} else if id.parked || id.header.BatchSeq >= validationFloor {
			return false
		}
	}
	for name := range ids {
		if _, exists := old.identities[name]; !exists && name != newHead {
			return false
		}
	}
	return true
}

// An ambiguous active boundary is validated by the ordinary rebuild/floor
// checks. Parked manifests have independent, non-expiring contributions.
func currentBoundary(ids map[string]pinManifestIdentity, floor uint64) (file uint64, unique bool) {
	for _, id := range ids {
		if id.header.BatchSeq != floor || id.parked {
			continue
		}
		if unique {
			return 0, false
		}
		file, unique = id.header.FileId, true
	}
	return
}

func (db *AccountsDb) rollingPinSet(head foldMeta, horizon, floor, validationFloor uint64, ids map[string]pinManifestIdentity,
	keep []retainedManifest, uniqueHead bool, headIndex int, sig string, scan *RewindScanStats) (*compactionPins, error) {
	cache := db.compactPinCache
	state := &rollingPinState{horizon: horizon, floor: floor, validationFloor: validationFloor, identities: ids}
	var pins *compactionPins
	if cache != nil && cache.rolling != nil && cache.rolling.horizon == horizon {
		old := cache.rolling
		switch {
		case head == cache.head && floor == old.floor && sig == cache.sig:
			scan.Path = "hit"
			return cache.pinned, nil
		case head == cache.head && floor == old.floor && old.sameHistory(ids, "", validationFloor):
			pins, state.refs, scan.Path = cache.pinned, old.refs, "hit"
		case uniqueHead && head.BatchSeq > cache.head.BatchSeq && head.BatchSeq-cache.head.BatchSeq == 1 &&
			head.ThroughSlot > cache.head.ThroughSlot && head.BatchSeq > floor &&
			(floor == old.floor || (floor > old.floor && floor-old.floor == 1)) &&
			old.sameHistory(ids, keep[headIndex].name, validationFloor):
			updated, err := cache.updateVerifiedWindow(keep, headIndex, floor, scan)
			if err != nil {
				return nil, err
			}
			if updated {
				pins, state.refs = cache.pinned, old.refs
			}
		}
	}
	if pins == nil {
		pins, state.refs = newCompactionPins(), newPinRefs()
		if err := pins.readPinManifests(keep, floor, scan, state.refs); err != nil {
			return nil, err
		}
		scan.Path = "full"
	}
	db.compactPinCache = &compactionPinCache{head: head, sig: sig, pinned: pins, rolling: state}
	return pins, nil
}

// The old boundary loses its own whole-file pin. The new boundary keeps its
// whole-file pin, but its predecessor records are no longer undoable. Decode
// those predecessors from the verified boundary rather than saving a second
// copy of every location in every retained manifest.
func (c *compactionPinCache) updateVerifiedWindow(keep []retainedManifest, headIndex int, floor uint64, scan *RewindScanStats) (bool, error) {
	old := c.rolling
	remove, removeRefs := newCompactionPins(), newPinRefs()
	rolling := floor != old.floor
	if rolling {
		var boundary []retainedManifest
		for i, k := range keep {
			if !k.parked && k.hdr.BatchSeq == floor {
				if boundary != nil {
					return false, nil
				}
				boundary = keep[i : i+1]
			}
		}
		if boundary == nil {
			return false, nil
		}
		if err := remove.readPinManifests(boundary, old.floor, scan, removeRefs); err != nil {
			return false, err
		}
		delete(remove.whole, boundary[0].hdr.FileId)
		if old.floor != 0 {
			file, ok := currentBoundary(old.identities, old.floor)
			if !ok {
				return false, nil
			}
			remove.whole[file] = struct{}{}
		}
	}
	add, addRefs := newCompactionPins(), newPinRefs()
	if err := add.readPinManifests(keep[headIndex:headIndex+1], floor, scan, addRefs); err != nil {
		return false, err
	}
	// A new key can legally occupy a location whose last old reference just
	// expired. A cold rebuild resolves this rare overlap, and reports an error
	// if any retained manifest still expects the other key. Neither path has
	// changed the previous table before this check.
	for file, rs := range add.records {
		target := c.pinned.records[file]
		for offset, key := range rs {
			if prev, exists := target[offset]; exists && prev != key {
				return false, nil
			}
		}
	}
	if rolling {
		c.removeWindowRefs(remove, removeRefs)
		scan.Path = "roll"
	} else {
		scan.Path = "append"
	}
	c.addWindowRefs(add, addRefs)
	return true, nil
}

// All inputs were decoded and validated before mutating the shared map.
// Access to maps and reference counts is serialized by foldMu.
func (c *compactionPinCache) removeWindowRefs(remove *compactionPins, counts *pinExtraRefs) {
	pins, refs := c.pinned, c.rolling.refs
	for file := range remove.whole {
		if refs.whole[file] == 0 {
			delete(pins.whole, file)
		} else if refs.whole[file] == 1 {
			delete(refs.whole, file)
		} else {
			refs.whole[file]--
		}
	}
	for file, rs := range remove.records {
		target := pins.records[file]
		for offset := range rs {
			k := pinLocation{file, offset}
			// Both operands omit the first occurrence.
			left := refs.records[k] - counts.records[k]
			switch left {
			case 0:
				delete(target, offset)
				delete(refs.records, k)
			case 1:
				delete(refs.records, k)
			default:
				refs.records[k] = left - 1
			}
		}
		if len(target) == 0 {
			delete(pins.records, file)
		}
	}
}

func (c *compactionPinCache) addWindowRefs(add *compactionPins, counts *pinExtraRefs) {
	pins, refs := c.pinned, c.rolling.refs
	for file := range add.whole {
		pins.addWhole(file, refs)
	}
	for file, rs := range add.records {
		target := pins.records[file]
		if target == nil {
			pins.records[file] = rs
			continue
		}
		for offset, key := range rs {
			if _, exists := target[offset]; exists {
				refs.records[pinLocation{file, offset}]++
			}
			target[offset] = key
		}
	}
	// The new head is a single active manifest. Its only whole pin is its
	// data file; any repeated record references still need their own counts.
	for loc, extra := range counts.records {
		refs.records[loc] += extra
	}
}
