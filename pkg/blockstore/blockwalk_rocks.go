//go:build !lite

package blockstore

import (
	"fmt"
	"sort"

	"github.com/Overclock-Validator/mithril/pkg/shred"
	"github.com/linxGnu/grocksdb"
	"k8s.io/klog/v2"
)

// BlockWalk walks blocks in ascending order over multiple RocksDB databases.
type BlockWalk struct {
	handles       []WalkHandle // sorted
	shredRevision int

	root *grocksdb.Iterator
}

func NewBlockWalk(handles []WalkHandle, shredRevision int) (*BlockWalk, error) {
	if err := sortWalkHandles(handles, shredRevision); err != nil {
		return nil, err
	}
	return &BlockWalk{
		handles:       handles,
		shredRevision: shredRevision,
	}, nil
}

// Seek skips ahead to a specific slot.
// The caller must call BlockWalk.Next after Seek.
func (m *BlockWalk) Seek(slot uint64) bool {
	for len(m.handles) > 0 {
		h := m.handles[0]
		if slot < h.Start {
			// trying to Seek to slot below lowest available
			return false
		}
		if slot <= h.Stop {
			h.Start = slot
			return true
		}
		m.pop()
	}
	return false
}

// SlotsAvailable returns the number of contiguous slots that lay ahead.
func (m *BlockWalk) SlotsAvailable() (total uint64) {
	if len(m.handles) == 0 {
		return 0
	}
	start := m.handles[0].Start
	for _, h := range m.handles {
		if h.Start > start {
			return
		}
		stop := h.Stop + 1
		total += stop - start
		start = stop
	}
	return
}

// Next seeks to the next slot.
func (m *BlockWalk) Next() (meta *SlotMeta, ok bool) {
	for {
		if len(m.handles) == 0 {
			return nil, false
		}
		h := m.handles[0]
		if m.root == nil {
			// Open Next database
			m.root = h.DB.DB.NewIteratorCF(grocksdb.NewDefaultReadOptions(), h.DB.CfRoot)
			key := MakeSlotKey(h.Start)
			m.root.Seek(key[:])
		}
		if !m.root.Valid() {
			// Close current DB and go to Next
			m.pop()
			continue
		}
	
		// Get key at current position.
		slot, ok := ParseSlotKey(m.root.Key().Data())
		if !ok {
			klog.Exitf("Invalid slot key: %x", m.root.Key().Data())
		}
		if slot > h.Stop {
			m.pop()
			continue
		}
		h.Start = slot
	
		// Get value at current position.
		var err error
		meta, err = h.DB.GetSlotMeta(slot)
		if err != nil {
			// Invalid slot metas are irrecoverable.
			// The CAR generation process must stop here.
			klog.Errorf("FATAL: invalid slot meta at slot %d, aborting CAR generation: %s", slot, err)
			return nil, false
		}
	
		// Seek iterator to Next entry.
		m.root.Next()
	
		return meta, true
	}
	
}

// Entries returns the entries at the current cursor.
// Caller must have made an ok call to BlockWalk.Next before calling this.
func (m *BlockWalk) Entries(meta *SlotMeta) ([][]shred.Entry, error) {
	h := m.handles[0]
	mapping, err := h.DB.GetEntries(meta, m.shredRevision)
	if err != nil {
		return nil, err
	}
	batches := make([][]shred.Entry, len(mapping))
	for i, batch := range mapping {
		batches[i] = batch.Entries
	}
	return batches, nil
}

// pop closes the current open DB.
func (m *BlockWalk) pop() {
	m.root.Close()
	m.root = nil
	m.handles[0].DB.Close()
	m.handles = m.handles[1:]
}

func (m *BlockWalk) Close() {
	if m.root != nil {
		m.root.Close()
		m.root = nil
	}
	for _, h := range m.handles {
		h.DB.Close()
	}
	m.handles = nil
}

type WalkHandle struct {
	DB    *DB
	Start uint64
	Stop  uint64 // inclusive
}

// sortWalkHandles detects bounds of each DB and sorts handles.
func sortWalkHandles(h []WalkHandle, shredRevision int) error {
	for i, db := range h {
		// Find lowest and highest available slot in DB.
		start, err := getLowestCompletedSlot(db.DB, shredRevision)
		if err != nil {
			return err
		}
		stop, err := db.DB.MaxRoot()
		if err != nil {
			return err
		}
		h[i] = WalkHandle{
			Start: start,
			Stop:  stop,
			DB:    db.DB,
		}
	}
	sort.Slice(h, func(i, j int) bool {
		return h[i].Start < h[j].Start
	})
	return nil
}

// getLowestCompleteSlot finds the lowest slot in a RocksDB from which slots are complete onwards.
func getLowestCompletedSlot(d *DB, shredRevision int) (uint64, error) {
	iter := d.DB.NewIteratorCF(grocksdb.NewDefaultReadOptions(), d.CfMeta)
	defer iter.Close()
	iter.SeekToFirst()

	// The Solana validator periodically prunes old slots to keep database space bounded.
	// Therefore, the first (few) slots might have valid meta entries but missing data shreds.
	// To work around this, we simply start at the lowest meta and iterate until we find a complete entry.

	const maxTries = 32
	for i := 0; iter.Valid() && i < maxTries; i++ {
		slot, ok := ParseSlotKey(iter.Key().Data())
		if !ok {
			return 0, fmt.Errorf(
				"getLowestCompletedSlot(%s): choked on invalid slot key: %x", d.DB.Name(), iter.Key().Data())
		}

		// RocksDB row writes are atomic, therefore meta should never be broken.
		// If we fail to decode meta, bail as early as possible, as we cannot guarantee compatibility.
		meta, err := ParseBincode[SlotMeta](iter.Value().Data())
		if err != nil {
			return 0, fmt.Errorf(
				"getLowestCompletedSlot(%s): choked on invalid meta for slot %d", d.DB.Name(), slot)
		}

		if _, err = d.GetEntries(meta, shredRevision); err == nil {
			// Success!
			return slot, nil
		}

		iter.Next()
	}

	return 0, fmt.Errorf("failed to find a valid complete slot in DB: %s", d.DB.Name())
}
