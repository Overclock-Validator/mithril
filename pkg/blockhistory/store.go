package blockhistory

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
)

const recordVersion = 1

var (
	ErrNotAvailable = errors.New("block history is not available")
	ErrSlotSkipped  = errors.New("slot was skipped")
)

type Record struct {
	Slot              uint64            `json:"slot"`
	ParentSlot        uint64            `json:"parentSlot,omitempty"`
	BlockHeight       uint64            `json:"blockHeight,omitempty"`
	Blockhash         string            `json:"blockhash,omitempty"`
	PreviousBlockhash string            `json:"previousBlockhash,omitempty"`
	BlockTime         *int64            `json:"blockTime"`
	Rewards           []rpc.BlockReward `json:"rewards"`
	Skipped           bool              `json:"skipped,omitempty"`
}

type batch struct {
	Version uint32   `json:"version"`
	Through uint64   `json:"through"`
	Records []Record `json:"records"`
}

// Store retains speculative summaries in memory and writes one immutable batch
// immediately before the matching AccountsDB fold commits. Readers are also
// bounded by rooted, so an abandoned fork or interrupted fold stays invisible.
type Store struct {
	dir            string
	retentionSlots uint64
	rooted         atomic.Uint64
	mu             sync.RWMutex
	pending        map[uint64]Record
	persisted      map[uint64]Record
	sourceBatch    map[uint64]uint64
	batches        map[uint64][]uint64
}

func Open(dir string, retentionSlots uint64) (*Store, error) {
	if retentionSlots == 0 {
		return nil, errors.New("block history retention must be greater than zero")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create block history directory: %w", err)
	}
	store := &Store{
		dir:            dir,
		retentionSlots: retentionSlots,
		pending:        make(map[uint64]Record),
		persisted:      make(map[uint64]Record),
		sourceBatch:    make(map[uint64]uint64),
		batches:        make(map[uint64][]uint64),
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read block history directory: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), "batch-") || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		through, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(entry.Name(), "batch-"), ".json"), 10, 64)
		if err != nil {
			continue
		}
		loaded, err := readBatch(filepath.Join(dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("load block history batch %d: %w", through, err)
		}
		if loaded.Through != through {
			return nil, fmt.Errorf("load block history batch %d: record contains through %d", through, loaded.Through)
		}
		store.install(loaded)
	}
	return store, nil
}

func (s *Store) RecordBlock(block *b.Block) error {
	if block == nil {
		return errors.New("record block history: nil block")
	}
	rewards := make([]rpc.BlockReward, len(block.Rewards))
	copy(rewards, block.Rewards)
	record := Record{
		Slot:              block.Slot,
		ParentSlot:        block.ParentSlot,
		BlockHeight:       block.BlockHeight,
		Blockhash:         solana.Hash(block.Blockhash).String(),
		PreviousBlockhash: solana.Hash(block.LastBlockhash).String(),
		Rewards:           rewards,
	}
	timestamp := block.UnixTimestamp
	if timestamp == 0 && block.FooterProducerTimeNanos != 0 {
		timestamp = int64(block.FooterProducerTimeNanos / 1_000_000_000)
	}
	if timestamp != 0 {
		record.BlockTime = &timestamp
	}
	s.mu.Lock()
	s.pending[record.Slot] = record
	s.mu.Unlock()
	return nil
}

func (s *Store) RecordSkipped(slot uint64) error {
	s.mu.Lock()
	s.pending[slot] = Record{Slot: slot, Skipped: true}
	s.mu.Unlock()
	return nil
}

// Prepare persists every pending outcome through the fold boundary. It must run
// before the corresponding AccountsDB commit; a leftover unselected batch is
// harmless because Get also checks the recovered rooted watermark.
func (s *Store) Prepare(through uint64) error {
	s.mu.RLock()
	records := make([]Record, 0)
	for slot, record := range s.pending {
		if slot <= through {
			records = append(records, record)
		}
	}
	s.mu.RUnlock()
	if len(records) == 0 {
		return nil
	}
	sort.Slice(records, func(i, j int) bool { return records[i].Slot < records[j].Slot })
	payload := batch{Version: recordVersion, Through: through, Records: records}
	tmp, err := os.CreateTemp(s.dir, ".block-history-*")
	if err != nil {
		return fmt.Errorf("create block history temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	writeErr := json.NewEncoder(tmp).Encode(payload)
	if writeErr == nil {
		writeErr = tmp.Sync()
	}
	closeErr := tmp.Close()
	if writeErr != nil {
		return fmt.Errorf("write block history through slot %d: %w", through, writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close block history through slot %d: %w", through, closeErr)
	}
	if err := os.Rename(tmpPath, s.batchPath(through)); err != nil {
		return fmt.Errorf("publish block history through slot %d: %w", through, err)
	}
	if err := syncDir(s.dir); err != nil {
		return err
	}
	s.mu.Lock()
	s.install(payload)
	s.mu.Unlock()
	return nil
}

func (s *Store) install(value batch) {
	slots := make([]uint64, 0, len(value.Records))
	for _, record := range value.Records {
		s.persisted[record.Slot] = record
		s.sourceBatch[record.Slot] = value.Through
		slots = append(slots, record.Slot)
	}
	s.batches[value.Through] = slots
}

func (s *Store) SetRooted(slot uint64) error {
	return s.setRooted(slot, false)
}

// Rewind adopts an older durable root and drops every speculative record from
// the abandoned replay attempt before that attempt is run again.
func (s *Store) Rewind(slot uint64) error {
	return s.setRooted(slot, true)
}

// DiscardUnrootedFrom drops a discarded fork suffix without changing the
// durable root. Pending records below from remain available for the next fold.
func (s *Store) DiscardUnrootedFrom(from uint64) error {
	s.mu.Lock()
	rooted := s.rooted.Load()
	if from <= rooted {
		s.mu.Unlock()
		return fmt.Errorf("cannot discard block history at or below rooted slot %d", rooted)
	}
	for slot := range s.pending {
		if slot >= from {
			delete(s.pending, slot)
		}
	}
	removed := false
	for through, slots := range s.batches {
		if through < from || through <= rooted {
			continue
		}
		if err := os.Remove(s.batchPath(through)); err != nil && !os.IsNotExist(err) {
			s.mu.Unlock()
			return fmt.Errorf("discard unrooted block history batch %d: %w", through, err)
		}
		removed = true
		for _, slot := range slots {
			if s.sourceBatch[slot] == through {
				delete(s.persisted, slot)
				delete(s.sourceBatch, slot)
			}
		}
		delete(s.batches, through)
	}
	s.mu.Unlock()
	if removed {
		return syncDir(s.dir)
	}
	return nil
}

func (s *Store) setRooted(slot uint64, rewind bool) error {
	s.rooted.Store(slot)
	s.mu.Lock()
	for pendingSlot := range s.pending {
		if rewind || pendingSlot <= slot {
			delete(s.pending, pendingSlot)
		}
	}
	pruneBefore := uint64(0)
	if slot > s.retentionSlots {
		pruneBefore = slot - s.retentionSlots
	}
	removed := false
	for through, slots := range s.batches {
		orphaned := through > slot
		expired := pruneBefore > 0 && through <= pruneBefore
		if !orphaned && !expired {
			continue
		}
		if err := os.Remove(s.batchPath(through)); err != nil && !os.IsNotExist(err) {
			s.mu.Unlock()
			return fmt.Errorf("prune block history batch %d: %w", through, err)
		}
		removed = true
		for _, recordSlot := range slots {
			if s.sourceBatch[recordSlot] == through {
				delete(s.persisted, recordSlot)
				delete(s.sourceBatch, recordSlot)
			}
		}
		delete(s.batches, through)
	}
	s.mu.Unlock()
	if removed {
		if err := syncDir(s.dir); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Rooted() uint64 { return s.rooted.Load() }

func (s *Store) Get(slot uint64) (Record, error) {
	if slot > s.rooted.Load() {
		return Record{}, ErrNotAvailable
	}
	s.mu.RLock()
	record, ok := s.persisted[slot]
	sourceBatch := s.sourceBatch[slot]
	s.mu.RUnlock()
	if !ok || sourceBatch > s.rooted.Load() {
		return Record{}, ErrNotAvailable
	}
	if record.Skipped {
		return Record{}, ErrSlotSkipped
	}
	return record, nil
}

func (s *Store) MinimumLedgerSlot() (uint64, error) { return s.first(false) }

func (s *Store) FirstAvailableBlock() (uint64, error) { return s.first(true) }

func (s *Store) first(requireBlock bool) (uint64, error) {
	rooted := s.rooted.Load()
	var first uint64
	found := false
	s.mu.RLock()
	defer s.mu.RUnlock()
	for slot, record := range s.persisted {
		if slot > rooted || s.sourceBatch[slot] > rooted || (requireBlock && record.Skipped) {
			continue
		}
		if !found || slot < first {
			first = slot
			found = true
		}
	}
	if !found {
		return 0, ErrNotAvailable
	}
	return first, nil
}

func (s *Store) batchPath(through uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("batch-%020d.json", through))
}

func readBatch(path string) (batch, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return batch{}, err
	}
	var value batch
	if err := json.Unmarshal(data, &value); err != nil {
		return batch{}, err
	}
	if value.Version != recordVersion {
		return batch{}, fmt.Errorf("unsupported block history version %d", value.Version)
	}
	return value, nil
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open block history directory: %w", err)
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return fmt.Errorf("sync block history directory: %w", err)
	}
	if err := dir.Close(); err != nil {
		return fmt.Errorf("close block history directory: %w", err)
	}
	return nil
}
