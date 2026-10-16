package epochrewards

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
	"github.com/gagliardetto/solana-go/rpc"
)

const recordVersion = 1

// Record describes one account's inflation reward for an epoch.
type Record struct {
	Epoch         uint64 `json:"epoch"`
	EffectiveSlot uint64 `json:"effectiveSlot"`
	Address       string `json:"address"`
	Amount        uint64 `json:"amount"`
	PostBalance   uint64 `json:"postBalance"`
	Commission    *uint8 `json:"commission,omitempty"`
}

type rewardKey struct {
	epoch   uint64
	address string
}

type batch struct {
	Version  uint32          `json:"version"`
	Through  uint64          `json:"through"`
	Records  []Record        `json:"records"`
	Coverage []epochCoverage `json:"coverage,omitempty"`
}

type coverageKey struct {
	epoch    uint64
	complete bool
}

type epochCoverage struct {
	Epoch        uint64 `json:"epoch"`
	Slot         uint64 `json:"slot"`
	BoundarySlot uint64 `json:"boundarySlot"`
	Complete     bool   `json:"complete"`
}

// Store keeps speculative rewards in memory and persists them with the same
// rooted fold that commits the credited account balances.
type Store struct {
	dir               string
	retentionSlots    uint64
	rooted            atomic.Uint64
	mu                sync.RWMutex
	pending           map[rewardKey]Record
	persisted         map[rewardKey]Record
	sourceBatch       map[rewardKey]uint64
	batches           map[uint64][]rewardKey
	pendingCoverage   map[coverageKey]epochCoverage
	coverage          map[coverageKey]epochCoverage
	coverageBatch     map[coverageKey]uint64
	readable          uint64
	unpublished       map[uint64][]Record
	published         map[rewardKey]Record
	publishedCoverage map[coverageKey]epochCoverage
}

// Open loads or creates an epoch reward store.
func Open(dir string, retentionSlots uint64) (*Store, error) {
	if retentionSlots == 0 {
		return nil, errors.New("epoch reward retention must be greater than zero")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create epoch reward directory: %w", err)
	}
	store := &Store{
		dir:               dir,
		retentionSlots:    retentionSlots,
		pending:           make(map[rewardKey]Record),
		persisted:         make(map[rewardKey]Record),
		sourceBatch:       make(map[rewardKey]uint64),
		batches:           make(map[uint64][]rewardKey),
		pendingCoverage:   make(map[coverageKey]epochCoverage),
		coverage:          make(map[coverageKey]epochCoverage),
		coverageBatch:     make(map[coverageKey]uint64),
		unpublished:       make(map[uint64][]Record),
		published:         make(map[rewardKey]Record),
		publishedCoverage: make(map[coverageKey]epochCoverage),
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read epoch reward directory: %w", err)
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
			return nil, fmt.Errorf("load epoch reward batch %d: %w", through, err)
		}
		if loaded.Through != through {
			return nil, fmt.Errorf("load epoch reward batch %d: record contains through %d", through, loaded.Through)
		}
		store.install(loaded)
	}
	return store, nil
}

// RecordBlock adds inflation rewards from a successfully replayed block.
func (s *Store) RecordBlock(block *b.Block) error {
	if block == nil {
		return errors.New("record epoch rewards: nil block")
	}
	if block.Epoch == 0 {
		return nil
	}
	rewardedEpoch := block.Epoch - 1
	s.mu.Lock()
	defer s.mu.Unlock()
	var captured []Record
	for _, reward := range block.Rewards {
		if reward.Lamports < 0 || (reward.RewardType != rpc.RewardTypeVoting && reward.RewardType != rpc.RewardTypeStaking) {
			continue
		}
		address := reward.Pubkey.String()
		commission := cloneUint8(reward.Commission)
		record := Record{
			Epoch: rewardedEpoch, EffectiveSlot: block.Slot, Address: address,
			Amount: uint64(reward.Lamports), PostBalance: reward.PostBalance, Commission: commission,
		}
		s.pending[rewardKey{epoch: rewardedEpoch, address: address}] = record
		captured = append(captured, record)
	}
	if len(captured) != 0 {
		s.unpublished[block.Slot] = captured
	}
	return nil
}

// RecordCoverage records an observed epoch boundary and reward completion.
// A completion without the matching boundary cannot establish full history.
func (s *Store) RecordCoverage(epoch, slot uint64, boundary, complete bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	startKey := coverageKey{epoch: epoch}
	if boundary {
		s.pendingCoverage[startKey] = epochCoverage{Epoch: epoch, Slot: slot, BoundarySlot: slot}
	}
	if !complete {
		return
	}
	start, ok := s.pendingCoverage[startKey]
	if !ok {
		start, ok = s.coverage[startKey]
	}
	if !ok || start.Slot > slot {
		return
	}
	endKey := coverageKey{epoch: epoch, complete: true}
	if end, ok := s.pendingCoverage[endKey]; ok && end.BoundarySlot == start.Slot {
		return
	}
	if end, ok := s.coverage[endKey]; ok && end.BoundarySlot == start.Slot {
		return
	}
	s.pendingCoverage[endKey] = epochCoverage{Epoch: epoch, Slot: slot, BoundarySlot: start.Slot, Complete: true}
}

// Prepare writes rewards through a fold boundary before the fold is committed.
func (s *Store) Prepare(through uint64) error {
	s.mu.RLock()
	records := make([]Record, 0)
	for _, record := range s.pending {
		if record.EffectiveSlot <= through {
			records = append(records, record)
		}
	}
	coverage := make([]epochCoverage, 0)
	for _, event := range s.pendingCoverage {
		if event.Slot <= through {
			coverage = append(coverage, event)
		}
	}
	s.mu.RUnlock()
	if len(records) == 0 && len(coverage) == 0 {
		return nil
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].Epoch != records[j].Epoch {
			return records[i].Epoch < records[j].Epoch
		}
		return records[i].Address < records[j].Address
	})
	sort.Slice(coverage, func(i, j int) bool {
		if coverage[i].Epoch != coverage[j].Epoch {
			return coverage[i].Epoch < coverage[j].Epoch
		}
		return !coverage[i].Complete && coverage[j].Complete
	})
	payload := batch{Version: recordVersion, Through: through, Records: records, Coverage: coverage}
	tmp, err := os.CreateTemp(s.dir, ".epoch-rewards-*")
	if err != nil {
		return fmt.Errorf("create epoch reward temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	writeErr := json.NewEncoder(tmp).Encode(payload)
	if writeErr == nil {
		writeErr = tmp.Sync()
	}
	closeErr := tmp.Close()
	if writeErr != nil {
		return fmt.Errorf("write epoch rewards through slot %d: %w", through, writeErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close epoch rewards through slot %d: %w", through, closeErr)
	}
	if err := os.Rename(tmpPath, s.batchPath(through)); err != nil {
		return fmt.Errorf("publish epoch rewards through slot %d: %w", through, err)
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
	keys := make([]rewardKey, 0, len(value.Records))
	for _, record := range value.Records {
		key := rewardKey{epoch: record.Epoch, address: record.Address}
		s.persisted[key] = record
		s.sourceBatch[key] = value.Through
		keys = append(keys, key)
	}
	s.batches[value.Through] = keys
	for _, event := range value.Coverage {
		key := coverageKey{epoch: event.Epoch, complete: event.Complete}
		s.coverage[key] = event
		s.coverageBatch[key] = value.Through
	}
}

// SetRooted sets the durable reward view and prunes stale batches.
func (s *Store) SetRooted(slot uint64) error {
	s.mu.Lock()
	if slot < s.rooted.Load() {
		// A failed prune must not expose rewards above the recovered account root.
		s.rooted.Store(slot)
		s.resetPublishedLocked()
	}
	for key, record := range s.pending {
		if record.EffectiveSlot <= slot {
			delete(s.pending, key)
		}
	}
	for key, event := range s.pendingCoverage {
		if event.Slot <= slot {
			delete(s.pendingCoverage, key)
		}
	}
	pruneBefore := uint64(0)
	if slot > s.retentionSlots {
		pruneBefore = slot - s.retentionSlots
	}
	removed := false
	for through := range s.batches {
		if through > slot {
			if err := s.restoreRetainedRecords(pruneBefore, slot); err != nil {
				s.mu.Unlock()
				return err
			}
			break
		}
	}
	for through, keys := range s.batches {
		orphaned := through > slot
		expired := pruneBefore > 0 && through <= pruneBefore
		if !orphaned && !expired {
			continue
		}
		if err := os.Remove(s.batchPath(through)); err != nil && !os.IsNotExist(err) {
			s.mu.Unlock()
			return fmt.Errorf("prune epoch reward batch %d: %w", through, err)
		}
		removed = true
		for _, key := range keys {
			if s.sourceBatch[key] == through {
				delete(s.persisted, key)
				delete(s.sourceBatch, key)
			}
		}
		delete(s.batches, through)
		s.removeCoverageBatch(through)
	}
	s.mu.Unlock()
	if removed {
		if err := syncDir(s.dir); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.rooted.Store(slot)
	s.readable = max(s.readable, slot)
	for key, record := range s.published {
		if record.EffectiveSlot <= slot {
			delete(s.published, key)
		}
	}
	for key, event := range s.publishedCoverage {
		if event.Slot <= slot {
			delete(s.publishedCoverage, key)
		}
	}
	for pendingSlot := range s.unpublished {
		if pendingSlot <= slot {
			delete(s.unpublished, pendingSlot)
		}
	}
	s.mu.Unlock()
	return nil
}

// Rewind removes speculative rewards produced by a discarded fork suffix.
func (s *Store) Rewind(fromSlot uint64) error {
	s.mu.Lock()
	// Invalidate the read overlay before any fallible disk cleanup.
	s.resetPublishedLocked()
	for slot := range s.unpublished {
		if slot >= fromSlot {
			delete(s.unpublished, slot)
		}
	}
	for key, record := range s.pending {
		if record.EffectiveSlot >= fromSlot {
			delete(s.pending, key)
		}
	}
	for key, event := range s.pendingCoverage {
		if event.Slot >= fromSlot {
			delete(s.pendingCoverage, key)
		}
	}
	removed := false
	for through := range s.batches {
		if through >= fromSlot && through > s.rooted.Load() {
			// Keep every committed batch, including when fromSlot is below the root.
			limit := max(fromSlot, s.rooted.Load()+1) - 1
			if err := s.restoreRetainedRecords(0, limit); err != nil {
				s.mu.Unlock()
				return err
			}
			break
		}
	}
	for through, keys := range s.batches {
		if through < fromSlot || through <= s.rooted.Load() {
			continue
		}
		if err := os.Remove(s.batchPath(through)); err != nil && !os.IsNotExist(err) {
			s.mu.Unlock()
			return fmt.Errorf("rewind epoch reward batch %d: %w", through, err)
		}
		removed = true
		for _, key := range keys {
			if s.sourceBatch[key] == through {
				delete(s.persisted, key)
				delete(s.sourceBatch, key)
			}
		}
		delete(s.batches, through)
		s.removeCoverageBatch(through)
	}
	s.mu.Unlock()
	if removed {
		return syncDir(s.dir)
	}
	return nil
}

// An interrupted multi-chunk fold can leave the same reward in several batches.
// Removing an uncommitted batch must reveal the preceding committed record.
func (s *Store) restoreRetainedRecords(pruneBefore, throughLimit uint64) error {
	throughs := make([]uint64, 0, len(s.batches))
	for through := range s.batches {
		if through <= throughLimit && (pruneBefore == 0 || through > pruneBefore) {
			throughs = append(throughs, through)
		}
	}
	sort.Slice(throughs, func(i, j int) bool { return throughs[i] < throughs[j] })
	persisted := make(map[rewardKey]Record)
	sources := make(map[rewardKey]uint64)
	coverage := make(map[coverageKey]epochCoverage)
	coverageBatches := make(map[coverageKey]uint64)
	for _, through := range throughs {
		value, err := readBatch(s.batchPath(through))
		if err != nil {
			return fmt.Errorf("restore epoch reward batch %d: %w", through, err)
		}
		if value.Through != through {
			return fmt.Errorf("restore epoch reward batch %d: record contains through %d", through, value.Through)
		}
		for _, record := range value.Records {
			key := rewardKey{epoch: record.Epoch, address: record.Address}
			persisted[key] = record
			sources[key] = through
		}
		for _, event := range value.Coverage {
			key := coverageKey{epoch: event.Epoch, complete: event.Complete}
			coverage[key], coverageBatches[key] = event, through
		}
	}
	s.persisted, s.sourceBatch = persisted, sources
	s.coverage, s.coverageBatch = coverage, coverageBatches
	return nil
}

func (s *Store) removeCoverageBatch(through uint64) {
	for key, source := range s.coverageBatch {
		if source == through {
			delete(s.coverage, key)
			delete(s.coverageBatch, key)
		}
	}
}

// EpochAvailable reports complete, retained history at the durable account root.
func (s *Store) EpochAvailable(epoch uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.epochAvailableLocked(epoch, s.rooted.Load(), false)
}

func (s *Store) epochAvailableLocked(epoch, root uint64, published bool) bool {
	startKey, endKey := coverageKey{epoch: epoch}, coverageKey{epoch: epoch, complete: true}
	coverage := func(key coverageKey) (epochCoverage, bool) {
		if published {
			if event, ok := s.publishedCoverage[key]; ok {
				return event, event.Slot <= root
			}
		}
		event, ok := s.coverage[key]
		return event, ok && event.Slot <= s.rooted.Load() && s.coverageBatch[key] <= s.rooted.Load()
	}
	start, started := coverage(startKey)
	end, completed := coverage(endKey)
	if !started || !completed || start.Slot != start.BoundarySlot || end.BoundarySlot != start.Slot || end.Slot < start.Slot {
		return false
	}
	if start.Slot > root || end.Slot > root {
		return false
	}
	// Retention follows durable folds, not process-local read publication. A
	// later completion marker cannot certify an expired boundary batch.
	durable := s.rooted.Load()
	return durable <= s.retentionSlots || start.Slot > durable-s.retentionSlots
}

// Get returns an address's reward for an epoch.
func (s *Store) Get(epoch uint64, address string, includePending bool) (Record, bool) {
	key := rewardKey{epoch: epoch, address: address}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if includePending {
		if record, ok := s.pending[key]; ok {
			return record, true
		}
	}
	record, ok := s.persisted[key]
	if !ok || record.EffectiveSlot > s.rooted.Load() || s.sourceBatch[key] > s.rooted.Load() {
		return Record{}, false
	}
	return record, true
}

// RootedSlot returns the durable reward watermark.
func (s *Store) RootedSlot() uint64 {
	return s.rooted.Load()
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
		return batch{}, fmt.Errorf("unsupported epoch reward version %d", value.Version)
	}
	seen := make(map[coverageKey]bool, len(value.Coverage))
	for _, event := range value.Coverage {
		key := coverageKey{epoch: event.Epoch, complete: event.Complete}
		if event.Slot > value.Through || event.BoundarySlot > event.Slot || (!event.Complete && event.BoundarySlot != event.Slot) || seen[key] {
			return batch{}, fmt.Errorf("invalid epoch reward coverage in batch %d", value.Through)
		}
		seen[key] = true
	}
	return value, nil
}

func syncDir(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open epoch reward directory: %w", err)
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return fmt.Errorf("sync epoch reward directory: %w", err)
	}
	if err := dir.Close(); err != nil {
		return fmt.Errorf("close epoch reward directory: %w", err)
	}
	return nil
}

func cloneUint8(value *uint8) *uint8 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
