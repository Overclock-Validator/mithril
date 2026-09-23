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
	Version uint32   `json:"version"`
	Through uint64   `json:"through"`
	Records []Record `json:"records"`
}

// Store keeps speculative rewards in memory and persists them with the same
// rooted fold that commits the credited account balances.
type Store struct {
	dir            string
	retentionSlots uint64
	rooted         atomic.Uint64
	mu             sync.RWMutex
	pending        map[rewardKey]Record
	persisted      map[rewardKey]Record
	sourceBatch    map[rewardKey]uint64
	batches        map[uint64][]rewardKey
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
		dir:            dir,
		retentionSlots: retentionSlots,
		pending:        make(map[rewardKey]Record),
		persisted:      make(map[rewardKey]Record),
		sourceBatch:    make(map[rewardKey]uint64),
		batches:        make(map[uint64][]rewardKey),
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
	}
	return nil
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
	s.mu.RUnlock()
	if len(records) == 0 {
		return nil
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].Epoch != records[j].Epoch {
			return records[i].Epoch < records[j].Epoch
		}
		return records[i].Address < records[j].Address
	})
	payload := batch{Version: recordVersion, Through: through, Records: records}
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
}

// SetRooted advances the durable reward view and prunes stale batches.
func (s *Store) SetRooted(slot uint64) error {
	s.mu.Lock()
	for key, record := range s.pending {
		if record.EffectiveSlot <= slot {
			delete(s.pending, key)
		}
	}
	pruneBefore := uint64(0)
	if slot > s.retentionSlots {
		pruneBefore = slot - s.retentionSlots
	}
	removed := false
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
	}
	s.mu.Unlock()
	if removed {
		if err := syncDir(s.dir); err != nil {
			return err
		}
	}
	s.rooted.Store(slot)
	return nil
}

// Rewind removes speculative rewards produced by a discarded fork suffix.
func (s *Store) Rewind(fromSlot uint64) error {
	s.mu.Lock()
	for key, record := range s.pending {
		if record.EffectiveSlot >= fromSlot {
			delete(s.pending, key)
		}
	}
	removed := false
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
	}
	s.mu.Unlock()
	if removed {
		return syncDir(s.dir)
	}
	return nil
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
