package epochrewards

import "slices"

// Publish advances the process-local read view through a verified canonical
// prefix. The caller must bound slot by the currently executed bank and apply
// the execution, finality and reward-completion gates before calling it.
// This does not commit accounts or change the durable recovery watermark.
func (s *Store) Publish(slot uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if slot <= s.readable {
		return
	}
	var slots []uint64
	for pendingSlot := range s.unpublished {
		if pendingSlot <= slot {
			slots = append(slots, pendingSlot)
		}
	}
	slices.Sort(slots)
	for _, pendingSlot := range slots {
		for _, record := range s.unpublished[pendingSlot] {
			s.published[rewardKey{epoch: record.Epoch, address: record.Address}] = record
		}
		delete(s.unpublished, pendingSlot)
	}
	for key, event := range s.pendingCoverage {
		if event.Slot <= slot {
			s.publishedCoverage[key] = event
		}
	}
	s.readable = slot
}

// ResetPublished discards the verified overlay. Recovered replay must verify
// pending capture again before it can be published; durable history is kept.
func (s *Store) ResetPublished() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resetPublishedLocked()
}

func (s *Store) resetPublishedLocked() {
	s.readable = s.rooted.Load()
	clear(s.published)
	clear(s.publishedCoverage)
	clear(s.unpublished)
	for _, record := range s.pending {
		s.unpublished[record.EffectiveSlot] = append(s.unpublished[record.EffectiveSlot], record)
	}
}

// PublishedSlot returns the verified read watermark, including the durable
// checkpoint when no newer process-local view is available.
func (s *Store) PublishedSlot() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.readable
}

// GetPublished returns a detached reward from the verified view. RPC callers
// serialize publication and recovery across the entire multi-address request.
func (s *Store) GetPublished(epoch uint64, address string) (Record, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := rewardKey{epoch: epoch, address: address}
	record, ok := s.published[key]
	if !ok {
		record, ok = s.persisted[key]
		ok = ok && record.EffectiveSlot <= s.rooted.Load() && s.sourceBatch[key] <= s.rooted.Load()
	}
	if !ok || record.EffectiveSlot > s.readable {
		return Record{}, false
	}
	record.Commission = cloneUint8(record.Commission)
	return record, true
}

// EpochPublished reports complete retained coverage in the verified read view.
func (s *Store) EpochPublished(epoch uint64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.epochAvailableLocked(epoch, s.readable, true)
}
