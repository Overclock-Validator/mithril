package scheduler

// Keeping every queued transaction's expanded instruction/account structures
// cost more than 750 MiB in a mainnet heap profile. Keep a small eager cache;
// admission, reward ordering and the 131,072-transaction fallback queue remain
// independent of whether an entry owns a preparation token.
const defaultMaxPreparedTransactions = 2048

func (s *Scheduler) prepareRetained(e *entry) {
	preparer := s.preparer.Load()
	if preparer == nil {
		return
	}
	for {
		used := s.preparedRetained.Load()
		if used >= s.maxPrepared {
			return
		}
		if s.preparedRetained.CompareAndSwap(used, used+1) {
			break
		}
	}
	e.preparationSlot = true
	e.prepared = preparer.Prepare(e.tx)
	if e.prepared == nil {
		s.releasePrepared(e)
		return
	}
	s.addStat(func(st *Stats) { st.PreparationEager++ })
}

// The caller owns e exclusively (before insertion, after eviction/pop, or
// under Buffer.mu during cleanup). A popped entry keeps its token across
// temporary retries, so retrying cannot exceed the retained cache bound.
func (s *Scheduler) releasePrepared(e *entry) {
	if e != nil && e.preparationSlot {
		e.preparationSlot = false
		e.prepared = nil
		s.preparedRetained.Add(-1)
	}
}
