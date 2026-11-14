package epochstakes

// CacheView retains immutable epoch payloads without copying validator maps.
// The outer view is private so callers cannot mutate snapshots after capture.
type CacheView struct {
	epochs []Snapshot
}

func (cache *EpochStakesCache) CaptureView() CacheView {
	cache.mu.RLock()
	defer cache.mu.RUnlock()
	view := CacheView{epochs: make([]Snapshot, 0, len(cache.stakeCache))}
	for epoch, stakes := range cache.stakeCache {
		view.epochs = append(view.epochs, Snapshot{Epoch: epoch, Generation: cache.generations[epoch], Stakes: stakes,
			VoteAccounts: cache.voteAcctCache[epoch], TotalStake: cache.totalStakeCache[epoch]})
	}
	return view
}

// RestoreView installs one retained bank's epoch material. Give restored and
// removed epochs fresh generations so verifier caches cannot reuse material
// derived from the abandoned sibling, including a same-epoch fork.
func (cache *EpochStakesCache) RestoreView(view CacheView) {
	cache.mu.Lock()
	defer cache.mu.Unlock()
	cache.ensureMapsLocked()
	for epoch := range cache.stakeCache {
		cache.bumpGenerationLocked(epoch)
	}
	clear(cache.stakeCache)
	clear(cache.voteAcctCache)
	clear(cache.totalStakeCache)
	for _, snapshot := range view.epochs {
		cache.stakeCache[snapshot.Epoch] = snapshot.Stakes
		cache.voteAcctCache[snapshot.Epoch] = snapshot.VoteAccounts
		cache.totalStakeCache[snapshot.Epoch] = snapshot.TotalStake
		cache.bumpGenerationLocked(snapshot.Epoch)
	}
}
