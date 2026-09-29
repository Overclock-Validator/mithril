package rpcserver

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"path/filepath"
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
)

var errStakeViewChanged = errors.New("rooted stake view changed")

const stakeCacheBatchSize = 512

type cachedDelegation struct {
	delegation  sealevel.Delegation
	active      uint64
	calculation uint64
	loaded      bool
}

// rootedStakeCache is disposable process-local derived state. It owns compact
// delegations and totals, never account buffers. Committed changes replace each
// delegation's contribution; speculative replay does not. Epoch/history/feature
// changes require RAM-only recalculation. Rewind, recovery and failed writes
// discard the view. No on-disk index or manifest format is changed here.
//
// RPC publication is separate: both bank identity and account version must
// match over the entire response. A commit alone cannot expose an unannounced
// bank. Initialization/recalculation merge concurrent commits in bounded chunks;
// they do not need replay to stop or one root to last for an entire stake scan.
type rootedStakeCache struct {
	mu          sync.Mutex
	building    chan struct{}
	version     uint64
	generation  uint64
	calculation uint64
	dataReady   bool
	totalsReady bool
	byStake     map[solana.PublicKey]cachedDelegation
	totals      map[solana.PublicKey]uint64
	// While candidate keys are being loaded/seeded, remember touched keys so an
	// old candidate cannot resurrect a concurrently closed account. Released as
	// soon as placeholders are seeded, before the account scan starts.
	bootstrapChanged map[solana.PublicKey]struct{}
	epoch            uint64
	history          sealevel.SysvarStakeHistory
	activationEpoch  *uint64
	// Work counters used by tests/benchmarks; protected by mu.
	scans          uint64
	recalculations uint64
	updates        uint64
	// Deterministic interleaving hooks; nil outside tests.
	afterBootstrapSeed       func()
	beforeRecalculationChunk func(int)
}

func stakeDelegation(account *accounts.Account) (sealevel.Delegation, bool) {
	if account == nil || account.Lamports == 0 || account.Owner != addresses.StakeProgramAddr {
		return sealevel.Delegation{}, false
	}
	state, err := sealevel.UnmarshalStakeState(account.Data)
	if err != nil || state.Status != sealevel.StakeStateV2StatusStake {
		return sealevel.Delegation{}, false
	}
	return state.Stake.Stake.Delegation, true
}

// AccountsChanged runs on the account writer: no I/O or whole-cache scan.
// Invalidate activation inputs before applying this batch's delegations, so
// correctness does not depend on the order of accounts in the batch.
func (c *rootedStakeCache) AccountsChanged(version uint64, changed []*accounts.Account, reset bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if reset || c.byStake == nil || c.version+2 != version {
		c.byStake, c.totals, c.bootstrapChanged = nil, nil, nil
		c.dataReady, c.totalsReady = false, false
		c.generation++
		c.version = version
		return
	}
	for _, account := range changed {
		if account != nil && (account.Key == sealevel.SysvarStakeHistoryAddr || account.Key == features.ReduceStakeWarmupCooldown.Address) {
			c.totals, c.totalsReady = nil, false
			c.calculation++
			break
		}
	}
	for _, account := range changed {
		if account == nil {
			continue
		}
		if c.bootstrapChanged != nil {
			c.bootstrapChanged[account.Key] = struct{}{}
		}
		old, known := c.byStake[account.Key]
		if !known && account.Owner != addresses.StakeProgramAddr {
			continue
		}
		if known && old.loaded && old.calculation == c.calculation && c.totals != nil {
			c.subtract(old)
		}
		delete(c.byStake, account.Key)
		if delegation, ok := stakeDelegation(account); ok {
			entry := cachedDelegation{delegation: delegation, loaded: true}
			if c.totals != nil {
				entry.active = delegation.Stake(c.epoch, &c.history, c.activationEpoch)
				entry.calculation = c.calculation
				if entry.active != 0 {
					c.totals[delegation.VoterPubkey] += entry.active
				}
			}
			c.byStake[account.Key] = entry
		}
		c.updates++
	}
	c.version = version
}

func (c *rootedStakeCache) subtract(entry cachedDelegation) {
	voter := entry.delegation.VoterPubkey
	total := c.totals[voter] - entry.active
	if total == 0 {
		delete(c.totals, voter)
	} else {
		c.totals[voter] = total
	}
}

// selectTotals copies only voter totals, or one total for a filtered request.
// Callers never receive mutable cache maps.
func (c *rootedStakeCache) selectTotals(vote *solana.PublicKey) map[solana.PublicKey]uint64 {
	if vote != nil {
		return map[solana.PublicKey]uint64{*vote: c.totals[*vote]}
	}
	return maps.Clone(c.totals)
}

func (s *RpcServer) rootedActivatedStakes(ctx context.Context, slot, epoch uint64, vote *solana.PublicKey) (map[solana.PublicKey]uint64, error) {
	s.stakeCacheOnce.Do(func() { s.acctsDb.ObserveAccountState(&s.stakeCache) })
	c := &s.stakeCache
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		bank := s.rootedBank.Load()
		version, stable := s.acctsDb.CommittedAccountVersion()
		if bank == nil || bank.Slot != slot || !stable || s.rootedPublicationPending(slot) {
			return nil, errStakeViewChanged
		}
		c.mu.Lock()
		// Do not clear a newer cache if a write completed between the initial
		// version read and acquiring the cache lock.
		current, valid := s.acctsDb.CommittedAccountVersion()
		if !valid || current != version || s.rootedBank.Load() != bank {
			c.mu.Unlock()
			return nil, errStakeViewChanged
		}
		if c.dataReady && c.totalsReady && c.version == version && c.epoch == epoch {
			totals := c.selectTotals(vote)
			c.mu.Unlock()
			return totals, nil
		}
		if done := c.building; done != nil {
			c.mu.Unlock()
			select {
			case <-done:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		c.building = make(chan struct{})
		cold := !c.dataReady || c.version != version
		if cold {
			c.generation++
			c.byStake = make(map[solana.PublicKey]cachedDelegation)
			c.bootstrapChanged = make(map[solana.PublicKey]struct{})
			c.dataReady, c.totalsReady = false, false
			c.totals = nil
			c.version = version
		}
		generation := c.generation
		c.mu.Unlock()

		var err error
		if cold {
			err = s.bootstrapStakeCache(ctx, generation)
		}
		if err == nil {
			err = s.recalculateStakeTotals(ctx, slot, epoch, generation)
		}
		c.mu.Lock()
		latest, valid := s.acctsDb.CommittedAccountVersion()
		if err == nil && (!valid || c.version != latest || s.rootedBank.Load() != bank || s.rootedPublicationPending(slot)) {
			err = errStakeViewChanged
		}
		var totals map[solana.PublicKey]uint64
		if err == nil {
			totals = c.selectTotals(vote)
		}
		close(c.building)
		c.building = nil
		c.mu.Unlock()
		return totals, err
	}
}
func (s *RpcServer) rootedStakeInputs(ctx context.Context, slot uint64) (sealevel.SysvarStakeHistory, *uint64, error) {
	var history sealevel.SysvarStakeHistory
	rooted, values, err := s.readRootedAccounts(ctx, []solana.PublicKey{sealevel.SysvarStakeHistoryAddr, features.ReduceStakeWarmupCooldown.Address})
	if err != nil {
		return history, nil, fmt.Errorf("read rooted stake history: %w", err)
	}
	if rooted.Slot != slot {
		return history, nil, errStakeViewChanged
	}
	if values[0] == nil || values[0].Lamports == 0 {
		return history, nil, fmt.Errorf("stake history unavailable at rooted slot %d", slot)
	}
	if err := history.UnmarshalWithDecoder(bin.NewBinDecoder(values[0].Data)); err != nil {
		return history, nil, fmt.Errorf("decode stake history at rooted slot %d: %w", slot, err)
	}
	var activation *uint64
	account := values[1]
	if account != nil && account.Lamports > 0 && account.Owner == addresses.FeatureAddr {
		var feature features.FeatureAcct
		if err := feature.UnmarshalWithDecoder(bin.NewBinDecoder(account.Data)); err != nil {
			return history, nil, fmt.Errorf("decode rooted stake warmup feature: %w", err)
		}
		if feature.ActivatedAt != nil && *feature.ActivatedAt <= slot {
			epoch := s.epochSchedule.GetEpoch(*feature.ActivatedAt)
			activation = &epoch
		}
	}
	return history, activation, nil
}

// bootstrapStakeCache seeds placeholders from the existing candidate index.
// Incoming commits win over placeholders, including deletion/owner changes.
// Each disk batch is coherent, but batches may come from different roots: the
// observer reconciles touched accounts to the latest committed state. Only a
// reset (rewind/recovery/failure), not ordinary root advancement, cancels this.
func (s *RpcServer) bootstrapStakeCache(ctx context.Context, generation uint64) (err error) {
	c := &s.stakeCache
	defer func() {
		if err != nil {
			c.mu.Lock()
			if c.generation == generation {
				c.byStake, c.bootstrapChanged = nil, nil
				c.dataReady = false
			}
			c.mu.Unlock()
		}
	}()
	// Read pending FIRST: if a flush moves entries to disk between the two
	// reads, either the pending snapshot or the subsequent file load contains them.
	pending := global.PendingStakeEntriesSnapshot()
	entries, err := global.LoadStakePubkeyIndex(filepath.Join(s.acctsDb.AcctsDir, ".."))
	if err != nil {
		return fmt.Errorf("load stake account index: %w", err)
	}
	for _, list := range [][]accountsdb.StakeIndexEntry{entries, pending} {
		for start := 0; start < len(list); start += stakeCacheBatchSize {
			if err = ctx.Err(); err != nil {
				return err
			}
			c.mu.Lock()
			if c.generation != generation {
				c.mu.Unlock()
				return errStakeViewChanged
			}
			for _, entry := range list[start:min(start+stakeCacheBatchSize, len(list))] {
				if _, changed := c.bootstrapChanged[entry.Pubkey]; !changed {
					if _, exists := c.byStake[entry.Pubkey]; !exists {
						c.byStake[entry.Pubkey] = cachedDelegation{}
					}
				}
			}
			c.mu.Unlock()
		}
	}
	c.mu.Lock()
	if c.generation != generation {
		c.mu.Unlock()
		return errStakeViewChanged
	}
	c.bootstrapChanged = nil
	c.mu.Unlock()
	if c.afterBootstrapSeed != nil {
		c.afterBootstrapSeed()
	}
	for _, list := range [][]accountsdb.StakeIndexEntry{entries, pending} {
		for start := 0; start < len(list); start += stakeCacheBatchSize {
			keys := make([]solana.PublicKey, 0, stakeCacheBatchSize)
			c.mu.Lock()
			if c.generation != generation {
				c.mu.Unlock()
				return errStakeViewChanged
			}
			for _, candidate := range list[start:min(start+stakeCacheBatchSize, len(list))] {
				if entry, exists := c.byStake[candidate.Pubkey]; exists && !entry.loaded {
					keys = append(keys, candidate.Pubkey)
				}
			}
			c.mu.Unlock()
			if len(keys) == 0 {
				continue
			}
			for {
				if err = ctx.Err(); err != nil {
					return err
				}
				version, stable := s.acctsDb.CommittedAccountVersion()
				if !stable {
					if err = waitForRootedPublication(ctx); err != nil {
						return err
					}
					continue
				}
				_, values, readErr := s.readRootedAccounts(ctx, keys)
				if readErr != nil {
					return readErr
				}
				c.mu.Lock()
				if c.generation != generation {
					c.mu.Unlock()
					return errStakeViewChanged
				}
				latest, stable := s.acctsDb.CommittedAccountVersion()
				if !stable || latest != version {
					c.mu.Unlock()
					continue
				}
				for i, account := range values {
					key := keys[i]
					// A commit already installed the latest value (or deleted it).
					if entry, exists := c.byStake[key]; !exists || entry.loaded {
						continue
					}
					delete(c.byStake, key)
					if delegation, ok := stakeDelegation(account); ok {
						c.byStake[key] = cachedDelegation{delegation: delegation, loaded: true}
					}
				}
				c.mu.Unlock()
				break
			}
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.generation != generation {
		return errStakeViewChanged
	}
	c.dataReady = true
	c.scans++
	return nil
}

// Recalculate in bounded chunks. A per-entry calculation tag makes concurrent
// committed updates part of the same sum exactly once. New accounts omitted
// from the starting key list are already included by the observer. An activation
// input change cancels this sum; ordinary account changes do not restart it.
func (s *RpcServer) recalculateStakeTotals(ctx context.Context, slot, epoch, generation uint64) (err error) {
	c := &s.stakeCache
	bank := s.rootedBank.Load()
	version, stable := s.acctsDb.CommittedAccountVersion()
	if bank == nil || bank.Slot != slot || !stable {
		return errStakeViewChanged
	}
	history, activation, err := s.rootedStakeInputs(ctx, slot)
	if err != nil {
		return err
	}
	c.mu.Lock()
	latest, stable := s.acctsDb.CommittedAccountVersion()
	if !stable || latest != version || c.version != version || c.generation != generation || s.rootedBank.Load() != bank {
		c.mu.Unlock()
		return errStakeViewChanged
	}
	c.calculation++
	calculation := c.calculation
	c.epoch, c.history, c.activationEpoch = epoch, history, activation
	c.totals = make(map[solana.PublicKey]uint64)
	c.totalsReady = false
	keys := make([]solana.PublicKey, 0, len(c.byStake))
	for key := range c.byStake {
		keys = append(keys, key)
	}
	c.mu.Unlock()
	defer func() {
		if err != nil {
			c.mu.Lock()
			if c.generation == generation && c.calculation == calculation {
				c.totals = nil
				c.totalsReady = false
			}
			c.mu.Unlock()
		}
	}()
	for start := 0; start < len(keys); start += stakeCacheBatchSize {
		if c.beforeRecalculationChunk != nil {
			c.beforeRecalculationChunk(start)
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		c.mu.Lock()
		if c.generation != generation || c.calculation != calculation {
			c.mu.Unlock()
			return errStakeViewChanged
		}
		for _, key := range keys[start:min(start+stakeCacheBatchSize, len(keys))] {
			entry, exists := c.byStake[key]
			if !exists || entry.calculation == calculation {
				continue
			}
			entry.active = entry.delegation.Stake(epoch, &history, activation)
			entry.calculation = calculation
			c.byStake[key] = entry
			if entry.active != 0 {
				c.totals[entry.delegation.VoterPubkey] += entry.active
			}
		}
		c.mu.Unlock()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.generation != generation || c.calculation != calculation {
		return errStakeViewChanged
	}
	c.totalsReady = true
	c.recalculations++
	return nil
}
