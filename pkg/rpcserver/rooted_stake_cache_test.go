package rpcserver

import (
	"bytes"
	"context"
	"math"
	"math/rand"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	bin "github.com/gagliardetto/binary"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func cacheStakeAccount(t testing.TB, key, voter byte, amount uint64, activation, deactivation uint64) *accounts.Account {
	t.Helper()
	state := &sealevel.StakeStateV2{Status: sealevel.StakeStateV2StatusStake}
	state.Stake.Stake.Delegation = sealevel.Delegation{VoterPubkey: solana.PublicKey{voter}, StakeLamports: amount, ActivationEpoch: activation, DeactivationEpoch: deactivation, WarmupCooldownRate: 0.25}
	data, err := sealevel.MarshalStakeStake(state)
	require.NoError(t, err)
	return &accounts.Account{Key: solana.PublicKey{key}, Owner: addresses.StakeProgramAddr, Lamports: amount + 1, Data: data}
}

func cacheHistoryAccount(t testing.TB, history sealevel.SysvarStakeHistory) *accounts.Account {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, history.MarshalWithEncoder(bin.NewBinEncoder(&buf)))
	return &accounts.Account{Key: sealevel.SysvarStakeHistoryAddr, Owner: addresses.SysvarOwnerAddr, Lamports: 1, Data: buf.Bytes()}
}

func seedStakeCacheServer(t *testing.T, initial ...*accounts.Account) *RpcServer {
	t.Helper()
	db := newRPCAccountsDB(t)
	global.ClearPendingStakePubkeys()
	for _, account := range initial {
		global.EnqueuePendingStakePubkey(10, account.Key)
	}
	// Use existing candidate-index publication, including its cache invalidation.
	_, err := global.FlushPendingStakePubkeys(filepath.Dir(db.AcctsDir))
	require.NoError(t, err)
	initial = append(initial, cacheHistoryAccount(t, nil))
	_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: 10, Delta: initial}}, 10, nil, nil)
	require.NoError(t, err)
	s := &RpcServer{acctsDb: db, epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: 100}}
	s.SetRootedBankState(10, 10, 0)
	t.Cleanup(global.ClearPendingStakePubkeys)
	return s
}

func commitCacheAccounts(t *testing.T, s *RpcServer, slot uint64, changed ...*accounts.Account) {
	t.Helper()
	for _, account := range changed {
		if account.Owner == addresses.StakeProgramAddr {
			global.EnqueuePendingStakePubkey(slot, account.Key)
		}
	}
	_, err := s.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: changed}}, slot, nil, nil)
	require.NoError(t, err)
	s.SetRootedBankState(slot, slot, 0)
}

func cacheTotals(t *testing.T, s *RpcServer) map[solana.PublicKey]uint64 {
	t.Helper()
	bank, _ := s.getRootedBankState()
	totals, err := s.rootedActivatedStakes(t.Context(), bank.Slot, s.epochSchedule.GetEpoch(bank.Slot), nil)
	require.NoError(t, err)
	return totals
}

func TestRootedStakeCacheIncrementalChangesAndRewind(t *testing.T) {
	s := seedStakeCacheServer(t, cacheStakeAccount(t, 21, 1, 100, math.MaxUint64, math.MaxUint64), cacheStakeAccount(t, 22, 2, 200, math.MaxUint64, math.MaxUint64))
	require.Equal(t, map[solana.PublicKey]uint64{{1}: 100, {2}: 200}, cacheTotals(t, s))
	require.Equal(t, uint64(1), s.stakeCache.scans)
	// RPC callers cannot mutate the retained totals.
	got := cacheTotals(t, s)
	got[solana.PublicKey{1}] = 999
	voter := solana.PublicKey{1}
	filtered, err := s.rootedActivatedStakes(t.Context(), 10, 0, &voter)
	require.NoError(t, err)
	require.Equal(t, map[solana.PublicKey]uint64{{1}: 100}, filtered)

	commitCacheAccounts(t, s, 11, cacheStakeAccount(t, 21, 2, 150, math.MaxUint64, math.MaxUint64), cacheStakeAccount(t, 23, 1, 300, math.MaxUint64, math.MaxUint64))
	require.Equal(t, map[solana.PublicKey]uint64{{1}: 300, {2}: 350}, cacheTotals(t, s))
	commitCacheAccounts(t, s, 12, &accounts.Account{Key: solana.PublicKey{22}, Owner: addresses.SystemProgramAddr, Lamports: 5}, &accounts.Account{Key: solana.PublicKey{23}})
	require.Equal(t, map[solana.PublicKey]uint64{{2}: 150}, cacheTotals(t, s))
	commitCacheAccounts(t, s, 13, &accounts.Account{Key: solana.PublicKey{21}, Owner: addresses.StakeProgramAddr, Lamports: 1, Data: []byte{0}})
	require.Empty(t, cacheTotals(t, s))
	require.Equal(t, uint64(1), s.stakeCache.scans, "all updates must be incremental")
	require.Equal(t, uint64(1), s.stakeCache.recalculations)

	_, err = s.acctsDb.RewindToBatchBoundary(10)
	require.NoError(t, err)
	s.SetRootedBankState(10, 10, 0)
	require.Equal(t, map[solana.PublicKey]uint64{{1}: 100, {2}: 200}, cacheTotals(t, s))
	require.Equal(t, uint64(2), s.stakeCache.scans)
	// A different fork can reuse exactly the same slots: version fencing must not.
	commitCacheAccounts(t, s, 11, cacheStakeAccount(t, 21, 1, 500, math.MaxUint64, math.MaxUint64))
	require.Equal(t, map[solana.PublicKey]uint64{{1}: 500, {2}: 200}, cacheTotals(t, s))
	_, err = s.acctsDb.RecoverFoldState()
	require.NoError(t, err)
	require.Equal(t, map[solana.PublicKey]uint64{{1}: 500, {2}: 200}, cacheTotals(t, s))
	require.Equal(t, uint64(3), s.stakeCache.scans)
}

func TestRootedStakeCacheEpochHistoryAndFeatureChanges(t *testing.T) {
	stake := cacheStakeAccount(t, 21, 1, 100, 0, 2)
	s := seedStakeCacheServer(t, stake)
	require.Empty(t, cacheTotals(t, s), "activation epoch has no effective stake")
	history := sealevel.SysvarStakeHistory{{Epoch: 0, Entry: sealevel.StakeHistoryEntry{Effective: 1000, Activating: 1000}}}
	commitCacheAccounts(t, s, 100, cacheHistoryAccount(t, history))
	delegation, _ := stakeDelegation(stake)
	require.Equal(t, delegation.Stake(1, &history, nil), cacheTotals(t, s)[solana.PublicKey{1}])
	require.Equal(t, uint64(25), cacheTotals(t, s)[solana.PublicKey{1}])
	activation := uint64(0)
	data, err := features.MarshalFeatureAcct(&features.FeatureAcct{ActivatedAt: &activation})
	require.NoError(t, err)
	// Include a stake update in the same batch BEFORE the feature account: input
	// invalidation cannot depend on the serialized account order.
	changed := cacheStakeAccount(t, 21, 1, 200, 0, 2)
	commitCacheAccounts(t, s, 101, changed, &accounts.Account{Key: features.ReduceStakeWarmupCooldown.Address, Owner: addresses.FeatureAddr, Lamports: 1, Data: data})
	delegation, _ = stakeDelegation(changed)
	require.Equal(t, delegation.Stake(1, &history, &activation), cacheTotals(t, s)[solana.PublicKey{1}])
	require.Equal(t, uint64(18), cacheTotals(t, s)[solana.PublicKey{1}])
	commitCacheAccounts(t, s, 300) // Epoch changes even without a history account write.
	require.Zero(t, cacheTotals(t, s)[solana.PublicKey{1}])
	require.Equal(t, uint64(1), s.stakeCache.scans, "activation changes must not reread all accounts")
	require.Equal(t, uint64(4), s.stakeCache.recalculations)
}

func TestRootedStakeCacheWaitersCancelAndDoNotPublishEarly(t *testing.T) {
	s := seedStakeCacheServer(t, cacheStakeAccount(t, 21, 1, 100, math.MaxUint64, math.MaxUint64))
	cacheTotals(t, s)
	_, err := s.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: 11, Delta: []*accounts.Account{cacheStakeAccount(t, 21, 1, 200, math.MaxUint64, math.MaxUint64)}}}, 11, nil, nil)
	require.NoError(t, err)
	_, err = s.rootedActivatedStakes(t.Context(), 10, 0, nil)
	require.ErrorIs(t, err, errStakeViewChanged, "DB commit alone must not publish a new RPC bank")
	s.SetRootedBankState(11, 11, 0)
	require.Equal(t, uint64(200), cacheTotals(t, s)[solana.PublicKey{1}])
	c := &s.stakeCache
	c.mu.Lock()
	c.byStake = nil
	c.dataReady = false
	c.building = make(chan struct{})
	c.mu.Unlock()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = s.rootedActivatedStakes(ctx, 11, 0, nil)
	require.ErrorIs(t, err, context.Canceled)
	ctx, cancel = context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	_, err = s.rootedActivatedStakes(ctx, 11, 0, nil)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	c.mu.Lock()
	close(c.building)
	c.building = nil
	c.mu.Unlock()
	require.Equal(t, uint64(200), cacheTotals(t, s)[solana.PublicKey{1}])
}

func TestRootedStakeCacheConcurrentBootstrap(t *testing.T) {
	s := seedStakeCacheServer(t, cacheStakeAccount(t, 21, 1, 100, math.MaxUint64, math.MaxUint64))
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			_, err := s.rootedActivatedStakes(t.Context(), 10, 0, nil)
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	require.Equal(t, uint64(1), s.stakeCache.scans)
}

func TestRootedStakeCacheLegacyStore(t *testing.T) {
	s := seedStakeCacheServer(t, cacheStakeAccount(t, 21, 1, 100, math.MaxUint64, math.MaxUint64))
	cacheTotals(t, s)
	s.acctsDb.RootedDurable = false
	done := make(chan struct{})
	require.NoError(t, s.acctsDb.StoreAccounts([]*accounts.Account{cacheStakeAccount(t, 21, 2, 500, math.MaxUint64, math.MaxUint64)}, 11, func() { close(done) }))
	<-done
	require.Eventually(t, func() bool { _, ok := s.acctsDb.CommittedAccountVersion(); return ok }, time.Second, time.Millisecond)
	_, err := s.rootedActivatedStakes(t.Context(), 10, 0, nil)
	require.ErrorIs(t, err, errStakeViewChanged, "a completed legacy write still needs RPC bank publication")
	s.SetRootedBankState(11, 11, 0)
	require.Equal(t, map[solana.PublicKey]uint64{{2}: 500}, cacheTotals(t, s))
	require.Equal(t, uint64(1), s.stakeCache.scans)
}

// Compare many independent delegation mutations with complete recomputation,
// including redelegation, closure, invalid state, and ownership changes.
func TestRootedStakeCacheDifferentialUpdates(t *testing.T) {
	c := &rootedStakeCache{version: 0, epoch: 3, byStake: make(map[solana.PublicKey]cachedDelegation), totals: make(map[solana.PublicKey]uint64)}
	live := make(map[solana.PublicKey]*accounts.Account)
	rng := rand.New(rand.NewSource(904))
	for step := uint64(1); step <= 500; step++ {
		key := byte(1 + rng.Intn(80))
		voter := byte(100 + rng.Intn(10))
		account := cacheStakeAccount(t, key, voter, uint64(1+rng.Intn(10000)), math.MaxUint64, math.MaxUint64)
		switch rng.Intn(5) {
		case 0:
			account.Lamports = 0
		case 1:
			account.Owner = addresses.SystemProgramAddr
		case 2:
			account.Data = []byte{255}
		}
		live[account.Key] = account
		c.AccountsChanged(step*2, []*accounts.Account{account}, false)
		expected := make(map[solana.PublicKey]uint64)
		for _, account := range live {
			if delegation, ok := stakeDelegation(account); ok {
				expected[delegation.VoterPubkey] += delegation.Stake(3, &c.history, nil)
			}
		}
		require.Equal(t, expected, c.totals, "step %d", step)
	}
}

func TestGetVoteAccountsConcurrentCommitConsistency(t *testing.T) {
	s := seedStakeCacheServer(t, cacheStakeAccount(t, 21, 1, 1000, math.MaxUint64, math.MaxUint64))
	previous, hadPrevious := global.EpochStakesSnapshot(0)
	global.PutEpochStakes(0, map[solana.PublicKey]uint64{{1}: 1000}, map[solana.PublicKey]*epochstakes.VoteAccount{{1}: {NodePubkey: solana.PublicKey{2}}}, 1000)
	t.Cleanup(func() {
		if hadPrevious {
			global.PutEpochStakes(0, previous.Stakes, previous.VoteAccounts, previous.TotalStake)
		} else {
			global.ClearEpochStakes(0)
		}
	})
	voteAccount := func(slot uint64) *accounts.Account {
		state := sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionCurrent}
		state.Current.NodePubkey = solana.PublicKey{2}
		state.Current.AuthorizedVoters.AuthorizedVoters.Set(0, solana.PublicKey{2})
		state.Current.Votes.PushBack(sealevel.LandedVote{Lockout: sealevel.VoteLockout{Slot: slot}})
		return voteAccountForRPC(t, solana.PublicKey{1}, &state)
	}
	commitCacheAccounts(t, s, 11, voteAccount(11), cacheStakeAccount(t, 21, 1, 1100, math.MaxUint64, math.MaxUint64))
	cacheTotals(t, s)
	var wg sync.WaitGroup
	wg.Go(func() {
		for slot := uint64(12); slot < 50; slot++ {
			changed := []*accounts.Account{voteAccount(slot), cacheStakeAccount(t, 21, 1, slot*100, math.MaxUint64, math.MaxUint64)}
			_, err := s.acctsDb.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: changed}}, slot, nil, nil)
			if err != nil {
				t.Error(err)
				return
			}
			s.SetRootedBankState(slot, slot, 0)
		}
	})
	for worker := range 4 {
		wg.Go(func() {
			params := mustRawParams(t, []interface{}{})
			if worker%2 == 0 {
				params = mustRawParams(t, []interface{}{map[string]interface{}{"votePubkey": solana.PublicKey{1}.String()}})
			}
			for range 30 {
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				got, err := s.GetVoteAccounts(ctx, params)
				cancel()
				if err != nil {
					t.Error(err)
					return
				}
				if len(got.Current) != 1 {
					t.Errorf("unexpected response: %+v", got)
					return
				}
				if got.Current[0].ActivatedStake != got.Current[0].LastVote*100 {
					t.Errorf("mixed committed banks: %+v", got.Current[0])
					return
				}
			}
		})
	}
	wg.Wait()
	require.Equal(t, uint64(1), s.stakeCache.scans)
}

func TestRootedStakeCacheBootstrapMergesConcurrentChanges(t *testing.T) {
	s := seedStakeCacheServer(t, cacheStakeAccount(t, 21, 1, 100, math.MaxUint64, math.MaxUint64), cacheStakeAccount(t, 22, 2, 200, math.MaxUint64, math.MaxUint64))
	s.stakeCache.afterBootstrapSeed = func() {
		commitCacheAccounts(t, s, 11, &accounts.Account{Key: solana.PublicKey{21}, Owner: addresses.SystemProgramAddr, Lamports: 1}, cacheStakeAccount(t, 22, 1, 250, math.MaxUint64, math.MaxUint64), cacheStakeAccount(t, 23, 3, 300, math.MaxUint64, math.MaxUint64))
	}
	_, err := s.rootedActivatedStakes(t.Context(), 10, 0, nil)
	require.ErrorIs(t, err, errStakeViewChanged, "request must retry its bank, not its completed bootstrap")
	require.True(t, s.stakeCache.dataReady)
	require.Equal(t, map[solana.PublicKey]uint64{{1}: 250, {3}: 300}, cacheTotals(t, s))
	require.Equal(t, uint64(1), s.stakeCache.scans)
}

func TestRootedStakeCacheRecalculationMergesConcurrentChanges(t *testing.T) {
	s := seedStakeCacheServer(t, cacheStakeAccount(t, 21, 1, 100, math.MaxUint64, math.MaxUint64), cacheStakeAccount(t, 22, 2, 200, math.MaxUint64, math.MaxUint64))
	s.stakeCache.beforeRecalculationChunk = func(start int) {
		if start == 0 {
			commitCacheAccounts(t, s, 11, cacheStakeAccount(t, 21, 1, 150, math.MaxUint64, math.MaxUint64), &accounts.Account{Key: solana.PublicKey{22}}, cacheStakeAccount(t, 23, 3, 300, math.MaxUint64, math.MaxUint64))
		}
	}
	_, err := s.rootedActivatedStakes(t.Context(), 10, 0, nil)
	require.ErrorIs(t, err, errStakeViewChanged)
	require.True(t, s.stakeCache.totalsReady)
	require.Equal(t, map[solana.PublicKey]uint64{{1}: 150, {3}: 300}, cacheTotals(t, s))
	require.Equal(t, uint64(1), s.stakeCache.scans)
	require.Equal(t, uint64(1), s.stakeCache.recalculations, "root advancement must not restart the full RAM calculation")
}

func TestRootedStakeCacheRewindCancelsBootstrap(t *testing.T) {
	s := seedStakeCacheServer(t, cacheStakeAccount(t, 21, 1, 100, math.MaxUint64, math.MaxUint64))
	commitCacheAccounts(t, s, 11, cacheStakeAccount(t, 21, 1, 200, math.MaxUint64, math.MaxUint64))
	var once sync.Once
	s.stakeCache.afterBootstrapSeed = func() {
		once.Do(func() {
			_, err := s.acctsDb.RewindToBatchBoundary(10)
			require.NoError(t, err)
			s.SetRootedBankState(10, 10, 0)
		})
	}
	_, err := s.rootedActivatedStakes(t.Context(), 11, 0, nil)
	require.ErrorIs(t, err, errStakeViewChanged)
	require.False(t, s.stakeCache.dataReady)
	require.Equal(t, map[solana.PublicKey]uint64{{1}: 100}, cacheTotals(t, s))
}
