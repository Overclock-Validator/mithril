package rpcserver

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/epochstakes"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// Opt-in resource experiment; run each size in a fresh process. This measures
// local fixture scaling, not mainnet capacity. Fixture writes warm the OS page
// cache. RPC timings include response construction/JSON, but exclude HTTP.
//
//	MITHRIL_STAKE_SCALE=1000000 GOMAXPROCS=2 go test ./pkg/rpcserver \
//	  -run '^TestRootedStakeCacheScale$' -count=1 -v -timeout=10m
func TestRootedStakeCacheScale(t *testing.T) {
	size := os.Getenv("MITHRIL_STAKE_SCALE")
	if size == "" {
		t.Skip("set MITHRIL_STAKE_SCALE to run the on-disk sizing experiment")
	}
	count, err := strconv.Atoi(size)
	require.NoError(t, err)
	require.GreaterOrEqual(t, count, 1000)
	require.LessOrEqual(t, count, 2_000_000)
	const voters = 1000
	const epochSlots = uint64(1_000_000)
	key := func(prefix byte, i int) solana.PublicKey {
		k := solana.PublicKey{prefix}
		binary.LittleEndian.PutUint64(k[1:], uint64(i))
		return k
	}
	emit := func(stage string, fields map[string]any) {
		fields["stage"], fields["stakes"], fields["voters"] = stage, count, voters
		raw, err := json.Marshal(fields)
		require.NoError(t, err)
		t.Log(string(raw))
	}
	heap := func() runtime.MemStats { runtime.GC(); var m runtime.MemStats; runtime.ReadMemStats(&m); return m }
	quantiles := func(samples []time.Duration) map[string]any {
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		out := map[string]any{"samples": len(samples)}
		if len(samples) > 0 {
			for name, q := range map[string]float64{"p50_ms": .5, "p95_ms": .95, "p99_ms": .99, "max_ms": 1} {
				out[name] = float64(samples[max(0, int(math.Ceil(float64(len(samples))*q))-1)]) / float64(time.Millisecond)
			}
		}
		return out
	}
	db := newRPCAccountsDB(t)
	global.ClearPendingStakePubkeys()
	t.Cleanup(global.ClearPendingStakePubkeys)
	// 80% bootstrap-active, 10% activating, 10% deactivating at epoch 100.
	makeStake := func(i int, amount uint64) *accounts.Account {
		activation, deactivation := uint64(math.MaxUint64), uint64(math.MaxUint64)
		switch (i / voters) % 10 {
		case 0:
			activation = 98
		case 1:
			deactivation = 100
		}
		state := &sealevel.StakeStateV2{Status: sealevel.StakeStateV2StatusStake}
		state.Stake.Stake.Delegation = sealevel.Delegation{VoterPubkey: key(0xb0, i%voters), StakeLamports: amount, ActivationEpoch: activation, DeactivationEpoch: deactivation, WarmupCooldownRate: .25}
		data, err := sealevel.MarshalStakeStake(state)
		require.NoError(t, err)
		return &accounts.Account{Key: key(0xa0, i), Owner: addresses.StakeProgramAddr, Lamports: amount + 1, Data: data}
	}
	fixtureStart := time.Now()
	for first := 0; first < count; first += 10_000 {
		delta := make([]*accounts.Account, 0, min(10_000, count-first))
		slot := uint64(first/10_000 + 1)
		for i := first; i < min(first+10_000, count); i++ {
			a := makeStake(i, 1_000_000)
			delta = append(delta, a)
			global.EnqueuePendingStakePubkey(slot, a.Key)
		}
		_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: delta}}, slot, nil, nil)
		require.NoError(t, err)
	}
	_, err = global.FlushPendingStakePubkeys(filepath.Dir(db.AcctsDir))
	require.NoError(t, err)
	var history sealevel.SysvarStakeHistory
	entry := sealevel.StakeHistoryEntry{Effective: 10_000_000_000, Activating: 2_000_000_000, Deactivating: 1_000_000_000}
	for e := uint64(100); e > 68; e-- {
		history = append(history, sealevel.StakeHistoryPair{Epoch: e - 1, Entry: entry})
	}
	delta := []*accounts.Account{cacheHistoryAccount(t, history)}
	stakes := make(map[solana.PublicKey]uint64, voters)
	for i := 0; i < voters; i++ {
		voter := key(0xb0, i)
		stakes[voter] = 1
		state := &sealevel.VoteStateVersions{Type: sealevel.VoteStateVersionV4}
		state.V4.NodePubkey = key(0xc0, i)
		state.V4.AuthorizedVoters.AuthorizedVoters.Set(0, state.V4.NodePubkey)
		state.V4.Votes.PushBack(sealevel.LandedVote{Lockout: sealevel.VoteLockout{Slot: 100 * epochSlots}})
		delta = append(delta, voteAccountForRPC(t, voter, state))
	}
	_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: 100 * epochSlots, Delta: delta}}, 100*epochSlots, nil, nil)
	require.NoError(t, err)
	require.NoError(t, db.SeedVoteAccountPubkeys(nil))
	for epoch := uint64(100); epoch <= 102; epoch++ {
		previous, hadPrevious := global.EpochStakesSnapshot(epoch)
		global.PutEpochStakes(epoch, stakes, map[solana.PublicKey]*epochstakes.VoteAccount{}, voters)
		t.Cleanup(func() {
			if hadPrevious {
				global.PutEpochStakes(epoch, previous.Stakes, previous.VoteAccounts, previous.TotalStake)
			} else {
				global.ClearEpochStakes(epoch)
			}
		})
	}
	s := &RpcServer{acctsDb: db, epochSchedule: &sealevel.SysvarEpochSchedule{SlotsPerEpoch: epochSlots}}
	s.SetRootedBankState(100*epochSlots, 100*epochSlots, 0)
	db.CommonAcctsCache.Clear()
	db.VoteAcctCache.Clear()
	delta = nil
	before := heap()
	emit("fixture", map[string]any{"seconds": time.Since(fixtureStart).Seconds(), "heap_bytes": before.HeapAlloc})
	allParams := mustRawParams(t, []interface{}{})
	oneParams := mustRawParams(t, []interface{}{map[string]interface{}{"votePubkey": key(0xb0, 0).String()}})
	query := func(ctx context.Context, filtered bool) error {
		params := allParams
		if filtered {
			params = oneParams
		}
		response, err := s.GetVoteAccounts(ctx, params)
		if err != nil {
			return err
		}
		want := voters
		if filtered {
			want = 1
		}
		if len(response.Current)+len(response.Delinquent) != want {
			return fmt.Errorf("got %d votes, want %d", len(response.Current)+len(response.Delinquent), want)
		}
		_, err = json.Marshal(response)
		return err
	}
	start := time.Now()
	require.NoError(t, query(t.Context(), false))
	coldDuration := time.Since(start)
	after := heap()
	emit("cold", map[string]any{"seconds": coldDuration.Seconds(), "heap_bytes": after.HeapAlloc, "heap_growth_bytes": int64(after.HeapAlloc) - int64(before.HeapAlloc), "allocated_bytes": after.TotalAlloc - before.TotalAlloc, "allocations": after.Mallocs - before.Mallocs, "gc_cycles": after.NumGC - before.NumGC})
	for _, filtered := range []bool{false, true} {
		samples := make([]time.Duration, 0, 100)
		var a, b runtime.MemStats
		runtime.ReadMemStats(&a)
		for i := 0; i < 100; i++ {
			start := time.Now()
			require.NoError(t, query(t.Context(), filtered))
			samples = append(samples, time.Since(start))
		}
		runtime.ReadMemStats(&b)
		stats := quantiles(samples)
		stats["bytes_per_query"] = (b.TotalAlloc - a.TotalAlloc) / 100
		stats["allocs_per_query"] = (b.Mallocs - a.Mallocs) / 100
		name := "warm_all"
		if filtered {
			name = "warm_one"
		}
		emit(name, stats)
	}
	// Keep a direct, full-scan aggregation as the independent update oracle.
	verify := func(stage string) {
		bank := s.rootedBank.Load()
		epoch := s.epochSchedule.GetEpoch(bank.Slot)
		got, err := s.rootedActivatedStakes(t.Context(), bank.Slot, epoch, nil)
		require.NoError(t, err)
		expected := make(map[solana.PublicKey]uint64)
		var mu sync.Mutex
		start := time.Now()
		_, err = global.StreamStakeAccounts(db, bank.Slot, func(_ solana.PublicKey, d *sealevel.Delegation, _ uint64) {
			active := d.Stake(epoch, &history, nil)
			if active > 0 {
				mu.Lock()
				expected[d.VoterPubkey] += active
				mu.Unlock()
			}
		})
		require.NoError(t, err)
		require.Equal(t, expected, got)
		emit(stage+"_oracle", map[string]any{"seconds": time.Since(start).Seconds()})
	}
	verify("cold")
	// Measure a quiet epoch recalculation before adding concurrent traffic.
	history = append(sealevel.SysvarStakeHistory{{Epoch: 100, Entry: entry}}, history...)
	_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: 101 * epochSlots, Delta: []*accounts.Account{cacheHistoryAccount(t, history)}}}, 101*epochSlots, nil, nil)
	require.NoError(t, err)
	s.SetRootedBankState(101*epochSlots, 101*epochSlots, 0)
	start = time.Now()
	require.NoError(t, query(t.Context(), false))
	emit("epoch_quiet", map[string]any{"seconds": time.Since(start).Seconds()})
	verify("epoch_quiet")
	// Four paced callers mix whole-cluster and single-voter requests while 100
	// durable folds update 64 stakes apiece. Include wall time waiting for roots.
	concurrent := func(stage string, readers int) {
		ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
		var wg sync.WaitGroup
		defer func() { cancel(); wg.Wait() }()
		var mu sync.Mutex
		var all, one []time.Duration
		failures := make(chan error, 4)
		for worker := 0; worker < readers; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					if ctx.Err() != nil {
						return
					}
					start := time.Now()
					err := query(ctx, worker%2 == 1)
					elapsed := time.Since(start)
					if err != nil {
						if ctx.Err() == nil {
							failures <- err
						}
						return
					}
					mu.Lock()
					if worker%2 == 1 {
						one = append(one, elapsed)
					} else {
						all = append(all, elapsed)
					}
					mu.Unlock()
					select {
					case <-ctx.Done():
						return
					case <-time.After(50 * time.Millisecond):
					}
				}
			}()
		}
		var commits []time.Duration
		startSlot := s.rootedBank.Load().Slot
		for n := 0; n < 100; n++ {
			changed := make([]*accounts.Account, 64)
			for i := range changed {
				changed[i] = makeStake((n*64+i)%count, uint64(1_000_000+n+1))
			}
			slot := startSlot + uint64(n+1)
			start := time.Now()
			_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: slot, Delta: changed}}, slot, nil, nil)
			if err != nil {
				cancel()
				wg.Wait()
				t.Fatal(err)
			}
			s.SetRootedBankState(slot, slot, 0)
			commits = append(commits, time.Since(start))
			time.Sleep(20 * time.Millisecond)
		}
		// Let in-flight initial requests finish before stopping the readers.
		require.NoError(t, query(ctx, false))
		time.Sleep(200 * time.Millisecond)
		cancel()
		wg.Wait()
		close(failures)
		for err := range failures {
			require.NoError(t, err)
		}
		emit(stage+"_all", quantiles(all))
		emit(stage+"_one", quantiles(one))
		emit(stage+"_commit", quantiles(commits))
		verify(stage)
	}
	concurrent("quiet_commits", 0)
	concurrent("warm_concurrent", 4)
	history = append(sealevel.SysvarStakeHistory{{Epoch: 101, Entry: entry}}, history...)
	_, err = db.CommitBatch([]accounts.SlotDelta{{Slot: 102 * epochSlots, Delta: []*accounts.Account{cacheHistoryAccount(t, history)}}}, 102*epochSlots, nil, nil)
	require.NoError(t, err)
	s.SetRootedBankState(102*epochSlots, 102*epochSlots, 0)
	concurrent("epoch_concurrent", 4)
	version, _ := db.CommittedAccountVersion()
	s.stakeCache.AccountsChanged(version, nil, true)
	db.CommonAcctsCache.Clear()
	db.VoteAcctCache.Clear()
	concurrent("bootstrap_concurrent", 4)
	require.Equal(t, uint64(2), s.stakeCache.scans, "concurrent root changes must not restart the account scan")
	retained := heap()
	version, _ = db.CommittedAccountVersion()
	s.stakeCache.AccountsChanged(version, nil, true)
	released := heap()
	emit("memory", map[string]any{"heap_before_drop_bytes": retained.HeapAlloc, "heap_after_drop_bytes": released.HeapAlloc, "derived_cache_retained_estimate_bytes": int64(retained.HeapAlloc) - int64(released.HeapAlloc), "scans": s.stakeCache.scans, "recalculations": s.stakeCache.recalculations})
	runtime.KeepAlive(s)
}
