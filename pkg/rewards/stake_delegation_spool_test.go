package rewards

import (
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestStakeDelegationSpoolConcurrentCaptureRepeatableAndExact(t *testing.T) {
	dir := t.TempDir()
	const count = 64
	var visited int
	var mu sync.Mutex
	delegation := sealevel.Delegation{VoterPubkey: solana.PublicKey{99}, StakeLamports: math.MaxUint64,
		ActivationEpoch: 33, DeactivationEpoch: 34, WarmupCooldownRate: math.Float64frombits(0x3fd0000000000001), CreditsObserved: 1234}
	spool, err := CaptureStakeDelegations(dir, func(visit StakeAccountVisitor) (int, error) {
		var wg sync.WaitGroup
		for i := 0; i < count; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				visit(solana.PublicKey{byte(i)}, &delegation, uint64(i))
			}(i)
		}
		wg.Wait()
		return count, nil
	}, func(_ solana.PublicKey, _ *sealevel.Delegation, _ uint64) {
		mu.Lock()
		visited++
		mu.Unlock()
	})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, spool.Close()) })
	require.Equal(t, count, visited)
	path := spool.file.Name()
	for scan := 0; scan < 3; scan++ {
		seen := make(map[solana.PublicKey]uint64)
		n, err := spool.Stream(func(pk solana.PublicKey, got *sealevel.Delegation, credits uint64) {
			require.Equal(t, delegation, *got)
			seen[pk] = credits
			// Visitors cannot alter the next scan or another record.
			got.StakeLamports = 0
		})
		require.NoError(t, err)
		require.Equal(t, count, n)
		require.Len(t, seen, count)
		for i := 0; i < count; i++ {
			require.Equal(t, uint64(i), seen[solana.PublicKey{byte(i)}])
		}
	}
	require.NoError(t, spool.Close())
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err))
	_, err = spool.Stream(func(solana.PublicKey, *sealevel.Delegation, uint64) {})
	require.ErrorContains(t, err, "closed")
}

func TestStakeDelegationSpoolRejectsTruncationAndCleansFailedCapture(t *testing.T) {
	dir := t.TempDir()
	source := func(visit StakeAccountVisitor) (int, error) {
		visit(solana.PublicKey{1}, &sealevel.Delegation{StakeLamports: 100}, 12)
		return 1, nil
	}
	spool, err := CaptureStakeDelegations(dir, source, nil)
	require.NoError(t, err)
	require.NoError(t, spool.file.Truncate(stakeDelegationRecordSize-1))
	_, err = spool.Stream(func(solana.PublicKey, *sealevel.Delegation, uint64) { t.Fatal("partial record visited") })
	require.Error(t, err)
	require.NoError(t, spool.Close())
	wantErr := errors.New("parent scan failed")
	_, err = CaptureStakeDelegations(dir, func(visit StakeAccountVisitor) (int, error) {
		_, _ = source(visit)
		return 1, wantErr
	}, nil)
	require.ErrorIs(t, err, wantErr)
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, files)
}

func TestRewardPlanFailedSourceDoesNotLeaveDirectory(t *testing.T) {
	dir := t.TempDir()
	db := &accountsdb.AccountsDb{AcctsDir: filepath.Join(dir, "accounts")}
	f := &features.Features{}
	wantErr := errors.New("parent scan failed")
	_, err := CalculateRewardsStreaming(db, 123, &sealevel.SysvarStakeHistory{}, nil, nil,
		PointValue{}, 33, [32]byte{}, &sealevel.SlotCtx{Features: f}, f,
		&sealevel.SysvarEpochSchedule{SlotsPerEpoch: 54000}, RewardCalculationMode{},
		func(StakeAccountVisitor) (int, error) { return 0, wantErr })
	require.ErrorIs(t, err, wantErr)
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, files)
}
