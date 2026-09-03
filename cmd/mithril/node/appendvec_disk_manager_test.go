package node

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type atomicDiskSpace struct {
	available atomic.Uint64
	total     uint64
}

func (space *atomicDiskSpace) probe(string) (accountsdb.AppendVecFilesystemSpace, error) {
	return accountsdb.AppendVecFilesystemSpace{
		AvailableBytes: space.available.Load(),
		TotalBytes:     space.total,
	}, nil
}

func newTestDiskManager(
	t *testing.T,
	space *atomicDiskSpace,
	config appendVecDiskPolicyConfig,
	runner appendVecCompactionRunner,
) (*appendVecDiskManager, context.Context) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	if config.AccountsPath == "" {
		config.AccountsPath = "/injected/accounts"
	}
	if config.EmergencyMinDeadFraction == 0 {
		config.EmergencyMinDeadFraction = defaultEmergencyMinDeadFraction
	}
	manager, err := newAppendVecDiskManager(ctx, cancel, config, space.probe, runner)
	require.NoError(t, err)
	return manager, ctx
}

func TestResolveAppendVecDiskThresholdsIsAdaptive(t *testing.T) {
	minFree, targetFree, scratch, err := resolveAppendVecDiskThresholds(500*appendVecDiskGiB, 0, 0)
	require.NoError(t, err)
	assert.Equal(t, uint64(25*appendVecDiskGiB), minFree)
	assert.Equal(t, uint64(50*appendVecDiskGiB), targetFree)
	assert.Equal(t, appendVecDiskGiB, scratch)

	_, _, _, err = resolveAppendVecDiskThresholds(1000, 900, 800)
	require.ErrorContains(t, err, "greater than minimum")
}

func TestAppendVecFoldAdmissionSerializesCompleteConsumers(t *testing.T) {
	space := &atomicDiskSpace{total: 1000}
	space.available.Store(900)
	manager, _ := newTestDiskManager(t, space, appendVecDiskPolicyConfig{
		CompactionEnabled:        true,
		ReserveEnforced:          true,
		MinFreeBytes:             100,
		TargetFreeBytes:          200,
		EmergencyMinDeadFraction: 0.2,
	}, func(context.Context, accountsdb.CompactionConfig) (accountsdb.CompactStats, error) {
		t.Fatal("compaction should not run with sufficient free space")
		return accountsdb.CompactStats{}, nil
	})

	releaseFirst, err := manager.AdmitFold(50)
	require.NoError(t, err)
	secondDone := make(chan error, 1)
	go func() {
		releaseSecond, admitErr := manager.AdmitFold(50)
		if admitErr == nil {
			releaseSecond()
		}
		secondDone <- admitErr
	}()
	select {
	case err := <-secondDone:
		t.Fatalf("second admission passed before the first fold released its reservation: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	releaseFirst()
	select {
	case err := <-secondDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("second admission did not proceed after release")
	}
	releaseFirst() // release is idempotent
}

func TestAppendVecPressureAdmissionCompactsSynchronously(t *testing.T) {
	space := &atomicDiskSpace{total: 1000}
	space.available.Store(120)
	var calls atomic.Uint64
	manager, _ := newTestDiskManager(t, space, appendVecDiskPolicyConfig{
		CompactionEnabled:        true,
		ReserveEnforced:          true,
		MinFreeBytes:             100,
		TargetFreeBytes:          200,
		EmergencyMinDeadFraction: 0.2,
		NormalCompaction: accountsdb.CompactionConfig{
			MinDeadFraction: 0.7,
			MaxSourceBytes:  64,
		},
	}, func(_ context.Context, cfg accountsdb.CompactionConfig) (accountsdb.CompactStats, error) {
		calls.Add(1)
		assert.False(t, cfg.NonBlocking)
		assert.True(t, cfg.ContinueOnOutputSpacePressure)
		assert.Zero(t, cfg.MaxSourceBytes)
		assert.Equal(t, 0.2, cfg.MinDeadFraction)
		space.available.Add(100)
		return accountsdb.CompactStats{
			EligibleCandidates: 1,
			CandidatesScanned:  1,
			FilesCompacted:     1,
			BytesReclaimed:     100,
			PassComplete:       true,
		}, nil
	})

	release, err := manager.AdmitFold(50)
	require.NoError(t, err)
	release()
	assert.Equal(t, uint64(1), calls.Load())
	assert.Equal(t, uint64(220), space.available.Load())
}

func TestAppendVecPressureAdmissionFailsAfterOneCompleteNoProgressPass(t *testing.T) {
	space := &atomicDiskSpace{total: 1000}
	space.available.Store(100)
	var calls atomic.Uint64
	manager, _ := newTestDiskManager(t, space, appendVecDiskPolicyConfig{
		CompactionEnabled:        true,
		ReserveEnforced:          true,
		MinFreeBytes:             100,
		TargetFreeBytes:          200,
		EmergencyMinDeadFraction: 0.2,
	}, func(context.Context, accountsdb.CompactionConfig) (accountsdb.CompactStats, error) {
		calls.Add(1)
		return accountsdb.CompactStats{
			EligibleCandidates: 3,
			CandidatesScanned:  1,
			PassComplete:       false,
		}, nil
	})

	_, err := manager.AdmitFold(1)
	require.ErrorIs(t, err, accountsdb.ErrAppendVecDiskPressure)
	assert.Equal(t, uint64(3), calls.Load(), "emergency admission must inspect one whole stable candidate pass")
}

func TestAppendVecNormalCycleAndPressureAdmissionDoNotOverlap(t *testing.T) {
	space := &atomicDiskSpace{total: 1000}
	space.available.Store(150) // below target, above minimum
	cycleStarted := make(chan struct{})
	releaseCycle := make(chan struct{})
	var concurrent atomic.Int64
	manager, _ := newTestDiskManager(t, space, appendVecDiskPolicyConfig{
		CompactionEnabled:        true,
		ReserveEnforced:          true,
		MinFreeBytes:             100,
		TargetFreeBytes:          200,
		EmergencyMinDeadFraction: 0.2,
		NormalCompaction: accountsdb.CompactionConfig{
			MaxSourceBytes: 64,
		},
	}, func(_ context.Context, cfg accountsdb.CompactionConfig) (accountsdb.CompactStats, error) {
		require.True(t, cfg.NonBlocking)
		require.True(t, cfg.ContinueOnOutputSpacePressure)
		require.Equal(t, int64(64), cfg.MaxSourceBytes)
		require.Equal(t, int64(1), concurrent.Add(1))
		close(cycleStarted)
		<-releaseCycle
		concurrent.Add(-1)
		return accountsdb.CompactStats{}, nil
	})

	normalDone := make(chan error, 1)
	go func() {
		_, err := manager.RunNormalCycle(t.Context())
		normalDone <- err
	}()
	select {
	case <-cycleStarted:
	case <-time.After(time.Second):
		t.Fatal("normal compaction did not start")
	}

	admissionDone := make(chan error, 1)
	go func() {
		release, err := manager.AdmitFold(0)
		if err == nil {
			assert.Zero(t, concurrent.Load())
			release()
		}
		admissionDone <- err
	}()
	select {
	case err := <-admissionDone:
		t.Fatalf("admission overlapped an active normal cycle: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseCycle)
	require.NoError(t, <-normalDone)
	require.NoError(t, <-admissionDone)
}

func TestAppendVecCompactionFailureCancelsReplayAndIsStable(t *testing.T) {
	space := &atomicDiskSpace{total: 1000}
	space.available.Store(150)
	injected := errors.New("corrupt appendvec manifest")
	manager, ctx := newTestDiskManager(t, space, appendVecDiskPolicyConfig{
		CompactionEnabled:        true,
		ReserveEnforced:          true,
		MinFreeBytes:             100,
		TargetFreeBytes:          200,
		EmergencyMinDeadFraction: 0.2,
	}, func(context.Context, accountsdb.CompactionConfig) (accountsdb.CompactStats, error) {
		return accountsdb.CompactStats{}, injected
	})

	_, err := manager.RunNormalCycle(t.Context())
	require.ErrorIs(t, err, injected)
	manager.Fail(err)
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("fatal compaction error did not cancel replay context")
	}
	require.ErrorIs(t, manager.Err(), injected)
	manager.Fail(errors.New("later error"))
	require.ErrorIs(t, manager.Err(), injected, "the first fatal cause must remain stable")
}

func TestAppendVecNormalCycleDefersOutputSpacePressureWithoutHalting(t *testing.T) {
	space := &atomicDiskSpace{total: 1000}
	space.available.Store(150)
	manager, _ := newTestDiskManager(t, space, appendVecDiskPolicyConfig{
		CompactionEnabled:        true,
		ReserveEnforced:          true,
		MinFreeBytes:             100,
		TargetFreeBytes:          200,
		EmergencyMinDeadFraction: 0.2,
	}, func(context.Context, accountsdb.CompactionConfig) (accountsdb.CompactStats, error) {
		return accountsdb.CompactStats{
				EligibleCandidates:   1,
				CandidatesScanned:    1,
				OutputSpaceDeferrals: 1,
				PassComplete:         true,
			}, &accountsdb.AppendVecDiskPressureError{
				Operation:      "compaction output preflight",
				AvailableBytes: 150,
				RequiredBytes:  200,
			}
	})

	stats, err := manager.RunNormalCycle(t.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, stats.OutputSpaceDeferrals)
	assert.NoError(t, manager.Err())
}

func TestAppendVecCompactionDisabledRetainsHardReserve(t *testing.T) {
	space := &atomicDiskSpace{total: 1000}
	space.available.Store(100)
	manager, _ := newTestDiskManager(t, space, appendVecDiskPolicyConfig{
		CompactionEnabled:        false,
		ReserveEnforced:          true,
		MinFreeBytes:             100,
		TargetFreeBytes:          200,
		EmergencyMinDeadFraction: 0.2,
	}, func(context.Context, accountsdb.CompactionConfig) (accountsdb.CompactStats, error) {
		t.Fatal("disabled compaction must not run")
		return accountsdb.CompactStats{}, nil
	})

	_, err := manager.AdmitFold(1)
	require.ErrorIs(t, err, accountsdb.ErrAppendVecDiskPressure)
}

func TestAppendVecNormalCycleSkipsWhenAdmissionGateIsHeld(t *testing.T) {
	space := &atomicDiskSpace{total: 1000}
	space.available.Store(150)
	var calls atomic.Uint64
	manager, _ := newTestDiskManager(t, space, appendVecDiskPolicyConfig{
		CompactionEnabled:        true,
		ReserveEnforced:          true,
		MinFreeBytes:             100,
		TargetFreeBytes:          200,
		EmergencyMinDeadFraction: 0.2,
	}, func(context.Context, accountsdb.CompactionConfig) (accountsdb.CompactStats, error) {
		calls.Add(1)
		return accountsdb.CompactStats{}, nil
	})

	release, err := manager.AdmitFold(0)
	require.NoError(t, err)
	var wait sync.WaitGroup
	wait.Add(1)
	go func() {
		defer wait.Done()
		_, cycleErr := manager.RunNormalCycle(t.Context())
		require.NoError(t, cycleErr)
	}()
	wait.Wait()
	release()
	assert.Zero(t, calls.Load())
}
