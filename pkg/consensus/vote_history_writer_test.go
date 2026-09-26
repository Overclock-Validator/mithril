package consensus

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func TestAsyncHistoryBlockedWriteDoesNotDelayVotesAndCleanCloseDrains(t *testing.T) {
	cfg := reservedTestConfig(t.TempDir())
	v, err := openReservedTestVoter(t, cfg, 39)
	require.NoError(t, err)
	reserveThrough(t, v.reservation, 44)
	require.NoError(t, v.historyWriter.close())
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	first := true // Owned only by the serial writer.
	v.historyWriter = newVoteHistoryWriter(func(s *alpenglow.VoteHistorySnapshot) error {
		if first {
			first = false
			close(entered)
			<-release
		}
		return alpenglow.SaveReservedVoteHistorySnapshot(cfg.HistoryDir, s)
	}, nil)
	voted, err := v.cast(alpenglow.NewSkipVote(44), false)
	require.NoError(t, err)
	require.True(t, voted)
	<-entered
	castDone := make(chan error, 1)
	go func() {
		for _, slot := range []uint64{45, 46} {
			ok, err := v.cast(alpenglow.NewSkipVote(slot), false)
			if err != nil || !ok {
				castDone <- errors.New("vote failed while history writer was blocked")
				return
			}
		}
		castDone <- nil
	}()
	select {
	case err := <-castDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("disk writer blocked voting")
	}
	submitted, written, coalesced := v.historyWriter.counters()
	require.Equal(t, uint64(3), submitted)
	require.Zero(t, written)
	require.Equal(t, uint64(1), coalesced)
	onDisk, err := alpenglow.LoadVoteHistory(cfg.HistoryDir, v.node)
	require.NoError(t, err)
	require.False(t, onDisk.HasSkipped(44), "I/O should still be blocked")
	// Even when disk history lags, complete in-memory decisions forbid conflict.
	voted, err = v.cast(alpenglow.NewNotarizationVote(44, solana.Hash{7}), false)
	require.NoError(t, err)
	require.False(t, voted)
	closed := make(chan error, 1)
	go func() { closed <- v.close() }()
	require.Eventually(t, func() bool {
		v.historyWriter.mu.Lock()
		defer v.historyWriter.mu.Unlock()
		return v.historyWriter.closing
	}, time.Second, time.Millisecond)
	select {
	case err := <-closed:
		t.Fatalf("close returned before draining its writer: %v", err)
	default:
	}
	r, err := alpenglow.LoadVoteReservation(cfg.HistoryDir, v.node)
	require.NoError(t, err)
	require.Empty(t, r.CleanHistoryDigest)
	unblock()
	require.NoError(t, <-closed)
	onDisk, err = alpenglow.LoadVoteHistory(cfg.HistoryDir, v.node)
	require.NoError(t, err)
	for _, slot := range []uint64{44, 45, 46} {
		require.True(t, onDisk.HasSkipped(slot))
	}
	r, err = alpenglow.LoadVoteReservation(cfg.HistoryDir, v.node)
	require.NoError(t, err)
	digest, err := alpenglow.VoteHistoryDigest(cfg.HistoryDir, v.node)
	require.NoError(t, err)
	require.Equal(t, digest, r.CleanHistoryDigest)
	_, written, _ = v.historyWriter.counters()
	require.Equal(t, uint64(2), written, "old in-flight snapshot must finish before newest complete snapshot")
}

func TestAsyncHistoryFailureIsStickyAndReportedWithoutMoreSubmissions(t *testing.T) {
	cfg := reservedTestConfig(t.TempDir())
	h := alpenglow.NewVoteHistory(voterTestValidatorSet(t, cfg.Identity, cfg.AuthorizedVoter, cfg.VoteAccount).Validators[0].NodePubkey, 39)
	h.ReservationRequired = true
	snapshot, err := alpenglow.PrepareReservedVoteHistory(h, cfg.Identity)
	require.NoError(t, err)
	diskErr := errors.New("injected disk failure")
	reported := make(chan error, 1)
	w := newVoteHistoryWriter(func(*alpenglow.VoteHistorySnapshot) error { return diskErr }, func(err error) { reported <- err })
	require.NoError(t, w.submit(snapshot))
	select {
	case err := <-reported:
		require.ErrorIs(t, err, diskErr)
	case <-time.After(time.Second):
		t.Fatal("background failure was not reported")
	}
	require.ErrorIs(t, w.submit(snapshot), diskErr)
	require.ErrorIs(t, w.close(), diskErr)
	require.ErrorIs(t, w.close(), diskErr)
}

func TestAsyncHistoryFailureStopsVoterAndPreventsCleanMarker(t *testing.T) {
	cfg := reservedTestConfig(t.TempDir())
	e, err := NewEngine(Config{AlpenglowIdentity: cfg.Identity, AlpenglowShredVersion: 0x1234})
	require.NoError(t, err)
	t.Cleanup(func() { _ = e.Close() })
	set := voterTestValidatorSet(t, cfg.Identity, cfg.AuthorizedVoter, cfg.VoteAccount)
	e.SetAlpenglowEpochLookup(cfg.EpochForSlot)
	require.NoError(t, e.SetAlpenglowValidatorSet(set))
	root := alpenglow.BlockID{Slot: 39, Hash: solana.Hash{39}}
	e.SetAlpenglowRoot(root)
	v, err := newAlpenglowVoterUnstarted(e, cfg, root, []alpenglow.ValidatorSet{set})
	require.NoError(t, err)
	t.Cleanup(func() { _ = v.close() })
	reserveThrough(t, v.reservation, 44)
	require.NoError(t, v.historyWriter.close())
	diskErr := errors.New("injected background disk failure")
	v.historyWriter = newVoteHistoryWriter(func(*alpenglow.VoteHistorySnapshot) error { return diskErr }, v.failHistoryWrite)
	require.NoError(t, v.history.AddVote(alpenglow.NewSkipVote(44)))
	require.NoError(t, v.saveHistory())
	select {
	case <-v.done:
	case <-time.After(time.Second):
		t.Fatal("disk failure did not stop the voter")
	}
	require.ErrorIs(t, e.safetyError(), diskErr)
	_, _, err = v.sign(alpenglow.NewSkipVote(45), false)
	require.ErrorIs(t, err, diskErr)
	require.Error(t, v.enqueue(voterEvent{kind: voterEventBlockTimeout, slot: 45}))
	require.ErrorIs(t, v.close(), diskErr)
	r, err := alpenglow.LoadVoteReservation(cfg.HistoryDir, v.node)
	require.NoError(t, err)
	require.Empty(t, r.CleanHistoryDigest)
}

// Kill a subprocess while its first history write is blocked and a newer
// snapshot is pending. Successful earlier reservation syncs survive this
// process crash; this is deliberately not a host power-loss test.
func TestAsyncHistoryProcessCrashLosesPendingSnapshots(t *testing.T) {
	const childEnv = "MITHRIL_ASYNC_HISTORY_TEST_DIR"
	if dir := os.Getenv(childEnv); dir != "" {
		cfg := reservedTestConfig(dir)
		v, err := openReservedTestVoter(t, cfg, 39)
		require.NoError(t, err)
		reserveThrough(t, v.reservation, 44)
		require.NoError(t, v.historyWriter.close())
		entered := make(chan struct{})
		v.historyWriter = newVoteHistoryWriter(func(*alpenglow.VoteHistorySnapshot) error {
			close(entered)
			select {}
		}, nil)
		for _, slot := range []uint64{44, 45} {
			voted, err := v.cast(alpenglow.NewSkipVote(slot), false)
			require.NoError(t, err)
			require.True(t, voted)
			if slot == 44 {
				<-entered
			}
		}
		require.NoError(t, os.WriteFile(filepath.Join(dir, "ready"), []byte("ready"), 0600))
		select {}
	}
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestAsyncHistoryProcessCrashLosesPendingSnapshots$", "-test.count=1")
	cmd.Env = append(os.Environ(), childEnv+"="+dir)
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	require.Eventually(t, func() bool { _, err := os.Stat(filepath.Join(dir, "ready")); return err == nil }, 10*time.Second, time.Millisecond)
	require.NoError(t, cmd.Process.Kill())
	require.Error(t, cmd.Wait())
	cfg := reservedTestConfig(dir)
	cfg.InitializeVoteReservation = false
	v, err := openReservedTestVoter(t, cfg, 39)
	require.NoError(t, err)
	require.False(t, v.history.HasSkipped(44))
	require.False(t, v.history.HasSkipped(45))
	h := v.reservation.recoverThrough
	require.GreaterOrEqual(t, h, uint64(45))
	_, _, err = v.sign(alpenglow.NewSkipVote(44), false)
	require.ErrorIs(t, err, errVoterNotReady)
	_, _, err = v.sign(alpenglow.NewSkipVote(h+1), false)
	require.ErrorIs(t, err, errVoterNotReady, "verified finality must reach the lost history's bound")
}
