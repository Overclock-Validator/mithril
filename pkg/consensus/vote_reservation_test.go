package consensus

import (
	"crypto/ed25519"
	"errors"
	"math"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

func reservedTestConfig(dir string) VotingConfig {
	return VotingConfig{Identity: voterTestKey(11), AuthorizedVoter: voterTestKey(12), VoteAccount: solana.PublicKey(voterTestKey(13).Public().(ed25519.PublicKey)), HistoryDir: dir, Genesis: solana.Hash{1}, ReservedHistory: true, InitializeVoteReservation: true, WaitToVoteSlot: 40, ReadyToVote: func(uint64) bool { return true }, EpochForSlot: func(uint64) uint64 { return 7 }, Peers: func([]alpenglow.ValidatorStake) []alpenglow.VotorPeer { return nil }}
}

func openReservedTestVoter(t *testing.T, cfg VotingConfig, root uint64) (*alpenglowVoter, error) {
	t.Helper()
	e, err := NewEngine(Config{AlpenglowIdentity: cfg.Identity, AlpenglowShredVersion: 0x1234})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, e.Close()) })
	set := voterTestValidatorSet(t, cfg.Identity, cfg.AuthorizedVoter, cfg.VoteAccount)
	e.SetAlpenglowEpochLookup(cfg.EpochForSlot)
	require.NoError(t, e.SetAlpenglowValidatorSet(set))
	block := alpenglow.BlockID{Slot: root, Hash: solana.Hash{byte(root)}}
	e.SetAlpenglowRoot(block)
	v, err := newAlpenglowVoterUnstarted(e, cfg, block, []alpenglow.ValidatorSet{set})
	if err == nil {
		t.Cleanup(func() { require.NoError(t, v.close()) })
	}
	return v, err
}

func reserveThrough(t *testing.T, r *signingReservation, slot uint64) {
	t.Helper()
	r.request(slot)
	require.Eventually(t, func() bool { return r.through.Load() >= slot }, time.Second, time.Millisecond)
}

// Simulate loss of this process without executing the clean shutdown protocol.
func crashReservedTestVoter(t *testing.T, v *alpenglowVoter) {
	t.Helper()
	v.shutdownOnce.Do(func() {
		v.closeOnce.Do(func() { close(v.done) })
		v.wg.Wait()
		v.reservation.halt()
		require.NoError(t, v.broadcaster.Close())
		require.NoError(t, v.historyLock.Close())
	})
}

func TestReservedVotingLostHistorySuffixAndRepeatedCrash(t *testing.T) {
	cfg := reservedTestConfig(t.TempDir())
	v, err := openReservedTestVoter(t, cfg, 39)
	require.NoError(t, err)
	reserveThrough(t, v.reservation, 60)
	baseline, err := os.ReadFile(alpenglow.VoteHistoryFilename(cfg.HistoryDir, v.node))
	require.NoError(t, err)
	voted, err := v.cast(alpenglow.NewNotarizationVote(60, solana.Hash{1}), false)
	require.NoError(t, err)
	require.True(t, voted)
	oldH := v.reservation.through.Load()
	crashReservedTestVoter(t, v)
	// Reproduce a host crash retaining a valid older version of detailed history.
	require.NoError(t, os.WriteFile(alpenglow.VoteHistoryFilename(cfg.HistoryDir, v.node), baseline, 0600))
	cfg.InitializeVoteReservation = false
	resumed, err := openReservedTestVoter(t, cfg, 39)
	require.NoError(t, err)
	require.Equal(t, oldH, resumed.reservation.recoverThrough)
	for _, vote := range []alpenglow.Vote{alpenglow.NewNotarizationVote(60, solana.Hash{2}), alpenglow.NewSkipVote(60), alpenglow.NewFinalizationVote(60), alpenglow.NewNotarizationFallbackVote(60, solana.Hash{2}), alpenglow.NewSkipFallbackVote(60), alpenglow.NewSkipVote(oldH + 1)} {
		// Check at the signing boundary, including the restoration bypass.
		for _, normal := range []bool{false, true} {
			_, _, err := resumed.sign(vote, normal)
			require.ErrorIs(t, err, errVoterNotReady)
		}
	}
	require.Equal(t, oldH, resumed.reservation.through.Load(), "recovery must not keep moving its target")
	crashReservedTestVoter(t, resumed)
	resumed, err = openReservedTestVoter(t, cfg, oldH)
	require.NoError(t, err)
	require.Equal(t, oldH, resumed.reservation.recoverThrough)
	reserveThrough(t, resumed.reservation, oldH+1)
	voted, err = resumed.cast(alpenglow.NewSkipVote(oldH+1), false)
	require.NoError(t, err)
	require.True(t, voted)
	voted, err = resumed.cast(alpenglow.NewSkipVote(oldH), false)
	require.NoError(t, err)
	require.False(t, voted)
}

func TestReservedVotingCleanMarkerConsumedBeforeSigning(t *testing.T) {
	cfg := reservedTestConfig(t.TempDir())
	v, err := openReservedTestVoter(t, cfg, 39)
	require.NoError(t, err)
	reserveThrough(t, v.reservation, 44)
	voted, err := v.cast(alpenglow.NewSkipVote(44), false)
	require.NoError(t, err)
	require.True(t, voted)
	require.NoError(t, v.close())
	r, err := alpenglow.LoadVoteReservation(cfg.HistoryDir, v.node)
	require.NoError(t, err)
	require.NotEmpty(t, r.CleanHistoryDigest)
	cfg.InitializeVoteReservation = false
	resumed, err := openReservedTestVoter(t, cfg, 39)
	require.NoError(t, err)
	require.Zero(t, resumed.reservation.recoverThrough)
	require.True(t, resumed.history.HasSkipped(44))
	r, err = alpenglow.LoadVoteReservation(cfg.HistoryDir, v.node)
	require.NoError(t, err)
	require.Empty(t, r.CleanHistoryDigest)
	voted, err = resumed.cast(alpenglow.NewSkipVote(45), false)
	require.NoError(t, err)
	require.True(t, voted)
	require.False(t, resumed.reservation.allow(45, 39, true), "clean vote history does not authorize repeating leader blocks")
	crashReservedTestVoter(t, resumed)
	again, err := openReservedTestVoter(t, cfg, 39)
	require.NoError(t, err)
	require.Equal(t, r.Through, again.reservation.recoverThrough)
	// A shutdown before recovering the uncertain range must not mark it clean.
	require.NoError(t, again.close())
	r, err = alpenglow.LoadVoteReservation(cfg.HistoryDir, v.node)
	require.NoError(t, err)
	require.Empty(t, r.CleanHistoryDigest)
}

func TestReservedVotingRejectsMissingCorruptOrWrongDomain(t *testing.T) {
	for _, which := range []string{"missing_history", "missing_bound", "corrupt_bound", "genesis", "authorized", "vote_account", "synchronous"} {
		t.Run(which, func(t *testing.T) {
			cfg := reservedTestConfig(t.TempDir())
			v, err := openReservedTestVoter(t, cfg, 39)
			require.NoError(t, err)
			reserveThrough(t, v.reservation, 44)
			crashReservedTestVoter(t, v)
			cfg.InitializeVoteReservation = false
			switch which {
			case "missing_history":
				require.NoError(t, os.Remove(alpenglow.VoteHistoryFilename(cfg.HistoryDir, v.node)))
			case "missing_bound":
				require.NoError(t, os.Remove(alpenglow.VoteReservationFilename(cfg.HistoryDir, v.node)))
				cfg.InitializeVoteReservation = true
			case "corrupt_bound":
				require.NoError(t, os.WriteFile(alpenglow.VoteReservationFilename(cfg.HistoryDir, v.node), []byte("{"), 0600))
			case "genesis":
				cfg.Genesis = solana.Hash{2}
			case "authorized":
				cfg.AuthorizedVoter = voterTestKey(19)
			case "vote_account":
				cfg.VoteAccount = solana.PublicKey{9}
			case "synchronous":
				cfg.ReservedHistory = false
			}
			_, err = openReservedTestVoter(t, cfg, 39)
			require.Error(t, err)
		})
	}
}

func TestReservedVotingRequiresEnrollmentAndExclusiveOwner(t *testing.T) {
	cfg := reservedTestConfig(t.TempDir())
	cfg.InitializeVoteReservation = false
	_, err := openReservedTestVoter(t, cfg, 39)
	require.Error(t, err)
	cfg.InitializeVoteReservation = true
	v, err := openReservedTestVoter(t, cfg, 39)
	require.NoError(t, err)
	_, err = openReservedTestVoter(t, cfg, 39)
	require.ErrorContains(t, err, "already owned")
	require.NoError(t, v.close())
}

func TestSigningReservationUnacknowledgedSyncCannotAuthorize(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	r := &signingReservation{record: alpenglow.VoteReservation{Through: 64, Generation: 1}, wake: make(chan struct{}, 1), changed: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	r.through.Store(64)
	var calls atomic.Uint64
	r.persist = func(next alpenglow.VoteReservation) error {
		calls.Add(1)
		close(entered)
		<-release
		return nil
	}
	go r.run()
	require.True(t, r.allow(64, 64, false))
	<-entered
	require.Equal(t, uint64(64), r.through.Load())
	require.False(t, r.allow(65, 64, false))
	close(release)
	require.Eventually(t, func() bool { return r.through.Load() > 64 }, time.Second, time.Millisecond)
	require.True(t, r.allow(65, 64, false))
	r.halt()
	require.Equal(t, uint64(1), calls.Load())
}

func TestSigningReservationUncertainWriteSurvivesRestart(t *testing.T) {
	cfg := reservedTestConfig(t.TempDir())
	v, err := openReservedTestVoter(t, cfg, 39)
	require.NoError(t, err)
	// Stop the original worker, then exercise a fresh worker against the real record.
	v.reservation.halt()
	record, err := alpenglow.LoadVoteReservation(cfg.HistoryDir, v.node)
	require.NoError(t, err)
	r := &signingReservation{record: record, wake: make(chan struct{}, 1), changed: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	r.through.Store(record.Through)
	wrote := make(chan struct{}, 1)
	r.persist = func(next alpenglow.VoteReservation) error {
		err := alpenglow.SaveVoteReservation(cfg.HistoryDir, next, cfg.Identity)
		select {
		case wrote <- struct{}{}:
		default:
		}
		if err != nil {
			return err
		}
		return errors.New("injected lost sync acknowledgement")
	}
	v.reservation = r
	go r.run()
	require.False(t, r.allow(60, 39, false))
	<-wrote
	r.halt()
	require.Equal(t, record.Through, r.through.Load())
	durable, err := alpenglow.LoadVoteReservation(cfg.HistoryDir, v.node)
	require.NoError(t, err)
	require.Greater(t, durable.Through, record.Through)
	require.ErrorContains(t, r.seal(cfg.HistoryDir, v.history, cfg.Identity), "uncertain")
	crashReservedTestVoter(t, v)
	cfg.InitializeVoteReservation = false
	resumed, err := openReservedTestVoter(t, cfg, 39)
	require.NoError(t, err)
	require.Equal(t, durable.Through, resumed.reservation.recoverThrough)
}

func TestSigningReservationNeverWraps(t *testing.T) {
	r := &signingReservation{record: alpenglow.VoteReservation{Through: 64, Generation: 1}, wake: make(chan struct{}, 1), changed: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}), persist: func(alpenglow.VoteReservation) error { t.Error("overflow attempted persistence"); return nil }}
	r.through.Store(64)
	go r.run()
	require.False(t, r.allow(math.MaxUint64, 64, false))
	r.halt()
	require.Equal(t, uint64(64), r.through.Load())
}

func TestReservationRetryRechecksFinality(t *testing.T) {
	cfg := reservedTestConfig(t.TempDir())
	v, err := openReservedTestVoter(t, cfg, 39)
	require.NoError(t, err)
	event := voterEvent{kind: voterEventBlockTimeout, slot: 44}
	require.NoError(t, v.handle(event))
	require.NotEmpty(t, v.reservationEvents)
	reserveThrough(t, v.reservation, 44)
	v.engine.SetAlpenglowRoot(alpenglow.BlockID{Slot: 47, Hash: solana.Hash{47}})
	pending := v.reservationEvents
	v.reservationEvents = nil
	for _, e := range pending {
		require.NoError(t, v.handle(e))
	}
	require.False(t, v.history.HasSkipped(44), "finalized work must not be signed after a delayed ack")
}

func TestReservedVotingEverySignatureTypeAtBound(t *testing.T) {
	cfg := reservedTestConfig(t.TempDir())
	v, err := openReservedTestVoter(t, cfg, 39)
	require.NoError(t, err)
	reserveThrough(t, v.reservation, 44)
	h := v.reservation.through.Load()
	// Freeze acknowledgement while leaving the signing guard active.
	v.reservation.halt()
	v.reservation.stopped.Store(false)
	defer v.reservation.stopped.Store(true)
	for _, slot := range []uint64{h, h + 1} {
		votes := []alpenglow.Vote{alpenglow.NewNotarizationVote(slot, solana.Hash{2}), alpenglow.NewSkipVote(slot), alpenglow.NewFinalizationVote(slot), alpenglow.NewNotarizationFallbackVote(slot, solana.Hash{2}), alpenglow.NewSkipFallbackVote(slot)}
		for _, vote := range votes {
			for _, normal := range []bool{false, true} {
				_, _, err := v.sign(vote, normal)
				if slot == h {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, errVoterNotReady)
				}
			}
		}
	}
}

func TestReservedCleanDigestMismatchUsesCrashRecovery(t *testing.T) {
	cfg := reservedTestConfig(t.TempDir())
	v, err := openReservedTestVoter(t, cfg, 39)
	require.NoError(t, err)
	baseline, err := os.ReadFile(alpenglow.VoteHistoryFilename(cfg.HistoryDir, v.node))
	require.NoError(t, err)
	reserveThrough(t, v.reservation, 44)
	voted, err := v.cast(alpenglow.NewSkipVote(44), false)
	require.NoError(t, err)
	require.True(t, voted)
	require.NoError(t, v.close())
	require.NoError(t, os.WriteFile(alpenglow.VoteHistoryFilename(cfg.HistoryDir, v.node), baseline, 0600))
	resumed, err := openReservedTestVoter(t, cfg, 39)
	require.NoError(t, err)
	require.NotZero(t, resumed.reservation.recoverThrough)
}

func TestReservationLoopRetriesAfterAcknowledgement(t *testing.T) {
	cfg := reservedTestConfig(t.TempDir())
	v, err := openReservedTestVoter(t, cfg, 39)
	require.NoError(t, err)
	v.start()
	require.NoError(t, v.enqueue(voterEvent{kind: voterEventBlockTimeout, slot: 44}))
	// Read via stats, not mutable voter history, while its loop is running.
	require.Eventually(t, func() bool { v.landingMu.RLock(); defer v.landingMu.RUnlock(); return v.stats.VotesCastThisRun == 4 }, time.Second, time.Millisecond)
	require.NoError(t, v.close())
	for slot := uint64(44); slot <= 47; slot++ {
		require.True(t, v.history.HasSkipped(slot))
	}
}

func TestReservationRetainsWindowCrossingBound(t *testing.T) {
	cfg := reservedTestConfig(t.TempDir())
	v, err := openReservedTestVoter(t, cfg, 39)
	require.NoError(t, err)
	reserveThrough(t, v.reservation, 44)
	h := v.reservation.through.Load() // 76: window ends at 79.
	v.retainReservationEvent(voterEvent{kind: voterEventBlockTimeout, slot: h})
	require.Len(t, v.reservationEvents, 1, "trailing skip slots require retry even if the first slot fits")
}
