package accountsdb

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/cockroachdb/pebble/v2"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

type voteScanBudgetContext struct {
	context.Context
	cancel context.CancelFunc
	checks atomic.Int64
}

type voteScanPauseContext struct {
	context.Context
	checks  atomic.Int64
	reached chan struct{}
	release chan struct{}
}

func (c *voteScanPauseContext) Err() error {
	if c.checks.Add(1) == 3000 {
		close(c.reached)
		<-c.release
	}
	return c.Context.Err()
}

func (c *voteScanBudgetContext) Err() error {
	if c.checks.Add(1) > 3000 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestVoteIndexMigratesExistingStoreAndPersists(t *testing.T) {
	db, dir := newFoldTestDb(t)
	vote := &accounts.Account{Key: solana.PublicKey{1}, Owner: addresses.VoteProgramAddr, Lamports: 1}
	ordinary := &accounts.Account{Key: solana.PublicKey{2}, Lamports: 1}
	_, err := db.CommitBatch(foldDeltas(accounts.SlotDelta{Slot: 10, Delta: []*accounts.Account{vote, ordinary}}), 10, nil, nil)
	require.NoError(t, err)
	// Mimic a database written before the vote index existed.
	require.NoError(t, db.Index.Delete(voteIndexKey(vote.Key), pebble.Sync))
	db.VoteAcctCache.Clear()
	got, err := db.VoteAccountPubkeys(context.Background())
	require.NoError(t, err)
	require.Equal(t, []solana.PublicKey{vote.Key}, got)

	db = reopenFoldTestDb(t, db, dir)
	defer db.CloseDb()
	_, err = db.RecoverFoldState()
	require.NoError(t, err)
	got, err = db.VoteAccountPubkeys(context.Background())
	require.NoError(t, err)
	require.Equal(t, []solana.PublicKey{vote.Key}, got)
}

func TestVoteIndexSnapshotSeedAndFoldRecovery(t *testing.T) {
	db, dir := newFoldTestDb(t)
	seedVote := solana.PublicKey{1}
	foldVote := &accounts.Account{Key: solana.PublicKey{2}, Owner: addresses.VoteProgramAddr, Lamports: 1}
	require.NoError(t, db.SeedVoteAccountPubkeys([]solana.PublicKey{seedVote}))
	_, err := db.CommitBatch(foldDeltas(accounts.SlotDelta{Slot: 10, Delta: []*accounts.Account{foldVote}}), 10, nil, nil)
	require.NoError(t, err)
	got, err := db.VoteAccountPubkeys(context.Background())
	require.NoError(t, err)
	require.Equal(t, []solana.PublicKey{seedVote, foldVote.Key}, got)

	// Simulate a lost index tail: recovery must restore the candidate from the
	// durable fold manifest along with the account's primary index entry.
	require.NoError(t, db.Index.Delete(voteIndexKey(foldVote.Key), pebble.Sync))
	require.NoError(t, db.Index.Delete(metaKeyLastBatch, pebble.Sync))
	db = reopenFoldTestDb(t, db, dir)
	defer db.CloseDb()
	_, err = db.RecoverFoldState()
	require.NoError(t, err)
	got, err = db.VoteAccountPubkeys(context.Background())
	require.NoError(t, err)
	require.Equal(t, []solana.PublicKey{seedVote, foldVote.Key}, got)
}

func TestVoteIndexMigrationCanRetryAfterCancellation(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := db.VoteAccountPubkeys(ctx)
	require.ErrorIs(t, err, context.Canceled)
	got, err := db.VoteAccountPubkeys(context.Background())
	require.NoError(t, err)
	require.Empty(t, got)
}

func TestVoteIndexRebuildsAfterLegacyStoreRewind(t *testing.T) {
	db, dir := newFoldTestDb(t)
	vote := foldAcct(1, 10, []byte("vote"))
	vote.Owner = addresses.VoteProgramAddr
	commitTestBatch(t, db, 10, vote)
	closed := foldAcct(1, 0, nil)
	closed.Owner = addresses.VoteProgramAddr
	commitTestBatch(t, db, 20, closed)
	require.NoError(t, db.Index.Delete(voteIndexKey(vote.Key), pebble.Sync))
	got, err := db.VoteAccountPubkeys(context.Background())
	require.NoError(t, err)
	require.Empty(t, got)
	require.NoError(t, db.Index.Set(voteIndexReadyKey, nil, pebble.Sync))
	_, err = db.RewindToBatchBoundaryAndPersist(10, func(RewindResult) error { return nil })
	require.NoError(t, err)
	_, _, err = db.Index.Get(voteIndexReadyKey)
	require.ErrorIs(t, err, pebble.ErrNotFound)
	db = reopenFoldTestDb(t, db, dir)
	defer db.CloseDb()
	_, err = db.RecoverFoldState()
	require.NoError(t, err)
	require.Equal(t, uint64(10), mustColdRead(t, db, 10, vote.Key).Lamports)
	got, err = db.VoteAccountPubkeys(context.Background())
	require.NoError(t, err)
	require.Contains(t, got, vote.Key)
}

func TestVoteIndexMigrationConcurrentWithRewind(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	vote := foldAcct(1, 10, []byte("vote"))
	vote.Owner = addresses.VoteProgramAddr
	commitTestBatch(t, db, 10, vote)
	closed := foldAcct(1, 0, nil)
	closed.Owner = addresses.VoteProgramAddr
	commitTestBatch(t, db, 20, closed)
	require.NoError(t, db.Index.Delete(voteIndexKey(vote.Key), pebble.Sync))

	start := make(chan struct{})
	done := make(chan error, 2)
	go func() {
		<-start
		_, err := db.VoteAccountPubkeys(context.Background())
		done <- err
	}()
	go func() {
		<-start
		_, err := db.RewindToBatchBoundaryAndPersist(10, func(RewindResult) error { return nil })
		done <- err
	}()
	close(start)
	for range 2 {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("migration and rewind did not complete")
		}
	}
	got, err := db.VoteAccountPubkeys(context.Background())
	require.NoError(t, err)
	require.Contains(t, got, vote.Key)
}

func TestVoteIndexRebuildsEarlierReadyMarker(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	vote := foldAcct(1, 10, []byte("vote"))
	vote.Owner = addresses.VoteProgramAddr
	commitTestBatch(t, db, 10, vote)
	require.NoError(t, db.Index.Delete(voteIndexKey(vote.Key), pebble.Sync))
	require.NoError(t, db.Index.Set(voteIndexReadyKey, nil, pebble.Sync))
	got, err := db.VoteAccountPubkeys(context.Background())
	require.NoError(t, err)
	require.Contains(t, got, vote.Key)
}

func TestVoteIndexRebuildsAfterOlderWriter(t *testing.T) {
	db, dir := newFoldTestDb(t)
	_, err := db.VoteAccountPubkeys(context.Background())
	require.NoError(t, err)
	vote := foldAcct(1, 10, []byte("vote"))
	vote.Owner = addresses.VoteProgramAddr
	commitTestBatch(t, db, 10, vote)
	// Simulate an older binary that writes accounts without maintaining this index.
	require.NoError(t, db.Index.Delete(voteIndexKey(vote.Key), pebble.Sync))
	db = reopenFoldTestDb(t, db, dir)
	defer db.CloseDb()
	require.Equal(t, uint64(10), mustColdRead(t, db, 10, vote.Key).Lamports)
	got, err := db.VoteAccountPubkeys(context.Background())
	require.NoError(t, err)
	require.Contains(t, got, vote.Key)
}

func TestVoteIndexScanResumesAfterCancellation(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	const count = 5000
	accts := make([]*accounts.Account, count)
	for i := range accts {
		var key solana.PublicKey
		binary.BigEndian.PutUint64(key[:8], uint64(i+1))
		accts[i] = &accounts.Account{Key: key, Lamports: 1}
	}
	accts[0].Owner = addresses.VoteProgramAddr
	accts[count-1].Owner = addresses.VoteProgramAddr
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 10, Delta: accts}}, 10, nil, nil)
	require.NoError(t, err)
	require.NoError(t, db.Index.Delete(voteIndexKey(accts[count-1].Key), pebble.Sync))
	require.NoError(t, db.Index.Delete(voteIndexKey(accts[0].Key), pebble.Sync))
	var got []solana.PublicKey
	for attempt := range 10 {
		base, cancel := context.WithCancel(context.Background())
		ctx := &voteScanBudgetContext{Context: base, cancel: cancel}
		got, err = db.VoteAccountPubkeys(ctx)
		cancel()
		if err == nil {
			break
		}
		require.ErrorIs(t, err, context.Canceled)
		if attempt == 0 {
			require.NotNil(t, db.voteIndexResume)
			require.Greater(t, binary.BigEndian.Uint64(db.voteIndexResume[:8]), uint64(2))
			updated := *accts[1]
			updated.Owner = addresses.VoteProgramAddr
			commitTestBatch(t, db, 20, &updated)
		}
	}
	require.NoError(t, err, "repeated cancelled requests should make scan progress")
	require.Contains(t, got, accts[0].Key)
	require.Contains(t, got, accts[1].Key)
	require.Contains(t, got, accts[count-1].Key)
}

func TestVoteIndexCancelledScanRestartsAfterRewind(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	const count = 5000
	accts := make([]*accounts.Account, count)
	for i := range accts {
		var key solana.PublicKey
		binary.BigEndian.PutUint64(key[:8], uint64(i+1))
		accts[i] = &accounts.Account{Key: key, Lamports: 1}
	}
	vote := accts[0]
	vote.Owner = addresses.VoteProgramAddr
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 10, Delta: accts}}, 10, nil, nil)
	require.NoError(t, err)
	commitTestBatch(t, db, 20, &accounts.Account{Key: vote.Key})
	require.NoError(t, db.Index.Delete(voteIndexKey(vote.Key), pebble.Sync))
	base, cancel := context.WithCancel(context.Background())
	ctx := &voteScanBudgetContext{Context: base, cancel: cancel}
	_, err = db.VoteAccountPubkeys(ctx)
	cancel()
	require.ErrorIs(t, err, context.Canceled)
	require.NotNil(t, db.voteIndexResume)
	_, err = db.RewindToBatchBoundaryAndPersist(10, func(RewindResult) error { return nil })
	require.NoError(t, err)
	got, err := db.VoteAccountPubkeys(context.Background())
	require.NoError(t, err)
	require.Contains(t, got, vote.Key)
}

func TestVoteIndexLockWaitHonorsCancellation(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	db.voteIndexMu.Lock()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := db.VoteAccountPubkeys(ctx)
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		db.voteIndexMu.Unlock()
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		db.voteIndexMu.Unlock()
		<-done
		t.Fatal("cancelled request remained blocked on the vote index lock")
	}
}

func TestVoteIndexRebuildsAfterInterruptedFoldRecovery(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	vote := foldAcct(1, 10, []byte("vote"))
	vote.Owner = addresses.VoteProgramAddr
	commitTestBatch(t, db, 10, vote)
	require.NoError(t, db.Index.Delete(voteIndexKey(vote.Key), pebble.Sync))
	db.foldHooks.afterPublicationStart = func() {
		got, err := db.VoteAccountPubkeys(context.Background())
		require.NoError(t, err)
		require.Empty(t, got)
		panic("interrupt fold")
	}
	require.Panics(t, func() {
		_, _ = db.CommitBatch([]accounts.SlotDelta{{Slot: 20, Delta: []*accounts.Account{{Key: vote.Key}}}}, 20, nil, nil)
	})
	db.foldHooks.afterPublicationStart = nil
	headers, err := ListFoldManifests(db.AcctsDir)
	require.NoError(t, err)
	for _, header := range headers {
		if header.ThroughSlot == 20 {
			require.NoError(t, os.Remove(header.Path))
		}
	}
	_, err = db.RecoverFoldState()
	require.NoError(t, err)
	require.Equal(t, uint64(10), mustColdRead(t, db, 10, vote.Key).Lamports)
	got, err := db.VoteAccountPubkeys(context.Background())
	require.NoError(t, err)
	require.Contains(t, got, vote.Key)
}

func TestVoteIndexRebuildsAfterFailedThenSuccessfulFold(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	vote := foldAcct(1, 10, []byte("vote"))
	vote.Owner = addresses.VoteProgramAddr
	commitTestBatch(t, db, 10, vote)
	require.NoError(t, db.Index.Delete(voteIndexKey(vote.Key), pebble.Sync))
	db.foldHooks.afterPublicationStart = func() {
		got, err := db.VoteAccountPubkeys(context.Background())
		require.NoError(t, err)
		require.Empty(t, got)
		panic("interrupt fold")
	}
	require.Panics(t, func() {
		_, _ = db.CommitBatch([]accounts.SlotDelta{{Slot: 20, Delta: []*accounts.Account{{Key: vote.Key}}}}, 20, nil, nil)
	})
	db.foldHooks.afterPublicationStart = nil
	commitTestBatch(t, db, 30, foldAcct(2, 10, nil))
	require.Equal(t, uint64(10), mustColdRead(t, db, 30, vote.Key).Lamports)
	got, err := db.VoteAccountPubkeys(context.Background())
	require.NoError(t, err)
	require.Contains(t, got, vote.Key)
}

func TestVoteIndexRetriesScanOverFailedFold(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	const count = 5000
	accts := make([]*accounts.Account, count)
	for i := range accts {
		var key solana.PublicKey
		binary.BigEndian.PutUint64(key[:8], uint64(i+1))
		accts[i] = &accounts.Account{Key: key, Lamports: 1}
	}
	vote := accts[0]
	vote.Owner = addresses.VoteProgramAddr
	_, err := db.CommitBatch([]accounts.SlotDelta{{Slot: 10, Delta: accts}}, 10, nil, nil)
	require.NoError(t, err)
	require.NoError(t, db.Index.Delete(voteIndexKey(vote.Key), pebble.Sync))
	published, releaseFold := make(chan struct{}), make(chan struct{})
	db.foldHooks.afterPublicationStart = func() {
		close(published)
		<-releaseFold
		panic("interrupt fold")
	}
	foldDone := make(chan any, 1)
	go func() {
		defer func() { foldDone <- recover() }()
		_, _ = db.CommitBatch([]accounts.SlotDelta{{Slot: 20, Delta: []*accounts.Account{{Key: vote.Key}}}}, 20, nil, nil)
	}()
	<-published
	ctx := &voteScanPauseContext{Context: context.Background(), reached: make(chan struct{}), release: make(chan struct{})}
	type scanResult struct {
		keys []solana.PublicKey
		err  error
	}
	scanDone := make(chan scanResult, 1)
	go func() {
		keys, err := db.VoteAccountPubkeys(ctx)
		scanDone <- scanResult{keys: keys, err: err}
	}()
	<-ctx.reached
	close(releaseFold)
	require.Equal(t, "interrupt fold", <-foldDone)
	db.foldHooks.afterPublicationStart = nil
	commitTestBatch(t, db, 30, foldAcct(2, 10, nil))
	close(ctx.release)
	result := <-scanDone
	require.NoError(t, result.err)
	require.Contains(t, result.keys, vote.Key)
}

func TestVoteIndexRewindCrashRecovery(t *testing.T) {
	if dir := os.Getenv("MITHRIL_VOTE_REWIND_DIR"); dir != "" {
		DisableIndexWAL = os.Getenv("MITHRIL_VOTE_REWIND_WAL_OFF") == "true"
		db, err := OpenDb(dir)
		require.NoError(t, err)
		db.InitCaches()
		_, err = db.RecoverFoldState()
		require.NoError(t, err)
		crash := func() { os.Exit(91) }
		if os.Getenv("MITHRIL_VOTE_REWIND_PHASE") == "before-flush" {
			db.rewindHooks.beforeIndexFlush = crash
		} else {
			db.rewindHooks.afterIndex = crash
		}
		_, err = db.RewindToBatchBoundaryAndPersist(10, func(RewindResult) error { return nil })
		require.NoError(t, err)
		os.Exit(92)
	}
	for _, walOff := range []bool{false, true} {
		for _, phase := range []string{"before-flush", "after-index"} {
			t.Run(fmt.Sprintf("walOff=%t/%s", walOff, phase), func(t *testing.T) {
				previous := DisableIndexWAL
				DisableIndexWAL = walOff
				defer func() { DisableIndexWAL = previous }()
				db, dir := newFoldTestDb(t)
				require.NoError(t, db.SeedVoteAccountPubkeys(nil))
				vote := foldAcct(1, 10, []byte("vote"))
				vote.Owner = addresses.VoteProgramAddr
				commitTestBatch(t, db, 10, vote)
				commitTestBatch(t, db, 20, &accounts.Account{Key: vote.Key})
				writeOpts := pebble.Sync
				if walOff {
					writeOpts = pebble.NoSync
				}
				require.NoError(t, db.Index.Delete(voteIndexKey(vote.Key), writeOpts))
				db.voteIndexChecked = false
				_, err := db.VoteAccountPubkeys(context.Background())
				require.NoError(t, err)
				db.CloseDb()
				cmd := exec.Command(os.Args[0], "-test.run=^TestVoteIndexRewindCrashRecovery$")
				cmd.Env = append(os.Environ(), "MITHRIL_VOTE_REWIND_DIR="+dir, "MITHRIL_VOTE_REWIND_WAL_OFF="+fmt.Sprint(walOff), "MITHRIL_VOTE_REWIND_PHASE="+phase)
				output, err := cmd.CombinedOutput()
				var exit *exec.ExitError
				require.ErrorAs(t, err, &exit, string(output))
				require.Equal(t, 91, exit.ExitCode(), string(output))
				require.NotContains(t, string(output), "WARNING: DATA RACE")
				db, err = OpenDb(dir)
				require.NoError(t, err)
				db.InitCaches()
				defer db.CloseDb()
				recovery, err := db.RecoverFoldState()
				require.NoError(t, err)
				require.True(t, recovery.RewindInProgress)
				_, err = db.RewindToBatchBoundaryAndPersist(10, func(RewindResult) error { return nil })
				require.NoError(t, err)
				got, err := db.VoteAccountPubkeys(context.Background())
				require.NoError(t, err)
				require.Contains(t, got, vote.Key)
			})
		}
	}
}
