package accountsdb

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// checkpointShadow is every test key's account while a boundary is the head.
type checkpointShadow map[solana.PublicKey]*accounts.Account

func captureCheckpointShadow(t *testing.T, db *AccountsDb, keys []solana.PublicKey) checkpointShadow {
	t.Helper()
	shadow := make(checkpointShadow, len(keys))
	for _, key := range keys {
		db.CommonAcctsCache.Delete(key)
		db.VoteAcctCache.Delete(key)
		account, err := db.GetAccount(0, key)
		if err == ErrNoAccount {
			continue
		}
		require.NoError(t, err)
		shadow[key] = account.Clone()
	}
	return shadow
}

func requireCheckpointMatches(t *testing.T, view *CheckpointView, keys []solana.PublicKey, want checkpointShadow, msg string) CheckpointLoadStats {
	t.Helper()
	got, stats, err := view.LoadAccounts(context.Background(), keys)
	require.NoError(t, err, msg)
	require.Len(t, got, len(keys))
	absent := uint64(0)
	for i, key := range keys {
		expected := want[key]
		if expected == nil {
			require.Nil(t, got[i], "%s: key %s must not exist at the boundary", msg, key)
			absent++
			continue
		}
		require.NotNil(t, got[i], "%s: key %s", msg, key)
		require.Equal(t, expected.Key, got[i].Key, msg)
		require.Equal(t, expected.Lamports, got[i].Lamports, "%s: key %s", msg, key)
		require.Equal(t, expected.Data, got[i].Data, "%s: key %s", msg, key)
		require.Equal(t, expected.Owner, got[i].Owner, msg)
		require.Equal(t, expected.Executable, got[i].Executable, msg)
		require.Equal(t, expected.RentEpoch, got[i].RentEpoch, msg)
	}
	require.Equal(t, uint64(len(keys)), stats.Requested)
	require.Equal(t, absent, stats.Absent, msg)
	require.Equal(t, uint64(len(keys)), stats.Changed+stats.Unchanged, msg)

	// Any number of readers, and a staged load split anywhere, give the same accounts.
	for _, workers := range []int{1, 3, 64} {
		again, againStats, err := view.LoadAccountsWith(context.Background(), keys, workers)
		require.NoError(t, err, msg)
		require.Equal(t, got, again, "%s: %d workers", msg, workers)
		require.Equal(t, stats, againStats, "%s: %d workers", msg, workers)
	}
	for _, split := range []int{0, 1, len(keys) / 3, len(keys)} {
		first, _, pending, err := view.LoadAccountsStaged(context.Background(), keys[:split], keys[split:], 2)
		require.NoError(t, err, msg)
		rest, _, err := pending.Wait()
		require.NoError(t, err, msg)
		require.Equal(t, got, append(append([]*accounts.Account{}, first...), rest...), "%s: staged at %d", msg, split)
	}
	return stats
}

// A view opened at any retained boundary returns exactly what the store held
// when that boundary was the head, however many folds have committed since,
// whether the view was opened before or after them, and matches an actual
// rewind to that boundary.
func TestCheckpointViewMatchesEveryBoundaryAndARewind(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	rng := rand.New(rand.NewSource(7))

	const keyCount, folds = 60, 12
	keys := make([]solana.PublicKey, keyCount)
	for i := range keys {
		keys[i] = solana.PublicKey{byte(i + 1), 0xC4}
	}
	version := make([]int, keyCount)
	type boundary struct {
		through uint64
		shadow  checkpointShadow
		early   *CheckpointView // opened while this boundary was the head
	}
	var boundaries []boundary
	for fold := 0; fold < folds; fold++ {
		through := uint64(100 + 10*fold)
		var delta []*accounts.Account
		for i := range keys {
			// Some keys never appear, some appear late, most change often.
			if i%7 == 0 || (i%5 == 0 && fold < 6) || rng.Intn(3) != 0 {
				continue
			}
			version[i]++
			data := make([]byte, rng.Intn(300))
			rng.Read(data)
			delta = append(delta, &accounts.Account{
				Key: keys[i], Lamports: uint64(1000*i + version[i]), Data: data,
				Owner: [32]byte{byte(i), byte(version[i])}, RentEpoch: uint64(fold),
			})
		}
		if len(delta) == 0 {
			delta = append(delta, &accounts.Account{Key: keys[1], Lamports: uint64(fold + 1)})
		}
		commitTestBatch(t, db, through, delta...)
		view, err := db.OpenCheckpointView(through)
		require.NoError(t, err)
		require.Equal(t, through, view.ThroughSlot())
		require.Equal(t, []byte(fmt.Sprintf("ctx-%d", through)), view.ResumeContext())
		shadow := captureCheckpointShadow(t, db, keys)
		// At its own head nothing has changed since, so nothing comes from a fold record.
		stats := requireCheckpointMatches(t, view, keys, shadow, fmt.Sprintf("fold %d at head", fold))
		require.Zero(t, stats.Changed)
		boundaries = append(boundaries, boundary{through, shadow, view})
	}

	shuffled := append([]solana.PublicKey(nil), keys...)
	rng.Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] })
	withRepeats := append(append([]solana.PublicKey(nil), shuffled...), shuffled[:10]...)
	for i, b := range boundaries {
		late, err := db.OpenCheckpointView(b.through)
		require.NoError(t, err)
		for name, view := range map[string]*CheckpointView{"opened early": b.early, "opened late": late} {
			stats := requireCheckpointMatches(t, view, withRepeats, b.shadow, fmt.Sprintf("boundary %d, %s", b.through, name))
			if i < len(boundaries)-1 {
				require.NotZero(t, stats.Changed, "later folds changed accounts, so fold records must supply versions")
			}
			single, err := view.GetAccount(keys[0])
			require.ErrorIs(t, err, ErrNoAccount, "key 0 is never written")
			require.Nil(t, single)
		}
	}

	// An actual rewind to a middle boundary agrees with the view of it.
	middle := boundaries[5]
	_, err := db.RewindToBatchBoundary(middle.through)
	require.NoError(t, err)
	require.Equal(t, middle.shadow, captureCheckpointShadow(t, db, keys), "the rewound store equals the boundary's shadow")
	requireCheckpointMatches(t, middle.early, keys, middle.shadow, "view of the boundary the store was rewound to")
	// Boundaries the rewind removed are gone; earlier ones still read correctly.
	_, _, err = boundaries[8].early.LoadAccounts(context.Background(), keys)
	require.ErrorIs(t, err, ErrCheckpointExpired)
	_, err = db.OpenCheckpointView(boundaries[8].through)
	require.ErrorIs(t, err, ErrCheckpointExpired)
	requireCheckpointMatches(t, boundaries[2].early, keys, boundaries[2].shadow, "an older boundary after the rewind")
	_, err = db.OpenCheckpointView(12345)
	require.ErrorIs(t, err, ErrCheckpointExpired)
}

// Folds commit while views are read. Under -race this checks the locking; the
// values check that a fold becoming visible in the index is always also seen
// as a change by the view.
func TestCheckpointViewReadsWhileFoldsCommit(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	const keyCount = 40
	keys := make([]solana.PublicKey, keyCount)
	first := make([]*accounts.Account, keyCount)
	for i := range keys {
		keys[i] = solana.PublicKey{byte(i + 1), 0xD5}
		first[i] = &accounts.Account{Key: keys[i], Lamports: uint64(i + 1), Data: []byte{byte(i)}}
	}
	commitTestBatch(t, db, 100, first...)
	view, err := db.OpenCheckpointView(100)
	require.NoError(t, err)
	shadow := captureCheckpointShadow(t, db, keys)

	var readers sync.WaitGroup
	stop := make(chan struct{})
	failures := make(chan string, 8)
	for range 4 {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				got, _, err := view.LoadAccounts(context.Background(), keys)
				if err != nil {
					failures <- err.Error()
					return
				}
				for i, key := range keys {
					if got[i] == nil || got[i].Lamports != shadow[key].Lamports || string(got[i].Data) != string(shadow[key].Data) {
						failures <- fmt.Sprintf("key %d changed under the view", i)
						return
					}
				}
			}
		}()
	}
	for fold := 1; fold <= 60; fold++ {
		var delta []*accounts.Account
		for i := fold % 3; i < keyCount; i += 3 {
			delta = append(delta, &accounts.Account{Key: keys[i], Lamports: uint64(1_000_000 + fold), Data: []byte{byte(fold), byte(i)}})
		}
		commitTestBatch(t, db, uint64(100+fold), delta...)
	}
	close(stop)
	readers.Wait()
	select {
	case failure := <-failures:
		t.Fatal(failure)
	default:
	}
	requireCheckpointMatches(t, view, keys, shadow, "after sixty later folds")
}

// Compaction moves live records and deletes dead files. A view within the
// rewind horizon keeps reading the boundary's versions through it; one whose
// boundary compaction lets expire reports that, and never a wrong account.
func TestCheckpointViewAcrossCompaction(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	const keyCount = 30
	keys := make([]solana.PublicKey, keyCount)
	for i := range keys {
		keys[i] = solana.PublicKey{byte(i + 1), 0xE6}
	}
	type boundary struct {
		through uint64
		shadow  checkpointShadow
		view    *CheckpointView
	}
	var boundaries []boundary
	for fold := 0; fold < 8; fold++ {
		through := uint64(100 + 10*fold)
		var delta []*accounts.Account
		for i := range keys {
			// Keys 0 to 4 are written once and stay live in the oldest file;
			// the rest are rewritten every fold, leaving dead versions behind.
			if i < 5 && fold > 0 {
				continue
			}
			delta = append(delta, &accounts.Account{Key: keys[i], Lamports: uint64(100*fold + i + 1), Data: make([]byte, 200+fold)})
		}
		commitTestBatch(t, db, through, delta...)
		view, err := db.OpenCheckpointView(through)
		require.NoError(t, err)
		boundaries = append(boundaries, boundary{through, captureCheckpointShadow(t, db, keys), view})
	}

	const horizon = 3
	stats, err := db.CompactOnce(CompactionConfig{RewindHorizonBatches: horizon, MinDeadFraction: 0.1})
	require.NoError(t, err)
	require.NotZero(t, stats.FilesDeleted+stats.FilesCompacted, "the fixture must give compaction something to do")

	head := len(boundaries) - 1
	for i, b := range boundaries {
		_, _, err := b.view.LoadAccounts(context.Background(), keys)
		if i < head-horizon {
			require.ErrorIs(t, err, ErrCheckpointExpired, "boundary %d is outside the horizon", b.through)
			_, openErr := db.OpenCheckpointView(b.through)
			require.ErrorIs(t, openErr, ErrCheckpointExpired)
			continue
		}
		require.NoError(t, err, "boundary %d is within the horizon", b.through)
		requireCheckpointMatches(t, b.view, keys, b.shadow, fmt.Sprintf("boundary %d after compaction", b.through))
		reopened, openErr := db.OpenCheckpointView(b.through)
		require.NoError(t, openErr)
		requireCheckpointMatches(t, reopened, keys, b.shadow, fmt.Sprintf("boundary %d reopened after compaction", b.through))
	}
}

// A staged load stops its second stage when the caller gives up.
func TestCheckpointViewStagedLoadHonorsCancellation(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	keys := make([]solana.PublicKey, 2000)
	batch := make([]*accounts.Account, len(keys))
	for i := range keys {
		keys[i] = solana.PublicKey{byte(i), byte(i >> 8), 0xF7}
		batch[i] = &accounts.Account{Key: keys[i], Lamports: 1}
	}
	commitTestBatch(t, db, 100, batch...)
	view, err := db.OpenCheckpointView(100)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	first, _, pending, err := view.LoadAccountsStaged(ctx, keys[:10], keys[10:], 2)
	require.NoError(t, err)
	require.Len(t, first, 10)
	cancel()
	if _, _, err := pending.Wait(); err != nil {
		require.ErrorIs(t, err, context.Canceled)
	}
	_, _, _, err = view.LoadAccountsStaged(ctx, keys[:10], keys[10:], 2)
	require.ErrorIs(t, err, context.Canceled, "a cancelled first stage starts no second")
}
