package accountsdb

import (
	"context"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/cockroachdb/pebble"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

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
