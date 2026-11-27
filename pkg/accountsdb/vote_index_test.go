package accountsdb

import (
	"context"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/cockroachdb/pebble/v2"
	"github.com/gagliardetto/solana-go"
	"github.com/stretchr/testify/require"
)

// A store written before the vote index existed is never scanned to fill it:
// the index lists only what a snapshot build seeded or a fold has added.
func TestVoteIndexDoesNotScanAnOlderStore(t *testing.T) {
	db, _ := newFoldTestDb(t)
	defer db.CloseDb()
	old := &accounts.Account{Key: solana.PublicKey{1}, Owner: addresses.VoteProgramAddr, Lamports: 1}
	_, err := db.CommitBatch(foldDeltas(accounts.SlotDelta{Slot: 10, Delta: []*accounts.Account{old}}), 10, nil, nil)
	require.NoError(t, err)
	require.NoError(t, db.Index.Delete(voteIndexKey(old.Key), pebble.Sync))
	got, err := db.VoteAccountPubkeys(context.Background())
	require.NoError(t, err)
	require.Empty(t, got)

	written := &accounts.Account{Key: solana.PublicKey{2}, Owner: addresses.VoteProgramAddr, Lamports: 1}
	_, err = db.CommitBatch(foldDeltas(accounts.SlotDelta{Slot: 11, Delta: []*accounts.Account{written}}), 11, nil, nil)
	require.NoError(t, err)
	got, err = db.VoteAccountPubkeys(context.Background())
	require.NoError(t, err)
	require.Equal(t, []solana.PublicKey{written.Key}, got)
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
