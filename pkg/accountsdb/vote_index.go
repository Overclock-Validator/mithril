package accountsdb

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/gagliardetto/solana-go"
)

// Candidate keys are separate from the 32-byte account-index keys. They are
// append-only; callers check the current account state before returning it.
const voteIndexPrefix = "\x00mithril.vote."

var voteIndexReadyKey = []byte("\x00mithril.meta.vote_index_ready")

func voteIndexKey(pubkey solana.PublicKey) []byte {
	key := make([]byte, len(voteIndexPrefix)+len(pubkey))
	copy(key, voteIndexPrefix)
	copy(key[len(voteIndexPrefix):], pubkey[:])
	return key
}

// SeedVoteAccountPubkeys seeds a freshly built snapshot index with vote-program
// account candidates observed while parsing its appendvecs.
func (db *AccountsDb) SeedVoteAccountPubkeys(pubkeys []solana.PublicKey) error {
	batch := db.Index.NewBatch()
	defer batch.Close()
	for _, pubkey := range pubkeys {
		if err := batch.Set(voteIndexKey(pubkey), nil, nil); err != nil {
			return err
		}
	}
	if err := batch.Set(voteIndexReadyKey, nil, nil); err != nil {
		return err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	if db.IndexWALDisabled {
		return db.Index.Flush()
	}
	return nil
}

// VoteAccountPubkeys returns every indexed vote-account candidate. A snapshot
// build seeds the index and every fold adds to it. A store built before the
// index existed is not scanned to fill it, because that would read every
// account while replay runs: it lists only vote accounts written since, and
// its callers add the staked ones from epoch stakes.
func (db *AccountsDb) VoteAccountPubkeys(ctx context.Context) ([]solana.PublicKey, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	iter, err := db.Index.NewIter(&pebble.IterOptions{
		LowerBound: []byte(voteIndexPrefix),
		UpperBound: []byte("\x00mithril.vote/"),
	})
	if err != nil {
		return nil, err
	}
	var pubkeys []solana.PublicKey
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			iter.Close()
			return nil, err
		}
		key := iter.Key()
		if len(key) != len(voteIndexPrefix)+32 {
			iter.Close()
			return nil, fmt.Errorf("invalid vote-index key length %d", len(key))
		}
		pubkeys = append(pubkeys, solana.PublicKeyFromBytes(key[len(voteIndexPrefix):]))
	}
	iterErr := iter.Error()
	closeErr := iter.Close()
	if iterErr != nil {
		return nil, iterErr
	}
	return pubkeys, closeErr
}
