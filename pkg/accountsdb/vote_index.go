package accountsdb

import (
	"context"
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/cockroachdb/pebble"
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

// VoteAccountPubkeys returns every known vote-account candidate. Existing
// stores built before this index was added are migrated once, on first use.
func (db *AccountsDb) VoteAccountPubkeys(ctx context.Context) ([]solana.PublicKey, error) {
	db.voteIndexMu.Lock()
	defer db.voteIndexMu.Unlock()
	if _, closer, err := db.Index.Get(voteIndexReadyKey); err == nil {
		if err := closer.Close(); err != nil {
			return nil, err
		}
	} else if err == pebble.ErrNotFound {
		if err := db.migrateVoteIndex(ctx); err != nil {
			return nil, err
		}
	} else {
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

// migrateVoteIndex scans an older store once. Finalized folds can run during
// the scan: they add their new vote candidates to the same append-only index.
func (db *AccountsDb) migrateVoteIndex(ctx context.Context) error {
	iter, err := db.Index.NewIter(nil)
	if err != nil {
		return err
	}
	defer iter.Close()
	batch := db.Index.NewBatch()
	defer batch.Close()
	keys := make([]solana.PublicKey, 0, 512)
	flush := func() error {
		if len(keys) == 0 {
			return nil
		}
		accounts, err := db.GetAccountsBatch(ctx, db.DurableThrough(), keys)
		if err != nil {
			return err
		}
		for i, account := range accounts {
			if account != nil && account.Lamports > 0 && account.Owner == addresses.VoteProgramAddr {
				if err := batch.Set(voteIndexKey(keys[i]), nil, nil); err != nil {
					return err
				}
			}
		}
		keys = keys[:0]
		return nil
	}
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if key := iter.Key(); len(key) == 32 {
			keys = append(keys, solana.PublicKeyFromBytes(key))
			if len(keys) == cap(keys) {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
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
