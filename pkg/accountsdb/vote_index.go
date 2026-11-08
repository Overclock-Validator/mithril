package accountsdb

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/addresses"
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
	db.voteIndexMu.Lock()
	defer db.voteIndexMu.Unlock()
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
	writeOpts := pebble.Sync
	if db.IndexWALDisabled {
		writeOpts = pebble.NoSync
	}
	if err := batch.Commit(writeOpts); err != nil {
		return err
	}
	if db.IndexWALDisabled {
		if err := db.Index.Flush(); err != nil {
			return err
		}
	}
	db.voteIndexChecked = true
	db.voteIndexGeneration = db.voteIndexInvalidations.Load()
	db.voteIndexResume = nil
	db.voteIndexPending = nil
	return nil
}

// VoteAccountPubkeys returns every known vote-account candidate. The index is
// checked on first use after opening a store or rewinding it.
func (db *AccountsDb) VoteAccountPubkeys(ctx context.Context) ([]solana.PublicKey, error) {
	locked := db.voteIndexMu.TryLock()
	if !locked {
		ticker := time.NewTicker(10 * time.Millisecond)
		for !locked {
			select {
			case <-ctx.Done():
				ticker.Stop()
				return nil, ctx.Err()
			case <-ticker.C:
				locked = db.voteIndexMu.TryLock()
			}
		}
		ticker.Stop()
	}
	defer db.voteIndexMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	for {
		generation := db.voteIndexInvalidations.Load()
		if generation != db.voteIndexGeneration {
			db.voteIndexChecked = false
			db.voteIndexResume = nil
			db.voteIndexPending = nil
			db.voteIndexGeneration = generation
		}
		// An older binary may have changed accounts without maintaining this index.
		// Verify once per open even if an on-disk ready marker survived a downgrade.
		if !db.voteIndexChecked {
			if err := db.migrateVoteIndex(ctx); err != nil {
				return nil, err
			}
			db.voteIndexChecked = true
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
		if closeErr != nil {
			return nil, closeErr
		}
		if db.voteIndexInvalidations.Load() == generation {
			return pubkeys, nil
		}
	}
}

// migrateVoteIndex scans stored accounts. Finalized folds can run during the
// scan: they add their new vote candidates to the same append-only index.
func (db *AccountsDb) migrateVoteIndex(ctx context.Context) error {
	iter, err := db.Index.NewIter(nil)
	if err != nil {
		return err
	}
	defer iter.Close()
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
				db.voteIndexPending = append(db.voteIndexPending, keys[i])
			}
		}
		last := keys[len(keys)-1]
		db.voteIndexResume = &last
		keys = keys[:0]
		return nil
	}
	if db.voteIndexResume == nil {
		iter.First()
	} else if iter.SeekGE(db.voteIndexResume[:]) && bytes.Equal(iter.Key(), db.voteIndexResume[:]) {
		iter.Next()
	}
	for ; iter.Valid(); iter.Next() {
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
	batch := db.Index.NewBatch()
	defer batch.Close()
	for _, key := range db.voteIndexPending {
		if err := batch.Set(voteIndexKey(key), nil, nil); err != nil {
			return err
		}
	}
	if err := batch.Set(voteIndexReadyKey, nil, nil); err != nil {
		return err
	}
	writeOpts := pebble.Sync
	if db.IndexWALDisabled {
		writeOpts = pebble.NoSync
	}
	if err := batch.Commit(writeOpts); err != nil {
		return err
	}
	if db.IndexWALDisabled {
		if err := db.Index.Flush(); err != nil {
			return err
		}
	}
	db.voteIndexResume = nil
	db.voteIndexPending = nil
	return nil
}
