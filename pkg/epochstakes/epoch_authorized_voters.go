package epochstakes

import (
	"github.com/gagliardetto/solana-go"
)

type EpochAuthorizedVotersCache struct {
	authorizedVoters map[solana.PublicKey][]solana.PublicKey
}

func NewEpochAuthorizedVotersCache() *EpochAuthorizedVotersCache {
	return &EpochAuthorizedVotersCache{authorizedVoters: make(map[solana.PublicKey][]solana.PublicKey)}
}

func (cache *EpochAuthorizedVotersCache) PutEntry(voteAcct solana.PublicKey, authorizedVoter solana.PublicKey) {
	cache.authorizedVoters[voteAcct] = append(cache.authorizedVoters[voteAcct], authorizedVoter)
}

func (cache *EpochAuthorizedVotersCache) IsAuthorizedVoter(voteAcct solana.PublicKey, pubkey solana.PublicKey) bool {
	for _, a := range cache.authorizedVoters[voteAcct] {
		if a == pubkey {
			return true
		}
	}
	return false
}

// Entries returns the underlying map for serialization/persistence.
func (cache *EpochAuthorizedVotersCache) Entries() map[solana.PublicKey][]solana.PublicKey {
	return cache.authorizedVoters
}

// Len returns the number of vote accounts in the cache.
func (cache *EpochAuthorizedVotersCache) Len() int {
	return len(cache.authorizedVoters)
}
