package adminrpc

import (
	"sync"

	"github.com/Overclock-Validator/mithril/pkg/identity"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
	"github.com/gagliardetto/solana-go"
)

// IdentityService implements set-identity the way Agave's
// set_identity_keypair does: validate the request, then swap the holder,
// whose updaters bring every subsystem onto the new key or abort.
type IdentityService struct {
	Holder *identity.Holder
	// StakedNodes returns node stakes for the epoch of the current root; ok
	// is false when they are not loaded.
	StakedNodes func() (stakes map[solana.PublicKey]uint64, ok bool)
	// VoteHistoryRequired reports whether node needs a saved vote history:
	// it is the node of a vote account that has already voted. Nil means
	// this node does not vote.
	VoteHistoryRequired func(node solana.PublicKey) bool
	// LoadVoteHistory checks that node has a vote history that loads and
	// verifies.
	LoadVoteHistory func(node solana.PublicKey) error

	mu sync.Mutex
}

func (s *IdentityService) SetIdentity(key []byte, requireTower, requireVoteHistory bool) error {
	next, err := identity.NewKeypair(key)
	if err != nil {
		return invalidParams("Failed to read identity keypair: %v", err)
	}
	newIdentity := next.PublicKey()

	s.mu.Lock()
	defer s.mu.Unlock()
	oldIdentity := s.Holder.PublicKey()
	if oldIdentity != newIdentity {
		stakes, ok := s.StakedNodes()
		if !ok {
			return &Error{Code: CodeInternalError, Message: "Missing epoch stakes for the root epoch"}
		}
		if stakes[oldIdentity] > 0 && stakes[newIdentity] > 0 {
			return invalidParams("Changing identity from staked node %s to staked node %s is not supported", oldIdentity, newIdentity)
		}
	}
	if requireTower {
		return invalidParams("Unable to load tower file for identity %s: Mithril has no tower file for this identity", newIdentity)
	}
	if requireVoteHistory && s.VoteHistoryRequired != nil && s.VoteHistoryRequired(newIdentity) {
		if err := s.LoadVoteHistory(newIdentity); err != nil {
			return invalidParams("Unable to load vote history file for identity %s: %v. Its vote account has prior Alpenglow votes. Ensure the vote history file is present or (dangerous) set require_vote_history to false if you know what you're doing", newIdentity, err)
		}
	}

	if _, err := s.Holder.Swap(key); err != nil {
		return &Error{Code: CodeInternalError, Message: err.Error()}
	}
	mlog.Log.Warnf("Identity set to %s (was %s)", newIdentity, oldIdentity)
	return nil
}
