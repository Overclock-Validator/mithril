package node

import (
	"fmt"
	"os"

	"github.com/Overclock-Validator/mithril/pkg/accountsdb"
	"github.com/Overclock-Validator/mithril/pkg/adminrpc"
	"github.com/Overclock-Validator/mithril/pkg/alpenglow"
	"github.com/Overclock-Validator/mithril/pkg/global"
	"github.com/Overclock-Validator/mithril/pkg/identity"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// startAdminRPC serves set-identity on <ledger>/admin.rpc, where ledger is
// the shredstore directory that also holds the vote history. stakesForSlot
// is nil when no epoch stakes are available, which makes identity changes
// fail. voteAccount is zero for a node that does not vote.
func startAdminRPC(ledger string, holder *identity.Holder, accountsDb *accountsdb.AccountsDb, voteAccount solana.PublicKey,
	stakesForSlot func(slot uint64) map[solana.PublicKey]uint64,
) (*adminrpc.Server, error) {
	service := &adminrpc.IdentityService{
		Holder: holder,
		StakedNodes: func() (map[solana.PublicKey]uint64, bool) {
			if stakesForSlot == nil {
				return nil, false
			}
			stakes := stakesForSlot(global.ReplayFrontier())
			return stakes, len(stakes) > 0
		},
		LoadVoteHistory: func(node solana.PublicKey) error {
			_, err := alpenglow.LoadVoteHistory(ledger, node)
			return err
		},
	}
	if !voteAccount.IsZero() {
		service.VoteHistoryRequired = func(node solana.PublicKey) bool {
			return voteAccountHasVotedAs(accountsDb, voteAccount, node)
		}
	}
	if err := os.MkdirAll(ledger, 0o755); err != nil {
		return nil, fmt.Errorf("create ledger directory %s: %w", ledger, err)
	}
	return adminrpc.Listen(adminrpc.SocketPath(ledger), service)
}

// voteAccountHasVotedAs mirrors Agave's should_require_vote_history_file on
// the rooted vote account: node is its validator and it has a recorded vote.
// Mithril does not track the Alpenglow genesis slot, so any recorded vote
// counts, which requires the history in at least every case Agave does. An
// unreadable vote account also requires it.
func voteAccountHasVotedAs(accountsDb *accountsdb.AccountsDb, voteAccount, node solana.PublicKey) bool {
	account, err := accountsDb.GetAccount(0, voteAccount)
	if err != nil {
		return true
	}
	if account == nil {
		return false
	}
	state, err := sealevel.UnmarshalVersionedVoteState(account.Data)
	if err != nil || state.NodePubkey() != node {
		return false
	}
	_, voted := state.ConvertToCurrent().LastVotedSlot()
	return voted
}
