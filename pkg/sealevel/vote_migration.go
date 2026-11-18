package sealevel

import (
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/gagliardetto/solana-go"
)

var voteAlpenglowClockAddr, _, _ = solana.FindProgramAddress(
	[][]byte{[]byte("alpenclock")}, solana.PublicKey(features.Alpenglow.Address),
)

// Alpenglow feature activation can precede migration. A populated alpenclock
// PDA marks the post-genesis state; activation or a prefunded empty PDA alone
// must not disable historical TowerBFT votes. Replay and local production pin
// this account alongside the parent sysvars before executing transactions.
func voteAlpenglowMigrationSucceeded(execCtx *ExecutionCtx) bool {
	if !execCtx.Features.IsActive(features.Alpenglow) && !execCtx.Features.IsActive(features.AlpenglowDevContext) {
		return false
	}
	if slotCtx := execCtx.SlotCtx; slotCtx != nil {
		if slotCtx.ParentAccts != nil {
			acct, err := slotCtx.GetParentAccount(voteAlpenglowClockAddr)
			return err == nil && populatedVoteAlpenglowClock(acct)
		}
		if slotCtx.Accounts != nil {
			acct, err := slotCtx.GetAccountShared(voteAlpenglowClockAddr)
			return err == nil && populatedVoteAlpenglowClock(acct)
		}
	}
	// Compatibility for isolated instruction execution without a SlotCtx.
	acct, ok := localSysvarAccount(execCtx, voteAlpenglowClockAddr)
	return ok && populatedVoteAlpenglowClock(acct)
}

func populatedVoteAlpenglowClock(acct *accounts.Account) bool {
	return acct != nil && acct.Key == voteAlpenglowClockAddr && acct.Lamports > 0 &&
		acct.Owner == a.SystemProgramAddr && !acct.Executable && len(acct.Data) == 8
}
