package replay

import (
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	a "github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/safemath"
)

// Incinerator deposits are real writes and are burned by ordinary bank
// finalization, but they must not increase the next epoch's inflation budget.
// Count only the newly credited amount: the staged parent may already hold
// lamports from other same-bank changes.
func capitalizingVotingRewardDeposits(updated, parents []*accounts.Account, deposited uint64) uint64 {
	for index, account := range updated {
		if account == nil || account.Key != a.IncineratorAddr {
			continue
		}
		if index >= len(parents) || parents[index] == nil || parents[index].Key != account.Key {
			panic("missing immediate parent for incinerator voting reward")
		}
		reward, err := safemath.CheckedSubU64(account.Lamports, parents[index].Lamports)
		if err != nil {
			panic(fmt.Errorf("invalid incinerator voting reward delta: %w", err))
		}
		deposited, err = safemath.CheckedSubU64(deposited, reward)
		if err != nil {
			panic(fmt.Errorf("incinerator voting reward exceeds total deposits: %w", err))
		}
	}
	return deposited
}
