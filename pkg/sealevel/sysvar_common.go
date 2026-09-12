package sealevel

import (
	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/gagliardetto/solana-go"
)

func addrObjectForLookup(execCtx *ExecutionCtx) *accounts.Accounts {
	if execCtx == nil {
		return nil
	}
	// A transaction always observes the accounts of the bank that executes it.
	// Replay describes the execution mode; it does not determine account
	// ownership. In particular, speculative leader banks have Replay=false but
	// still own a complete bank-local sysvar view.
	if execCtx.SlotCtx != nil && execCtx.SlotCtx.Accounts != nil {
		return &execCtx.SlotCtx.Accounts
	}
	return &execCtx.Accounts
}

// localSysvarAccount returns a sysvar account from the execution bank when it
// is explicitly available. Bank snapshots are handled by the typed readers
// before this helper; this is the compatibility path for older/isolated SlotCtx
// fixtures. A present local account is authoritative over the process-global
// bootstrap cache.
func localSysvarAccount(execCtx *ExecutionCtx, pubkey solana.PublicKey) (*accounts.Account, bool) {
	accts := addrObjectForLookup(execCtx)
	if accts == nil || *accts == nil {
		return nil, false
	}
	key := [32]byte(pubkey)
	acct, err := (*accts).GetAccount(&key)
	if err != nil || acct == nil {
		return nil, false
	}
	return acct, true
}
