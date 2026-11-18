package replay

import (
	"fmt"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/features"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/gagliardetto/solana-go"
)

// PrepareLeaderTransactionParents retains the original parent images of
// accounts a producer transaction touched, before publishing its writes. The
// producer calls this under its bank lock after admission. Repeated writes in
// a slot keep the first parent image; scratch bundle values are never parents.
// Fees-only failures without an execution context, and footer-only writes,
// are covered by ensureParentAccountsForModified at finalization.
//
// This is preparation for LtHash only: retaining an image does not publish an
// account, mark it modified, or affect a rejected transaction's state.
func PrepareLeaderTransactionParents(slotCtx *sealevel.SlotCtx, outputs ...LoadAndExecuteTransactionOutput) error {
	if slotCtx == nil || slotCtx.ParentAccts == nil || slotCtx.Features == nil ||
		!slotCtx.Features.IsActive(features.AccountsLtHash) ||
		(slotCtx.UnrootedRead == nil && slotCtx.AccountsDb == nil) {
		return nil
	}
	for _, output := range outputs {
		if output.ExecCtx == nil || output.ExecCtx.TransactionContext == nil {
			continue
		}
		accts := &output.ExecCtx.TransactionContext.Accounts
		if len(accts.Accounts) != len(accts.Touched) {
			return fmt.Errorf("leader parent preparation: account states/touched length mismatch")
		}
		for i, touched := range accts.Touched {
			if !touched || accts.Accounts[i] == nil {
				continue
			}
			if err := retainLeaderParentImage(slotCtx, accts.Accounts[i].Key); err != nil {
				return err
			}
		}
	}
	return nil
}

func retainLeaderParentImage(slotCtx *sealevel.SlotCtx, key solana.PublicKey) error {
	// Use the store's locking API: the same parent store is read by speculative
	// Harmonic workers while the producer publishes a previously unseen key.
	pubkey := [32]byte(key)
	if _, err := slotCtx.ParentAccts.GetAccount(&pubkey); err == nil {
		return nil
	}
	acct, err := slotCtx.ParentAccountReadOnly(key)
	if err != nil || acct == nil {
		// Preserve the finalizer's zero-before-value for absent accounts.
		acct = &accounts.Account{Key: key}
	}
	// ParentAccountReadOnly borrows only from explicitly immutable sources;
	// its fallback gives us an owned read. A private header keeps bookkeeping
	// separate while immutable payload storage is retained without copying.
	parent := *acct
	return slotCtx.ParentAccts.SetAccount(&pubkey, &parent)
}
